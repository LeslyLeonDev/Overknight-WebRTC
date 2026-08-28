const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;
const ROOM_TTL_MS = 5 * 60 * 1000;

// The lobby supports many players (up to 16), so a room must be able to
// mediate several simultaneous guest handshakes and must stay alive for as
// long as the host is around — not just until the first guest finishes.
//
// Each room tracks:
//   host          - the host's signalling socket
//   guestsById    - Map<peerId, socket>       (peer ID known, from an offer)
//   pendingGuests - Set<socket>                (joined, no offer sent yet)
const rooms = new Map();

function makeRoom(code, hostSocket) {
  return {
    code,
    host: hostSocket,
    guestsById: new Map(),
    pendingGuests: new Set(),
    createdAt: Date.now(),
  };
}

function safeSend(socket, obj) {
  if (socket && socket.readyState === socket.OPEN) {
    socket.send(JSON.stringify(obj));
  }
}

function closeRoom(code) {
  rooms.delete(code);
}

function roomIsEmpty(room) {
  return room.guestsById.size === 0 && room.pendingGuests.size === 0;
}

function cleanupStaleRooms() {
  const now = Date.now();
  for (const [code, room] of rooms.entries()) {
    if (now - room.createdAt > ROOM_TTL_MS && roomIsEmpty(room)) {
      closeRoom(code);
    }
  }
}
setInterval(cleanupStaleRooms, 60 * 1000);

// Once we learn a guest socket's Godot peer ID (from the first offer the
// host sends it, or the first answer/candidate it sends back), move it
// from pendingGuests into guestsById so future messages route directly.
function resolveGuestId(room, socket, peerId) {
  if (peerId === undefined || peerId === null) return;
  if (!room.guestsById.has(peerId)) {
    room.guestsById.set(peerId, socket);
  }
  room.pendingGuests.delete(socket);
  socket.peerId = peerId;
}

const wss = new WebSocketServer({ port: PORT });

wss.on("connection", (socket) => {
  socket.roomCode = null;
  socket.role = null;
  socket.peerId = null;

  socket.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch (e) {
      return;
    }

    if (msg.type === "host") {
      const code = String(msg.code || "").toUpperCase();
      if (!code) {
        safeSend(socket, { type: "error", message: "Missing room code." });
        return;
      }
      if (rooms.has(code)) {
        safeSend(socket, { type: "error", message: "Room code already in use." });
        return;
      }
      rooms.set(code, makeRoom(code, socket));
      socket.roomCode = code;
      socket.role = "host";
      safeSend(socket, { type: "hosting", code: code });
      return;
    }

    if (msg.type === "join") {
      const code = String(msg.code || "").toUpperCase();
      const room = rooms.get(code);
      if (!room) {
        safeSend(socket, { type: "error", message: "Room not found." });
        return;
      }
      if (!room.host || room.host.readyState !== room.host.OPEN) {
        safeSend(socket, { type: "error", message: "The host is no longer connected." });
        closeRoom(code);
        return;
      }
      room.pendingGuests.add(socket);
      socket.roomCode = code;
      socket.role = "guest";
      safeSend(socket, { type: "joined", code: code });
      safeSend(room.host, { type: "peer_joined", code: code });
      return;
    }

    if (msg.type === "host_closing") {
      // The host has decided to stop accepting new online joiners (e.g.
      // the match started). Tell any still-pending guests so they don't
      // hang waiting on a room that's about to disappear, then free it.
      if (socket.role !== "host" || !socket.roomCode) return;
      const room = rooms.get(socket.roomCode);
      if (!room) return;
      for (const g of room.pendingGuests) safeSend(g, { type: "peer_left" });
      for (const g of room.guestsById.values()) safeSend(g, { type: "peer_left" });
      closeRoom(socket.roomCode);
      return;
    }

    if (msg.type === "done") {
      // A guest sends this once its own handshake succeeded and it no
      // longer needs signalling. This never affects the room or the host
      // — more guests can keep joining the same room afterward.
      if (!socket.roomCode) return;
      const room = rooms.get(socket.roomCode);
      if (!room) return;
      if (socket.role === "guest" && socket.peerId !== null) {
        room.guestsById.delete(socket.peerId);
      }
      room.pendingGuests.delete(socket);
      return;
    }

    // offer / answer / candidate: opaque signalling payloads relayed
    // between the host and the specific guest the message is about.
    if (!socket.roomCode) return;
    const room = rooms.get(socket.roomCode);
    if (!room) return;

    if (socket.role === "host") {
      const toPeerId = msg.to_peer_id;
      let target = room.guestsById.get(toPeerId);
      if (!target && room.pendingGuests.size > 0) {
        // First message to a brand-new guest: it doesn't have a known
        // peer ID yet, so target the oldest still-pending guest and
        // remember the mapping from here on.
        target = room.pendingGuests.values().next().value;
        resolveGuestId(room, target, toPeerId);
      }
      safeSend(target, msg);
    } else if (socket.role === "guest") {
      const fromPeerId = msg.from_peer_id;
      resolveGuestId(room, socket, fromPeerId);
      safeSend(room.host, msg);
    }
  });

  socket.on("close", () => {
    if (!socket.roomCode) return;
    const room = rooms.get(socket.roomCode);
    if (!room) return;

    if (socket.role === "host") {
      // The host leaving ends the whole online session for everyone
      // still connected through signalling.
      for (const g of room.pendingGuests) safeSend(g, { type: "peer_left" });
      for (const g of room.guestsById.values()) safeSend(g, { type: "peer_left" });
      closeRoom(socket.roomCode);
    } else if (socket.role === "guest") {
      // One guest dropping never affects the room or any other guest.
      // Only tell the host if this guest hadn't already finished (a
      // clean "done" already removed it from these collections).
      const wasTracked = room.pendingGuests.has(socket) ||
        (socket.peerId !== null && room.guestsById.get(socket.peerId) === socket);
      room.pendingGuests.delete(socket);
      if (socket.peerId !== null && room.guestsById.get(socket.peerId) === socket) {
        room.guestsById.delete(socket.peerId);
      }
      if (wasTracked) {
        safeSend(room.host, { type: "peer_left", peer_id: socket.peerId });
      }
    }
  });
});

console.log("Signalling server listening on port " + PORT);
