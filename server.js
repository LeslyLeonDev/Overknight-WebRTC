const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;
const ROOM_TTL_MS = 5 * 60 * 1000;

const rooms = new Map();

function makeRoom(code, hostSocket) {
  return { code, host: hostSocket, guest: null, createdAt: Date.now(), doneCount: 0 };
}

function safeSend(socket, obj) {
  if (socket && socket.readyState === socket.OPEN) {
    socket.send(JSON.stringify(obj));
  }
}

function closeRoom(code) {
  const room = rooms.get(code);
  if (!room) return;
  rooms.delete(code);
}

function cleanupStaleRooms() {
  const now = Date.now();
  for (const [code, room] of rooms.entries()) {
    if (now - room.createdAt > ROOM_TTL_MS && !room.guest) {
      closeRoom(code);
    }
  }
}
setInterval(cleanupStaleRooms, 60 * 1000);

const wss = new WebSocketServer({ port: PORT });

// Send a ping every 30s to keep reverse proxies from timing out idle host connections
const heartbeat = setInterval(() => {
  wss.clients.forEach((socket) => {
    if (socket.isAlive === false) return socket.terminate();
    socket.isAlive = false;
    socket.ping();
  });
}, 30000);

wss.on("close", () => clearInterval(heartbeat));

wss.on("connection", (socket) => {
  socket.isAlive = true;
  socket.on("pong", () => {
    socket.isAlive = true;
  });

  socket.roomCode = null;
  socket.role = null;
  // Set once this socket has told us it's closing on purpose (WebRTC
  // handshake succeeded, P2P channel is up, signalling is no longer
  // needed) rather than dropping unexpectedly.
  socket.closingCleanly = false;

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
      if (room.guest) {
        safeSend(socket, { type: "error", message: "Room already has a guest." });
        return;
      }
      room.guest = socket;
      socket.roomCode = code;
      socket.role = "guest";
      safeSend(socket, { type: "joined", code: code });
      safeSend(room.host, { type: "peer_joined", code: code });
      return;
    }

    if (msg.type === "done") {
      // Sent by a client right before it intentionally disconnects its
      // signalling socket because WebRTC negotiation succeeded and the
      // peer-to-peer channel is now up — the socket is about to close,
      // but that close is expected and must NOT be treated as the other
      // player leaving, and must NOT delete the room out from under a
      // side that hasn't sent "done" yet.
      socket.closingCleanly = true;

      if (!socket.roomCode) return;
      const room = rooms.get(socket.roomCode);
      if (!room) return;

      room.doneCount += 1;

      // Once BOTH sides have confirmed they're done with signalling, the
      // room has served its purpose and can be freed immediately instead
      // of waiting on the 5-minute stale sweep.
      if (room.doneCount >= 2) {
        closeRoom(socket.roomCode);
      }
      return;
    }

    // Every other message type (offer/answer/candidate/etc.) is treated as
    // an opaque signalling payload and simply relayed verbatim to the OTHER
    // side of the room — this server never parses WebRTC internals, so it
    // keeps working across any future Godot/plugin version differences.
    if (!socket.roomCode) return;
    const room = rooms.get(socket.roomCode);
    if (!room) return;

    const other = socket.role === "host" ? room.guest : room.host;
    safeSend(other, msg);
  });

  socket.on("close", () => {
    if (!socket.roomCode) return;
    const room = rooms.get(socket.roomCode);
    if (!room) return;

    if (socket.closingCleanly) {
      // Expected close after a successful handshake. Don't tell the other
      // side "peer_left" (they didn't leave — they're happily connected
      // over WebRTC now), and don't delete the room if the other side
      // hasn't sent its own "done" yet — just remove this socket's slot.
      if (socket.role === "host" && room.host === socket) {
        room.host = null;
      } else if (socket.role === "guest" && room.guest === socket) {
        room.guest = null;
      }
      if (!room.host && !room.guest) {
        closeRoom(socket.roomCode);
      }
      return;
    }

    const other = socket.role === "host" ? room.guest : room.host;
    safeSend(other, { type: "peer_left" });
    closeRoom(socket.roomCode);
  });
});

console.log("Signalling server listening on port " + PORT);
