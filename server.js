const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;
const ROOM_TTL_MS = 5 * 60 * 1000;

// --- Design notes -----------------------------------------------------
//
// This follows the same protocol shape as Godot's own official WebRTC
// signalling demo (godot-demo-projects/networking/webrtc_signaling):
//
//   * The SERVER assigns each connected socket a real numeric peer ID the
//     moment it connects — the client never invents or guesses IDs.
//   * Every relayed message (offer/answer/candidate) has its `id` field
//     REWRITTEN by the server to the true sender's peer ID before being
//     forwarded, so the receiving side always knows exactly who it came
//     from with zero ambiguity — this is what earlier, hand-rolled
//     versions of this server got wrong (it tried to guess which pending
//     guest a message belonged to, which broke under any real-world
//     timing variance).
//   * A room ("lobby" in Godot's terms) is host + any number of guests.
//     It lives as long as the host's socket is open. A guest joining,
//     finishing its handshake, or leaving never affects the room itself
//     or any other guest — only the host leaving (or explicitly closing
//     the room) ends it for everyone.
//
// Extensive logging is included throughout specifically so connection
// issues are diagnosable from the server logs alone.
// ------------------------------------------------------------------------

let _nextPeerId = 2; // 1 is reserved for the host, matching Godot's own convention
const rooms = new Map(); // code -> Room

function log(...args) {
  console.log(new Date().toISOString(), ...args);
}

function makeRoom(code, hostSocket) {
  return {
    code,
    host: hostSocket,
    guests: new Map(), // peerId -> socket
    createdAt: Date.now(),
  };
}

function safeSend(socket, obj) {
  if (socket && socket.readyState === socket.OPEN) {
    socket.send(JSON.stringify(obj));
    return true;
  }
  return false;
}

function closeRoom(code, reason) {
  const room = rooms.get(code);
  if (!room) return;
  log(`[room ${code}] closing (${reason})`);
  rooms.delete(code);
}

function cleanupStaleRooms() {
  const now = Date.now();
  for (const [code, room] of rooms.entries()) {
    if (now - room.createdAt > ROOM_TTL_MS && room.guests.size === 0) {
      closeRoom(code, "stale, no guest ever joined");
    }
  }
}
setInterval(cleanupStaleRooms, 60 * 1000);

const wss = new WebSocketServer({ port: PORT });

wss.on("connection", (socket, req) => {
  socket.peerId = _nextPeerId++;
  socket.roomCode = null;
  socket.role = null; // "host" | "guest"

  log(`[peer ${socket.peerId}] connected from ${req.socket.remoteAddress}`);
  safeSend(socket, { type: "id", id: socket.peerId });

  socket.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch (e) {
      log(`[peer ${socket.peerId}] sent invalid JSON, ignoring`);
      return;
    }

    const msgType = msg.type;
    log(`[peer ${socket.peerId}] -> ${msgType}` + (socket.roomCode ? ` (room ${socket.roomCode})` : ""));

    if (msgType === "host") {
      const code = String(msg.code || "").toUpperCase();
      if (!code) {
        safeSend(socket, { type: "error", message: "Missing room code." });
        return;
      }
      if (socket.roomCode) {
        safeSend(socket, { type: "error", message: "This connection is already hosting or joined a room." });
        return;
      }
      if (rooms.has(code)) {
        safeSend(socket, { type: "error", message: "Room code already in use." });
        return;
      }
      rooms.set(code, makeRoom(code, socket));
      socket.roomCode = code;
      socket.role = "host";
      log(`[room ${code}] created by peer ${socket.peerId}`);
      safeSend(socket, { type: "hosting", code: code, id: socket.peerId });
      return;
    }

    if (msgType === "join") {
      const code = String(msg.code || "").toUpperCase();
      const room = rooms.get(code);
      if (!room) {
        log(`[peer ${socket.peerId}] tried to join "${code}" — no such room`);
        safeSend(socket, { type: "error", message: "Room not found." });
        return;
      }
      if (room.host.readyState !== room.host.OPEN) {
        log(`[room ${code}] host socket is dead, closing stale room`);
        closeRoom(code, "host socket dead on join attempt");
        safeSend(socket, { type: "error", message: "Room not found." });
        return;
      }
      room.guests.set(socket.peerId, socket);
      socket.roomCode = code;
      socket.role = "guest";
      log(`[room ${code}] peer ${socket.peerId} joined as guest (${room.guests.size} guest(s) now)`);
      safeSend(socket, { type: "joined", code: code, id: socket.peerId, host_id: room.host.peerId });
      safeSend(room.host, { type: "peer_connect", id: socket.peerId });
      return;
    }

    // offer / answer / candidate: opaque relay payloads. The `id` field is
    // ALWAYS overwritten with the true sender's peer ID before relaying —
    // the sender's own `id` field (the intended destination) is only used
    // to pick where to send it, never trusted as "who this is from".
    if (msgType === "offer" || msgType === "answer" || msgType === "candidate") {
      if (!socket.roomCode) {
        log(`[peer ${socket.peerId}] sent ${msgType} but isn't in a room, ignoring`);
        return;
      }
      const room = rooms.get(socket.roomCode);
      if (!room) {
        log(`[peer ${socket.peerId}] sent ${msgType} but its room is gone, ignoring`);
        return;
      }

      const destId = msg.id;
      let target = null;
      if (socket.role === "host") {
        target = room.guests.get(destId) || null;
      } else if (socket.role === "guest") {
        target = room.host;
      }

      if (!target) {
        log(`[room ${socket.roomCode}] peer ${socket.peerId} tried to send ${msgType} to unknown peer ${destId}`);
        return;
      }

      msg.id = socket.peerId; // rewrite to the TRUE sender, per Godot's own protocol
      safeSend(target, msg);
      return;
    }

    log(`[peer ${socket.peerId}] sent unknown message type "${msgType}", ignoring`);
  });

  socket.on("close", () => {
    log(`[peer ${socket.peerId}] disconnected` + (socket.roomCode ? ` (was in room ${socket.roomCode} as ${socket.role})` : ""));

    if (!socket.roomCode) return;
    const room = rooms.get(socket.roomCode);
    if (!room) return;

    if (socket.role === "host") {
      for (const guestSocket of room.guests.values()) {
        safeSend(guestSocket, { type: "peer_disconnect", id: socket.peerId });
      }
      closeRoom(socket.roomCode, `host (peer ${socket.peerId}) disconnected`);
    } else if (socket.role === "guest") {
      if (room.guests.delete(socket.peerId)) {
        safeSend(room.host, { type: "peer_disconnect", id: socket.peerId });
        log(`[room ${socket.roomCode}] guest peer ${socket.peerId} removed (${room.guests.size} guest(s) remain)`);
      }
    }
  });

  socket.on("error", (err) => {
    log(`[peer ${socket.peerId}] socket error: ${err.message}`);
  });
});

log(`Signalling server listening on port ${PORT}`);
