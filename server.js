const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;
const ROOM_TTL_MS = 5 * 60 * 1000;

const rooms = new Map();

function makeRoom(code, hostSocket) {
  return { code, host: hostSocket, guest: null, createdAt: Date.now() };
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

wss.on("connection", (socket) => {
  socket.roomCode = null;
  socket.role = null;

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

    const other = socket.role === "host" ? room.guest : room.host;
    safeSend(other, { type: "peer_left" });
    closeRoom(socket.roomCode);
  });
});

console.log("Signalling server listening on port " + PORT);
