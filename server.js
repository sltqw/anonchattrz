'use strict';
const path = require('path');
const http = require('http');
const express = require('express');
const { Server } = require('socket.io');

const PORT = Number(process.env.PORT || 3000);

const app = express();
app.disable('x-powered-by');
app.use(express.static(path.join(__dirname, 'public'), { extensions: ['html'] }));
app.get('/health', (_req, res) => res.json({ ok: true }));

const server = http.createServer(app);
const io = new Server(server, {
  maxHttpBufferSize: 1e6,
  pingInterval: 25000,
  pingTimeout: 20000
});

const online = new Map(); // socketId -> { userId, roomId }
const lastSent = new Map();

// Жадта сақталатын деректер (сервер қайта қосылғанда жаңарады)
const profilesById = new Map(); // id -> { id, roomId, nick, avatarType, avatarValue, updatedAt }
const messagesByRoom = new Map(); // roomId -> [{ id, userId, nick, avatarType, avatarValue, text, createdAt }]
const privateMessages = []; // [{ id, fromId, toId, fromNick, text, createdAt }]
let nextMsgId = 1;
let nextDmId = 1;

const clean = (v, max = 100) =>
  String(v ?? '').replace(/[\u0000-\u001f\u007f]/g, '').trim().slice(0, max);

function safeProfile(p) {
  const id = clean(p?.id, 40);
  const nick = clean(p?.nick, 20) || 'Гость';
  const avatarType = ['gif', 'image'].includes(p?.avatarType) ? p.avatarType : 'gif';
  let avatarValue = clean(p?.avatarValue, 700000);
  if (!/^(data:image\/(png|jpeg|jpg|webp|gif);base64,|[a-zA-Z0-9_.-]+\.gif$)/i.test(avatarValue)) {
    avatarValue = '';
  }
  if (!/^ID-[0-9]{6}$/.test(id)) throw new Error('Некорректный ID профиля');
  return { id, nick, avatarType, avatarValue };
}

function roomOnline(roomId) {
  return new Set(
    [...online.values()].filter((x) => x.roomId === roomId).map((x) => x.userId)
  ).size;
}

function formatTime(ms) {
  return new Date(ms).toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' });
}

function emitPresence(roomId) {
  io.to(roomId).emit('online_count', roomOnline(roomId));
  const ids = new Set(
    [...online.values()].filter((x) => x.roomId === roomId).map((x) => x.userId)
  );
  const users = [...profilesById.values()]
    .filter((u) => u.roomId === roomId)
    .sort((a, b) => b.updatedAt - a.updatedAt)
    .slice(0, 200)
    .map((u) => ({
      id: u.id,
      nick: u.nick,
      avatarType: u.avatarType,
      avatarValue: u.avatarValue,
      isOnline: ids.has(u.id)
    }));
  io.to(roomId).emit('users_list', users);
}

function dmRoom(a, b) {
  return 'dm:' + [a, b].sort().join(':');
}

io.on('connection', (socket) => {
  socket.on('join_room', (payload) => {
    try {
      const roomId = clean(payload?.roomId || 'anon.42spina', 60);
      if (!/^[\w.-]+$/.test(roomId)) throw new Error('Некорректная комната');
      const p = safeProfile(payload?.profile);
      const now = Date.now();

      profilesById.set(p.id, {
        ...p,
        roomId,
        updatedAt: now
      });

      if (socket.data.roomId) socket.leave(socket.data.roomId);
      socket.join(roomId);
      socket.data.userId = p.id;
      socket.data.roomId = roomId;
      online.set(socket.id, { userId: p.id, roomId });

      const roomMsgs = messagesByRoom.get(roomId) || [];
      const history = roomMsgs.slice(-100).map((m) => ({
        id: m.userId,
        nick: m.nick,
        avatarType: m.avatarType,
        avatarValue: m.avatarValue,
        text: m.text,
        time: formatTime(m.createdAt)
      }));

      socket.emit('chat_history', history);
      emitPresence(roomId);
    } catch (e) {
      socket.emit('server_error', e.message || 'Не удалось войти в чат');
    }
  });

  socket.on('get_users', ({ roomId } = {}) => {
    const room = clean(roomId || socket.data.roomId || '', 60);
    if (!room || room !== socket.data.roomId) return;
    emitPresence(room);
  });

  socket.on('send_message', (payload) => {
    const roomId = socket.data.roomId;
    const userId = socket.data.userId;
    if (!roomId || !userId || clean(payload?.roomId, 60) !== roomId) return;
    const text = clean(payload?.text, 2000);
    if (!text) return;

    const now = Date.now();
    if (now - (lastSent.get(socket.id) || 0) < 500) {
      return socket.emit('server_error', 'Не отправляйте сообщения слишком часто');
    }
    lastSent.set(socket.id, now);

    const p = profilesById.get(userId);
    if (!p || p.roomId !== roomId) return;

    const list = messagesByRoom.get(roomId) || [];
    list.push({
      id: nextMsgId++,
      roomId,
      userId: p.id,
      nick: p.nick,
      avatarType: p.avatarType,
      avatarValue: p.avatarValue,
      text,
      createdAt: now
    });
    if (list.length > 500) list.splice(0, list.length - 500);
    messagesByRoom.set(roomId, list);

    io.to(roomId).emit('new_message', {
      id: p.id,
      nick: p.nick,
      avatarType: p.avatarType,
      avatarValue: p.avatarValue,
      text,
      time: formatTime(now)
    });
  });

  socket.on('get_dm_history', ({ toId } = {}) => {
    const me = socket.data.userId;
    const other = clean(toId, 40);
    if (!me || !/^ID-[0-9]{6}$/.test(other) || other === me) return;

    const rows = privateMessages
      .filter(
        (m) =>
          (m.fromId === me && m.toId === other) ||
          (m.fromId === other && m.toId === me)
      )
      .slice(-100)
      .map((m) => ({
        fromId: m.fromId,
        toId: m.toId,
        fromNick: m.fromNick,
        text: m.text,
        time: formatTime(m.createdAt)
      }));

    socket.emit('dm_history', rows);
  });

  socket.on('send_dm', ({ toId, text: rawText } = {}) => {
    const fromId = socket.data.userId;
    const to = clean(toId, 40);
    const text = clean(rawText, 2000);
    if (!fromId || !/^ID-[0-9]{6}$/.test(to) || to === fromId || !text) return;

    const now = Date.now();
    if (now - (lastSent.get(socket.id) || 0) < 500) {
      return socket.emit('server_error', 'Не отправляйте сообщения слишком часто');
    }
    lastSent.set(socket.id, now);

    const fromProfile = profilesById.get(fromId);
    const fromNick = fromProfile?.nick || 'Гость';
    const message = {
      fromId,
      toId: to,
      fromNick,
      text,
      time: formatTime(now)
    };

    privateMessages.push({
      id: nextDmId++,
      fromId,
      toId: to,
      fromNick,
      text,
      createdAt: now
    });
    if (privateMessages.length > 1000) {
      privateMessages.splice(0, privateMessages.length - 1000);
    }

    io.to(dmRoom(fromId, to)).emit('new_dm', message);
    for (const [sid, info] of online) {
      if (info.userId === fromId || info.userId === to) {
        io.to(sid).emit('new_dm', message);
      }
    }
  });

  socket.on('disconnect', () => {
    const info = online.get(socket.id);
    online.delete(socket.id);
    lastSent.delete(socket.id);
    if (info) emitPresence(info.roomId);
  });
});

server.listen(PORT, '0.0.0.0', () => console.log(`Chat server listening on ${PORT}`));
