'use strict';
const path = require('path');
const fs = require('fs');
const http = require('http');
const express = require('express');
const { Server } = require('socket.io');
const Database = require('better-sqlite3');

const PORT = Number(process.env.PORT || 3000);
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
fs.mkdirSync(DATA_DIR, { recursive: true });
const db = new Database(path.join(DATA_DIR, 'chat.sqlite'));
db.pragma('journal_mode = WAL');
db.pragma('foreign_keys = ON');
db.exec(`
CREATE TABLE IF NOT EXISTS profiles (
  id TEXT PRIMARY KEY, room_id TEXT NOT NULL, nick TEXT NOT NULL,
  avatar_type TEXT NOT NULL DEFAULT 'gif', avatar_value TEXT NOT NULL DEFAULT '',
  updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_profiles_room ON profiles(room_id);
CREATE TABLE IF NOT EXISTS messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT, room_id TEXT NOT NULL, user_id TEXT NOT NULL,
  nick TEXT NOT NULL, avatar_type TEXT NOT NULL, avatar_value TEXT NOT NULL,
  text TEXT NOT NULL, created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_messages_room ON messages(room_id, id);
CREATE TABLE IF NOT EXISTS private_messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT, from_id TEXT NOT NULL, to_id TEXT NOT NULL,
  from_nick TEXT NOT NULL, text TEXT NOT NULL, created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dm_pair ON private_messages(from_id, to_id, id);
`);

const app = express();
app.disable('x-powered-by');
app.use(express.static(path.join(__dirname, 'public'), { extensions: ['html'] }));
app.get('/health', (_req, res) => res.json({ ok: true }));
const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 1e6, pingInterval: 25000, pingTimeout: 20000 });

const online = new Map(); // socketId -> { userId, roomId }
const lastSent = new Map();
const clean = (v, max = 100) => String(v ?? '').replace(/[\u0000-\u001f\u007f]/g, '').trim().slice(0, max);
function safeProfile(p) {
  const id = clean(p?.id, 40);
  const nick = clean(p?.nick, 20) || 'Гость';
  const avatarType = ['gif', 'image'].includes(p?.avatarType) ? p.avatarType : 'gif';
  let avatarValue = clean(p?.avatarValue, 700000);
  if (!/^(data:image\/(png|jpeg|jpg|webp|gif);base64,|[a-zA-Z0-9_.-]+\.gif$)/i.test(avatarValue)) avatarValue = '';
  if (!/^ID-[0-9]{6}$/.test(id)) throw new Error('Некорректный ID профиля');
  return { id, nick, avatarType, avatarValue };
}
function roomOnline(roomId) {
  return new Set([...online.values()].filter(x => x.roomId === roomId).map(x => x.userId)).size;
}
function emitPresence(roomId) {
  io.to(roomId).emit('online_count', roomOnline(roomId));
  const ids = new Set([...online.values()].filter(x => x.roomId === roomId).map(x => x.userId));
  const users = db.prepare(`SELECT id,nick,avatar_type AS avatarType,avatar_value AS avatarValue FROM profiles WHERE room_id=? ORDER BY updated_at DESC LIMIT 200`).all(roomId)
    .map(user => ({ ...user, isOnline: ids.has(user.id) }));
  io.to(roomId).emit('users_list', users);
}
function dmRoom(a, b) { return 'dm:' + [a,b].sort().join(':'); }

io.on('connection', socket => {
  socket.on('join_room', payload => {
    try {
      const roomId = clean(payload?.roomId || 'anon.42spina', 60);
      if (!/^[\w.-]+$/.test(roomId)) throw new Error('Некорректная комната');
      const p = safeProfile(payload?.profile);
      const now = Date.now();
      db.prepare(`INSERT INTO profiles(id,room_id,nick,avatar_type,avatar_value,updated_at)
        VALUES(@id,@roomId,@nick,@avatarType,@avatarValue,@updatedAt)
        ON CONFLICT(id) DO UPDATE SET room_id=excluded.room_id,nick=excluded.nick,avatar_type=excluded.avatar_type,avatar_value=excluded.avatar_value,updated_at=excluded.updated_at`)
        .run({ ...p, roomId, updatedAt: now });
      if (socket.data.roomId) socket.leave(socket.data.roomId);
      socket.join(roomId); socket.data.userId = p.id; socket.data.roomId = roomId;
      online.set(socket.id, { userId: p.id, roomId });
      const history = db.prepare(`SELECT user_id AS id,nick,avatar_type AS avatarType,avatar_value AS avatarValue,text,
        strftime('%H:%M', created_at/1000, 'unixepoch','localtime') AS time FROM messages WHERE room_id=? ORDER BY id DESC LIMIT 100`).all(roomId).reverse();
      socket.emit('chat_history', history);
      emitPresence(roomId);
    } catch (e) { socket.emit('server_error', e.message || 'Не удалось войти в чат'); }
  });

  socket.on('get_users', ({ roomId } = {}) => {
    const room = clean(roomId || socket.data.roomId || '', 60);
    if (!room || room !== socket.data.roomId) return;
    emitPresence(room);
  });

  socket.on('send_message', payload => {
    const roomId = socket.data.roomId, userId = socket.data.userId;
    if (!roomId || !userId || clean(payload?.roomId,60) !== roomId) return;
    const text = clean(payload?.text, 2000);
    if (!text) return;
    const now = Date.now();
    if (now - (lastSent.get(socket.id) || 0) < 500) return socket.emit('server_error','Не отправляйте сообщения слишком часто');
    lastSent.set(socket.id, now);
    const p = db.prepare('SELECT id,nick,avatar_type AS avatarType,avatar_value AS avatarValue FROM profiles WHERE id=? AND room_id=?').get(userId, roomId);
    if (!p) return;
    db.prepare('INSERT INTO messages(room_id,user_id,nick,avatar_type,avatar_value,text,created_at) VALUES(?,?,?,?,?,?,?)')
      .run(roomId,p.id,p.nick,p.avatarType,p.avatarValue,text,now);
    io.to(roomId).emit('new_message',{...p,text,time:new Date(now).toLocaleTimeString('ru-RU',{hour:'2-digit',minute:'2-digit'})});
  });

  socket.on('get_dm_history', ({ toId } = {}) => {
    const me=socket.data.userId, other=clean(toId,40);
    if (!me || !/^ID-[0-9]{6}$/.test(other) || other===me) return;
    const rows=db.prepare(`SELECT from_id AS fromId,to_id AS toId,from_nick AS fromNick,text,
      strftime('%H:%M',created_at/1000,'unixepoch','localtime') AS time FROM private_messages
      WHERE (from_id=? AND to_id=?) OR (from_id=? AND to_id=?) ORDER BY id DESC LIMIT 100`).all(me,other,other,me).reverse();
    socket.emit('dm_history',rows);
  });

  socket.on('send_dm', ({ toId, text: rawText } = {}) => {
    const fromId=socket.data.userId, to=clean(toId,40), text=clean(rawText,2000);
    if (!fromId || !/^ID-[0-9]{6}$/.test(to) || to===fromId || !text) return;
    const now=Date.now();
    if (now-(lastSent.get(socket.id)||0)<500) return socket.emit('server_error','Не отправляйте сообщения слишком часто');
    lastSent.set(socket.id,now);
    const from=db.prepare('SELECT nick FROM profiles WHERE id=?').get(fromId);
    const message={fromId,toId:to,fromNick:from?.nick||'Гость',text,time:new Date(now).toLocaleTimeString('ru-RU',{hour:'2-digit',minute:'2-digit'})};
    db.prepare('INSERT INTO private_messages(from_id,to_id,from_nick,text,created_at) VALUES(?,?,?,?,?)').run(fromId,to,message.fromNick,text,now);
    io.to(dmRoom(fromId,to)).emit('new_dm',message);
    // Deliver to all active sockets belonging to either participant.
    for (const [sid, info] of online) if (info.userId===fromId || info.userId===to) io.to(sid).emit('new_dm',message);
  });

  socket.on('disconnect', () => {
    const info=online.get(socket.id); online.delete(socket.id); lastSent.delete(socket.id);
    if (info) emitPresence(info.roomId);
  });
});

server.listen(PORT, '0.0.0.0', () => console.log(`Chat server listening on ${PORT}`));
