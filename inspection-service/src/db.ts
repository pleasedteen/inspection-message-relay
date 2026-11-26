import fs from 'node:fs';
import path from 'node:path';

// Node 22 内置同步 SQLite。类型在不同 @types/node 版本上不稳定，这里按 any 使用，
// 运行时行为在 Node v22.23 上验证（DatabaseSync#prepare/exec + changes/lastInsertRowid）。
// eslint-disable-next-line @typescript-eslint/no-var-requires
const nodeSqlite: any = require('node:sqlite');
const DatabaseSync: new (file: string) => SqliteDatabase = nodeSqlite.DatabaseSync;

export interface SqliteStatement {
  get(...params: unknown[]): any;
  all(...params: unknown[]): any[];
  run(...params: unknown[]): { changes: number | bigint; lastInsertRowid: number | bigint };
}
export interface SqliteDatabase {
  exec(sql: string): void;
  prepare(sql: string): SqliteStatement;
  close(): void;
}

/**
 * Schema 设计要点（与“领取不随积压线性增长”直接相关）：
 *
 * receipts 为每个 (订阅 × 已发布消息) 保存一条状态。所有热查询都以 sub_id 为前导列，
 * “其他订阅的积压”物理上位于别的索引前缀，不会被本订阅的领取扫描到。
 *
 * heads 保存每个 (订阅, 设备) 当前第一个未决回执的 seq（队头指针），
 * 领取任意消息时每个有未决项的设备只占一行，与历史积压条数无关。
 */
export const SCHEMA = `
CREATE TABLE IF NOT EXISTS subscriptions (
  id            TEXT PRIMARY KEY,
  name          TEXT NOT NULL UNIQUE,
  effective_at  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS devices (
  device_id  TEXT PRIMARY KEY,
  last_seq   INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS messages (
  id                TEXT PRIMARY KEY,
  device_id         TEXT NOT NULL,
  seq               INTEGER NOT NULL,
  published_at      INTEGER NOT NULL,
  first_deadline_at INTEGER NOT NULL,
  idempotency_key   TEXT,
  body_reclaimed    INTEGER NOT NULL DEFAULT 0,
  reclaimed_at      INTEGER,
  UNIQUE(device_id, seq)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_idem
  ON messages(idempotency_key) WHERE idempotency_key IS NOT NULL;
-- 清理夹具用：只列尚未回收正文的消息（局部索引，回收后自动移出）
CREATE INDEX IF NOT EXISTS idx_messages_unreclaimed
  ON messages(id) WHERE body_reclaimed = 0;

CREATE TABLE IF NOT EXISTS bodies (
  msg_id  TEXT PRIMARY KEY REFERENCES messages(id),
  payload TEXT NOT NULL,
  bytes   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS receipts (
  sub_id            TEXT NOT NULL REFERENCES subscriptions(id),
  msg_id            TEXT NOT NULL REFERENCES messages(id),
  device_id         TEXT NOT NULL,
  seq               INTEGER NOT NULL,
  state             TEXT NOT NULL CHECK (state IN ('pending','claimed','acked','skipped')),
  lease_token       TEXT,
  lease_expires_at  INTEGER,
  claim_count       INTEGER NOT NULL DEFAULT 0,
  first_claimed_at  INTEGER,
  last_claimed_at   INTEGER,
  acked_at          INTEGER,
  terminal_at       INTEGER,
  first_deadline_at INTEGER NOT NULL,
  PRIMARY KEY (sub_id, msg_id)
) WITHOUT ROWID;

-- 按 订阅+设备+顺序号 的精确定位 / 队头推进
CREATE UNIQUE INDEX IF NOT EXISTS idx_receipts_sub_dev_seq
  ON receipts(sub_id, device_id, seq);

-- 未决回执局部索引：队头推进时直接取该设备 MIN(seq) 的未决项，
-- 不遍历已 acked/skipped 的历史积压
CREATE INDEX IF NOT EXISTS idx_receipts_open
  ON receipts(sub_id, device_id, seq)
  WHERE state IN ('pending','claimed');

-- 领取触发的过期清扫：只覆盖 pending，按 (订阅, 首次截止) 做范围定位
CREATE INDEX IF NOT EXISTS idx_receipts_pending_deadline
  ON receipts(sub_id, first_deadline_at)
  WHERE state = 'pending';

-- 清理判定的 NOT EXISTS 按 msg_id 探测
CREATE INDEX IF NOT EXISTS idx_receipts_msg
  ON receipts(msg_id);
-- 清理判定中“已领取但未确认”的局部索引
CREATE INDEX IF NOT EXISTS idx_receipts_claimed_msg
  ON receipts(msg_id) WHERE state = 'claimed';

CREATE TABLE IF NOT EXISTS heads (
  sub_id     TEXT NOT NULL REFERENCES subscriptions(id),
  device_id  TEXT NOT NULL,
  seq        INTEGER NOT NULL,
  PRIMARY KEY (sub_id, device_id)
) WITHOUT ROWID;
`;

export function openDatabase(file: string): SqliteDatabase {
  if (file !== ':memory:') {
    fs.mkdirSync(path.dirname(path.resolve(file)), { recursive: true });
  }
  const db = new DatabaseSync(file);
  // WAL：kill -9 已提交事务不丢；FULL：每次提交 fsync，保证异常退出后时间边界原样恢复。
  db.exec('PRAGMA journal_mode=WAL;');
  db.exec('PRAGMA synchronous=FULL;');
  db.exec('PRAGMA foreign_keys=ON;');
  db.exec('PRAGMA busy_timeout=8000;');
  db.exec(SCHEMA);
  return db;
}

/** 在一个 BEGIN IMMEDIATE 事务内执行同步工作；抛错回滚。 */
export function withTx<T>(db: SqliteDatabase, fn: () => T): T {
  db.exec('BEGIN IMMEDIATE');
  try {
    const out = fn();
    db.exec('COMMIT');
    return out;
  } catch (err) {
    try {
      db.exec('ROLLBACK');
    } catch {
      /* 事务可能已被 SQLite 回滚 */
    }
    throw err;
  }
}
