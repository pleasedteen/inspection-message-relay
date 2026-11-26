import { randomUUID } from 'node:crypto';
import { SqliteDatabase, withTx } from './db';
import { ApiError, badRequest, conflict, notFound } from './errors';
import { Meter, MeterSnapshot } from './meter';

export const DEFAULT_LEASE_MS = 30_000;
export const MAX_LEASE_MS = 10 * 60_000;

export interface PublishInput {
  device_id?: unknown;
  payload?: unknown;
  /** 绝对截止时间：epoch 毫秒，或 ISO 字符串 */
  first_deadline_at?: number | string;
  /** 相对首次领取有效期（毫秒），与 first_deadline_at 二选一 */
  first_claim_ttl_ms?: number;
  idempotency_key?: string;
}

export interface ClaimOptions {
  lease_ms?: number;
  device_id?: string;
}

interface SubRow {
  id: string;
  name: string;
  effective_at: number;
}

/** 热语句的查询计划（基准与 /admin/query-plans 用它证明走索引而非全表扫描）。 */
const HOT_SQL: Record<string, { sql: string; params: unknown[] }> = {
  head_get: {
    sql: `SELECT sub_id, device_id, seq FROM heads WHERE sub_id = ? AND device_id = ?`,
    params: ['s', 'd'],
  },
  heads_list: {
    sql: `SELECT sub_id, device_id, seq FROM heads WHERE sub_id = ?`,
    params: ['s'],
  },
  receipt_by_triple: {
    sql: `SELECT * FROM receipts WHERE sub_id = ? AND device_id = ? AND seq = ?`,
    params: ['s', 'd', 1],
  },
  open_min: {
    sql: `SELECT MIN(seq) AS seq FROM receipts
          WHERE sub_id = ? AND device_id = ? AND state IN ('pending','claimed')`,
    params: ['s', 'd'],
  },
  sweep_pending: {
    sql: `UPDATE receipts SET state = 'skipped', terminal_at = ?
          WHERE sub_id = ? AND state = 'pending' AND first_deadline_at <= ?`,
    params: [1, 's', 1],
  },
  cleanup_predicate: {
    sql: `SELECT m.id FROM messages m
          WHERE m.body_reclaimed = 0 AND NOT EXISTS (
            SELECT 1 FROM receipts r WHERE r.msg_id = m.id AND (
              r.state = 'claimed'
              OR (r.state = 'pending' AND m.first_deadline_at > ?)
            ))`,
    params: [1],
  },
};

export class InspectionService {
  constructor(private readonly db: SqliteDatabase) {}

  // ---------------------------------------------------------------- 订阅注册

  /** 幂等注册：重名时返回既有订阅及其原始生效时间，不会刷新 effective_at。 */
  registerSubscription(name: unknown, now = Date.now()): {
    id: string;
    name: string;
    effective_at: number;
    created: boolean;
  } {
    if (typeof name !== 'string' || name.trim().length === 0) {
      badRequest('NAME_REQUIRED', '订阅名称 name 必须是非空字符串');
    }
    const cleanName = name.trim();
    const id = randomUUID();
    return withTx(this.db, () => {
      const ins = this.db
        .prepare(`INSERT OR IGNORE INTO subscriptions(id, name, effective_at) VALUES(?, ?, ?)`)
        .run(id, cleanName, now);
      const row = this.db
        .prepare(`SELECT id, name, effective_at FROM subscriptions WHERE name = ?`)
        .get(cleanName) as SubRow;
      return {
        id: row.id,
        name: row.name,
        effective_at: Number(row.effective_at),
        created: Number(ins.changes) === 1,
      };
    });
  }

  /** 支持用订阅 ID 或唯一名称定位。 */
  getSubscription(idOrName: string): SubRow | null {
    let row = this.db
      .prepare(`SELECT id, name, effective_at FROM subscriptions WHERE id = ?`)
      .get(idOrName) as SubRow | undefined;
    if (!row) {
      row = this.db
        .prepare(`SELECT id, name, effective_at FROM subscriptions WHERE name = ?`)
        .get(idOrName) as SubRow | undefined;
    }
    return row ?? null;
  }

  private requireSub(idOrName: string, meter?: Meter): SubRow {
    if (meter) meter.seek(); // subscriptions PK
    const sub = this.getSubscription(idOrName);
    if (!sub) notFound('SUBSCRIPTION_NOT_FOUND', `订阅不存在: ${idOrName}`);
    if (meter) meter.read();
    return sub!;
  }

  // ---------------------------------------------------------------- 发布

  publish(input: PublishInput, now = Date.now()) {
    const deviceId = input.device_id;
    if (typeof deviceId !== 'string' || deviceId.trim().length === 0) {
      badRequest('DEVICE_ID_REQUIRED', 'device_id 必须是非空字符串');
    }
    const dev = deviceId.trim();

    let deadline: number;
    if (input.first_deadline_at !== undefined) {
      const raw = input.first_deadline_at;
      deadline =
        typeof raw === 'number' ? raw : Date.parse(String(raw));
      if (!Number.isFinite(deadline)) {
        badRequest('BAD_DEADLINE', 'first_deadline_at 不是合法的时间/epoch 毫秒');
      }
    } else if (input.first_claim_ttl_ms !== undefined) {
      const ttl = Number(input.first_claim_ttl_ms);
      if (!Number.isFinite(ttl) || ttl < 0) {
        badRequest('BAD_TTL', 'first_claim_ttl_ms 必须是 >= 0 的毫秒数');
      }
      deadline = now + ttl;
    } else {
      badRequest(
        'DEADLINE_REQUIRED',
        '发布必须指定首次领取截止：first_deadline_at（绝对时间）或 first_claim_ttl_ms（相对毫秒）',
      );
    }

    let payloadText: string;
    let bytes: number;
    try {
      payloadText = JSON.stringify(input.payload ?? null);
      bytes = Buffer.byteLength(payloadText, 'utf8');
    } catch (e) {
      badRequest('BAD_PAYLOAD', `payload 无法 JSON 序列化: ${(e as Error).message}`);
    }

    const idem =
      typeof input.idempotency_key === 'string' && input.idempotency_key
        ? input.idempotency_key
        : null;

    return withTx(this.db, () => {
      if (idem) {
        const existing = this.db
          .prepare(`SELECT id FROM messages WHERE idempotency_key = ?`)
          .get(idem) as { id: string } | undefined;
        if (existing) {
          const view = this.getMessageView(existing.id);
          return { ...view, deduped: true as const };
        }
      }

      // 同一设备的并发发布在同一个 IMMEDIATE 写事务里原子取号，
      // 因此得到统一、连续的顺序 seq。
      this.db
        .prepare(
          `INSERT INTO devices(device_id, last_seq) VALUES(?, 1)
           ON CONFLICT(device_id) DO UPDATE SET last_seq = last_seq + 1`,
        )
        .run(dev);
      const devRow = this.db
        .prepare(`SELECT last_seq FROM devices WHERE device_id = ?`)
        .get(dev) as { last_seq: number };
      const seq = Number(devRow.last_seq);

      const msgId = randomUUID();
      this.db
        .prepare(
          `INSERT INTO messages(id, device_id, seq, published_at, first_deadline_at, idempotency_key)
           VALUES(?, ?, ?, ?, ?, ?)`,
        )
        .run(msgId, dev, seq, now, deadline, idem);
      this.db
        .prepare(`INSERT INTO bodies(msg_id, payload, bytes) VALUES(?, ?, ?)`)
        .run(msgId, payloadText, bytes);

      // 扇出给“此刻已经注册生效”的全部订阅；之后注册的订阅没有回执，天然收不到旧消息。
      const subs = this.db.prepare(`SELECT id FROM subscriptions`).all() as { id: string }[];
      const insRcpt = this.db.prepare(
        `INSERT INTO receipts(sub_id, msg_id, device_id, seq, state, first_deadline_at)
         VALUES(?, ?, ?, ?, 'pending', ?)`,
      );
      // 新 seq 一定更大；仅当该 (订阅,设备) 还没有队头时建立队头。
      const insHead = this.db.prepare(
        `INSERT INTO heads(sub_id, device_id, seq) VALUES(?, ?, ?)
         ON CONFLICT(sub_id, device_id) DO NOTHING`,
      );
      for (const s of subs) {
        insRcpt.run(s.id, msgId, dev, seq, deadline);
        insHead.run(s.id, dev, seq);
      }

      return { ...this.getMessageView(msgId)!, deduped: false as const, recipients: subs.length };
    });
  }

  getMessageView(msgId: string): Record<string, unknown> | null {
    const row = this.db
      .prepare(
        `SELECT m.id, m.device_id, m.seq, m.published_at, m.first_deadline_at,
                m.body_reclaimed, m.reclaimed_at, m.idempotency_key, b.payload
         FROM messages m LEFT JOIN bodies b ON b.msg_id = m.id
         WHERE m.id = ?`,
      )
      .get(msgId) as any;
    if (!row) return null;
    let payload: unknown = null;
    if (row.payload != null) {
      try {
        payload = JSON.parse(row.payload);
      } catch {
        payload = row.payload;
      }
    }
    return {
      id: row.id,
      device_id: row.device_id,
      seq: Number(row.seq),
      published_at: Number(row.published_at),
      first_deadline_at: Number(row.first_deadline_at),
      body_reclaimed: !!row.body_reclaimed,
      reclaimed_at: row.reclaimed_at == null ? null : Number(row.reclaimed_at),
      idempotency_key: row.idempotency_key,
      payload,
    };
  }

  // ---------------------------------------------------------------- 领取

  claim(
    idOrName: string,
    opts: ClaimOptions = {},
    now = Date.now(),
    meter: Meter = new Meter(),
  ): {
    claim: Record<string, unknown> | null;
    blocked?: Record<string, unknown> | null;
    meter: MeterSnapshot;
    server_time: number;
  } {
    const sub = this.requireSub(idOrName, meter);
    const leaseMs = normalizeLease(opts.lease_ms);
    const onlyDevice = opts.device_id ? String(opts.device_id).trim() : null;

    return withTx(this.db, () => {
      // —— 本次领取触发的清扫①：本订阅所有已过首次截止、仍 pending 的回执一次性置 skipped。
      // 走 (sub_id, first_deadline_at) 局部索引范围；不触碰其他订阅（含其重投积压）。
      const swept = this.db
        .prepare(
          `UPDATE receipts SET state = 'skipped', terminal_at = ?
           WHERE sub_id = ? AND state = 'pending' AND first_deadline_at <= ?`,
        )
        .run(now, sub.id, now);
      meter.seek(); // 索引范围定位 1 次
      // 局部索引 (sub_id, first_deadline_at) 的覆盖区间内，检查到的项全部命中并被更新
      meter.step(Number(swept.changes));
      meter.write(swept.changes);
      meter.sweep(swept.changes);

      // —— 取本订阅当前各设备队头。每个有未决项的设备恰好 1 行。
      let headKeys: { device_id: string; seq: number }[];
      if (onlyDevice) {
        meter.seek();
        const h = this.db
          .prepare(`SELECT device_id, seq FROM heads WHERE sub_id = ? AND device_id = ?`)
          .get(sub.id, onlyDevice) as { device_id: string; seq: number } | undefined;
        headKeys = h ? [{ device_id: h.device_id, seq: Number(h.seq) }] : [];
      } else {
        meter.seek();
        const rows = this.db
          .prepare(`SELECT device_id, seq FROM heads WHERE sub_id = ? ORDER BY device_id`)
          .all(sub.id) as { device_id: string; seq: number }[];
        meter.step(rows.length); // heads PK 前缀扫描，取出 H 个设备队头
        headKeys = rows.map((r) => ({ device_id: r.device_id, seq: Number(r.seq) }));
      }

      let firstBlocked: Record<string, unknown> | null = null;

      for (const headKey of headKeys) {
        // 清扫可能已把队头置为 skipped；每次推进都由局部未决索引直接取 MIN(seq)，
        // 不会逐条翻过历史 acked/skipped 行。guard 只防御意外的悬空队头。
        let cur: { device_id: string; seq: number } | null = headKey;
        for (let guard = 0; guard < 64 && cur; guard++) {
          const rcpt = this.getReceiptTriple(sub.id, cur.device_id, cur.seq, meter);
          if (!rcpt) {
            cur = this.advanceHead(sub.id, cur.device_id, cur.seq, meter);
            continue;
          }

          if (rcpt.state === 'acked' || rcpt.state === 'skipped') {
            cur = this.advanceHead(sub.id, cur.device_id, cur.seq, meter);
            continue;
          }

          if (rcpt.state === 'pending') {
            if (Number(rcpt.first_deadline_at) <= now) {
              // 兜底（批量清扫正常已处理）
              const r = this.db
                .prepare(
                  `UPDATE receipts SET state='skipped', terminal_at=?
                   WHERE sub_id=? AND msg_id=? AND state='pending'`,
                )
                .run(now, sub.id, rcpt.msg_id);
              meter.write(r.changes);
              meter.sweep(r.changes);
              cur = this.advanceHead(sub.id, cur.device_id, cur.seq, meter);
              continue;
            }
            const token = randomUUID();
            const expiresAt = now + leaseMs;
            const r = this.db
              .prepare(
                `UPDATE receipts
                   SET state='claimed', lease_token=?, lease_expires_at=?,
                       claim_count=claim_count+1,
                       first_claimed_at=COALESCE(first_claimed_at, ?),
                       last_claimed_at=?
                 WHERE sub_id=? AND msg_id=? AND state='pending'`,
              )
              .run(token, expiresAt, now, now, sub.id, rcpt.msg_id);
            meter.write(r.changes);
            if (Number(r.changes) === 1) {
              return this.claimResponse(rcpt.msg_id, token, expiresAt, 1, false, meter, now);
            }
            continue; // 极端竞争下重试
          }

          if (rcpt.state === 'claimed') {
            if (Number(rcpt.lease_expires_at) > now) {
              // 有效租期内：同设备后项必须等待；claim-any 可转去其他设备。
              const blocked = {
                reason: 'LEASE_ACTIVE',
                device_id: cur.device_id,
                seq: cur.seq,
                message_id: rcpt.msg_id,
                lease_expires_at: Number(rcpt.lease_expires_at),
              };
              if (!firstBlocked) firstBlocked = blocked;
              break;
            }
            // 租期已过：允许同订阅任意工作者重领，旧 token 随之失效。
            const token = randomUUID();
            const expiresAt = now + leaseMs;
            const r = this.db
              .prepare(
                `UPDATE receipts
                   SET lease_token=?, lease_expires_at=?, claim_count=claim_count+1,
                       last_claimed_at=?
                 WHERE sub_id=? AND msg_id=? AND state='claimed' AND lease_expires_at <= ?`,
              )
              .run(token, expiresAt, now, sub.id, rcpt.msg_id, now);
            meter.write(r.changes);
            if (Number(r.changes) === 1) {
              const count = Number(rcpt.claim_count) + 1;
              return this.claimResponse(rcpt.msg_id, token, expiresAt, count, true, meter, now);
            }
            continue;
          }
        }

        // 指定设备时不扫描 / 领取其他设备，直接把该设备的阻塞原因返回。
        if (onlyDevice) {
          return { claim: null, blocked: firstBlocked, meter: meter.snapshot(), server_time: now };
        }
      }

      return { claim: null, blocked: firstBlocked, meter: meter.snapshot(), server_time: now };
    });
  }

  private getReceiptTriple(
    subId: string,
    deviceId: string,
    seq: number,
    meter: Meter,
  ): any | null {
    meter.seek(); // idx_receipts_sub_dev_seq 精确定位
    const row = this.db
      .prepare(`SELECT * FROM receipts WHERE sub_id = ? AND device_id = ? AND seq = ?`)
      .get(subId, deviceId, seq) as any | undefined;
    if (row) meter.read();
    return row ?? null;
  }

  /**
   * 队头推进：用未决局部索引直接取该设备下一个 pending/claimed 的 MIN(seq)。
   * 返回新的队头键；没有未决项时删除 heads 行并返回 null。
   */
  private advanceHead(
    subId: string,
    deviceId: string,
    oldSeq: number,
    meter: Meter,
  ): { device_id: string; seq: number } | null {
    meter.seek(); // idx_receipts_open 的 MIN 探测
    const row = this.db
      .prepare(
        `SELECT MIN(seq) AS seq FROM receipts
         WHERE sub_id = ? AND device_id = ? AND state IN ('pending','claimed')`,
      )
      .get(subId, deviceId) as { seq: number | null };

    if (row && row.seq != null) {
      const nextSeq = Number(row.seq);
      const r = this.db
        .prepare(`UPDATE heads SET seq = ? WHERE sub_id = ? AND device_id = ? AND seq = ?`)
        .run(nextSeq, subId, deviceId, oldSeq);
      meter.write(r.changes);
      meter.sweep(1);
      return { device_id: deviceId, seq: nextSeq };
    }
    const r = this.db
      .prepare(`DELETE FROM heads WHERE sub_id = ? AND device_id = ? AND seq = ?`)
      .run(subId, deviceId, oldSeq);
    meter.write(r.changes);
    meter.sweep(1);
    return null;
  }

  private claimResponse(
    msgId: string,
    token: string,
    expiresAt: number,
    deliveryCount: number,
    redelivery: boolean,
    meter: Meter,
    now: number,
  ) {
    meter.seek(); // messages PK
    meter.seek(); // bodies PK
    const view = this.getMessageView(msgId)!;
    meter.read(2);
    return {
      claim: {
        ...view,
        lease_token: token,
        lease_expires_at: expiresAt,
        delivery_count: deliveryCount,
        redelivery,
      },
      blocked: null,
      meter: meter.snapshot(),
      server_time: now,
    };
  }

  // ---------------------------------------------------------------- 确认

  ack(
    idOrName: string,
    body: { message_id?: unknown; token?: unknown },
    now = Date.now(),
  ): { acked: true; message_id: string; acked_at: number } {
    const sub = this.requireSub(idOrName);
    const messageId = body?.message_id;
    const token = body?.token;
    if (typeof messageId !== 'string' || messageId.length === 0) {
      badRequest('MESSAGE_ID_REQUIRED', 'ack 需要 message_id');
    }
    if (typeof token !== 'string' || token.length === 0) {
      badRequest('TOKEN_REQUIRED', 'ack 需要领取时下发的 lease_token');
    }

    return withTx(this.db, () => {
      const rcpt = this.db
        .prepare(`SELECT * FROM receipts WHERE sub_id = ? AND msg_id = ?`)
        .get(sub.id, messageId) as any | undefined;
      if (!rcpt) notFound('RECEIPT_NOT_FOUND', '该订阅没有这条消息的回执');

      if (rcpt.state === 'acked') {
        conflict('ALREADY_ACKED', '该消息已经确认过，不能重复确认');
      }
      if (rcpt.state !== 'claimed') {
        conflict('NOT_CLAIMED', `回执状态为 ${rcpt.state}，只有领取中的消息可以确认`);
      }
      if (rcpt.lease_token !== token) {
        conflict('INVALID_TOKEN', 'lease_token 不是当前有效领取的令牌（可能已被重领）');
      }
      if (Number(rcpt.lease_expires_at) <= now) {
        conflict(
          'LEASE_EXPIRED',
          '当前领取的租期已过，确认无效；消息可被重新领取，新的领取才能确认',
          { lease_expires_at: Number(rcpt.lease_expires_at), server_time: now },
        );
      }

      const r = this.db
        .prepare(
          `UPDATE receipts SET state='acked', acked_at=?, terminal_at=?
           WHERE sub_id=? AND msg_id=? AND state='claimed'
             AND lease_token=? AND lease_expires_at > ?`,
        )
        .run(now, now, sub.id, messageId, token, now);
      if (Number(r.changes) !== 1) {
        conflict('ACK_CONDITION_FAILED', '确认条件未满足（令牌/租期/状态已变化）');
      }

      // 若确认的正是队头，推进队头，释放同设备后项。
      const head = this.db
        .prepare(`SELECT seq FROM heads WHERE sub_id = ? AND device_id = ?`)
        .get(sub.id, rcpt.device_id) as { seq: number } | undefined;
      if (head && Number(head.seq) === Number(rcpt.seq)) {
        this.advanceHead(sub.id, rcpt.device_id, Number(rcpt.seq), new Meter());
      }
      return { acked: true as const, message_id: messageId, acked_at: now };
    });
  }

  // ---------------------------------------------------------------- 清理

  /**
   * 手动清理。以调用时刻 now 作为唯一判定基准：
   * 回收所有“没有任何订阅仍可首次领取（pending 且 deadline>now），
   * 也没有任何订阅已领取但未确认（state='claimed'，无论租期是否已过）”的消息正文。
   * 返回实际新回收的正文条数；重复调用第二次为 0。
   */
  cleanup(now = Date.now()): {
    decided_at: number;
    reclaimed_bodies: number;
    candidate_messages: number;
  } {
    return withTx(this.db, () => {
      const rows = this.db
        .prepare(HOT_SQL.cleanup_predicate.sql)
        .all(now) as { id: string }[];
      const ids = rows.map((r) => r.id);

      let reclaimed = 0;
      // 分块绑定 IN 参数（SQLite 变量上限足够大，500 一条很安全）
      for (let i = 0; i < ids.length; i += 500) {
        const chunk = ids.slice(i, i + 500);
        const placeholders = chunk.map(() => '?').join(',');
        const del = this.db
          .prepare(`DELETE FROM bodies WHERE msg_id IN (${placeholders})`)
          .run(...chunk);
        reclaimed += Number(del.changes);
        this.db
          .prepare(
            `UPDATE messages SET body_reclaimed=1, reclaimed_at=?
             WHERE id IN (${placeholders}) AND body_reclaimed=0`,
          )
          .run(now, ...chunk);
      }
      return {
        decided_at: now,
        reclaimed_bodies: reclaimed,
        candidate_messages: ids.length,
      };
    });
  }

  // ---------------------------------------------------------------- 管理/诊断

  stats() {
    const one = (sql: string, ...p: unknown[]) => (this.db.prepare(sql).get(...p) as any)?.n ?? 0;
    return {
      time: Date.now(),
      subscriptions: one(`SELECT COUNT(*) n FROM subscriptions`),
      devices: one(`SELECT COUNT(*) n FROM devices`),
      messages: one(`SELECT COUNT(*) n FROM messages`),
      bodies_present: one(`SELECT COUNT(*) n FROM bodies`),
      bodies_reclaimed: one(`SELECT COUNT(*) n FROM messages WHERE body_reclaimed=1`),
      heads: one(`SELECT COUNT(*) n FROM heads`),
      receipts: {
        pending: one(`SELECT COUNT(*) n FROM receipts WHERE state='pending'`),
        claimed: one(`SELECT COUNT(*) n FROM receipts WHERE state='claimed'`),
        acked: one(`SELECT COUNT(*) n FROM receipts WHERE state='acked'`),
        skipped: one(`SELECT COUNT(*) n FROM receipts WHERE state='skipped'`),
      },
    };
  }

  /** 管理端查看某条消息在各订阅下的回执状态（测试与排障用）。 */
  messageReceipts(messageId: string) {
    const msg = this.getMessageView(messageId);
    if (!msg) notFound('MESSAGE_NOT_FOUND', messageId);
    const rows = this.db
      .prepare(
        `SELECT sub_id, state, seq, claim_count, lease_expires_at, lease_token,
                first_deadline_at, acked_at
         FROM receipts WHERE msg_id = ? ORDER BY sub_id`,
      )
      .all(messageId) as any[];
    return {
      message: msg,
      receipts: rows.map((r) => ({
        sub_id: r.sub_id,
        state: r.state,
        seq: Number(r.seq),
        claim_count: Number(r.claim_count),
        lease_expires_at: r.lease_expires_at == null ? null : Number(r.lease_expires_at),
        // 不回传完整 token，仅给前缀，避免接口变成令牌查询口
        lease_token_prefix: r.lease_token ? String(r.lease_token).slice(0, 8) : null,
        first_deadline_at: Number(r.first_deadline_at),
        acked_at: r.acked_at == null ? null : Number(r.acked_at),
      })),
    };
  }

  /** 热语句的真实 EXPLAIN QUERY PLAN（绑定虚拟参数，不实际执行）。 */
  hotQueryPlans(): Record<string, string[]> {
    const out: Record<string, string[]> = {};
    for (const [name, item] of Object.entries(HOT_SQL)) {
      try {
        const rows = this.db
          .prepare(`EXPLAIN QUERY PLAN ${item.sql}`)
          .all(...item.params) as { detail: string }[];
        out[name] = rows.map((r) => r.detail);
      } catch (e) {
        out[name] = [`PLAN_ERROR: ${(e as Error).message}`];
      }
    }
    return out;
  }
}

export function normalizeLease(v: unknown): number {
  if (v === undefined || v === null) return DEFAULT_LEASE_MS;
  const n = Number(v);
  if (!Number.isFinite(n) || n <= 0) {
    throw new ApiError(400, 'BAD_LEASE', 'lease_ms 必须是正数毫秒');
  }
  return Math.min(Math.max(Math.floor(n), 50), MAX_LEASE_MS);
}
