#!/usr/bin/env node
/**
 * 领取开销基准夹具。
 *
 * 场景（与题目要求一一对应）：
 *  - 固定目标订阅 target、固定一台设备 bench-pump、固定一条“新消息”和一次 claim；
 *  - 只改变历史积压 N：N 条旧消息全部被“另一个订阅 needy”在首次截止前领取、
 *    至今未确认且租约已过期（仍可重投），因此消息正文被清理判定保留；
 *  - 目标订阅 target 对这 N 条均已 ack（处理完毕），其队头已越过全部积压；
 *  - 测量新消息的第一次领取：逻辑访问计量（含本次领取触发的过期清扫）+ 墙钟；
 *  - 另跑一个“退化全量扫描”查询作为线性反例校准（不是服务领取路径）；
 *  - 打印热语句 EXPLAIN QUERY PLAN，并对计量不随 N 增长做硬断言。
 *
 * 运行：npm run bench   （或 node --experimental-sqlite dist/bench.js）
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { performance } from 'node:perf_hooks';
import { openDatabase, SqliteDatabase, withTx } from './db';
import { InspectionService } from './service';
import { Meter, MeterSnapshot } from './meter';

export interface ScaleResult {
  n: number;
  dbBytes: number;
  firstMeter: MeterSnapshot;
  claimAvgUs: number;
  claimP95Us: number;
  claimMaxUs: number;
  naiveRows: number;
  naiveMs: number;
  cleanupWithBacklog: number;
  rounds: number;
}

export interface BenchReport {
  dir: string;
  scales: ScaleResult[];
  plans: Record<string, string[]>;
  failures: string[];
  passed: boolean;
}

export interface BenchOptions {
  scales?: number[];
  rounds?: number;
  dir?: string;
  quiet?: boolean;
}

export async function runBenchmark(opts: BenchOptions = {}): Promise<BenchReport> {
  const scales = opts.scales ?? [0, 1_000, 10_000, 100_000];
  const rounds = opts.rounds ?? 200;
  const dir = opts.dir ?? fs.mkdtempSync(path.join(os.tmpdir(), 'insp-bench-'));
  fs.mkdirSync(dir, { recursive: true });

  const scalesOut: ScaleResult[] = [];
  let plans: Record<string, string[]> = {};
  const failures: string[] = [];

  for (const n of scales) {
    const file = path.join(dir, `bench-${n}.db`);
    const db = openDatabase(file);
    const svc = new InspectionService(db);
    const now = Date.now();
    const t0 = now - 10 * 24 * 3600_000;

    const target = svc.registerSubscription('target', t0);
    const needy = svc.registerSubscription('needy', t0);

    seedBacklog(db, target.id, needy.id, n, now);

    // 积压存在时手动清理：needy 对每条都是 claimed-unacked，必须全部保留正文。
    const gc = svc.cleanup(now);
    if (gc.reclaimed_bodies !== 0) {
      failures.push(`N=${n}: 清理在重投积压存在时回收了 ${gc.reclaimed_bodies} 条，应为 0`);
    }

    // 固定的“新消息”（真实发布路径：取号 + 扇出 + 建队头）
    const fresh = svc.publish(
      { device_id: 'bench-pump', first_claim_ttl_ms: 600_000, payload: { seq: 'new' } },
      now,
    );

    // —— 题目指定的那一次领取（带完整计量，含触发的清扫）——
    const first = svc.claim(target.id, { lease_ms: 60_000 }, now + 1, new Meter());
    if (!first.claim || first.claim.seq !== n + 1) {
      failures.push(`N=${n}: 固定领取未拿到新消息 seq=${n + 1}`);
    }
    const firstMeter = first.meter;

    // —— 线性反例校准：若领取退化为扫描本订阅全部回执 ——
    const tNaive = performance.now();
    const allMine = db.prepare(`SELECT * FROM receipts WHERE sub_id = ?`).all(target.id);
    const naiveMs = performance.now() - tNaive;
    const naiveRows = allMine.length;
    if (naiveRows !== n + 1) {
      failures.push(`N=${n}: 反例扫描行数 ${naiveRows} != ${n + 1}`);
    }

    if (n === 100_000) plans = svc.hotQueryPlans();

    // 确认固定领取，使队头释放；之后每轮 publish→claim(计时)→ack，形状与固定领取一致。
    svc.ack(
      target.id,
      { message_id: first.claim!.id as string, token: first.claim!.lease_token as string },
      now + 2,
    );

    const samples: number[] = [];
    for (let r = 0; r < rounds; r++) {
      svc.publish(
        { device_id: 'bench-pump', first_claim_ttl_ms: 600_000, payload: { r } },
        now + 10 + r,
      );
      const a = performance.now();
      const res = svc.claim(target.id, { lease_ms: 60_000 }, now + 20 + r);
      const b = performance.now();
      if (!res.claim) {
        failures.push(`N=${n} r=${r}: 轮次领取为空`);
        break;
      }
      samples.push((b - a) * 1000); // 微秒
      svc.ack(
        target.id,
        { message_id: res.claim.id as string, token: res.claim.lease_token as string },
        now + 21 + r,
      );
    }

    samples.sort((x, y) => x - y);
    const avgUs = samples.reduce((s, x) => s + x, 0) / Math.max(1, samples.length);
    const p95Us = samples[Math.floor(samples.length * 0.95) - 1] ?? avgUs;
    const maxUs = samples[samples.length - 1] ?? avgUs;

    db.close();
    const dbBytes = fileSizeWithWal(file);

    scalesOut.push({
      n,
      dbBytes,
      firstMeter,
      claimAvgUs: avgUs,
      claimP95Us: p95Us,
      claimMaxUs: maxUs,
      naiveRows,
      naiveMs,
      cleanupWithBacklog: gc.reclaimed_bodies,
      rounds: samples.length,
    });
  }

  // —— 硬性质：10 万档的计量条目必须与 0 档完全一致（访问形状不随积压变化）——
  const m0 = scalesOut[0]?.firstMeter;
  const mMax = scalesOut.find((s) => s.n === 100_000)?.firstMeter;
  if (m0 && mMax) {
    if (mMax.total !== m0.total) {
      failures.push(
        `计量总数随积压变化: N=0 为 ${m0.total}，N=100000 为 ${mMax.total}（不允许线性增长）`,
      );
    }
    if (mMax.total > 24) {
      failures.push(`单次领取计量条目 ${mMax.total} 超过常数上界 24`);
    }
    for (const k of ['probes', 'index_steps', 'rows_read', 'rows_written'] as const) {
      if (mMax[k] !== m0[k]) {
        failures.push(`计量分量 ${k} 随积压变化: ${m0[k]} -> ${mMax[k]}`);
      }
    }
  }

  // —— 硬性质：热语句必须走 SEARCH/索引，不允许 receipts/messages/bodies 全表 SCAN ——
  for (const name of ['head_get', 'heads_list', 'receipt_by_triple', 'open_min', 'sweep_pending']) {
    const lines = plans[name] ?? [];
    const joined = lines.join(' | ');
    if (!/SEARCH/.test(joined)) {
      failures.push(`查询 ${name} 没有 SEARCH 计划: ${joined}`);
    }
    if (/(^|\|)\s*SCAN (receipts|messages|bodies|heads)(\s|$)/.test(joined)) {
      failures.push(`查询 ${name} 出现全表 SCAN: ${joined}`);
    }
  }

  const report: BenchReport = { dir, scales: scalesOut, plans, failures, passed: failures.length === 0 };
  if (!opts.quiet) printBenchmark(report);
  return report;
}

function seedBacklog(
  db: SqliteDatabase,
  targetId: string,
  needyId: string,
  n: number,
  now: number,
): void {
  if (n === 0) return;
  const insMsg = db.prepare(
    `INSERT INTO messages(id, device_id, seq, published_at, first_deadline_at)
     VALUES(?, 'bench-pump', ?, ?, ?)`,
  );
  const insBody = db.prepare(`INSERT INTO bodies(msg_id, payload, bytes) VALUES(?, ?, ?)`);
  const insRcpt = db.prepare(
    `INSERT INTO receipts(sub_id, msg_id, device_id, seq, state, first_deadline_at,
                          claim_count, lease_token, lease_expires_at,
                          first_claimed_at, last_claimed_at, acked_at, terminal_at)
     VALUES(?, ?, 'bench-pump', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
  );
  const payload = JSON.stringify({ kind: 'backlog', padding: 'x'.repeat(160) });
  const expiredDeadline = now - 3_600_000;
  const expiredLease = now - 1_000;

  const CHUNK = 2_000;
  // needy 的队头停在 seq=1；target 全部 ack，不建队头（与真实 ack 推进结果一致）
  db.prepare(`INSERT INTO heads(sub_id, device_id, seq) VALUES(?, 'bench-pump', 1)`).run(needyId);
  for (let base = 0; base < n; base += CHUNK) {
    const end = Math.min(base + CHUNK, n);
    withTx(db, () => {
      for (let i = base + 1; i <= end; i++) {
        const id = `backlog-${i}`;
        insMsg.run(id, i, now - (n - i) * 1000, expiredDeadline);
        insBody.run(id, payload, payload.length);
        // 目标订阅：早已确认（终态，不阻塞队头）
        insRcpt.run(
          targetId, id, i, 'acked', expiredDeadline, 1,
          null, null, now - 500_000, now - 500_000, now - 400_000, now - 400_000,
        );
        // 其他订阅：已领取未确认、租约过期 → 仍可重投，正文必须保留
        insRcpt.run(
          needyId, id, i, 'claimed', expiredDeadline, 1,
          `needy-token-${i}`, expiredLease, now - 900_000, expiredLease, null, null,
        );
      }
      db.prepare(
        `INSERT INTO devices(device_id, last_seq) VALUES('bench-pump', ?)
         ON CONFLICT(device_id) DO UPDATE SET last_seq = MAX(last_seq, ?)`,
      ).run(n, n);
    });
  }
}

function fileSizeWithWal(file: string): number {
  let bytes = 0;
  for (const suffix of ['', '-wal', '-shm']) {
    try {
      bytes += fs.statSync(file + suffix).size;
    } catch {
      /* ignore */
    }
  }
  return bytes;
}

export function printBenchmark(r: BenchReport): void {
  const fmtN = (n: number) => n.toLocaleString('en-US');
  console.log('');
  console.log('══ 领取开销基准（固定 target 订阅 / bench-pump / 一条新消息 / 一次 claim）══');
  console.log('积压语义：N 条旧消息均被另一订阅 needy 领取未确认且租约过期（仍需重投），');
  console.log('          target 已全部 ack；手动清理在该积压下回收 0 条。');
  console.log('计量：seek=精确索引探测, step=范围扫描取出的索引项, r/o=记录读, w=记录写,');
  console.log('      sweep=本次领取触发的清扫/推进条目（已含在前面计数中）。');
  console.log('');
  const header =
    '积压N'.padStart(8) +
    ' | ' +
    'DB(MB)'.padStart(7) +
    ' | ' +
    'claim均值µs'.padStart(11) +
    ' | ' +
    'p95µs'.padStart(8) +
    ' | ' +
    '计量合计'.padStart(6) +
    ' | ' +
    'seek'.padStart(4) +
    ' | ' +
    'step'.padStart(4) +
    ' | ' +
    '读'.padStart(3) +
    ' | ' +
    '写'.padStart(3) +
    ' | ' +
    '清扫'.padStart(4) +
    ' | ' +
    '反例全扫行'.padStart(9) +
    ' | ' +
    '全扫ms'.padStart(7);
  console.log(header);
  console.log('-'.repeat(header.length));
  for (const s of r.scales) {
    const m = s.firstMeter;
    console.log(
      fmtN(s.n).padStart(8) +
        ' | ' +
        (s.dbBytes / 1024 / 1024).toFixed(1).padStart(7) +
        ' | ' +
        s.claimAvgUs.toFixed(1).padStart(11) +
        ' | ' +
        s.claimP95Us.toFixed(0).padStart(8) +
        ' | ' +
        String(m.total).padStart(6) +
        ' | ' +
        String(m.probes).padStart(4) +
        ' | ' +
        String(m.index_steps).padStart(4) +
        ' | ' +
        String(m.rows_read).padStart(3) +
        ' | ' +
        String(m.rows_written).padStart(3) +
        ' | ' +
        String(m.sweep_steps).padStart(4) +
        ' | ' +
        fmtN(s.naiveRows).padStart(9) +
        ' | ' +
        s.naiveMs.toFixed(2).padStart(7),
    );
  }
  console.log('');
  console.log('热语句 EXPLAIN QUERY PLAN（N=100,000 库）：');
  for (const [name, lines] of Object.entries(r.plans)) {
    console.log(`  • ${name}`);
    for (const l of lines) console.log(`      ${l}`);
  }
  console.log('');
  if (r.passed) {
    console.log('✅ 基准断言通过：领取计量条目在 0→100,000 积压间完全不变，热路径全部为索引 SEARCH。');
  } else {
    console.log('❌ 基准断言失败：');
    for (const f of r.failures) console.log(`   - ${f}`);
  }
  console.log(`   基准数据库目录：${r.dir}`);
}

if (require.main === module) {
  runBenchmark({ rounds: Number(process.env.BENCH_ROUNDS ?? 300) })
    .then((r) => {
      process.exit(r.passed ? 0 : 1);
    })
    .catch((e) => {
      console.error(e);
      process.exit(1);
    });
}
