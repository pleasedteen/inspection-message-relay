/**
 * 一条命令的全量验证：并发发布统一顺序、注册生效边界、同设备队头阻塞 / 其他设备放行、
 * 首次领取截止 vs 已领取重投、租期与令牌、断线（不 ack 退出）重投、
 * kill -9 异常退出后按原状态与绝对时间边界恢复、手动清理计数、
 * 长等领取（事件唤醒 / 不轮询 / 定向隔离 / 尝试数恒定 / 断连撤销 / 重启重等），
 * 最后跑 0→100k 基准。
 *
 * 运行：npm test   （构建后 node --experimental-sqlite dist/test/run-all.js）
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawn, ChildProcessWithoutNullStreams } from 'node:child_process';
// stdio 固定为 ignore/pipe/pipe，运行时一定是带非空 stdout/stderr 的子进程
const spawnNode = (...args: Parameters<typeof spawn>): ChildProcessWithoutNullStreams =>
  spawn(...args) as unknown as ChildProcessWithoutNullStreams;
import { InspectionClient, ClientError } from '../client';
import { runBenchmark } from '../bench';

const DIST_DIR = path.resolve(__dirname, '..');
const MAIN_JS = path.join(DIST_DIR, 'main.js');
const PUBLISH_JS = path.join(DIST_DIR, 'fixtures', 'publish.js');
const SUBSCRIBE_JS = path.join(DIST_DIR, 'fixtures', 'subscribe.js');

const startedAt = Date.now();
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'insp-e2e-'));
const dbFile = path.join(tmp, 'e2e.db');
const portFile = path.join(tmp, 'port.json');

let pass = 0;
const failures: string[] = [];
let serverLog = '';

function section(title: string): void {
  console.log(`\n\x1b[1m── ${title} ──\x1b[0m`);
}
async function check(name: string, fn: () => Promise<void> | void): Promise<void> {
  try {
    await fn();
    pass++;
    console.log(`  \x1b[32m✓\x1b[0m ${name}`);
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    failures.push(`${name}: ${msg}`);
    console.log(`  \x1b[31m✗ ${name}\x1b[0m\n      ${msg.split('\n').join('\n      ')}`);
  }
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
async function waitUntil(pred: () => boolean | Promise<boolean>, timeoutMs: number, stepMs = 40) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await pred()) return true;
    await sleep(stepMs);
  }
  return false;
}
async function mustWait(
  pred: () => boolean | Promise<boolean>,
  timeoutMs: number,
  what: string,
): Promise<void> {
  const ok = await waitUntil(pred, timeoutMs);
  assert.ok(ok, `等待超时: ${what}`);
}

function startServer(): Promise<{ child: ChildProcessWithoutNullStreams; url: string }> {
  try {
    fs.rmSync(portFile, { force: true });
  } catch {
    /* ignore */
  }
  serverLog = '';
  const child = spawnNode(
    process.execPath,
    [...process.execArgv, MAIN_JS, '--port', '0', '--db', dbFile, '--port-file', portFile],
    { stdio: ['ignore', 'pipe', 'pipe'] },
  );
  child.stdout.on('data', (d) => (serverLog += d.toString()));
  child.stderr.on('data', (d) => (serverLog += d.toString()));

  return new Promise((resolve, reject) => {
    const exitTimer = setInterval(() => {
      if (child.exitCode !== null) {
        clearInterval(exitTimer);
        reject(new Error(`服务进程提前退出 code=${child.exitCode}\n${serverLog.slice(-2000)}`));
      }
    }, 100);
    const ready = async () => {
      const ok = await waitUntil(() => fs.existsSync(portFile), 8000);
      if (!ok) {
        clearInterval(exitTimer);
        reject(new Error(`等待 port-file 超时\n${serverLog.slice(-2000)}`));
        return;
      }
      const info = JSON.parse(fs.readFileSync(portFile, 'utf8'));
      const client = new InspectionClient(info.url);
      try {
        await client.waitHealthy(8000);
        clearInterval(exitTimer);
        resolve({ child, url: info.url });
      } catch (e) {
        clearInterval(exitTimer);
        reject(e);
      }
    };
    void ready();
  });
}

async function kill9(child: ChildProcessWithoutNullStreams): Promise<void> {
  await new Promise<void>((resolve) => {
    child.once('exit', () => resolve());
    child.kill('SIGKILL');
  });
}

interface CmdResult {
  code: number;
  stdout: string;
  stderr: string;
}
function runNode(script: string, args: string[]): Promise<CmdResult> {
  return new Promise((resolve) => {
    const child = spawnNode(process.execPath, [...process.execArgv, script, ...args], {
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (d) => (stdout += d.toString()));
    child.stderr.on('data', (d) => (stderr += d.toString()));
    child.on('exit', (code) => resolve({ code: code ?? -1, stdout, stderr }));
  });
}
function markers(stdout: string, marker: string): any[] {
  return stdout
    .split('\n')
    .filter((l) => l.startsWith(marker))
    .map((l) => {
      try {
        return JSON.parse(l.slice(marker.length));
      } catch {
        return null;
      }
    })
    .filter(Boolean);
}
/** 流式拉起一个夹具进程：stdout 累积可读，进程可中途 SIGKILL（长等断连用）。 */
function spawnFixture(script: string, args: string[]) {
  const child = spawnNode(process.execPath, [...process.execArgv, script, ...args], {
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let stdout = '';
  let stderr = '';
  let exited = false;
  child.on('exit', () => (exited = true));
  child.stdout.on('data', (d) => (stdout += d.toString()));
  child.stderr.on('data', (d) => (stderr += d.toString()));
  return { child, hasExited: () => exited, getStdout: () => stdout, getStderr: () => stderr };
}
async function expectClientError(p: Promise<unknown>, code: string): Promise<void> {
  try {
    await p;
  } catch (e) {
    assert.ok(e instanceof ClientError, `应为 ClientError，实际 ${(e as Error)?.message}`);
    assert.equal((e as ClientError).code, code);
    return;
  }
  throw new Error(`期望失败 ${code}，但调用成功`);
}

async function main(): Promise<void> {
  console.log('════════════════════════════════════════════════════════════');
  console.log(' 机房巡检消息服务 · 全量验证');
  console.log(` Node ${process.version} · tmp=${tmp}`);
  console.log('════════════════════════════════════════════════════════════');

  let server = await startServer();
  let client = new InspectionClient(server.url);

  // ------------------------------------------------------------ 1. 注册生效边界
  section('1. 订阅注册生效边界（注册前的消息不收）');
  const C: any = {};

  await check('健康检查', async () => {
    const h = await client.health();
    assert.ok(h.ok);
  });

  await check('grp-a 注册后能收到新消息', async () => {
    C.a = await client.register('grp-a');
    C.afterA = await client.publish({ device_id: 'dev-reg-a', first_claim_ttl_ms: 60_000 });
    const r = await client.claim(C.a.id, { device_id: 'dev-reg-a', lease_ms: 5_000 });
    assert.equal(r.claim?.id, C.afterA.id);
    await client.ack(C.a.id, r.claim!.id, r.claim!.lease_token);
  });

  await check('后注册的 grp-late 收不到注册前发布的消息', async () => {
    C.beforeLate = await client.publish({ device_id: 'dev-late', first_claim_ttl_ms: 60_000 });
    C.late = await client.register('grp-late');
    assert.ok(C.late.effective_at >= C.beforeLate.published_at - 1);
    C.afterLate = await client.publish({ device_id: 'dev-late', first_claim_ttl_ms: 60_000 });
    const r = await client.claim(C.late.id, { device_id: 'dev-late', lease_ms: 5_000 });
    assert.equal(r.claim?.id, C.afterLate.id, '应只领到注册后发布的那一条');
  });

  // ------------------------------------------------------------ 2. 多进程并发发布 + 统一顺序
  section('2. 三个调度进程并发发布同一设备 → 统一连续顺序');
  await check('3 个发布夹具进程 × 各 20 条 / 进程内并发 6，seq 为 1..60', async () => {
    C.procSub = await client.register('grp-proc-order');
    const common = ['--url', server.url, '--device', 'dev-proc', '--count', '20',
      '--concurrency', '6', '--ttl-ms', '120000', '--quiet', 'true'];
    const runs = await Promise.all([
      runNode(PUBLISH_JS, [...common, '--scheduler-id', 'sched-1']),
      runNode(PUBLISH_JS, [...common, '--scheduler-id', 'sched-2']),
      runNode(PUBLISH_JS, [...common, '--scheduler-id', 'sched-3']),
    ]);
    for (const r of runs) {
      assert.equal(r.code, 0, `发布夹具退出码 ${r.code}\n${r.stderr.slice(-800)}`);
    }
    const allSeqs = runs.flatMap((r) => markers(r.stdout, '@@SUMMARY@@')[0]?.seqs ?? []);
    assert.equal(allSeqs.length, 60);
    assert.deepEqual([...allSeqs].sort((a, b) => a - b), Array.from({ length: 60 }, (_, i) => i + 1));
    C.procRuns = runs.length;
  });

  await check('订阅按统一顺序领到 1..60（领取并逐条确认）', async () => {
    for (let expected = 1; expected <= 60; expected++) {
      const r = await client.claim(C.procSub.id, { device_id: 'dev-proc', lease_ms: 30_000 });
      assert.ok(r.claim, `第 ${expected} 条领取为空`);
      assert.equal(r.claim!.seq, expected, `顺序错乱 at ${expected}`);
      await client.ack(C.procSub.id, r.claim!.id, r.claim!.lease_token);
    }
  });

  // ------------------------------------------------------------ 3. 队头阻塞 / 其他设备
  section('3. 同设备队头阻塞，其他设备继续');
  await check('未确认的 h1 阻塞 h2，但 claim-any 放行另一设备 o1；ack h1 后得到 h2', async () => {
    C.hol = await client.register('grp-hol');
    C.h1 = await client.publish({ device_id: 'dev-hol', first_claim_ttl_ms: 120_000, payload: 'h1' });
    C.h2 = await client.publish({ device_id: 'dev-hol', first_claim_ttl_ms: 120_000, payload: 'h2' });
    C.o1 = await client.publish({ device_id: 'dev-other', first_claim_ttl_ms: 120_000, payload: 'o1' });

    const r1 = await client.claim(C.hol.id, { lease_ms: 30_000 });
    assert.equal(r1.claim?.id, C.h1.id);
    C.h1token = r1.claim!.lease_token;

    // 指定 dev-hol：队头租约有效，领不到 h2
    const rDev = await client.claim(C.hol.id, { device_id: 'dev-hol', lease_ms: 30_000 });
    assert.equal(rDev.claim, null);
    assert.equal(rDev.blocked?.reason, 'LEASE_ACTIVE');
    assert.equal(rDev.blocked?.message_id, C.h1.id);

    // claim-any：绕过阻塞设备，领到其他设备的 o1
    const r2 = await client.claim(C.hol.id, { lease_ms: 30_000 });
    assert.equal(r2.claim?.id, C.o1.id, '其他设备不应被阻塞');

    // h2 仍不可领
    const rDev2 = await client.claim(C.hol.id, { device_id: 'dev-hol' });
    assert.equal(rDev2.claim, null);

    await client.ack(C.hol.id, C.h1.id, C.h1token);
    const r3 = await client.claim(C.hol.id, { lease_ms: 30_000 });
    assert.equal(r3.claim?.id, C.h2.id, 'h1 确认后 h2 应立即可领');
    await client.ack(C.hol.id, r3.claim!.id, r3.claim!.lease_token);
    await client.ack(C.hol.id, C.o1.id, r2.claim!.lease_token);
  });

  // ------------------------------------------------------------ 4. 首次截止 vs 已领取
  section('4. 首次领取截止：未领取者跳过，已领取未确认者保留');
  await check('截止后 grp-skip 回执变 skipped；grp-keep 仍 claimed 且可确认', async () => {
    C.skip = await client.register('grp-skip');
    C.keep = await client.register('grp-keep');
    C.dl = await client.publish({ device_id: 'dev-dl', first_claim_ttl_ms: 450 });
    const k = await client.claim(C.keep.id, { device_id: 'dev-dl', lease_ms: 20_000 });
    assert.ok(k.claim);
    C.keepToken = k.claim!.lease_token;

    await sleep(600); // 越过首次领取截止
    const s = await client.claim(C.skip.id, { device_id: 'dev-dl' });
    assert.equal(s.claim, null);

    const detail = await client.messageReceipts(C.dl.id);
    const skipRcpt = detail.receipts.find((r: any) => r.sub_id === C.skip.id);
    const keepRcpt = detail.receipts.find((r: any) => r.sub_id === C.keep.id);
    assert.equal(skipRcpt.state, 'skipped');
    assert.equal(keepRcpt.state, 'claimed');
    await client.ack(C.keep.id, C.dl.id, C.keepToken);
  });

  // ------------------------------------------------------------ 5. 断线重投 + 令牌/租期
  section('5. 订阅者领到后断线（夹具不 ack 退出）→ 租期后重投，旧令牌失效');
  await check('drop 夹具首次领取成功并退出', async () => {
    C.redel = await client.register('grp-redel');
    C.rd = await client.publish({ device_id: 'dev-rd', first_claim_ttl_ms: 120_000 });
    const r = await runNode(SUBSCRIBE_JS, [
      '--url', server.url, '--name', 'grp-redel', '--mode', 'drop',
      '--lease-ms', '700', '--max', '1',
    ]);
    assert.equal(r.code, 0, r.stderr.slice(-600));
    const claims = markers(r.stdout, '@@CLAIM@@');
    assert.equal(claims.length, 1, r.stdout);
    C.c1 = claims[0];
    assert.equal(C.c1.delivery_count, 1);
  });

  await check('错误令牌确认被拒 INVALID_TOKEN', async () => {
    await expectClientError(client.ack(C.redel.id, C.rd.id, 'bogus-token'), 'INVALID_TOKEN');
  });

  await check('租期过后重新领取：redelivery=true, delivery_count=2，旧令牌失效', async () => {
    await sleep(850);
    const r2 = await client.claim(C.redel.id, { device_id: 'dev-rd', lease_ms: 700 });
    assert.ok(r2.claim);
    assert.equal(r2.claim!.id, C.rd.id);
    assert.equal(r2.claim!.redelivery, true);
    assert.equal(r2.claim!.delivery_count, 2);
    C.t2 = r2.claim!.lease_token;
    await expectClientError(client.ack(C.redel.id, C.rd.id, C.c1.token), 'INVALID_TOKEN');
  });

  await check('租期一过即使令牌仍匹配也 LEASE_EXPIRED；再次重领后可用新令牌确认', async () => {
    await sleep(780);
    await expectClientError(client.ack(C.redel.id, C.rd.id, C.t2), 'LEASE_EXPIRED');
    const r3 = await client.claim(C.redel.id, { device_id: 'dev-rd', lease_ms: 30_000 });
    assert.equal(r3.claim!.delivery_count, 3);
    await client.ack(C.redel.id, C.rd.id, r3.claim!.lease_token);
  });

  // ------------------------------------------------------------ 6. kill -9 崩溃恢复
  section('6. kill -9 异常退出后重启：状态与绝对时间边界原样恢复');
  await check('崩溃前制造 注册/待领/已确认/已领取未确认/带截止 五类状态', async () => {
    C.crash = await client.register('grp-crash');

    C.cHeld = await client.publish({ device_id: 'dev-ca', first_claim_ttl_ms: 600_000 });
    const h = await client.claim(C.crash.id, { device_id: 'dev-ca', lease_ms: 1_600 });
    C.heldToken = h.claim!.lease_token;
    C.heldExpires = h.claim!.lease_expires_at;

    C.cDone = await client.publish({ device_id: 'dev-cb', first_claim_ttl_ms: 600_000 });
    const d = await client.claim(C.crash.id, { device_id: 'dev-cb' });
    await client.ack(C.crash.id, C.cDone.id, d.claim!.lease_token);

    C.cPend = await client.publish({ device_id: 'dev-cc', first_claim_ttl_ms: 600_000 });
    C.cDl = await client.publish({ device_id: 'dev-cd', first_claim_ttl_ms: 1_200 });
    C.dlDeadline = C.cDl.first_deadline_at;
    assert.ok(C.heldExpires > Date.now());
  });

  await check('SIGKILL 服务进程并以同一 DB 文件重启', async () => {
    await kill9(server.child);
    server = await startServer();
    client = new InspectionClient(server.url);
    await client.waitHealthy();
  });

  await check('注册与原始 effective_at 恢复', async () => {
    const res = await fetch(`${server.url}/subscriptions/${C.crash.id}`);
    assert.equal(res.status, 200);
    const body = (await res.json()) as { name: string; effective_at: number };
    assert.equal(body.name, 'grp-crash');
    assert.equal(body.effective_at, C.crash.effective_at);
  });

  await check('已领取未确认：state/claim_count/lease_expires_at 毫秒边界完全一致', async () => {
    const detail = await client.messageReceipts(C.cHeld.id);
    const r = detail.receipts.find((x: any) => x.sub_id === C.crash.id);
    assert.equal(r.state, 'claimed');
    assert.equal(r.claim_count, 1);
    assert.equal(r.lease_expires_at, C.heldExpires, '租期绝对时间边界必须原样恢复');
  });

  await check('已确认消息不重投；未领取消息仍可首次领取(delivery_count=1)', async () => {
    const done = await client.claim(C.crash.id, { device_id: 'dev-cb' });
    assert.equal(done.claim, null);
    const pend = await client.claim(C.crash.id, { device_id: 'dev-cc', lease_ms: 30_000 });
    assert.equal(pend.claim?.id, C.cPend.id);
    assert.equal(pend.claim?.delivery_count, 1);
    C.pendToken = pend.claim!.lease_token;
    await client.ack(C.crash.id, C.cPend.id, C.pendToken);
  });

  await check('越过崩溃前的租期边界后可重投，新领取可确认', async () => {
    await waitUntil(() => Date.now() >= C.heldExpires + 40, 4_000);
    const r = await client.claim(C.crash.id, { device_id: 'dev-ca', lease_ms: 30_000 });
    assert.equal(r.claim?.id, C.cHeld.id);
    assert.equal(r.claim?.redelivery, true);
    assert.equal(r.claim?.delivery_count, 2);
    await client.ack(C.crash.id, C.cHeld.id, r.claim!.lease_token);
  });

  await check('首次领取截止的绝对时间恢复；越界后领取为空且回执 skipped', async () => {
    assert.equal(C.dlDeadline, C.cDl.first_deadline_at);
    const before = await client.messageReceipts(C.cDl.id);
    const rr = before.receipts.find((x: any) => x.sub_id === C.crash.id);
    assert.equal(rr.first_deadline_at, C.dlDeadline);
    await waitUntil(() => Date.now() >= C.dlDeadline + 40, 4_000);
    const r = await client.claim(C.crash.id, { device_id: 'dev-cd' });
    assert.equal(r.claim, null);
    const after = await client.messageReceipts(C.cDl.id);
    const rr2 = after.receipts.find((x: any) => x.sub_id === C.crash.id);
    assert.equal(rr2.state, 'skipped');
  });

  // ------------------------------------------------------------ 7. 手动清理
  section('7. 手动清理：按判定时刻回收无保护正文，返回实际条数，再清为 0');
  await check('claimed 与 pending-alive 保留；过期未领 / 已确认且全员过截止 被回收', async () => {
    C.gc = await client.register('grp-gc');
    C.gcClaimed = await client.publish({ device_id: 'dev-g1', first_claim_ttl_ms: 600_000 });
    const g = await client.claim(C.gc.id, { device_id: 'dev-g1', lease_ms: 60_000 });
    C.gcClaimToken = g.claim!.lease_token; // 故意不 ack

    C.gcPending = await client.publish({ device_id: 'dev-g2', first_claim_ttl_ms: 600_000 });
    C.gcExpired = await client.publish({ device_id: 'dev-g3', first_claim_ttl_ms: 250 });
    C.gcAcked = await client.publish({ device_id: 'dev-g4', first_claim_ttl_ms: 250 });
    const a = await client.claim(C.gc.id, { device_id: 'dev-g4', lease_ms: 10_000 });
    await client.ack(C.gc.id, C.gcAcked.id, a.claim!.lease_token);

    await sleep(400);
    C.gcResult = await client.cleanup();
    assert.ok(C.gcResult.reclaimed_bodies >= 2, `应至少回收 2 条，实际 ${C.gcResult.reclaimed_bodies}`);

    const dExpired = await client.messageReceipts(C.gcExpired.id);
    const dAcked = await client.messageReceipts(C.gcAcked.id);
    const dClaimed = await client.messageReceipts(C.gcClaimed.id);
    const dPending = await client.messageReceipts(C.gcPending.id);
    assert.equal(dExpired.message.body_reclaimed, true);
    assert.equal(dAcked.message.body_reclaimed, true);
    assert.equal(dClaimed.message.body_reclaimed, false, '已领取未确认必须保留正文');
    assert.equal(dPending.message.body_reclaimed, false, '仍可首次领取必须保留正文');
  });

  await check('紧接着第二次清理实际回收 0 条', async () => {
    const again = await client.cleanup();
    assert.equal(again.reclaimed_bodies, 0);
  });

  // ------------------------------------------------------------ 8. 长等领取（长轮询）
  section('8. 长等领取：立即语义不变 / 事件唤醒 / 不轮询 / 定向隔离 / 尝试数恒定 / 断连撤销 / 重启重等');

  const waitersInfo = () => client.waiters();
  const statDelta = (before: Record<string, number>, after: Record<string, number>) => {
    const d: Record<string, number> = {};
    for (const k of Object.keys(after)) d[k] = after[k] - (before[k] ?? 0);
    return d;
  };
  interface WaitOutcome {
    status: 'claim' | 'empty' | 'error';
    waited_ms: number;
    claim?: any;
    error?: unknown;
  }
  const lp = (
    sub: string,
    opts: { lease_ms?: number; device_id?: string; wait_ms: number },
    cli?: InspectionClient,
  ): Promise<WaitOutcome> =>
    (cli ?? client)
      .claim(sub, opts)
      .then(
        (r) => ({
          status: r.claim ? ('claim' as const) : ('empty' as const),
          waited_ms: r.waited_ms ?? -1,
          claim: r.claim ?? undefined,
        }),
        (error: unknown) => ({ status: 'error' as const, waited_ms: -1, error }),
      );
  const fmtOuts = (outs: WaitOutcome[]) =>
    outs.map((o) => `${o.status}${o.waited_ms >= 0 ? `@${o.waited_ms}ms` : ''}`).join(' | ');

  await check('8.1 立即领取语义照旧；长等有匹配队头立即领（waited_ms≈0）', async () => {
    const g = await client.register('grp-lp1');
    const t0 = Date.now();
    const imm = await client.claim(g.id, { device_id: 'dev-lp1' });
    const immMs = Date.now() - t0;
    assert.equal(imm.claim, null);
    assert.ok(immMs < 1500, `立即领取应快速返回，实际 ${immMs}ms`);
    assert.equal(imm.waited_ms, undefined, '未带 wait_ms 的响应不应携带 waited_ms');

    const m = await client.publish({ device_id: 'dev-lp1', first_claim_ttl_ms: 60_000, payload: 'p1' });
    const r = await client.claim(g.id, { device_id: 'dev-lp1', lease_ms: 30_000, wait_ms: 5_000 });
    assert.ok(r.claim, '有匹配队头应立即领到');
    assert.equal(r.claim!.id, m.id);
    assert.ok((r.waited_ms ?? 9999) <= 500, `应立即返回，实际 waited_ms=${r.waited_ms}`);
    await client.ack(g.id, m.id, r.claim!.lease_token);
    console.log(`    等待结果: 立即空返 ${immMs}ms · 长等立即领取 waited_ms=${r.waited_ms}`);
  });

  await check('8.2 空等不轮询数据库，超时返回空', async () => {
    const g = await client.register('grp-lp2');
    const before = (await waitersInfo()).stats;
    const t0 = Date.now();
    const out = await lp(g.id, { device_id: 'dev-lp2-none', wait_ms: 900 });
    const wall = Date.now() - t0;
    const after = (await waitersInfo()).stats;
    const d = statDelta(before, after);
    assert.equal(out.status, 'empty', '无活时应超时返回空');
    assert.ok(wall >= 850 && wall < 4000, `应在超时附近返回，实际 ${wall}ms`);
    assert.ok(out.waited_ms >= 850, `waited_ms=${out.waited_ms}`);
    assert.equal(d.wake_attempts, 0, '空等期间不得有任何事件驱动领取尝试');
    assert.equal(d.dispatches, 0, '空等期间不得触发任何调度（即不轮询数据库）');
    assert.equal(d.initial_attempts, 1, '仅登记时的一次先试领');
    console.log(
      `    等待结果: ${fmtOuts([out])} · 实际领取尝试数: 初始 ${d.initial_attempts} · 唤醒 ${d.wake_attempts} · 调度 ${d.dispatches}`,
    );
  });

  await check('8.3 定向与不限等待并存：发布精确唤醒，无关设备不触发定向复查', async () => {
    const g = await client.register('grp-lp3');
    const w1 = lp(g.id, { device_id: 'dev-lp3a', lease_ms: 30_000, wait_ms: 2_500 });
    await mustWait(
      async () => (await waitersInfo()).waiters.some((w) => w.sub_id === g.id && w.device_id === 'dev-lp3a'),
      3_000, 'W1(定向a) 登记',
    );
    const w2 = lp(g.id, { device_id: 'dev-lp3b', lease_ms: 30_000, wait_ms: 2_500 });
    await mustWait(
      async () => (await waitersInfo()).waiters.some((w) => w.sub_id === g.id && w.device_id === 'dev-lp3b'),
      3_000, 'W2(定向b) 登记',
    );
    const w3 = lp(g.id, { lease_ms: 30_000, wait_ms: 2_500 });
    await mustWait(
      async () => (await waitersInfo()).waiters.filter((w) => w.sub_id === g.id).length === 3,
      3_000, 'W3(不限) 登记',
    );

    const s0 = (await waitersInfo()).stats;
    const mA = await client.publish({ device_id: 'dev-lp3a', first_claim_ttl_ms: 60_000, payload: 'A' });
    const o1 = await w1;
    assert.equal(o1.status, 'claim', '定向 dev-lp3a 的 W1 应被发布唤醒并领到');
    assert.equal(o1.claim.id, mA.id);

    const mC = await client.publish({ device_id: 'dev-lp3c', first_claim_ttl_ms: 60_000, payload: 'C' });
    const o3 = await w3;
    assert.equal(o3.status, 'claim', '不限设备的 W3 应被 dev-lp3c 发布唤醒并领到');
    assert.equal(o3.claim.id, mC.id);

    const o2 = await w2;
    assert.equal(o2.status, 'empty', '定向 dev-lp3b 的 W2 无发布，应超时返回空');
    assert.ok(o2.waited_ms >= 2_400, `W2 应等满超时，实际 ${o2.waited_ms}ms`);

    const s1 = (await waitersInfo()).stats;
    const d = statDelta(s0, s1);
    assert.equal(d.wake_attempts, 2, `两次发布各只唤醒一次领取尝试，实际 ${d.wake_attempts}`);
    for (const m of [mA, mC]) {
      const det = await client.messageReceipts(m.id);
      assert.equal(
        det.receipts.find((r: any) => r.sub_id === g.id).claim_count, 1, '只发一份有效租约',
      );
    }
    console.log(
      `    等待结果: W1(定向a)=${o1.status}@${o1.waited_ms}ms · W2(定向b)=${o2.status}@${o2.waited_ms}ms · W3(不限)=${o3.status}@${o3.waited_ms}ms`,
    );
    console.log(
      `    实际领取尝试数: 唤醒 ${d.wake_attempts}（2 次相关发布 × 1；dev-lp3b 定向等待者 0 次复查）· 初始 ${d.initial_attempts}`,
    );
  });

  await check('8.4 前项确认（ack 推进队头）唤醒等待者', async () => {
    const g = await client.register('grp-lp4');
    const m1 = await client.publish({ device_id: 'dev-lp4', first_claim_ttl_ms: 60_000, payload: 'm1' });
    const m2 = await client.publish({ device_id: 'dev-lp4', first_claim_ttl_ms: 60_000, payload: 'm2' });
    const c1 = await client.claim(g.id, { device_id: 'dev-lp4', lease_ms: 30_000 });
    assert.equal(c1.claim!.id, m1.id);

    const w = lp(g.id, { device_id: 'dev-lp4', lease_ms: 30_000, wait_ms: 3_000 });
    await mustWait(
      async () => (await waitersInfo()).waiters.some((x) => x.sub_id === g.id),
      3_000, '等待者登记',
    );
    await sleep(400);
    const s0 = (await waitersInfo()).stats;
    await client.ack(g.id, m1.id, c1.claim!.lease_token); // 推进队头 → 唤醒
    const o = await w;
    const s1 = (await waitersInfo()).stats;
    assert.equal(o.status, 'claim');
    assert.equal(o.claim.id, m2.id, 'ack 前项后应领到后项 m2');
    assert.equal(o.claim.delivery_count, 1);
    assert.ok(o.waited_ms < 2_000, `ack 后应快速唤醒，实际 ${o.waited_ms}ms`);
    const d = statDelta(s0, s1);
    assert.equal(d.wake_attempts, 1, `ack 唤醒应只做一次领取尝试，实际 ${d.wake_attempts}`);
    await client.ack(g.id, m2.id, o.claim.lease_token);
    console.log(`    等待结果: claim@${o.waited_ms}ms（ack 唤醒）· 实际领取尝试数: 唤醒 ${d.wake_attempts}`);
  });

  await check('8.5 租约到期使队头可领：按绝对到期时刻唤醒重领，旧令牌失效', async () => {
    const g = await client.register('grp-lp5');
    const m = await client.publish({ device_id: 'dev-lp5', first_claim_ttl_ms: 60_000 });
    const a = await client.claim(g.id, { device_id: 'dev-lp5', lease_ms: 900 });
    const tokenA = a.claim!.lease_token;
    const expiresA = a.claim!.lease_expires_at;

    const w = lp(g.id, { device_id: 'dev-lp5', lease_ms: 30_000, wait_ms: 4_000 });
    await mustWait(
      async () => (await waitersInfo()).waiters.some((x) => x.sub_id === g.id),
      3_000, '等待者登记',
    );
    const o = await w;
    const tClaim = Date.now();
    assert.equal(o.status, 'claim', '租约到期后等待者应被唤醒重领');
    assert.equal(o.claim.id, m.id);
    assert.equal(o.claim.redelivery, true);
    assert.equal(o.claim.delivery_count, 2);
    assert.ok(tClaim >= expiresA - 120, `不得早于原租约到期，实际偏差 ${tClaim - expiresA}ms`);
    assert.ok(tClaim <= expiresA + 1_500, `到期后应很快唤醒，实际偏差 +${tClaim - expiresA}ms`);
    await expectClientError(client.ack(g.id, m.id, tokenA), 'INVALID_TOKEN');
    await client.ack(g.id, m.id, o.claim.lease_token);
    console.log(`    等待结果: claim@${o.waited_ms}ms（租约到期唤醒，相对到期时刻偏差 ${tClaim - expiresA}ms）`);
  });

  await check('8.6 客户端断连：服务端撤销等待，退出后不再参与领取', async () => {
    const fx = spawnFixture(SUBSCRIBE_JS, [
      '--url', server.url, '--name', 'grp-lp6', '--wait-ms', '20000', '--max', '1', '--lease-ms', '5000',
    ]);
    await mustWait(() => fx.getStdout().includes('@@READY@@'), 5_000, '夹具 READY');
    const subId = markers(fx.getStdout(), '@@READY@@')[0].id;
    await mustWait(
      async () => (await waitersInfo()).waiters.some((w) => w.sub_id === subId),
      5_000, '长等登记',
    );
    const s0 = (await waitersInfo()).stats;
    fx.child.kill('SIGKILL');
    await mustWait(() => fx.hasExited(), 3_000, '夹具退出');
    await mustWait(
      async () => !(await waitersInfo()).waiters.some((w) => w.sub_id === subId),
      5_000, '断连撤销等待',
    );
    const s1 = (await waitersInfo()).stats;
    assert.equal(s1.cancels - s0.cancels, 1, '应记录一次断连撤销');

    // 发布后不得为已退出的等待者做任何领取尝试，队头保持 pending、不发租约
    const m = await client.publish({ device_id: 'dev-lp6', first_claim_ttl_ms: 60_000 });
    await sleep(300);
    const s2 = (await waitersInfo()).stats;
    assert.equal(s2.wake_attempts - s1.wake_attempts, 0, '退出后不再参与领取（0 次唤醒尝试）');
    const det = await client.messageReceipts(m.id);
    const rcpt = det.receipts.find((r: any) => r.sub_id === subId);
    assert.equal(rcpt.state, 'pending');
    assert.equal(rcpt.claim_count, 0, '不得为已退出等待者发放租约');
    const imm = await client.claim(subId, { device_id: 'dev-lp6' });
    assert.equal(imm.claim!.delivery_count, 1, '消息应原样留给后续领取者');
    await client.ack(subId, m.id, imm.claim!.lease_token);
    console.log(
      `    等待结果: 断连撤销 cancels+${s1.cancels - s0.cancels} · 发布后唤醒尝试 +${s2.wake_attempts - s1.wake_attempts} · 队头仍 pending(claim_count=0)`,
    );
  });

  await check('8.7 单个可领队头：领取尝试数不随等待人数增长，只发一份有效租约', async () => {
    // 对照组：1 个等待者
    const g1 = await client.register('grp-lp7a');
    const w1 = lp(g1.id, { wait_ms: 2_200, lease_ms: 30_000 });
    await mustWait(
      async () => (await waitersInfo()).waiters.some((w) => w.sub_id === g1.id),
      3_000, '1 人等待登记',
    );
    const s0 = (await waitersInfo()).stats;
    await client.publish({ device_id: 'dev-lp7a', first_claim_ttl_ms: 60_000 });
    const o1 = await w1;
    const s1 = (await waitersInfo()).stats;
    assert.equal(o1.status, 'claim');
    const d1 = statDelta(s0, s1);

    // 实验组：6 个等待者（3 定向 + 3 不限）等同一台设备
    const g2 = await client.register('grp-lp7b');
    const ws: Promise<WaitOutcome>[] = [];
    for (let i = 0; i < 3; i++) {
      ws.push(lp(g2.id, { device_id: 'dev-lp7b', lease_ms: 30_000, wait_ms: 2_200 }));
      ws.push(lp(g2.id, { lease_ms: 30_000, wait_ms: 2_200 }));
    }
    await mustWait(
      async () => (await waitersInfo()).waiters.filter((w) => w.sub_id === g2.id).length === 6,
      5_000, '6 人等待登记',
    );
    const s2 = (await waitersInfo()).stats;
    const m2 = await client.publish({ device_id: 'dev-lp7b', first_claim_ttl_ms: 60_000 });
    const outs = await Promise.all(ws);
    const s3 = (await waitersInfo()).stats;
    const d2 = statDelta(s2, s3);

    const claimed = outs.filter((o) => o.status === 'claim');
    const empties = outs.filter((o) => o.status === 'empty');
    assert.equal(claimed.length, 1, '6 个等待者中恰有 1 人领到');
    assert.equal(empties.length, 5, '其余 5 人应超时返回空');
    assert.equal(d2.wake_attempts, 1, `6 人等待只应产生 1 次唤醒领取尝试，实际 ${d2.wake_attempts}`);
    assert.equal(d1.wake_attempts, 1, `1 人等待应产生 1 次唤醒领取尝试，实际 ${d1.wake_attempts}`);
    assert.equal(d2.wake_attempts, d1.wake_attempts, '领取尝试数不随等待人数增长');

    const det = await client.messageReceipts(m2.id);
    assert.equal(
      det.receipts.find((r: any) => r.sub_id === g2.id).claim_count, 1, '只发一份有效租约',
    );
    console.log(`    等待结果: 1人组=${fmtOuts([o1])} · 6人组=${fmtOuts(outs)}`);
    console.log(
      `    实际领取尝试数: 1人等待 唤醒${d1.wake_attempts} 次 · 6人等待 唤醒${d2.wake_attempts} 次（相等=不随人数增长）· 租约 claim_count=1`,
    );
  });

  await check('8.8 kill -9 后原库重启：客户端重新等待仍按原租约到期重领，旧令牌不能确认新租约', async () => {
    const g = await client.register('grp-lp8');
    const m = await client.publish({ device_id: 'dev-lp8', first_claim_ttl_ms: 600_000 });
    const a = await client.claim(g.id, { device_id: 'dev-lp8', lease_ms: 6_000 });
    const tokenA = a.claim!.lease_token;
    const expiresA = a.claim!.lease_expires_at; // 原租约绝对到期时刻（持久化在库中）

    let gateOpen = false;
    const waitOnce = async (): Promise<WaitOutcome> => {
      try {
        const r = await new InspectionClient(server.url).claim(g.id, {
          device_id: 'dev-lp8', lease_ms: 30_000, wait_ms: 30_000,
        });
        return { status: r.claim ? 'claim' : 'empty', waited_ms: r.waited_ms ?? -1, claim: r.claim ?? undefined };
      } catch (e) {
        return { status: 'error', waited_ms: -1, error: e };
      }
    };
    const p = (async () => {
      const first = await waitOnce(); // 会被 kill -9 打断
      assert.equal(first.status, 'error', 'kill 应打断进行中的长等');
      await waitUntil(() => gateOpen, 20_000);
      return waitOnce(); // 用原库重启后，客户端重新等待
    })();

    await mustWait(
      async () => (await waitersInfo()).waiters.some((w) => w.sub_id === g.id),
      5_000, '长等登记',
    );
    const tKill = Date.now();
    await kill9(server.child);
    server = await startServer(); // 同一 DB 文件重启
    client = new InspectionClient(server.url);
    await client.waitHealthy();
    const restartMs = Date.now() - tKill;
    assert.equal((await waitersInfo()).waiters.length, 0, '内存等待表不随重启保留');
    gateOpen = true;
    await mustWait(
      async () => (await waitersInfo()).waiters.some((w) => w.sub_id === g.id),
      8_000, '客户端重新等待',
    );

    const o = await p;
    const tClaim = Date.now();
    assert.equal(o.status, 'claim', `应领到，实际 ${o.status}: ${(o.error as Error)?.message ?? ''}`);
    assert.equal(o.claim.id, m.id);
    assert.equal(o.claim.redelivery, true);
    assert.equal(o.claim.delivery_count, 2);
    assert.ok(tClaim >= expiresA - 150, `不得早于原租约到期，实际偏差 ${tClaim - expiresA}ms`);
    assert.ok(
      tClaim <= expiresA + Math.max(800, restartMs * 0.5),
      `应按原租约到期时刻重领（而非重启后重算 ${restartMs}ms），实际偏差 +${tClaim - expiresA}ms`,
    );
    await expectClientError(client.ack(g.id, m.id, tokenA), 'INVALID_TOKEN');
    await client.ack(g.id, m.id, o.claim.lease_token);
    console.log(
      `    等待结果: 重等后 claim@${o.waited_ms}ms · 相对原租约到期偏差 ${tClaim - expiresA}ms（重启耗时 ${restartMs}ms）· 旧令牌 INVALID_TOKEN`,
    );
  });

  // ------------------------------------------------------------ 9. 基准
  section('9. 领取开销基准（0 / 1k / 10k / 100k，含计量与查询计划断言）');
  let benchOk = false;
  try {
    const report = await runBenchmark({ scales: [0, 1_000, 10_000, 100_000], rounds: 150 });
    benchOk = report.passed;
    if (!benchOk) failures.push(...report.failures.map((f) => `bench: ${f}`));
    assert.ok(report.passed, report.failures.join('; '));
    pass++;
  } catch (e) {
    failures.push(`基准运行失败: ${(e as Error).message}`);
    console.log('  ✗ 基准运行失败:', (e as Error).message);
  }

  // ------------------------------------------------------------ 汇总
  const elapsed = ((Date.now() - startedAt) / 1000).toFixed(1);
  console.log('\n════════════════════════════════════════════════════════════');
  if (failures.length === 0) {
    console.log(`\x1b[32m✅ 全部通过：${pass} 个检查组 · 总耗时 ${elapsed}s（要求 < 80s）\x1b[0m`);
  } else {
    console.log(`\x1b[31m❌ ${failures.length} 个失败：\x1b[0m`);
    for (const f of failures) console.log(`   - ${f}`);
    console.log('\n--- 服务端日志尾部 ---');
    console.log(serverLog.slice(-3000));
  }
  console.log(`临时目录（含 DB 与基准库）：${tmp}`);
  console.log('════════════════════════════════════════════════════════════');

  try {
    server.child.kill('SIGTERM');
  } catch {
    /* ignore */
  }
  process.exit(failures.length === 0 && Number(elapsed) < 80 ? 0 : 1);
}

main().catch((e) => {
  console.error('运行器致命错误:', e);
  console.log(serverLog.slice(-3000));
  process.exit(1);
});
