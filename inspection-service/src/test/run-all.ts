/**
 * 一条命令的全量验证：并发发布统一顺序、注册生效边界、同设备队头阻塞 / 其他设备放行、
 * 首次领取截止 vs 已领取重投、租期与令牌、断线（不 ack 退出）重投、
 * kill -9 异常退出后按原状态与绝对时间边界恢复、手动清理计数，最后跑 0→100k 基准。
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

  // ------------------------------------------------------------ 8. 基准
  section('8. 领取开销基准（0 / 1k / 10k / 100k，含计量与查询计划断言）');
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
