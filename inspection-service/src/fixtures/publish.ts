#!/usr/bin/env node
/**
 * 本地发布夹具：模拟一个调度进程，以指定并发向同一/多个设备发布巡检请求。
 *
 * 示例：
 *   node dist/fixtures/publish.js --url http://127.0.0.1:8787 \
 *        --scheduler-id sched-A --device pump-01 --count 20 --concurrency 8
 *
 * 多个该进程同时跑、打同一 device_id，即可验证“同设备并发发布的统一顺序”。
 * 末行输出 @@SUMMARY@@<json>，方便自动化编排解析。
 */
import { InspectionClient } from '../client';

interface PubArgs {
  url: string;
  device: string;
  count: number;
  concurrency: number;
  ttlMs: number;
  schedulerId: string;
  quiet: boolean;
  devices?: number;
  stableIdem: boolean;
}

function parse(): PubArgs {
  const a: Record<string, string> = {};
  for (let i = 2; i < process.argv.length; i++) {
    const k = process.argv[i];
    if (!k.startsWith('--')) continue;
    const key = k.slice(2);
    const next = process.argv[i + 1];
    if (next === undefined || next.startsWith('--')) {
      a[key] = 'true';
    } else {
      a[key] = next;
      i++;
    }
  }
  return {
    url: a.url ?? a.base ?? 'http://127.0.0.1:8787',
    device: a.device ?? 'pump-01',
    count: Number(a.count ?? 10),
    concurrency: Math.max(1, Number(a.concurrency ?? 4)),
    ttlMs: Number(a['ttl-ms'] ?? 60_000),
    schedulerId: a['scheduler-id'] ?? `sched-${process.pid}`,
    quiet: a.quiet === 'true' || a.quiet === '1',
    devices: a.devices ? Number(a.devices) : undefined,
    // 默认每次运行带进程级 nonce，避免用固定 scheduler-id 重跑时被幂等去重；
    // 需要演示幂等时传 --stable-idem。
    stableIdem: 'stable-idem' in a,
  };
}

async function main(): Promise<void> {
  const args = parse();
  const client = new InspectionClient(args.url);
  await client.waitHealthy();

  const results: { index: number; device_id: string; id: string; seq: number }[] = [];
  let cursor = 0;
  const started = Date.now();
  const runNonce = Math.random().toString(36).slice(2, 8);

  async function worker(workerId: number): Promise<void> {
    while (true) {
      const i = cursor++;
      if (i >= args.count) return;
      // --devices K：把消息轮询打到 device-0..K-1，用于演示其他设备不被队头阻塞
      const deviceId = args.devices
        ? `${args.device}-${i % args.devices}`
        : args.device;
      const msg = await client.publish({
        device_id: deviceId,
        first_claim_ttl_ms: args.ttlMs,
        idempotency_key: args.stableIdem
          ? `${args.schedulerId}-${i}`
          : `${args.schedulerId}-${runNonce}-${i}`,
        payload: {
          kind: 'inspection-request',
          scheduler: args.schedulerId,
          worker: workerId,
          index: i,
          issued_at: Date.now(),
          tasks: ['temperature', 'vibration', 'leak'],
        },
      });
      results.push({ index: i, device_id: deviceId, id: msg.id, seq: msg.seq });
      if (!args.quiet) {
        console.log(
          `[${args.schedulerId}] #${i} ${deviceId} seq=${msg.seq} id=${msg.id.slice(0, 8)}`,
        );
      }
    }
  }

  await Promise.all(Array.from({ length: args.concurrency }, (_, w) => worker(w)));
  results.sort((a, b) => a.index - b.index);
  const elapsedMs = Date.now() - started;
  const seqs = results
    .filter((r) => r.device_id === args.device)
    .map((r) => r.seq)
    .sort((a, b) => a - b);

  console.log(
    `@@SUMMARY@@${JSON.stringify({
      scheduler_id: args.schedulerId,
      device: args.device,
      count: results.length,
      elapsed_ms: elapsedMs,
      seqs,
      results: args.quiet ? undefined : results,
    })}`,
  );
  if (!args.quiet) {
    console.log(
      `[${args.schedulerId}] published ${results.length} in ${elapsedMs}ms ` +
        `(${((results.length / elapsedMs) * 1000).toFixed(0)} msg/s)`,
    );
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
