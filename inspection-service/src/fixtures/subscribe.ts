#!/usr/bin/env node
/**
 * 本地订阅夹具：代表一个巡检组（或组内一个工作者）长轮询领取并处理。
 *
 * 正常处理并确认：
 *   node dist/fixtures/subscribe.js --name inspectors-a --max 5 --work-ms 200
 *
 * 模拟“领到任务后断线/崩溃，不确认”（用于租期重投演示/测试）：
 *   node dist/fixtures/subscribe.js --name inspectors-a --mode drop --lease-ms 1500
 *
 * 长等领取（可选长轮询）：没活时挂起等待，而不是立即空返轮询：
 *   node dist/fixtures/subscribe.js --name inspectors-a --wait-ms 15000 --max Infinity
 *
 * 关键事件以 @@CLAIM@@<json> / @@ACK@@<json> / @@WAIT@@<json> 输出，便于自动化解析；
 * drop 模式用 fs.writeSync 强制把最后一行写入管道后立即退出。
 */
import fs from 'node:fs';
import { InspectionClient, ClaimedMessage } from '../client';

interface SubArgs {
  url: string;
  name: string;
  leaseMs: number;
  max: number;
  mode: 'ack' | 'drop';
  workMs: number;
  device?: string;
  pollMs: number;
  waitMs: number;
}

function parse(): SubArgs {
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
    name: a.name ?? 'inspectors-a',
    leaseMs: Number(a['lease-ms'] ?? 8000),
    max: Number(a.max ?? 1),
    mode: (a.mode as 'ack' | 'drop') ?? 'ack',
    workMs: Number(a['work-ms'] ?? 200),
    device: a.device,
    pollMs: Number(a['poll-ms'] ?? 100),
    waitMs: Number(a['wait-ms'] ?? 0),
  };
}

function emit(marker: string, obj: unknown): void {
  fs.writeSync(1, `${marker}${JSON.stringify(obj)}\n`);
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function main(): Promise<void> {
  const args = parse();
  const client = new InspectionClient(args.url);
  await client.waitHealthy();
  const sub = await client.register(args.name);
  emit('@@READY@@', { id: sub.id, name: sub.name, effective_at: sub.effective_at });

  let handled = 0;
  while (handled < args.max) {
    const res = await client.claim(sub.id, {
      lease_ms: args.leaseMs,
      device_id: args.device,
      wait_ms: args.waitMs > 0 ? args.waitMs : undefined,
    });
    const msg: ClaimedMessage | null = res.claim;
    if (!msg) {
      if (args.waitMs > 0) {
        // 长等空返（超时）：报告本次等待结果，直接进入下一轮长等，不轮询
        emit('@@WAIT@@', { empty: true, waited_ms: res.waited_ms ?? null });
      } else {
        if (args.max === Infinity) {
          process.stdout.write('.');
        }
        await sleep(args.pollMs);
      }
      continue;
    }
    handled++;
    emit('@@CLAIM@@', {
      message_id: msg.id,
      device_id: msg.device_id,
      seq: msg.seq,
      token: msg.lease_token,
      lease_expires_at: msg.lease_expires_at,
      delivery_count: msg.delivery_count,
      redelivery: msg.redelivery,
      waited_ms: res.waited_ms ?? 0,
      payload: msg.payload,
    });

    if (args.mode === 'drop') {
      // 不发 ack，立即退出，模拟工作者断线/崩溃；服务端只依赖租期判定。
      fs.writeSync(1, `[subscriber:${args.name}] DROP after claim, exiting without ack\n`);
      process.exit(0);
    }

    await sleep(args.workMs);
    await client.ack(sub.id, msg.id, msg.lease_token);
    emit('@@ACK@@', { message_id: msg.id, seq: msg.seq });
    fs.writeSync(
      1,
      `[subscriber:${args.name}] processed ${msg.device_id}#${msg.seq}` +
        ` (delivery=${msg.delivery_count}) and acked\n`,
    );
  }
  emit('@@DONE@@', { handled });
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
