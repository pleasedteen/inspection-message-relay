import http from 'node:http';
import { AddressInfo } from 'node:net';
import fs from 'node:fs';
import { InspectionService } from './service';
import { ApiError } from './errors';
import { WaitRegistry, normalizeWaitMs } from './waiter';

export interface ServerHandle {
  server: http.Server;
  port: number;
  url: string;
  registry: WaitRegistry;
  close: () => Promise<void>;
}

/**
 * 极简 JSON HTTP 接口（仅用 Node 内置模块）：
 *   GET  /health
 *   POST /subscriptions                      {name}
 *   GET  /subscriptions/:id
 *   POST /subscriptions/:id/claim            {lease_ms?, device_id?, wait_ms?}
 *   POST /subscriptions/:id/ack              {message_id, token}
 *   POST /messages                           PublishInput
 *   GET  /admin/messages/:id
 *   GET  /admin/stats
 *   GET  /admin/waiters                      长等等待者与领取尝试计数
 *   POST /admin/cleanup
 *   GET  /admin/query-plans
 */
export function createHttpServer(
  service: InspectionService,
  registry: WaitRegistry = new WaitRegistry(service),
): http.Server {
  return http.createServer(async (req, res) => {
    const started = Date.now();
    try {
      const url = new URL(req.url ?? '/', 'http://localhost');
      const path = url.pathname;
      const method = req.method ?? 'GET';

      if (method === 'GET' && path === '/health') {
        return sendJson(res, 200, { ok: true, service: 'inspection', time: Date.now() });
      }

      if (method === 'POST' && path === '/subscriptions') {
        const body = await readJson(req);
        return sendJson(res, 201, service.registerSubscription(body?.name));
      }

      let m = /^\/subscriptions\/([^/]+)$/.exec(path);
      if (m && method === 'GET') {
        const sub = service.getSubscription(decodeURIComponent(m[1]));
        if (!sub) throw new ApiError(404, 'SUBSCRIPTION_NOT_FOUND', '订阅不存在');
        return sendJson(res, 200, sub);
      }

      m = /^\/subscriptions\/([^/]+)\/claim$/.exec(path);
      if (m && method === 'POST') {
        const body = await readJson(req).catch(() => ({}));
        const subIdOrName = decodeURIComponent(m[1]);
        const wantMeter = url.searchParams.has('meter');
        const waitMs = normalizeWaitMs(body?.wait_ms);

        if (waitMs === 0) {
          // —— 原立即领取语义：完全不变 ——
          const result = service.claim(
            subIdOrName,
            { lease_ms: body?.lease_ms, device_id: body?.device_id },
            Date.now(),
          );
          // 默认不回传计量；?meter=1 时附带，方便通过真实 HTTP 核对。
          if (!wantMeter) delete (result as any).meter;
          delete (result as any).sub_id;
          delete (result as any).blocked_devices;
          return sendJson(res, 200, result);
        }

        // —— 可选长等领取：先试领一次，无活则挂起等待事件/租约到期/超时 ——
        let settled = false;
        const waiterId = registry.handleLongPoll({
          subIdOrName,
          leaseMs: body?.lease_ms === undefined ? undefined : Number(body.lease_ms),
          deviceId: body?.device_id === undefined ? undefined : String(body.device_id),
          waitMs,
          wantMeter,
          respond: (status, payload) => {
            if (settled) return;
            settled = true;
            if (!res.writableEnded && !res.destroyed) {
              try {
                sendJson(res, status, payload);
              } catch {
                /* 连接已断，丢弃应答 */
              }
            }
          },
        });
        if (waiterId >= 0) {
          // 客户端断连（进程被杀/网络断开）→ 立即撤销等待，不再参与领取
          res.on('close', () => {
            if (!res.writableFinished && !settled) {
              settled = true;
              registry.cancel(waiterId);
            }
          });
        }
        return;
      }

      m = /^\/subscriptions\/([^/]+)\/ack$/.exec(path);
      if (m && method === 'POST') {
        const body = await readJson(req);
        const out = service.ack(decodeURIComponent(m[1]), body ?? {});
        const moved = (out as any).head_moved as { sub_id: string; device_id: string } | null;
        delete (out as any).head_moved;
        sendJson(res, 200, out);
        // 前项确认推进了队头 → 精确唤醒该设备（及不限设备）的长等
        if (moved) registry.notifyHead(moved.sub_id, moved.device_id);
        return;
      }

      if (method === 'POST' && path === '/messages') {
        const body = await readJson(req);
        if (!body || typeof body !== 'object') {
          throw new ApiError(400, 'BAD_BODY', '请求体必须是 JSON 对象');
        }
        const out = service.publish(body);
        const newHeadSubs = (out as any).new_head_subs as string[] | undefined;
        delete (out as any).new_head_subs;
        sendJson(res, 201, out);
        // 只对“新建立了队头”的订阅点名该设备；无关设备/订阅不被惊扰
        if (newHeadSubs) {
          for (const sid of newHeadSubs) registry.notifyHead(sid, String((out as any).device_id));
        }
        return;
      }

      if (method === 'POST' && path === '/admin/cleanup') {
        await readJson(req).catch(() => ({}));
        return sendJson(res, 200, service.cleanup());
      }

      if (method === 'GET' && path === '/admin/stats') {
        return sendJson(res, 200, service.stats());
      }

      if (method === 'GET' && path === '/admin/waiters') {
        return sendJson(res, 200, registry.info());
      }

      if (method === 'GET' && path === '/admin/query-plans') {
        return sendJson(res, 200, service.hotQueryPlans());
      }

      m = /^\/admin\/messages\/([^/]+)$/.exec(path);
      if (m && method === 'GET') {
        return sendJson(res, 200, service.messageReceipts(decodeURIComponent(m[1])));
      }

      sendJson(res, 404, { error: { code: 'NOT_FOUND', message: `${method} ${path} 不存在` } });
    } catch (err) {
      if (err instanceof ApiError) {
        return sendJson(res, err.status, {
          error: { code: err.code, message: err.message, details: err.details },
        });
      }
      const e = err as Error;
      // node:sqlite 约束错误等
      const status = /UNIQUE|constraint/i.test(e.message) ? 409 : 500;
      sendJson(res, status, { error: { code: 'INTERNAL', message: e.message } });
    } finally {
      void started;
    }
  });
}

export function listen(
  service: InspectionService,
  opts: { port: number; host?: string; portFile?: string },
): Promise<ServerHandle> {
  const registry = new WaitRegistry(service);
  const server = createHttpServer(service, registry);
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(opts.port, opts.host ?? '127.0.0.1', () => {
      const info = server.address() as AddressInfo;
      const port = info.port;
      const url = `http://${opts.host ?? '127.0.0.1'}:${port}`;
      if (opts.portFile) {
        fs.mkdirSync(require('node:path').dirname(opts.portFile), { recursive: true });
        fs.writeFileSync(opts.portFile, JSON.stringify({ port, url, pid: process.pid }));
      }
      resolve({
        server,
        port,
        url,
        registry,
        close: () =>
          new Promise<void>((res, rej) => {
            registry.shutdown(); // 撤销全部长等，避免 close 挂起
            server.close((e) => (e ? rej(e) : res()));
          }),
      });
    });
  });
}

function sendJson(res: http.ServerResponse, status: number, data: unknown): void {
  const buf = Buffer.from(JSON.stringify(data), 'utf8');
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': buf.length,
  });
  res.end(buf);
}

async function readJson(req: http.IncomingMessage): Promise<any> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let size = 0;
    req.on('data', (c: Buffer) => {
      size += c.length;
      if (size > 16 * 1024 * 1024) {
        reject(new ApiError(413, 'BODY_TOO_LARGE', '请求体超过 16MB'));
        req.destroy();
        return;
      }
      chunks.push(c);
    });
    req.on('end', () => {
      if (chunks.length === 0) return resolve({});
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString('utf8')));
      } catch (e) {
        reject(new ApiError(400, 'BAD_JSON', `JSON 解析失败: ${(e as Error).message}`));
      }
    });
    req.on('error', reject);
  });
}
