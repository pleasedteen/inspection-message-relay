/** 巡检消息服务的极简 HTTP 客户端 SDK（Node 18+ 全局 fetch）。 */

export class ClientError extends Error {
  constructor(
    public readonly status: number,
    public readonly code: string,
    message: string,
    public readonly details?: unknown,
  ) {
    super(message);
  }
}

export class InspectionClient {
  constructor(public readonly baseUrl: string) {}

  private async request<T>(method: string, path: string, body?: unknown): Promise<T> {
    const res = await fetch(`${this.baseUrl}${path}`, {
      method,
      headers: body === undefined ? undefined : { 'content-type': 'application/json' },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    const text = await res.text();
    const json = text ? JSON.parse(text) : null;
    if (!res.ok) {
      throw new ClientError(
        res.status,
        json?.error?.code ?? 'HTTP_ERROR',
        json?.error?.message ?? res.statusText,
        json?.error?.details,
      );
    }
    return json as T;
  }

  health() {
    return this.request<{ ok: boolean; time: number }>('GET', '/health');
  }

  register(name: string) {
    return this.request<{ id: string; name: string; effective_at: number; created: boolean }>(
      'POST',
      '/subscriptions',
      { name },
    );
  }

  publish(input: {
    device_id: string;
    payload?: unknown;
    first_deadline_at?: number | string;
    first_claim_ttl_ms?: number;
    idempotency_key?: string;
  }) {
    return this.request<PublishedMessage>('POST', '/messages', input);
  }

  claim(
    sub: string,
    opts: { lease_ms?: number; device_id?: string; meter?: boolean; wait_ms?: number } = {},
  ): Promise<ClaimResponse> {
    const q = opts.meter ? '?meter=1' : '';
    return this.request('POST', `/subscriptions/${encodeURIComponent(sub)}/claim${q}`, {
      lease_ms: opts.lease_ms,
      device_id: opts.device_id,
      wait_ms: opts.wait_ms,
    });
  }

  ack(sub: string, message_id: string, token: string) {
    return this.request<{ acked: true }>(
      'POST',
      `/subscriptions/${encodeURIComponent(sub)}/ack`,
      { message_id, token },
    );
  }

  cleanup() {
    return this.request<{ decided_at: number; reclaimed_bodies: number }>(
      'POST',
      '/admin/cleanup',
      {},
    );
  }

  stats() {
    return this.request<Record<string, unknown>>('GET', '/admin/stats');
  }

  waiters() {
    return this.request<{
      now: number;
      waiters: {
        sub_id: string;
        device_id: string | null;
        waited_ms: number;
        remaining_ms: number;
      }[];
      stats: Record<string, number>;
    }>('GET', '/admin/waiters');
  }

  messageReceipts(id: string) {
    return this.request<any>('GET', `/admin/messages/${id}`);
  }

  queryPlans() {
    return this.request<Record<string, string[]>>('GET', '/admin/query-plans');
  }

  async waitHealthy(timeoutMs = 10_000): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    let lastErr: unknown;
    while (Date.now() < deadline) {
      try {
        await this.health();
        return;
      } catch (e) {
        lastErr = e;
        await new Promise((r) => setTimeout(r, 80));
      }
    }
    throw new Error(`服务在 ${timeoutMs}ms 内未就绪: ${(lastErr as Error)?.message}`);
  }
}

export interface PublishedMessage {
  id: string;
  device_id: string;
  seq: number;
  published_at: number;
  first_deadline_at: number;
  deduped?: boolean;
  recipients?: number;
}

export interface ClaimedMessage extends PublishedMessage {
  payload: unknown;
  lease_token: string;
  lease_expires_at: number;
  delivery_count: number;
  redelivery: boolean;
}

export interface ClaimResponse {
  claim: ClaimedMessage | null;
  blocked?: {
    reason: string;
    device_id: string;
    seq: number;
    message_id: string;
    lease_expires_at: number;
  } | null;
  /** 全部阻塞队头中最早的租约到期时刻（无阻塞为 null/缺省） */
  retry_at?: number | null;
  /** 仅长等（wait_ms>0）响应携带：本次请求实际等待的毫秒数 */
  waited_ms?: number;
  meter?: unknown;
  server_time: number;
}
