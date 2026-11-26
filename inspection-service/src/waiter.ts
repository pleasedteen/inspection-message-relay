/**
 * 长等领取注册表（可选长轮询模式）—— 全部状态在内存，不写库、不轮询库。
 *
 * 语义：
 *  - 请求先同步试领一次：有匹配队头立即返回领取结果（waited_ms≈0）；
 *  - 无活则登记为等待者，直到 (a) 新单发布建立了新队头、(b) 前项确认推进了队头、
 *    (c) 阻塞队头的租约到期（按库中绝对到期时刻挂一次性定时器），或 (d) 超时返回空；
 *  - 客户端断连即撤销等待，之后不再参与任何领取；
 *  - 同一订阅的“定向设备”与“不限设备”等待可并存；事件只携带设备号精确唤醒匹配的
 *    等待者，无关设备变动不触发定向复查；
 *  - 调度每轮对每个可领队头只弹出一个等待者做一次领取尝试，尝试数不随等待人数增长；
 *    底层条件更新（state / lease_expires_at 守卫）保证只发一份有效租约；
 *  - 进程重启后等待表清空；租约/截止都是库中绝对时间，客户端重新等待即可按原边界重领。
 */
import { InspectionService, ClaimResult } from './service';
import { ApiError } from './errors';

export const MAX_WAIT_MS = 120_000;

/** wait_ms 归一化：缺省/0 = 立即模式（原语义）；>0 进入长等，封顶 2 分钟。 */
export function normalizeWaitMs(v: unknown): number {
  if (v === undefined || v === null) return 0;
  const n = Number(v);
  if (!Number.isFinite(n) || n < 0) {
    throw new ApiError(400, 'BAD_WAIT', 'wait_ms 必须是 >= 0 的毫秒数（0 表示立即返回）');
  }
  if (n === 0) return 0;
  return Math.min(Math.max(Math.floor(n), 50), MAX_WAIT_MS);
}

export interface WaiterSnapshot {
  sub_id: string;
  device_id: string | null;
  waited_ms: number;
  remaining_ms: number;
}

interface Waiter {
  id: number;
  subId: string;
  deviceId: string | null;
  leaseMs?: number;
  wantMeter: boolean;
  startedAt: number;
  waitMs: number;
  timer: NodeJS.Timeout;
  lastBlocked: Record<string, unknown> | null;
  lastRetryAt: number | null;
  respond: (status: number, body: Record<string, unknown>) => void;
}

interface SubState {
  waiters: Waiter[];
  /** 每个设备一个“阻塞租约到期”一次性定时器（绝对时刻来自 DB，重启后由重新等待重建） */
  leaseTimers: Map<string, { at: number; timer: NodeJS.Timeout }>;
  /** 自上次调度以来被点到名的设备（发布新队头 / ack 推进 / 租约到期） */
  hot: Set<string>;
  scheduled: boolean;
}

export class WaitRegistry {
  private readonly subs = new Map<string, SubState>();
  private nextId = 0;
  readonly stats = {
    initial_attempts: 0, // 长等请求登记前的同步试领
    wake_attempts: 0, //    事件/定时器唤醒后的真实领取尝试（核心计量：不随等待人数增长）
    dispatches: 0, //       调度轮数
    wakes: 0, //            成功领到并唤醒的等待者数
    timeouts: 0, //         超时返回空的等待者数
    cancels: 0, //          断连撤销的等待者数
  };

  constructor(private readonly service: InspectionService) {}

  /**
   * 长等入口：先同步试领；无活则登记等待。
   * 返回等待者 id；若已立即应答（领到）返回 -1。
   */
  handleLongPoll(params: {
    subIdOrName: string;
    leaseMs?: number;
    deviceId?: string;
    waitMs: number;
    wantMeter: boolean;
    respond: (status: number, body: Record<string, unknown>) => void;
  }): number {
    const startedAt = Date.now();
    this.stats.initial_attempts++;
    const first = this.service.claim(
      params.subIdOrName,
      { lease_ms: params.leaseMs, device_id: params.deviceId },
      startedAt,
    );
    if (first.claim) {
      params.respond(200, this.publicResult(first, params.wantMeter, 0));
      return -1;
    }

    const subId = first.sub_id!;
    let st = this.subs.get(subId);
    if (!st) {
      st = { waiters: [], leaseTimers: new Map(), hot: new Set(), scheduled: false };
      this.subs.set(subId, st);
    }
    const id = ++this.nextId;
    const waiter: Waiter = {
      id,
      subId,
      deviceId: params.deviceId ?? null,
      leaseMs: params.leaseMs,
      wantMeter: params.wantMeter,
      startedAt,
      waitMs: params.waitMs,
      lastBlocked: (first.blocked as Record<string, unknown>) ?? null,
      lastRetryAt: first.retry_at ?? null,
      respond: params.respond,
      timer: setTimeout(() => this.onTimeout(subId, id), params.waitMs),
    };
    waiter.timer.unref?.();
    st.waiters.push(waiter);
    // 试领时看到的阻塞队头：按其绝对租约到期时刻挂定时器（到期自动唤醒，不轮询）
    this.armFromBlocked(subId, st, first);
    return id;
  }

  /** 发布建立了新队头 / ack 推进了队头 / 租约定时器到期 → 点名设备，合并成一轮调度。 */
  notifyHead(subId: string, deviceId: string): void {
    const st = this.subs.get(subId);
    if (!st || st.waiters.length === 0) return;
    st.hot.add(deviceId);
    if (st.scheduled) return;
    st.scheduled = true;
    setImmediate(() => this.dispatch(subId));
  }

  /** 客户端断连：撤销等待，之后不再参与领取。 */
  cancel(waiterId: number): boolean {
    for (const [subId, st] of this.subs) {
      const idx = st.waiters.findIndex((w) => w.id === waiterId);
      if (idx < 0) continue;
      const [w] = st.waiters.splice(idx, 1);
      clearTimeout(w.timer);
      this.stats.cancels++;
      this.maybeCleanup(subId, st);
      return true;
    }
    return false;
  }

  info(): { now: number; waiters: WaiterSnapshot[]; stats: Record<string, number> } {
    const now = Date.now();
    const waiters: WaiterSnapshot[] = [];
    for (const st of this.subs.values()) {
      for (const w of st.waiters) {
        waiters.push({
          sub_id: w.subId,
          device_id: w.deviceId,
          waited_ms: now - w.startedAt,
          remaining_ms: Math.max(0, w.startedAt + w.waitMs - now),
        });
      }
    }
    return { now, waiters, stats: { ...this.stats } };
  }

  /** 服务关闭：撤销全部等待并应答 503，清理定时器。 */
  shutdown(): void {
    for (const [subId, st] of this.subs) {
      for (const w of st.waiters) {
        clearTimeout(w.timer);
        this.safeRespond(w, 503, {
          error: { code: 'SHUTTING_DOWN', message: '服务正在关闭，长等被取消' },
        });
      }
      for (const t of st.leaseTimers.values()) clearTimeout(t.timer);
      st.waiters.length = 0;
      st.leaseTimers.clear();
      this.subs.delete(subId);
    }
  }

  // ---------------------------------------------------------------- 内部

  /**
   * 一轮调度：hot 里每个可领队头只弹出一个相关等待者做一次领取尝试。
   * 定向等待者只在自己的设备被点名时参与；不限设备的等待者任何点名都可参与。
   */
  private dispatch(subId: string): void {
    const st = this.subs.get(subId);
    if (!st) return;
    st.scheduled = false;
    this.stats.dispatches++;
    try {
      while (st.waiters.length > 0 && st.hot.size > 0) {
        const idx = st.waiters.findIndex((w) => w.deviceId === null || st.hot.has(w.deviceId));
        if (idx < 0) break; // 没有与被点名设备相关的等待者
        const [w] = st.waiters.splice(idx, 1);
        let res: ClaimResult;
        try {
          this.stats.wake_attempts++;
          res = this.service.claim(
            w.subId,
            { lease_ms: w.leaseMs, device_id: w.deviceId ?? undefined },
            Date.now(),
          );
        } catch (e) {
          this.safeRespond(w, 500, {
            error: { code: 'INTERNAL', message: (e as Error).message },
          });
          clearTimeout(w.timer);
          continue;
        }
        if (res.claim) {
          const dev = String((res.claim as { device_id: unknown }).device_id);
          // 该队头已租出：同设备后项被队头阻塞，其他等待者暂不可领
          st.hot.delete(dev);
          // 仍有人等待时，新租约的绝对到期时刻是下一个可能可领的边界
          if (st.waiters.length > 0) {
            this.armLeaseTimer(
              subId,
              st,
              dev,
              Number((res.claim as { lease_expires_at: unknown }).lease_expires_at),
            );
          }
          clearTimeout(w.timer);
          this.stats.wakes++;
          this.safeRespond(w, 200, this.publicResult(res, w.wantMeter, Date.now() - w.startedAt));
          // 继续：hot 中其他设备可能仍有可领队头
        } else {
          this.armFromBlocked(subId, st, res);
          if (w.deviceId === null) {
            // 不限设备的尝试已检查全部队头：当前没有任何可领 → 全部继续等待
            st.waiters.unshift(w);
            break;
          }
          st.hot.delete(w.deviceId); // 该定向设备暂无可领；其余被点名设备仍可尝试
          st.waiters.unshift(w);
        }
      }
    } finally {
      st.hot.clear();
      this.maybeCleanup(subId, st);
    }
  }

  private onTimeout(subId: string, waiterId: number): void {
    const st = this.subs.get(subId);
    if (!st) return;
    const idx = st.waiters.findIndex((w) => w.id === waiterId);
    if (idx < 0) return;
    const [w] = st.waiters.splice(idx, 1);
    this.stats.timeouts++;
    this.safeRespond(w, 200, {
      claim: null,
      blocked: w.lastBlocked,
      retry_at: w.lastRetryAt,
      waited_ms: w.waitMs,
      server_time: Date.now(),
    });
    this.maybeCleanup(subId, st);
  }

  private armFromBlocked(subId: string, st: SubState, res: ClaimResult): void {
    for (const b of res.blocked_devices ?? []) {
      this.armLeaseTimer(subId, st, b.device_id, b.lease_expires_at);
    }
  }

  /** 按 DB 中的绝对租约到期时刻挂一次性定时器；同设备同边界去重。 */
  private armLeaseTimer(subId: string, st: SubState, deviceId: string, expiresAt: number): void {
    const cur = st.leaseTimers.get(deviceId);
    if (cur && cur.at === expiresAt) return;
    if (cur) clearTimeout(cur.timer);
    const timer = setTimeout(() => {
      st.leaseTimers.delete(deviceId);
      this.notifyHead(subId, deviceId);
    }, Math.max(1, expiresAt - Date.now()));
    timer.unref?.();
    st.leaseTimers.set(deviceId, { at: expiresAt, timer });
  }

  private maybeCleanup(subId: string, st: SubState): void {
    if (st.waiters.length > 0) return;
    for (const t of st.leaseTimers.values()) clearTimeout(t.timer);
    st.leaseTimers.clear();
    if (!st.scheduled) this.subs.delete(subId);
  }

  private publicResult(
    res: ClaimResult,
    wantMeter: boolean,
    waitedMs: number,
  ): Record<string, unknown> {
    const out: Record<string, unknown> = { ...res };
    delete out.sub_id;
    delete out.blocked_devices;
    if (!wantMeter) delete out.meter;
    out.waited_ms = waitedMs;
    return out;
  }

  private safeRespond(w: Waiter, status: number, body: Record<string, unknown>): void {
    try {
      w.respond(status, body);
    } catch {
      /* 连接已断，丢弃应答 */
    }
  }
}
