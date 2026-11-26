/**
 * AccessMeter —— 一次领取事务访问的“存储记录 / 索引项”逻辑计量器。
 *
 * 计量口径（README 基准章节有同样说明）：
 *  - probes     ：一次精确键（PK / UNIQUE）探测，计 1 个被定位的索引叶子项；
 *  - indexSteps ：索引前缀 / 范围扫描中，每实际取出 1 个索引项计 1（按语句真实返回行数累计）；
 *  - rowsRead   ：每读取 1 条表记录计 1；
 *  - rowsWritten：每插入 / 更新 / 删除 1 条记录计 1（写索引的维护量与写行数成正比，已含在常数项中）；
 *  - sweepSteps ：其中“本次领取触发的清扫 + 队头推进”所处理的条目数（是上面计数的子集，单列便于核对）。
 *
 * 注意：B-tree 精确探测途经的内部页为 O(log N)，不随“这批积压的条数”线性增长，
 * 因此不计入“条目”；基准同时打印墙钟耗时与 EXPLAIN QUERY PLAN 作为独立佐证。
 */
export class Meter {
  probes = 0;
  indexSteps = 0;
  rowsRead = 0;
  rowsWritten = 0;
  sweepSteps = 0;

  reset(): this {
    this.probes = 0;
    this.indexSteps = 0;
    this.rowsRead = 0;
    this.rowsWritten = 0;
    this.sweepSteps = 0;
    return this;
  }

  /** 一次精确键索引探测（命中与否都计 1）。 */
  seek(n = 1): void {
    this.probes += n;
  }

  /** 范围 / 前缀扫描实际取出的索引项数。 */
  step(n: number): void {
    this.indexSteps += n;
  }

  read(n = 1): void {
    this.rowsRead += n;
  }

  write(n: number | bigint = 1): void {
    this.rowsWritten += Number(n);
  }

  sweep(n: number | bigint = 1): void {
    const v = Number(n);
    this.sweepSteps += v;
  }

  /** 记录 + 索引项的总访问条目数。 */
  total(): number {
    return this.probes + this.indexSteps + this.rowsRead + this.rowsWritten;
  }

  snapshot(): MeterSnapshot {
    return {
      probes: this.probes,
      index_steps: this.indexSteps,
      rows_read: this.rowsRead,
      rows_written: this.rowsWritten,
      sweep_steps: this.sweepSteps,
      total: this.total(),
    };
  }
}

export interface MeterSnapshot {
  probes: number;
  index_steps: number;
  rows_read: number;
  rows_written: number;
  sweep_steps: number;
  total: number;
}
