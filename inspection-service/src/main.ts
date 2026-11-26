#!/usr/bin/env node
import path from 'node:path';
import { openDatabase } from './db';
import { InspectionService } from './service';
import { listen } from './server';

interface Args {
  port: number;
  db: string;
  host: string;
  portFile?: string;
}

export function parseArgs(argv: string[]): Args {
  const args: Record<string, string> = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a.startsWith('--')) {
      const key = a.slice(2);
      const next = argv[i + 1];
      if (next === undefined || next.startsWith('--')) {
        args[key] = 'true';
      } else {
        args[key] = next;
        i++;
      }
    }
  }
  return {
    port: Number(args.port ?? process.env.PORT ?? 8787),
    db: args.db ?? process.env.DB_PATH ?? path.join(process.cwd(), 'data', 'inspection.db'),
    host: args.host ?? process.env.HOST ?? '127.0.0.1',
    portFile: args['port-file'],
  };
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  const db = openDatabase(args.db);
  const service = new InspectionService(db);
  const handle = await listen(service, {
    port: Number.isFinite(args.port) ? args.port : 8787,
    host: args.host,
    portFile: args.portFile,
  });

  console.log(
    `[inspection-service] listening on ${handle.url} (pid=${process.pid}, db=${path.resolve(args.db)})`,
  );

  let closing = false;
  const shutdown = async (sig: string) => {
    if (closing) return;
    closing = true;
    console.log(`[inspection-service] received ${sig}, closing...`);
    try {
      await handle.close();
      db.close();
    } catch (e) {
      console.error('[inspection-service] close error:', e);
    }
    process.exit(0);
  };
  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
}

if (require.main === module) {
  main().catch((e) => {
    console.error(e);
    process.exit(1);
  });
}
