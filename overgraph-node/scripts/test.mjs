// Benchmarks are explicit tools, never side effects of the correctness suite.
import { readdirSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
const files = readdirSync(new URL('../__test__/', import.meta.url))
  .filter(name => name.endsWith('.mjs') && !name.startsWith('benchmark'))
  .sort().map(name => `__test__/${name}`);
const result = spawnSync(process.execPath, ['--test', '--test-concurrency=4', ...files], { stdio: 'inherit' });
if (result.error) throw result.error;
process.exitCode = result.status ?? 1;
