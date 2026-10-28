// napi's object structs describe the permissive Rust wire format, not the public
// discriminated index contracts. Keep the maintained header versions, removing
// only their generated duplicates. Run after builds and before packaging.
import { readFileSync, writeFileSync } from 'node:fs';

const headerPath = new URL('../dts-header.d.ts', import.meta.url);
const declarationsPath = new URL('../index.d.ts', import.meta.url);
const header = readFileSync(headerPath, 'utf8').trimEnd();
const declarations = readFileSync(declarationsPath, 'utf8');
if (!declarations.startsWith(header)) {
  throw new Error('Generated declarations must start with dts-header.d.ts; rebuild first');
}
let generated = declarations.slice(header.length);
for (const name of ['SecondaryIndexField', 'SecondaryIndexSpec']) {
  const pattern = new RegExp(`^export interface ${name} \\{\\r?\\n[^]*?^\\}\\r?\\n`, 'gm');
  const matches = [...generated.matchAll(pattern)];
  if (matches.length > 1) {
    throw new Error(`Unexpected duplicate generated declaration: ${name}`);
  }
  generated = generated.replace(pattern, '');
}
const finalized = header + generated;
if (finalized !== declarations) writeFileSync(declarationsPath, finalized);
