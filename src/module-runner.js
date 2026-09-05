import vm from 'node:vm';

let raw = '';
for await (const chunk of process.stdin) raw += chunk;

function fail(error) {
  process.stdout.write(JSON.stringify({ ok: false, error: { name: error?.name ?? 'Error', message: error?.message ?? String(error), stack: error?.stack ?? null } }) + '\n');
  process.exitCode = 1;
}

try {
  const request = JSON.parse(raw || '{}');
  const records = new Map((request.modules ?? []).map((entry) => [entry.id, entry]));
  const instances = new Map();
  const context = vm.createContext(Object.freeze({
    console,
    TextEncoder,
    TextDecoder,
    URL,
    URLSearchParams,
    setTimeout,
    clearTimeout,
    setInterval,
    clearInterval,
    queueMicrotask,
    structuredClone,
  }));

  async function moduleFor(id) {
    if (instances.has(id)) return instances.get(id);
    const record = records.get(id);
    if (!record) throw new Error(`Parallel module graph references unknown module '${id}'`);
    const module = new vm.SourceTextModule(record.code, {
      context,
      identifier: `parallel:${id}`,
      initializeImportMeta(meta) { meta.url = `parallel:${id}`; },
      async importModuleDynamically(specifier) {
        const target = record.dynamicDependencies?.[specifier];
        if (!target) throw new Error(`Parallel dynamic import denied: ${specifier}`);
        const child = await moduleFor(target);
        if (child.status === 'unlinked') await linkModule(child, target);
        if (child.status === 'linked') await child.evaluate();
        return child;
      },
    });
    instances.set(id, module);
    return module;
  }

  async function linkModule(module, id) {
    const record = records.get(id);
    await module.link(async (specifier) => {
      const target = record.dependencies?.[specifier];
      if (!target) throw new Error(`Parallel undeclared static import '${specifier}' in '${id}'`);
      return moduleFor(target);
    });
  }

  const entry = await moduleFor(request.entry);
  await linkModule(entry, request.entry);
  await entry.evaluate();
  const namespace = entry.namespace;
  const exportNames = Object.keys(namespace).sort();
  let result = null;
  if (request.invoke) {
    const fn = namespace[request.invoke.export];
    if (typeof fn !== 'function') throw new Error(`Parallel entry export '${request.invoke.export}' is not callable`);
    result = await fn(...(request.invoke.args ?? []));
    structuredClone(result);
  }
  process.stdout.write(JSON.stringify({ ok: true, exports: exportNames, result }) + '\n');
} catch (error) {
  fail(error);
}
