import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';

test('wrapper retains failures, validates factorial, is read-only by default and redacts credentials', () => {
  const temporary = fs.mkdtempSync(path.join(os.tmpdir(), 'p07-live-stub-'));
  const tools = path.join(temporary, 'tools'); fs.mkdirSync(tools);
  const tool = (name, content) => { const file = path.join(tools, name); fs.writeFileSync(file, content, { mode: 0o700 }); return file; };
  tool('uname', '#!/bin/sh\nprintf "Linux 7.0.14-19-pve x86_64\\n"\n');
  tool('getconf', '#!/bin/sh\nprintf "10\\n"\n');
  tool('go', '#!/bin/sh\nif [ "$1" = build ]; then cp "' + path.join(tools, 'benchmark') + '" "$4"; fi\nprintf "go version go1.27.1 linux/amd64\\n"\n');
  const binary = tool('benchmark', `#!/bin/sh
if [ "$P07_SEED_ONLY" = 1 ]; then printf '{"seeded":true}\\n'; exit 0; fi
printf '{"environment":{"GOOS":"linux","GOARCH":"amd64","gomaxprocs":10},"offered_load":{"config":{"factorial":true,"load_case":"%s"}}}\\n' "$P07_OFFERED_CASE"
printf 'retained failure with key path %s\\n' "$6" >&2
exit 7
`);
  const key = path.join(temporary, 'credential'); fs.writeFileSync(key, 'SECRET NEVER CAPTURE');
  const buildCapture = path.join(temporary, 'prepared');
  const base = ['integration/p07/live-cluster.sh', '--build-capture', buildCapture, '--binary', binary, '--monitors', '192.0.2.1:3300', '--fsid', '00000000-0000-0000-0000-000000000001', '--key-file', key, '--pool', 'dedicated-p07', '--repetitions', '1', '--cases', 'cpu,alloc'];
  const execute = (capture, extra = []) => spawnSync('sh', [...base, '--capture', capture, ...extra], { encoding: 'utf8', env: { ...process.env, PATH: `${tools}:${process.env.PATH}`, P07_SEED_ONLY: '1', P07_TRACE_FILE: 'must-not-leak' } });
  try {
    const prepared = spawnSync('sh', ['integration/p07/live-cluster.sh', '--prepare', '--capture', buildCapture], { encoding: 'utf8', env: { ...process.env, PATH: `${tools}:${process.env.PATH}` } });
    assert.equal(prepared.status, 0, prepared.stdout + prepared.stderr);
    const build = JSON.parse(fs.readFileSync(path.join(buildCapture, 'build-go.exit.json')));
    assert.ok(!build.args.includes('-tags'));
    const capture = path.join(temporary, 'capture');
    const result = execute(capture, ['--entity', 'client.amakura']);
    assert.equal(result.status, 1, result.stderr);
    const summary = JSON.parse(result.stdout);
    assert.equal(JSON.parse(fs.readFileSync(path.join(capture, 'measurement.json'))).entity, 'client.amakura');
    assert.deepEqual(summary.results.find(entry => entry.name === 'primary-cpu-1').args.slice(-2), ['-entity', 'client.amakura']);
    assert.equal(summary.results.filter(entry => entry.exit_code === 7).length, 2);
    assert.ok(!summary.results.some(entry => entry.name.endsWith('-validation')));
    assert.ok(!fs.existsSync(path.join(capture, 'seed.stdout')));
    assert.ok(!summary.results.some(entry => entry.name === 'source-unchanged'));
    const serialized = fs.readdirSync(capture).map(file => fs.readFileSync(path.join(capture, file), 'utf8')).join('\n');
    assert.ok(!serialized.includes('SECRET NEVER CAPTURE'));
    assert.ok(!serialized.includes(key));
    assert.ok(!serialized.includes('must-not-leak'));
    assert.ok(serialized.includes('retained failure'));
    const again = execute(capture); assert.equal(again.status, 1); assert.equal(JSON.parse(again.stdout).capture, null);
    const seeded = execute(path.join(temporary, 'seeded'), ['--seed']);
    assert.match(seeded.stderr, /WARNING/); assert.ok(fs.existsSync(path.join(temporary, 'seeded', 'seed.stdout')));
    const observed = execute(path.join(temporary, 'observed'), ['--observe']);
    assert.equal(JSON.parse(observed.stdout).results.filter(entry => entry.name.endsWith('-observation')).length, 2);
    assert.ok(!fs.readdirSync(path.join(temporary, 'observed')).some(name => name.startsWith('primary-')));
    tool('uname', '#!/bin/sh\nprintf "Darwin arm64 stub\\n"\n');
    const unsupported = execute(path.join(temporary, 'unsupported'));
    assert.match(JSON.parse(unsupported.stdout).results.at(-1).error, /Linux amd64/);
    assert.ok(!fs.existsSync(path.join(temporary, 'unsupported', 'primary-cpu-1.stdout')));
    tool('uname', '#!/bin/sh\nprintf "Linux x86_64 stub\\n"\n');
    tool('getconf', '#!/bin/sh\nprintf "9\\n"\n');
    const insufficient = execute(path.join(temporary, 'insufficient'));
    assert.ok(JSON.parse(insufficient.stdout).results.some(entry => /ten processors/.test(entry.error ?? '')));
  } finally { fs.rmSync(temporary, { recursive: true, force: true }); }
});

test('wrapper rejects missing cluster arguments and non-absolute captures before measurement', () => {
  const result = spawnSync('sh', ['integration/p07/live-cluster.sh', '--capture', 'relative'], { encoding: 'utf8' });
  assert.equal(result.status, 1);
  assert.match(JSON.parse(result.stdout).results[0].error, /absolute/);
});

function fixture(context, report) {
  const temporary = fs.mkdtempSync(path.join(os.tmpdir(), 'p07-live-binding-'));
  context.after(() => fs.rmSync(temporary, { recursive: true, force: true }));
  const workspace = path.join(temporary, 'workspace');
  const tools = path.join(temporary, 'tools');
  fs.mkdirSync(path.join(workspace, 'integration/p07/benchmark'), { recursive: true });
  fs.mkdirSync(tools);
  fs.copyFileSync('integration/p07/live-cluster.sh', path.join(workspace, 'integration/p07/live-cluster.sh'));
  fs.writeFileSync(path.join(workspace, 'integration/p07/benchmark/main.go'), 'package main\nfunc main() {}\n');
  fs.writeFileSync(path.join(workspace, 'go.mod'), 'module fixture\ngo 1.27\n');
  fs.writeFileSync(path.join(workspace, 'untracked.go'), 'package fixture\n');
  const tool = (name, content) => {
    const file = path.join(tools, name);
    fs.writeFileSync(file, content, { mode: 0o700 });
    return file;
  };
  tool('uname', '#!/bin/sh\nprintf "Linux x86_64 stub\\n"\n');
  tool('getconf', '#!/bin/sh\nprintf "10\\n"\n');
  tool('git', '#!/bin/sh\nexit 0\n');
  tool('benchmark', `#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');
const instrumented = Boolean(process.env.P07_OFFERED_OBSERVATION_DIR);
const report = ${report === undefined ? `JSON.stringify({
  environment: { GOOS: 'linux', GOARCH: 'amd64', go_version: 'go1.27.1', gomaxprocs: 10 },
  offered_load: { config: { factorial: true, load_case: process.env.P07_OFFERED_CASE || 'none', instrumented, gomaxprocs: 10 }, attempted: 1 },
})` : JSON.stringify(report)};
if (instrumented) {
  const directory = process.env.P07_OFFERED_OBSERVATION_DIR;
  fs.mkdirSync(directory);
  fs.writeFileSync(path.join(directory, 'timing.json'), JSON.stringify({ schema: 1, label: process.env.P07_OFFERED_OBSERVATION_LABEL, calls: [{}] }));
  fs.writeFileSync(path.join(directory, 'trace.out'), 'stub trace');
}
console.log(report);
`);
  fs.writeFileSync(path.join(workspace, 'integration/p07/trace-stall-summary.mjs'), `import fs from 'node:fs';
if (process.argv[2] === '--decode') {
  if (fs.readFileSync(process.argv[3], 'utf8') !== 'stub trace') process.exit(1);
  fs.writeFileSync(process.argv[4], 'stub decoded trace');
  console.log(JSON.stringify(Object.fromEntries(Object.entries(process.env).filter(([name]) => name.startsWith('P07_')))));
} else {
  if (fs.readFileSync(process.argv[2], 'utf8') !== 'stub decoded trace') process.exit(1);
  JSON.parse(fs.readFileSync(process.argv[3], 'utf8'));
}
`);
  tool('go', `#!/bin/sh
if [ "$1" = build ]; then cp '${path.join(tools, 'benchmark')}' "$4"; fi
printf 'go version go1.27.1 linux/amd64\\n'
`);
  tool('cc', `#!/bin/sh
for output do :; done
cp '${path.join(tools, 'benchmark')}' "$output"
`);
  const key = path.join(temporary, 'credential');
  const conf = path.join(temporary, 'ceph.conf');
  fs.writeFileSync(key, 'SECRET NEVER CAPTURE');
  fs.writeFileSync(conf, 'PRIVATE CONFIG NEVER CAPTURE');
  const buildCapture = path.join(temporary, 'prepared');
  let counter = 0;
  const execute = (args, cwd = workspace) => {
    const result = spawnSync('sh', ['integration/p07/live-cluster.sh', ...args], {
      cwd, encoding: 'utf8', env: { ...process.env, PATH: `${tools}:${process.env.PATH}`, P07_TRACE_DEBUG_MAX_BUFFER_MB: '256', P07_PRIVATE_TOKEN: 'INHERITED SECRET NEVER CAPTURE' },
    });
    return { ...result, summary: JSON.parse(result.stdout) };
  };
  const prepare = (extra = []) => execute(['--prepare', '--capture', buildCapture, ...extra]);
  const measure = (extra = [], cwd = workspace) => {
    const capture = path.join(temporary, `measurement-${++counter}`);
    return { ...execute(['--capture', capture, '--build-capture', buildCapture, '--binary', path.join(buildCapture, 'build_linuxamd64'),
      '--monitors', '192.0.2.1:3300', '--fsid', '00000000-0000-0000-0000-000000000001', '--key-file', key,
      '--pool', 'dedicated-p07', '--repetitions', '1', '--cases', 'none', ...extra], cwd), capture };
  };
  const update = (name, mutate) => {
    const file = path.join(buildCapture, name);
    const value = JSON.parse(fs.readFileSync(file, 'utf8'));
    mutate(value);
    fs.writeFileSync(file, JSON.stringify(value));
  };
  return { temporary, workspace, buildCapture, key, conf, prepare, measure, execute, update };
}

function rejectedBeforeMeasurement(result, expected) {
  assert.equal(result.status, 1, result.stderr);
  assert.ok(result.summary.results.some(entry => expected.test(entry.error ?? '')), JSON.stringify(result.summary));
  assert.ok(!result.summary.results.some(entry => /^(primary-|closed-|seed$)/.test(entry.name)));
}

for (const observed of [false, true]) {
  test(`actual report schema accepts all four ${observed ? 'instrumented' : 'primary'} cases`, context => {
    const setup = fixture(context);
    assert.equal(setup.prepare().status, 0);
    const result = setup.measure(['--cases', 'none,cpu,alloc,both', ...(observed ? ['--observe'] : [])]);
    assert.equal(result.status, 0, result.stdout + result.stderr);
    assert.equal(result.summary.status, 'passed');
    const label = observed ? 'instrumented' : 'primary';
    const measurement = JSON.parse(fs.readFileSync(path.join(result.capture, 'measurement.json')));
    assert.equal(measurement.label, label);
    assert.deepEqual(measurement.cases, ['none', 'cpu', 'alloc', 'both']);
    for (const loadCase of measurement.cases) {
      const name = `${label}-${loadCase}-1`;
      const output = JSON.parse(fs.readFileSync(path.join(result.capture, `${name}.report.json`)));
      assert.equal(output.valid, true);
      assert.equal(output.observed, observed);
      assert.deepEqual(output.report.environment, { GOOS: 'linux', GOARCH: 'amd64', go_version: 'go1.27.1', gomaxprocs: 10 });
      assert.equal(output.report.offered_load.config.load_case, loadCase);
      assert.equal(output.report.offered_load.config.factorial, true);
      assert.equal(output.report.offered_load.config.instrumented, observed);
      if (observed) {
        const timing = JSON.parse(fs.readFileSync(path.join(result.capture, `${name}.observation`, 'timing.json')));
        assert.equal(timing.schema, 1);
        assert.equal(timing.label, 'instrumented');
        assert.equal(timing.calls.length, output.report.offered_load.attempted);
        assert.equal(result.summary.results.find(entry => entry.name === `${name}-decode`).exit_code, 0);
        const expectedEnv = { P07_TRACE_DEBUG_MAX_BUFFER_MB: '1024' };
        assert.deepEqual(result.summary.results.find(entry => entry.name === `${name}-decode`).environment, expectedEnv);
        assert.deepEqual(JSON.parse(fs.readFileSync(path.join(result.capture, `${name}-decode.stdout`), 'utf8')), expectedEnv);
        assert.equal(result.summary.results.find(entry => entry.name === `${name}-stalls`).exit_code, 0);
      }
    }
    assert.ok(result.summary.results.filter(entry => !entry.name.endsWith('-decode')).every(entry => !Object.hasOwn(entry.environment ?? {}, 'P07_TRACE_DEBUG_MAX_BUFFER_MB')));
    assert.ok(!JSON.stringify(result.summary).includes('INHERITED SECRET NEVER CAPTURE'));
    assert.ok(!result.summary.results.some(entry => entry.name.startsWith(observed ? 'primary-' : 'instrumented-')));
  });
}

for (const limit of ['16', '512', '1024']) {
  test(`trace decoder receives explicit validated ${limit} MiB limit only`, context => {
    const setup = fixture(context);
    assert.equal(setup.prepare().status, 0);
    const result = setup.measure(['--observe', '--trace-debug-max-buffer-mb', limit]);
    assert.equal(result.status, 0, result.stdout + result.stderr);
    const name = 'instrumented-none-1-decode';
    const expectedEnv = { P07_TRACE_DEBUG_MAX_BUFFER_MB: limit };
    const entry = JSON.parse(fs.readFileSync(path.join(result.capture, `${name}.exit.json`)));
    assert.equal(entry.command, process.execPath);
    assert.equal(entry.args[1], '--decode');
    assert.deepEqual(entry.environment, expectedEnv);
    assert.deepEqual(JSON.parse(fs.readFileSync(path.join(result.capture, `${name}.stdout`), 'utf8')), expectedEnv);
    assert.ok(result.summary.results.filter(entry => entry.name !== name).every(entry => !Object.hasOwn(entry.environment ?? {}, 'P07_TRACE_DEBUG_MAX_BUFFER_MB')));
  });
}

for (const limit of ['NaN', '0', '15', '1025', '16.5']) {
  test(`wrapper rejects invalid trace debug limit ${limit} before capture`, context => {
    const setup = fixture(context);
    const result = setup.measure(['--observe', '--trace-debug-max-buffer-mb', limit]);
    rejectedBeforeMeasurement(result, /--trace-debug-max-buffer-mb must be an integer from 16 to 1024/);
    assert.equal(result.summary.capture, null);
    assert.ok(!fs.existsSync(result.capture));
  });
}

test('preparation binds untracked sources, outside-workspace binaries and relocated staged source', context => {
  const setup = fixture(context);
  const prepared = setup.prepare();
  assert.equal(prepared.status, 0, prepared.stdout + prepared.stderr);
  assert.equal(prepared.summary.mode, 'preparation');
  assert.equal(prepared.summary.status, 'passed');
  const source = JSON.parse(fs.readFileSync(path.join(setup.buildCapture, 'source-before.json')));
  assert.ok(source.some(entry => entry.file === 'untracked.go'));
  assert.ok(source.every(entry => !path.isAbsolute(entry.file)));
  assert.ok(!source.some(entry => entry.file.includes('prepared')));
  const staged = path.join(setup.temporary, 'staged');
  fs.cpSync(setup.workspace, staged, { recursive: true });
  const result = setup.measure([], staged);
  assert.equal(result.status, 0, result.stdout + result.stderr);
  const binding = JSON.parse(fs.readFileSync(path.join(result.capture, 'build-binding.json')));
  assert.equal(binding.build_capture, setup.buildCapture);
  assert.equal(binding.source_before, path.join(setup.buildCapture, 'source-before.json'));
  assert.equal(binding.status, 'passed');
});

test('measurement requires build capture and rejects missing manifests', context => {
  const setup = fixture(context);
  const missing = setup.execute(['--capture', path.join(setup.temporary, 'missing')]);
  assert.equal(missing.status, 1);
  assert.match(missing.summary.results[0].error, /missing --build-capture/);
  rejectedBeforeMeasurement(setup.measure(), /ENOENT/);
});

for (const scenario of ['wrong binary', 'changed binary', 'changed source', 'source after drift', 'binary after drift', 'failed preparation', 'measurement capture', 'absolute source paths']) {
  test(`measurement rejects ${scenario} before running workloads`, context => {
    const setup = fixture(context);
    assert.equal(setup.prepare().status, 0);
    let extra = [];
    let expected = /does not match preparation/;
    if (scenario === 'wrong binary') {
      const wrong = path.join(setup.temporary, 'wrong-binary');
      fs.writeFileSync(wrong, '#!/bin/sh\nexit 0\n', { mode: 0o700 });
      extra = ['--binary', wrong];
    } else if (scenario === 'changed binary') {
      fs.appendFileSync(path.join(setup.buildCapture, 'build_linuxamd64'), '\nexit 0\n');
    } else if (scenario === 'changed source') {
      fs.appendFileSync(path.join(setup.workspace, 'untracked.go'), '\nvar changed = true\n');
    } else if (scenario === 'source after drift') {
      setup.update('source-after.json', value => { value[0].sha256 = '0'.repeat(64); });
    } else if (scenario === 'binary after drift') {
      setup.update('binary-after.json', value => { value.sha256 = '0'.repeat(64); });
    } else if (scenario === 'failed preparation') {
      setup.update('summary.json', value => { value.failed = true; value.status = 'failed'; });
      expected = /passed preparation/;
    } else if (scenario === 'measurement capture') {
      setup.update('summary.json', value => { value.mode = 'measurement'; });
      expected = /passed preparation/;
    } else {
      setup.update('source-before.json', value => { value[0].file = path.join(setup.workspace, value[0].file); });
      expected = /invalid preparation source manifest/;
    }
    rejectedBeforeMeasurement(setup.measure(extra), expected);
  });
}

for (const kind of ['file', 'directory', 'credential', 'dangling']) {
  test(`preparation rejects ${kind} source symlinks without capturing target secrets`, context => {
    const setup = fixture(context);
    const target = kind === 'file' ? path.join(setup.workspace, 'untracked.go') : kind === 'directory' ? path.join(setup.workspace, 'integration') : kind === 'credential' ? setup.key : path.join(setup.temporary, 'missing');
    fs.symlinkSync(target, path.join(setup.workspace, 'linked.go'));
    const result = setup.prepare();
    assert.equal(result.status, 1);
    assert.ok(!result.summary.results.some(entry => entry.name === 'build-go'));
    const artifacts = fs.readdirSync(setup.buildCapture).map(name => fs.readFileSync(path.join(setup.buildCapture, name), 'utf8')).join('\n');
    assert.ok(!artifacts.includes('SECRET NEVER CAPTURE'));
  });
}

test('measurement excludes credential symlinks and hardlinks without traversing a secret directory', context => {
  const setup = fixture(context);
  assert.equal(setup.prepare().status, 0);
  fs.symlinkSync(setup.key, path.join(setup.workspace, 'secret.go'));
  fs.linkSync(setup.key, path.join(setup.workspace, 'hardlink.go'));
  const result = setup.measure();
  assert.equal(result.status, 0, result.stdout + result.stderr);
  const serialized = fs.readdirSync(result.capture).map(name => fs.readFileSync(path.join(result.capture, name), 'utf8')).join('\n');
  assert.ok(!serialized.includes('SECRET NEVER CAPTURE'));
  assert.ok(!serialized.includes(setup.key));
  assert.ok(!serialized.includes('secret.go'));
  const secretDirectory = path.join(setup.temporary, 'secrets');
  fs.mkdirSync(secretDirectory);
  fs.writeFileSync(path.join(secretDirectory, 'never-read.go'), 'SECRET NEVER CAPTURE');
  fs.symlinkSync(secretDirectory, path.join(setup.workspace, 'secret-directory'));
  rejectedBeforeMeasurement(setup.measure(), /source symlinks/);
});

test('measurement rejects credential paths inside workspace, including external aliases', context => {
  const setup = fixture(context);
  assert.equal(setup.prepare().status, 0);
  const inside = path.join(setup.workspace, 'custom-key');
  fs.writeFileSync(inside, 'SECRET NEVER CAPTURE');
  rejectedBeforeMeasurement(setup.measure(['--key-file', inside]), /outside the workspace/);
  const alias = path.join(setup.temporary, 'key-alias');
  fs.symlinkSync(inside, alias);
  rejectedBeforeMeasurement(setup.measure(['--key-file', alias]), /outside the workspace/);
});

for (const report of ['not JSON', '{"environment":{"GOOS":"darwin","GOARCH":"amd64","gomaxprocs":10}}', '{"environment":{"GOOS":"linux","GOARCH":"amd64","gomaxprocs":9}}', '{"environment":{"GOOS":"linux","GOARCH":"amd64","GOMAXPROCS":10}}']) {
  test(`closed-loop invalid zero-exit report fails capture: ${report}`, context => {
    const setup = fixture(context, report);
    assert.equal(setup.prepare(['--build-native']).status, 0);
    const result = setup.measure(['--closed-loop', '--entity', 'client.amakura', '--native-binary', path.join(setup.buildCapture, 'native-benchmark'), '--native-conf', setup.conf, '--native-keyring', setup.key]);
    assert.equal(result.status, 1, result.stdout + result.stderr);
    assert.equal(result.summary.status, 'failed');
    assert.equal(result.summary.results.find(entry => entry.name === 'closed-go-1').exit_code, 0);
    assert.equal(result.summary.results.find(entry => entry.name === 'closed-go-1-validation').exit_code, 1);
    assert.ok(result.summary.results.some(entry => entry.name === 'closed-native-1'));
    assert.equal(result.summary.results.find(entry => entry.name === 'closed-native-1').args.at(-1), 'client.amakura');
    const output = JSON.parse(fs.readFileSync(path.join(result.capture, 'closed-go-1.report.json')));
    assert.equal(output.valid, false);
    if (report !== 'not JSON') assert.match(output.error, /report runtime must be Linux amd64 with ten active Ps/);
  });
}