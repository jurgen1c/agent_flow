#!/usr/bin/env node
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const repositoryRoot = path.resolve(fileURLToPath(new URL("..", import.meta.url)));
const temporaryRoot = fs.mkdtempSync(path.join(os.tmpdir(), "agent-flow-package-"));
const coreTarball = coreTarballArgument(process.argv.slice(2));

try {
  run("bun", ["run", "build"]);
  const dryRun = run("npm", [
    "pack",
    "--dry-run",
    "--json",
    "--ignore-scripts",
    "--pack-destination",
    temporaryRoot
  ]);
  const manifest = parsePackOutput(dryRun)[0];
  const files = manifest.files.map((entry) => entry.path);

  for (const required of [
    "package.json",
    "README.md",
    "LICENSE",
    "dist/index.js",
    "dist/index.d.ts",
    "dist/cli/router.js",
    "dist/cli/router.d.ts",
    "dist/agent-flow.js",
    "schemas/config.schema.json",
    "schemas/workflow.schema.json"
  ]) {
    if (!files.includes(required)) fail(`Packed artifact is missing ${required}.`);
  }

  const forbidden = files.filter((file) =>
    /(^|\/)(?:src|tests?|fixtures?|examples?|coverage|node_modules|\.git)(?:\/|$)/i.test(file)
    || /(^|\/)\.env(?:\.|$)/i.test(file)
    || /\.(?:sqlite|db|pem|key|log)$/i.test(file)
  );
  if (forbidden.length > 0) {
    fail(`Packed artifact contains forbidden files:\n${forbidden.join("\n")}`);
  }

  const packedPackage = JSON.parse(
    fs.readFileSync(path.join(repositoryRoot, "package.json"), "utf8")
  );
  if (
    Object.values(packedPackage.dependencies ?? {}).some(
      (range) => typeof range === "string" && /^(?:workspace:|file:)/.test(range)
    )
  ) {
    fail("Published dependencies must use registry semver ranges.");
  }

  const packed = parsePackOutput(
    run("npm", [
      "pack",
      "--json",
      "--ignore-scripts",
      "--pack-destination",
      temporaryRoot
    ])
  )[0];
  const tarballPath = path.join(temporaryRoot, packed.filename);
  const consumerRoot = path.join(temporaryRoot, "consumer");
  fs.mkdirSync(consumerRoot);
  fs.writeFileSync(
    path.join(consumerRoot, "package.json"),
    `${JSON.stringify({ private: true, type: "module" }, null, 2)}\n`
  );
  fs.writeFileSync(
    path.join(consumerRoot, "smoke.mjs"),
    `import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import {
  openAgentFlowRunState,
  parseAgentFlowWorkflowOrThrow,
  validateAgentFlowWorkflow
} from "@jurgen1c/agent-flow";
import { dispatch } from "@jurgen1c/agent-flow/cli";

const workflow = parseAgentFlowWorkflowOrThrow(
  "name: smoke\\nversion: 1\\nstyle: pipeline\\nmaturity: stable\\nsteps: []\\n"
);
if (!validateAgentFlowWorkflow(workflow).valid) {
  throw new Error("Agent Flow root API smoke test failed.");
}
if (!dispatch(["help"]).stdout?.includes("agent-flow")) {
  throw new Error("Agent Flow CLI subpath smoke test failed.");
}
const repository = fs.mkdtempSync(path.join(os.tmpdir(), "agent-flow-smoke-"));
fs.mkdirSync(path.join(repository, ".git"));
const store = await openAgentFlowRunState({ cwd: repository });
if (!store.databasePath.endsWith(path.join(".agent-flow", "agent-flow.sqlite"))) {
  throw new Error("Agent Flow state path smoke test failed.");
}
store.close();
const binary = path.join(
  process.cwd(),
  "node_modules",
  "@jurgen1c",
  "agent-flow",
  "dist",
  "agent-flow.js"
);
for (const args of [["help"], ["--version"]]) {
  const result = spawnSync(process.execPath, [binary, ...args], { encoding: "utf8" });
  if (result.status !== 0 || !result.stdout.includes(args[0] === "help" ? "agent-flow" : "0.1.0")) {
    throw new Error(\`Agent Flow binary smoke test failed for \${args.join(" ")}: \${result.stderr}\`);
  }
}
console.log("Agent Flow tarball smoke test passed.");
`
  );

  const installTargets = coreTarball === null
    ? [tarballPath]
    : [path.resolve(coreTarball), tarballPath];
  run("npm", [
    "install",
    "--no-audit",
    "--no-fund",
    "--ignore-scripts",
    ...installTargets
  ], consumerRoot);
  run("npm", ["audit", "--audit-level", "moderate"], consumerRoot);
  run(process.execPath, ["smoke.mjs"], consumerRoot);

  console.log(
    `Verified ${manifest.name}@${manifest.version}: ${files.length} packed files and a clean consumer install.`
  );
} finally {
  fs.rmSync(temporaryRoot, { recursive: true, force: true });
}

function coreTarballArgument(args) {
  if (args.length === 0) return null;
  if (args.length === 2 && args[0] === "--core-tarball") return args[1];
  fail("Usage: verify-package.mjs [--core-tarball <path>]");
}

function run(command, args, cwd = repositoryRoot) {
  const result = spawnSync(command, args, {
    cwd,
    encoding: "utf8",
    env: {
      ...process.env,
      npm_config_cache: path.join(temporaryRoot, "npm-cache")
    }
  });

  if (result.error || result.status !== 0) {
    if (result.error) console.error(`${command} failed to start: ${result.error.message}`);
    process.stderr.write(result.stdout ?? "");
    process.stderr.write(result.stderr ?? "");
    process.exit(result.status ?? 1);
  }

  return result.stdout.length > 0 ? result.stdout : result.stderr;
}

function parsePackOutput(output) {
  try {
    return JSON.parse(output);
  } catch {
    fail(`Could not parse npm pack output:\n${output}`);
  }
}

function fail(message) {
  console.error(message);
  process.exit(1);
}
