import { describe, expect, test } from "bun:test";
import fs from "node:fs";
import path from "node:path";

const repositoryRoot = path.join(import.meta.dir, "..");

describe("standalone Agent Flow architecture", () => {
  test("publishes one registry-safe package that consumes Agent Core", () => {
    const packageJson = JSON.parse(
      fs.readFileSync(path.join(repositoryRoot, "package.json"), "utf8")
    ) as {
      name: string;
      bin?: Record<string, string>;
      dependencies?: Record<string, string>;
      engines?: { node?: string };
      exports?: Record<string, unknown>;
    };

    expect(packageJson.name).toBe("@jurgen1c/agent-flow");
    expect(packageJson.bin).toEqual({ "agent-flow": "dist/agent-flow.js" });
    expect(packageJson.dependencies).toEqual({
      "@jurgen1c/agent-core": "^0.2.0",
      "smol-toml": "^1.8.0"
    });
    expect(packageJson.engines?.node).toBe(">=25.9.0");
    expect(Object.keys(packageJson.exports ?? {}).sort()).toEqual([
      ".",
      "./cli",
      "./schemas/approval",
      "./schemas/challenge",
      "./schemas/config",
      "./schemas/consult",
      "./schemas/decision-record",
      "./schemas/disagreement",
      "./schemas/failure-classification",
      "./schemas/review",
      "./schemas/workflow"
    ]);
    for (const range of Object.values(packageJson.dependencies ?? {})) {
      expect(range).not.toMatch(/^(?:workspace:|file:)/);
    }
  });

  test("provisions the native provider sandbox before release verification", () => {
    const publishWorkflow = fs.readFileSync(
      path.join(repositoryRoot, ".github/workflows/publish.yml"),
      "utf8"
    );
    const sandboxInstall = publishWorkflow.indexOf("sudo apt-get install --yes bubblewrap util-linux");
    const releaseCi = publishWorkflow.indexOf("- run: bun run ci");

    expect(sandboxInstall).toBeGreaterThan(-1);
    expect(releaseCi).toBeGreaterThan(sandboxInstall);
    expect(publishWorkflow).toContain("kernel.apparmor_restrict_unprivileged_userns=0");
  });

  test("publishes consult status and blocking pairs consistently", () => {
    const schema = JSON.parse(
      fs.readFileSync(path.join(repositoryRoot, "schemas/consult.schema.json"), "utf8")
    ) as { oneOf?: Array<{ properties?: Record<string, { const?: unknown }> }> };

    expect(schema.oneOf?.map((entry) => ({
      status: entry.properties?.status?.const,
      blocking: entry.properties?.blocking?.const
    }))).toEqual([
      { status: "advice", blocking: false },
      { status: "blocked", blocking: true }
    ]);
  });

  test("publishes the durable disagreement result contract", () => {
    const schema = JSON.parse(
      fs.readFileSync(path.join(repositoryRoot, "schemas/disagreement.schema.json"), "utf8")
    ) as {
      required?: string[];
      oneOf?: Array<{ properties?: Record<string, { const?: unknown }>; required?: string[]; not?: { required?: string[] } }>;
    };

    expect(schema.required).toEqual(["status", "rationale"]);
    expect(schema.oneOf).toEqual([
      {
        properties: { status: { const: "resolved" } },
        required: ["decision"]
      },
      {
        properties: { status: { const: "unresolved" } },
        not: { required: ["decision"] }
      }
    ]);
  });

  test("contains no Agent Memory implementation, dependency, or adapter", () => {
    const files = sourceFiles(path.join(repositoryRoot, "src"));
    const source = files.map((file) => fs.readFileSync(file, "utf8")).join("\n");

    expect(source).not.toMatch(/@jurgen1c\/agent-memory/);
    expect(source).not.toMatch(/agent[-_ ]memory[-_ ]adapter/i);
    expect(fs.existsSync(path.join(repositoryRoot, "packages"))).toBe(false);

    const lockfile = fs.readFileSync(path.join(repositoryRoot, "bun.lock"), "utf8");
    expect(lockfile).not.toMatch(/agent-memory|file:\/|workspace:|\/tmp\//i);
  });

  test("uses Agent Core for YAML, repository safety, and SQLite", () => {
    const source = sourceFiles(path.join(repositoryRoot, "src"))
      .map((file) => fs.readFileSync(file, "utf8"))
      .join("\n");

    expect(source).toContain("@jurgen1c/agent-core/yaml");
    expect(source).toContain("@jurgen1c/agent-core/repository");
    expect(source).toContain("@jurgen1c/agent-core/sqlite");
    expect(source).not.toMatch(/from ["']yaml["']/);
  });
});

function sourceFiles(directory: string): string[] {
  return fs.readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const candidate = path.join(directory, entry.name);
    if (entry.isDirectory()) return sourceFiles(candidate);
    return entry.isFile() && entry.name.endsWith(".ts") ? [candidate] : [];
  });
}
