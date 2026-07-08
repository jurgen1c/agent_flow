import { describe, expect, test } from "bun:test";
import fs from "node:fs";
import path from "node:path";

const repoRoot = path.resolve(".");
const docPath = path.join(repoRoot, "docs/features/agentflow-monorepo-architecture.md");
const gitignorePath = path.join(repoRoot, ".gitignore");

describe("Agentflow monorepo architecture doc", () => {
  test("covers AM-6 package boundaries and compatibility rules", () => {
    const doc = fs.readFileSync(docPath, "utf8");

    for (const required of [
      "@jurgen1c/agent-memory-cli",
      "@jurgen1c/agent-memory-core",
      "@jurgen1c/agent-memory-schemas",
      "@jurgen1c/agentflow",
      "@jurgen1c/agentflow-core",
      "@jurgen1c/agentflow-cli",
      "@jurgen1c/agentflow-schemas",
      "@jurgen1c/agentflow-agent-memory-adapter",
      "@jurgen1c/agentflow-examples",
      "@jurgen1c/agent-tools",
      "@jurgen1c/agent-memory-cli-workspace",
      "root `package.json` publishes the package",
      "packages/agent-tools",
      "packages/agentflow-core",
      "packages/agentflow-cli",
      "packages/agentflow-schemas",
      "packages/agentflow-agent-memory-adapter",
      "packages/web"
    ]) {
      expect(doc).toContain(required);
    }

    expect(doc).toContain("Agent Memory must not import Agentflow runtime code");
    expect(doc).toContain("@jurgen1c/agent-tools -> no Agent Memory or Agentflow runtime dependency");
    expect(doc).toContain("intentionally limited to help and");
    expect(doc).toContain(".agent-memory/memory.sqlite");
    expect(doc).toContain(".agentflow/");
    expect(doc).toContain("The existing CLI remains the compatibility anchor");
    expect(doc).toContain("Implementation Order");
    expect(doc).toContain("Release Compatibility");
  });

  test("keeps documented generated Agentflow state out of commits", () => {
    const gitignore = fs.readFileSync(gitignorePath, "utf8");

    expect(gitignore).toContain(".agentflow/runs/");
  });
});
