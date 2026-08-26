import { describe, expect, test } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { withAgentFlowWorkspaceWriteLock } from "../../src/runtime/workspace_lock";

describe("Agent Flow workspace write lock", () => {
  test("hardens an existing group- or world-writable runtime directory before locking", async () => {
    if (process.platform !== "linux" || !fs.existsSync("/usr/bin/flock")) return;

    for (const mode of [0o720, 0o702]) {
      const repoRoot = fs.mkdtempSync(path.join(os.tmpdir(), "agent-flow-workspace-lock-"));
      try {
        const runtimeDirectory = path.join(repoRoot, ".agent-flow");
        fs.mkdirSync(runtimeDirectory, { mode: 0o700 });
        fs.chmodSync(runtimeDirectory, mode);
        let invoked = false;

        await expect(withAgentFlowWorkspaceWriteLock(
          repoRoot,
          new AbortController().signal,
          async () => {
            invoked = true;
          },
          { required: true }
        )).resolves.toBeUndefined();
        expect(invoked).toBe(true);
        expect(fs.statSync(runtimeDirectory).mode & 0o777).toBe(0o700);
      } finally {
        fs.rmSync(repoRoot, { recursive: true, force: true });
      }
    }
  });
});
