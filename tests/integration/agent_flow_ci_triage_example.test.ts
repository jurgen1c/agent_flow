import { describe, expect, test } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  createAgentFlowFixtureSessionProvider,
  createAgentFlowLifecycleRun,
  createAgentFlowSessionProviderRegistry,
  createAgentFlowWorkflowRegistry,
  executeAgentFlowCommandPipeline,
  openAgentFlowRunState,
  parseAgentFlowSimulationFixture,
  parseAgentFlowWorkflowOrThrow,
  simulateAgentFlowWorkflow,
  validateAgentFlowWorkflow
} from "../../src/runtime";
import { runCli } from "../../src/cli/router";

const repositoryRoot = path.resolve(".");
const examplePath = path.join(repositoryRoot, "examples/workflows/ci-triage.yml");
const fixtureRoot = path.join(repositoryRoot, "examples/fixtures/ci-triage");
const fixtureNames = ["flake", "formatting", "implementation", "environment", "unknown", "requires-user"] as const;

function loadWorkflow() {
  return parseAgentFlowWorkflowOrThrow(fs.readFileSync(examplePath, "utf8"));
}

function loadFixture(name: typeof fixtureNames[number]) {
  const parsed = parseAgentFlowSimulationFixture(
    fs.readFileSync(path.join(fixtureRoot, `${name}.json`), "utf8")
  );
  if (!parsed.ok) throw new Error(parsed.error);
  return parsed.fixture;
}

describe("CI triage nested recovery example", () => {
  test("validates and simulates every documented classification path from example fixtures", () => {
    const workflow = loadWorkflow();
    expect(validateAgentFlowWorkflow(workflow)).toEqual({ valid: true, errors: [] });

    const expected = {
      flake: { status: "completed", steps: ["classify", "route", "return_remediated"], terminal: "remediated" },
      formatting: { status: "completed", steps: ["classify", "route", "fix_formatting", "return_remediated"], terminal: "remediated" },
      implementation: { status: "completed", steps: ["classify", "route", "fix_with_fm", "return_remediated"], terminal: "remediated" },
      environment: { status: "unresolved", steps: ["classify", "route", "return_unresolved"], terminal: "unresolved" },
      unknown: { status: "paused", steps: ["classify", "route"], terminal: "paused" },
      "requires-user": { status: "paused", steps: ["classify", "route", "ask_user"], terminal: "paused" }
    } as const;

    for (const name of fixtureNames) {
      const result = simulateAgentFlowWorkflow(workflow, loadFixture(name));
      expect(result.status, name).toBe(expected[name].status);
      expect(result.visitedSteps.map((step) => step.id), name).toEqual(expected[name].steps);
      expect(result.terminalStates.at(-1)?.status, name).toBe(expected[name].terminal);
    }
  });

  test("runs every path offline through the CLI fixture provider", async () => {
    const root = temporaryRepo();
    fs.cpSync(path.join(repositoryRoot, "examples"), path.join(root, "examples"), { recursive: true });
    fs.mkdirSync(path.join(root, "bin"));
    fs.writeFileSync(path.join(root, "bin", "rubocop"), "#!/bin/sh\nprintf 'formatted\\n'\n");
    fs.chmodSync(path.join(root, "bin", "rubocop"), 0o755);

    const expectedExitCodes = {
      flake: 0,
      formatting: 0,
      implementation: 0,
      environment: 1,
      unknown: 3,
      "requires-user": 3
    } as const;

    for (const name of fixtureNames) {
      const result = await captureCli([
        "run",
        "examples/workflows/ci-triage.yml",
        "--id",
        `ci-triage-${name}`,
        "--fixture",
        `examples/fixtures/ci-triage/${name}.json`
      ], root);
      expect(result.exitCode, `${name}: ${result.stderr}`).toBe(expectedExitCodes[name]);
    }

    expect((await captureCli(["artifacts", "ci-triage-formatting"], root)).stdout)
      .toContain("ci/formatting-fix.log");
    expect((await captureCli(["status", "ci-triage-requires-user"], root)).stdout)
      .toContain("Answer artifact: user-input/ci-decision.md");
  });

  test("uses nested remediated and unresolved results to drive the parent workflow", async () => {
    for (const [name, expectedStatus] of [
      ["flake", "completed"],
      ["implementation", "completed"],
      ["environment", "paused"],
      ["requires-user", "paused"]
    ] as const) {
      const root = temporaryRepo();
      fs.cpSync(path.join(repositoryRoot, "examples"), path.join(root, "examples"), { recursive: true });
      const parent = parseAgentFlowWorkflowOrThrow(`name: parent-${name}
version: 1
style: recovery_pipeline
maturity: experimental
limits: { max_recovery_cycles: 1, max_step_attempts: { ci: 2 } }
steps:
  - id: ci
    type: command
    command: test -f ci-attempted && exit 0; touch ci-attempted; exit 1
    on_failure:
      route_to:
        workflow: ci-triage
        inputs: { failure_payload: "{{ failure.path }}", failed_step: "{{ step.id }}" }
      on_remediated: { return_to: ci }
      on_unresolved: { then: pause }
`);
      const fixture = loadFixture(name);
      const responses = Object.fromEntries(Object.entries(fixture.steps ?? {}).flatMap(([stepId, step]) => {
        if (step.outputs === undefined || Array.isArray(step.outputs)) return [];
        return [[stepId, {
          outputs: Object.fromEntries(Object.entries(step.outputs).map(([key, value]) => [
            key,
            typeof value === "string" ? value : JSON.stringify(value)
          ]))
        }]];
      }));
      const providers = createAgentFlowSessionProviderRegistry().register(
        "fixture",
        createAgentFlowFixtureSessionProvider(responses)
      );
      const store = await openAgentFlowRunState({ cwd: root });
      createAgentFlowLifecycleRun(store, { id: `parent-${name}`, workflow: parent });

      const result = await executeAgentFlowCommandPipeline(
        store,
        `parent-${name}`,
        parent,
        undefined,
        providers,
        undefined,
        undefined,
        createAgentFlowWorkflowRegistry().register("ci-triage", loadWorkflow())
      );

      expect(result.status).toBe(expectedStatus);
      expect(store.listEvents(`parent-${name}`).map((event) => event.type)).toEqual(expect.arrayContaining([
        "recovery.routed",
        "recovery.completed"
      ]));
      if (name === "implementation") {
        expect(store.readArtifact(`parent-${name}`, "implementation-summary.md").content.toString())
          .toBe("Fixture FM applied the smallest safe implementation fix.\n");
      }
      store.close();
    }
  });
});

function temporaryRepo(): string {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "agent-flow-ci-triage-"));
  fs.mkdirSync(path.join(root, ".git"));
  return root;
}

async function captureCli(args: string[], cwd: string): Promise<{ exitCode: number; stdout: string; stderr: string }> {
  let stdout = "";
  let stderr = "";
  const exitCode = await runCli(args, {
    stdout: { write: (chunk: string) => { stdout += chunk; return true; } },
    stderr: { write: (chunk: string) => { stderr += chunk; return true; } }
  }, { cwd });
  return { exitCode, stdout, stderr };
}
