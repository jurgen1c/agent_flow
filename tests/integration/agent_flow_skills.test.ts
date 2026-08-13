import { describe, expect, test } from "bun:test";
import fs from "node:fs";
import path from "node:path";
import { dispatch } from "../../src/cli/router";
import { bundledAgentFlowSkillNames } from "../../src/skills";
import {
  createAgentFlowLifecycleRun,
  executeAgentFlowCommandPipeline,
  openAgentFlowRunState,
  parseAgentFlowWorkflowOrThrow,
  validateAgentFlowWorkflow
} from "../../src/runtime";

const repoRoot = path.resolve(".");
const skillsRoot = path.join(repoRoot, "skills");

describe("Agent Flow bundled skills", () => {
  test("lists all authoring and review skills", () => {
    expect(dispatch(["skills", "list"])).toEqual({
      exitCode: 0,
      stdout: bundledAgentFlowSkillNames.join("\n")
    });
    expect(dispatch(["help", "skills"]).stdout).toContain(
      "agent-flow skills install --destination <agents|codex>"
    );
  });

  test("installs skills into a repository-local agents directory", () => {
    const repository = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-skills-agents-"));
    fs.mkdirSync(path.join(repository, ".git"));

    const result = dispatch(
      ["skills", "install", "--destination", "agents"],
      { cwd: repository }
    );

    expect(result).toEqual({
      exitCode: 0,
      stdout: `Installed 8 Agent Flow skills to ${path.join(repository, ".agents", "skills")}.`
    });
    for (const name of bundledAgentFlowSkillNames) {
      expect(fs.statSync(path.join(repository, ".agents", "skills", name, "SKILL.md")).isFile()).toBe(true);
      expect(fs.statSync(path.join(repository, ".agents", "skills", name, "agents", "openai.yaml")).isFile()).toBe(true);
    }
  });

  test("anchors an agents installation at the repository root", () => {
    const repository = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-skills-root-"));
    const nestedDirectory = path.join(repository, "packages", "app");
    fs.mkdirSync(path.join(repository, ".git"));
    fs.mkdirSync(nestedDirectory, { recursive: true });

    const result = dispatch(
      ["skills", "install", "--destination", "agents"],
      { cwd: nestedDirectory }
    );

    expect(result.exitCode).toBe(0);
    expect(result.stdout).toContain(path.join(repository, ".agents", "skills"));
    expect(fs.existsSync(path.join(nestedDirectory, ".agents"))).toBe(false);
    expect(fs.statSync(path.join(repository, ".agents", "skills", "policy-author", "SKILL.md")).isFile()).toBe(true);
  });

  test("installs skills into an explicit Codex home", () => {
    const codexHome = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-skills-codex-"));

    const result = dispatch(
      ["skills", "install", "--destination", "codex"],
      { env: { CODEX_HOME: codexHome } }
    );

    expect(result.exitCode).toBe(0);
    expect(result.stdout).toContain(path.join(codexHome, "skills"));
    expect(fs.statSync(path.join(codexHome, "skills", "workflow-reviewer", "SKILL.md")).isFile()).toBe(true);
  });

  test("preflights all targets and preserves an existing skill", () => {
    const repository = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-skills-existing-"));
    fs.mkdirSync(path.join(repository, ".git"));
    const existing = path.join(repository, ".agents", "skills", "pipeline-designer");
    fs.mkdirSync(existing, { recursive: true });
    fs.writeFileSync(path.join(existing, "marker.txt"), "owned by user\n");

    const result = dispatch(
      ["skills", "install", "--destination", "agents"],
      { cwd: repository }
    );

    expect(result.exitCode).toBe(2);
    expect(result.stderr).toContain("Refusing to replace existing skill destination");
    expect(fs.readFileSync(path.join(existing, "marker.txt"), "utf8")).toBe("owned by user\n");
    expect(fs.existsSync(path.join(repository, ".agents", "skills", "workflow-designer"))).toBe(false);
  });

  test("rejects repository-local installation through a symlinked skills directory", () => {
    const repository = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-skills-link-"));
    const outside = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-skills-outside-"));
    fs.mkdirSync(path.join(repository, ".git"));
    fs.mkdirSync(path.join(repository, ".agents"));
    fs.symlinkSync(outside, path.join(repository, ".agents", "skills"), "dir");

    const result = dispatch(
      ["skills", "install", "--destination", "agents"],
      { cwd: repository }
    );

    expect(result.exitCode).toBe(2);
    expect(result.stderr).toContain("symbolic-link components");
    expect(fs.readdirSync(outside)).toEqual([]);
  });

  test("removes staged files when copying a skill fails", () => {
    const repository = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-skills-rollback-"));
    const sourceRoot = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-skills-source-"));
    fs.mkdirSync(path.join(repository, ".git"));

    for (const name of bundledAgentFlowSkillNames) {
      const source = path.join(sourceRoot, name);
      fs.mkdirSync(source);
      fs.writeFileSync(path.join(source, "SKILL.md"), `---\nname: ${name}\n---\n`);
    }
    const unreadable = path.join(sourceRoot, "policy-author", "unreadable.txt");
    fs.writeFileSync(unreadable, "cannot copy\n", { mode: 0 });

    try {
      const result = dispatch(
        ["skills", "install", "--destination", "agents"],
        { cwd: repository, skillsSourceRoot: sourceRoot }
      );

      expect(result.exitCode).toBe(2);
      const destinationRoot = path.join(repository, ".agents", "skills");
      expect(bundledAgentFlowSkillNames.some((name) => pathExists(path.join(destinationRoot, name)))).toBe(false);
      expect(pathExists(destinationRoot)
        ? fs.readdirSync(destinationRoot).filter((entry) => entry.startsWith(".agent-flow-install-"))
        : []).toEqual([]);
    } finally {
      fs.chmodSync(unreadable, 0o600);
    }
  });

  test("keeps every skill concise, reference-backed, and non-executing", () => {
    for (const name of bundledAgentFlowSkillNames) {
      const skill = fs.readFileSync(path.join(skillsRoot, name, "SKILL.md"), "utf8");
      const metadata = fs.readFileSync(path.join(skillsRoot, name, "agents", "openai.yaml"), "utf8");
      const references = fs.readdirSync(path.join(skillsRoot, name, "references"));

      expect(skill.split("\n").length).toBeLessThan(40);
      expect(skill).toContain("references/");
      expect(skill).toContain("Never execute a workflow.");
      expect(skill).toContain("Do not invoke `run`, `resume`, `inject`");
      expect(metadata).toContain(`$${name}`);
      expect(references.length).toBeGreaterThan(0);
    }
  });

  test("makes the workflow reviewer cover the required defect classes", () => {
    const reviewText = [
      fs.readFileSync(path.join(skillsRoot, "workflow-reviewer", "SKILL.md"), "utf8"),
      fs.readFileSync(path.join(skillsRoot, "workflow-reviewer", "references", "review-checklist.md"), "utf8")
    ].join("\n").toLowerCase();

    expect(reviewText).toContain("schema");
    expect(reviewText).toContain("dead paths");
    expect(reviewText).toContain("unsafe policy");
    expect(reviewText).toContain("ownership");
  });

  test("ships a pipeline example accepted by the validator and offline runtime", async () => {
    const reference = fs.readFileSync(
      path.join(skillsRoot, "pipeline-designer", "references", "pipeline-patterns.md"),
      "utf8"
    );
    const example = /```yaml\n([\s\S]*?)```/.exec(reference)?.[1];

    expect(example).toBeDefined();
    const workflow = parseAgentFlowWorkflowOrThrow(example!);
    expect(validateAgentFlowWorkflow(workflow).errors).toEqual([]);

    const repository = fs.mkdtempSync(path.join(process.env.TMPDIR ?? "/tmp", "agent-flow-skill-example-"));
    fs.mkdirSync(path.join(repository, ".git"));
    const store = await openAgentFlowRunState({ cwd: repository });
    try {
      createAgentFlowLifecycleRun(store, { id: "skill-example", workflow });
      const result = await executeAgentFlowCommandPipeline(store, "skill-example", workflow);

      expect(result.status).toBe("completed");
      expect(store.readArtifact("skill-example", "summaries/check.txt").content.toString()).toBe("1\n");
    } finally {
      store.close();
    }
  });
});

function pathExists(candidate: string): boolean {
  try {
    fs.lstatSync(candidate);
    return true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
    throw error;
  }
}
