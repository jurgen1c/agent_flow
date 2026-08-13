import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import {
  findGitRepositoryRoot,
  nearestExistingAncestor,
  resolveContainedPath
} from "@jurgen1c/agent-core/repository";

export const bundledAgentFlowSkillNames = [
  "workflow-designer",
  "pipeline-designer",
  "recovery-designer",
  "collaboration-designer",
  "workflow-reviewer",
  "workflow-debugger",
  "workflow-simplifier",
  "policy-author"
] as const;

export type AgentFlowSkillDestination = "agents" | "codex";

export interface InstallAgentFlowSkillsOptions {
  destination: AgentFlowSkillDestination;
  cwd?: string;
  env?: Readonly<Record<string, string | undefined>>;
  homeDir?: string;
  sourceRoot?: string;
  copyDirectory?: (source: string, destination: string) => void;
}

export interface InstalledAgentFlowSkills {
  destinationRoot: string;
  skills: readonly string[];
}

interface AgentFlowSkillsDestination {
  containmentRoot: string;
  destinationRoot: string;
}

export function installAgentFlowSkills(options: InstallAgentFlowSkillsOptions): InstalledAgentFlowSkills {
  const sourceRoot = options.sourceRoot ?? resolveBundledSkillsRoot();
  const copyDirectory = options.copyDirectory ?? copySkillDirectory;
  const { containmentRoot, destinationRoot } = resolveAgentFlowSkillsDestinationDetails(options);
  const targets = bundledAgentFlowSkillNames.map((name) => ({
    name,
    source: path.join(sourceRoot, name),
    destination: path.join(destinationRoot, name)
  }));

  for (const target of targets) {
    if (!fs.statSync(path.join(target.source, "SKILL.md"), { throwIfNoEntry: false })?.isFile()) {
      throw new Error(`Bundled Agent Flow skill ${target.name} is incomplete.`);
    }
  }

  const destinationExisted = pathExists(destinationRoot);
  assertSafeDestinationPath(containmentRoot, destinationRoot);
  fs.mkdirSync(destinationRoot, { recursive: true });
  assertSafeDestinationPath(containmentRoot, destinationRoot);

  const stagingRoot = path.join(destinationRoot, `.agent-flow-install-${randomUUID()}`);
  const published: string[] = [];
  try {
    preflightSkillTargets(targets);
    fs.mkdirSync(stagingRoot);
    for (const target of targets) {
      copyDirectory(target.source, path.join(stagingRoot, target.name));
    }

    assertSafeDestinationPath(containmentRoot, destinationRoot);
    preflightSkillTargets(targets);
    for (const target of targets) {
      fs.renameSync(path.join(stagingRoot, target.name), target.destination);
      published.push(target.destination);
    }
  } catch (error) {
    for (const publishedPath of published.reverse()) {
      fs.rmSync(publishedPath, { recursive: true, force: true });
    }
    throw error;
  } finally {
    fs.rmSync(stagingRoot, { recursive: true, force: true });
    if (!destinationExisted && pathExists(destinationRoot)) {
      removeEmptyDirectory(destinationRoot);
    }
  }

  return { destinationRoot, skills: bundledAgentFlowSkillNames };
}

export function resolveAgentFlowSkillsDestination(options: InstallAgentFlowSkillsOptions): string {
  return resolveAgentFlowSkillsDestinationDetails(options).destinationRoot;
}

function resolveAgentFlowSkillsDestinationDetails(
  options: InstallAgentFlowSkillsOptions
): AgentFlowSkillsDestination {
  if (options.destination === "agents") {
    const start = path.resolve(options.cwd ?? process.cwd());
    const repositoryRoot = findGitRepositoryRoot(start);
    if (repositoryRoot === null) {
      throw new Error(`Could not find a Git repository from ${start}.`);
    }
    return {
      containmentRoot: repositoryRoot,
      destinationRoot: path.join(repositoryRoot, ".agents", "skills")
    };
  }

  const env = { ...process.env, ...options.env };
  const configuredCodexHome = env.CODEX_HOME?.trim();
  const codexHome = configuredCodexHome
    ? path.resolve(configuredCodexHome)
    : path.join(options.homeDir ?? os.homedir(), ".codex");
  const containmentRoot = nearestExistingAncestor(codexHome);
  if (containmentRoot === null) {
    throw new Error(`Could not find an existing parent for Codex home ${codexHome}.`);
  }
  return {
    containmentRoot,
    destinationRoot: path.join(codexHome, "skills")
  };
}

function assertSafeDestinationPath(containmentRoot: string, destinationRoot: string): void {
  resolveContainedPath(containmentRoot, destinationRoot, {
    rejectFinalSymlink: true,
    rejectSymlinkComponents: true
  });
}

function preflightSkillTargets(
  targets: ReadonlyArray<{ destination: string }>
): void {
  for (const target of targets) {
    if (pathExists(target.destination)) {
      throw new Error(`Refusing to replace existing skill destination: ${target.destination}`);
    }
  }
}

function pathExists(candidate: string): boolean {
  try {
    fs.lstatSync(candidate);
    return true;
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (code === "ENOENT" || code === "ENOTDIR") return false;
    throw error;
  }
}

function removeEmptyDirectory(directory: string): void {
  try {
    fs.rmdirSync(directory);
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (code !== "ENOENT" && code !== "ENOTEMPTY") throw error;
  }
}

function copySkillDirectory(source: string, destination: string): void {
  fs.cpSync(source, destination, {
    recursive: true,
    errorOnExist: true,
    force: false
  });
}

function resolveBundledSkillsRoot(): string {
  const candidates = [
    fileURLToPath(new URL("../skills/", import.meta.url)),
    fileURLToPath(new URL("../../skills/", import.meta.url))
  ];
  const root = candidates.find((candidate) => fs.existsSync(path.join(candidate, "workflow-designer", "SKILL.md")));
  if (!root) throw new Error("Could not locate the bundled Agent Flow skills.");
  return root;
}
