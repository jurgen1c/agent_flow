import fs from "node:fs";
import { plannedAgentflowRuntimeCommands } from "@jurgen1c/agentflow-core";

export interface AgentflowCliStreams {
  stdout: Pick<NodeJS.WriteStream, "write">;
  stderr: Pick<NodeJS.WriteStream, "write">;
}

export interface AgentflowCliResult {
  exitCode: number;
  stdout?: string;
  stderr?: string;
}

export async function runCli(args: string[], streams: AgentflowCliStreams = process): Promise<number> {
  const result = dispatch(args);

  if (result.stdout) {
    streams.stdout.write(result.stdout.endsWith("\n") ? result.stdout : `${result.stdout}\n`);
  }

  if (result.stderr) {
    streams.stderr.write(result.stderr.endsWith("\n") ? result.stderr : `${result.stderr}\n`);
  }

  return result.exitCode;
}

export function dispatch(args: string[]): AgentflowCliResult {
  const [command, ...rest] = args;

  if (!command || command === "help" || command === "--help" || command === "-h") {
    return {
      exitCode: 0,
      stdout: renderHelp(command === "help" ? rest[0] : undefined)
    };
  }

  if (command === "--version" || command === "-v" || command === "version") {
    return {
      exitCode: 0,
      stdout: `agentflow ${readRootPackageVersion()}`
    };
  }

  if (isPlannedRuntimeCommand(command)) {
    return {
      exitCode: 7,
      stderr: `Agentflow command "${command}" is reserved but not active yet.\nOnly help and version are available in this skeleton.`
    };
  }

  return {
    exitCode: 7,
    stderr: `Unknown Agentflow command: ${command}\nRun \`agentflow help\` to see available commands.`
  };
}

function renderHelp(topic?: string): string {
  if (topic && topic !== "help" && topic !== "version") {
    return [
      `agentflow ${topic}`,
      "",
      "This command name is reserved for a future Agentflow runtime surface.",
      "Only help and version are active in this skeleton."
    ].join("\n");
  }

  return [
    "Agentflow",
    "",
    "Usage:",
    "  agentflow help",
    "  agentflow --version",
    "",
    "Available now:",
    "  help       Show this help output.",
    "  version    Print the Agentflow package version.",
    "",
    "Reserved placeholders:",
    `  ${plannedAgentflowRuntimeCommands.join(", ")}`,
    "",
    "No workflow execution commands are active yet."
  ].join("\n");
}

function isPlannedRuntimeCommand(command: string): boolean {
  return plannedAgentflowRuntimeCommands.includes(command as (typeof plannedAgentflowRuntimeCommands)[number]);
}

function readRootPackageVersion(): string {
  const candidates = [
    new URL("../package.json", import.meta.url),
    new URL("../../../package.json", import.meta.url)
  ];

  for (const packageUrl of candidates) {
    try {
      const packageJson = JSON.parse(fs.readFileSync(packageUrl, "utf8")) as { name?: unknown; version?: unknown };

      if (packageJson.name === "@jurgen1c/agent-memory-cli" && typeof packageJson.version === "string" && packageJson.version.length > 0) {
        return packageJson.version;
      }
    } catch {
      // Try the next source/bundled package.json location.
    }
  }

  return "0.0.0";
}
