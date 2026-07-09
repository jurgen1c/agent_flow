export interface AgentflowCliStreams {
  stdout: { write(chunk: string): unknown };
  stderr: { write(chunk: string): unknown };
}

export interface AgentflowCliResult {
  exitCode: number;
  stdout?: string;
  stderr?: string;
}

export function runCli(args: string[], streams?: AgentflowCliStreams): Promise<number>;

export function dispatch(args: string[]): AgentflowCliResult;
