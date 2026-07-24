import { spawnSync } from "node:child_process";
import type { AgentflowRunStateStore, AgentflowRunStatus } from "./run_state";
import type { AgentflowWorkflow, AgentflowYamlMapping, AgentflowYamlValue } from "./workflow";

export type AgentflowNotificationEvent =
  | "workflow.completed"
  | "workflow.failed"
  | "workflow.paused";

export interface AgentflowNotification {
  runId: string;
  workflowName: string;
  event: AgentflowNotificationEvent;
  channel: string;
  title: string;
  message: string;
  required: boolean;
}

export type AgentflowNotificationAdapter = (notification: AgentflowNotification) => undefined;

export interface AgentflowNotificationDeliveryResult {
  requiredFailure?: {
    channel: string;
    event: AgentflowNotificationEvent;
    message: string;
  };
}

export interface AgentflowNotificationIssue {
  code: string;
  message: string;
  path: string;
}

export class AgentflowNotificationRegistry {
  private readonly adapters = new Map<string, AgentflowNotificationAdapter>();

  register(channel: string, adapter: AgentflowNotificationAdapter): this {
    const normalized = nonEmptyString(channel);
    if (normalized === undefined) throw new Error("Notification channel names must be non-empty strings.");
    this.adapters.set(normalized, adapter);
    return this;
  }

  get(channel: string): AgentflowNotificationAdapter | undefined {
    return this.adapters.get(channel.trim());
  }
}

export function createAgentflowNotificationRegistry(
  adapters: {
    terminal?: AgentflowNotificationAdapter;
    system?: AgentflowNotificationAdapter;
  } = {}
): AgentflowNotificationRegistry {
  return new AgentflowNotificationRegistry()
    .register("terminal", adapters.terminal ?? terminalNotificationAdapter)
    .register("system", adapters.system ?? systemNotificationAdapter);
}

export function validateAgentflowNotifications(workflow: AgentflowWorkflow): AgentflowNotificationIssue[] {
  if (workflow.style !== "pipeline") return [];
  if (workflow.notify !== undefined && !Array.isArray(workflow.notify)) {
    return [issue(
      "workflow.notification.rules.invalid",
      "notify",
      "Pipeline notifications must be a list."
    )];
  }
  const errors: AgentflowNotificationIssue[] = [];
  for (const [index, value] of (workflow.notify ?? []).entries()) {
    const path = `notify[${index}]`;
    const rule = mapping(value);
    if (rule === undefined) {
      errors.push(issue(
        "workflow.notification.rule.invalid",
        path,
        "Notification rules must be mappings."
      ));
      continue;
    }

    const event = nonEmptyString(rule.on);
    if (event === undefined || !NOTIFICATION_EVENTS.has(event as AgentflowNotificationEvent)) {
      errors.push(issue(
        "workflow.notification.event.unsupported",
        `${path}.on`,
        "Notification on must be workflow.completed, workflow.failed, or workflow.paused."
      ));
    }

    const channels = stringList(rule.channels);
    if (!Array.isArray(rule.channels) || channels.length === 0 || channels.length !== rule.channels.length) {
      errors.push(issue(
        "workflow.notification.channels.invalid",
        `${path}.channels`,
        "Notification channels must be a non-empty list of non-empty static channel names."
      ));
    } else if (new Set(channels).size !== channels.length) {
      errors.push(issue(
        "workflow.notification.channel.duplicate",
        `${path}.channels`,
        "Notification channels must not contain duplicates."
      ));
    }

    if (rule.required !== undefined && typeof rule.required !== "boolean") {
      errors.push(issue(
        "workflow.notification.required.invalid",
        `${path}.required`,
        "Notification required must be a boolean."
      ));
    }
  }
  return errors;
}

export function deliverAgentflowNotifications(
  store: AgentflowRunStateStore,
  runId: string,
  workflow: AgentflowWorkflow,
  status: AgentflowRunStatus,
  registry: AgentflowNotificationRegistry
): AgentflowNotificationDeliveryResult {
  const event = notificationEvent(status);
  if (event === undefined) return {};
  let requiredFailure: AgentflowNotificationDeliveryResult["requiredFailure"];

  for (const value of workflow.notify ?? []) {
    const rule = mapping(value);
    if (nonEmptyString(rule?.on) !== event) continue;
    const required = rule?.required === true;
    for (const channel of stringList(rule?.channels)) {
      const notification = buildNotification(runId, workflow.name, event, channel, required);
      let failureMessage: string | undefined;
      try {
        const adapter = registry.get(channel);
        if (adapter === undefined) throw new Error(`No notification adapter is registered for channel "${channel}".`);
        const result: unknown = adapter(notification);
        if (isPromiseLike(result)) {
          void Promise.resolve(result).catch(() => {});
          throw new Error(
            `Notification adapter for channel "${channel}" returned a promise; asynchronous adapters are not supported.`
          );
        }
      } catch (error) {
        failureMessage = error instanceof Error ? error.message : String(error);
      }

      if (failureMessage === undefined) {
        store.appendRunEvent(runId, {
          type: "notification.delivered",
          payload: { channel, event, required }
        });
      } else {
        store.appendRunEvent(runId, {
          type: "notification.failed",
          payload: { channel, event, message: failureMessage, required }
        });
        if (required && requiredFailure === undefined) {
          requiredFailure = { channel, event, message: failureMessage };
        }
      }
    }
  }

  return requiredFailure === undefined ? {} : { requiredFailure };
}

const NOTIFICATION_EVENTS = new Set<AgentflowNotificationEvent>([
  "workflow.completed",
  "workflow.failed",
  "workflow.paused"
]);

function notificationEvent(status: AgentflowRunStatus): AgentflowNotificationEvent | undefined {
  if (status === "completed") return "workflow.completed";
  if (status === "failed") return "workflow.failed";
  if (status === "paused") return "workflow.paused";
  return undefined;
}

function buildNotification(
  runId: string,
  workflowName: string,
  event: AgentflowNotificationEvent,
  channel: string,
  required: boolean
): AgentflowNotification {
  const status = event.slice("workflow.".length);
  return {
    runId,
    workflowName,
    event,
    channel,
    title: `Agentflow: ${workflowName}`,
    message: `Agentflow workflow ${workflowName} run ${runId} ${status}.`,
    required
  };
}

function terminalNotificationAdapter(notification: AgentflowNotification): undefined {
  process.stderr.write(`${notification.message}\n`);
  return undefined;
}

function systemNotificationAdapter(notification: AgentflowNotification): undefined {
  let command: string;
  let args: string[];
  if (process.platform === "darwin") {
    command = "osascript";
    args = [
      "-e",
      `display notification ${JSON.stringify(notification.message)} with title ${JSON.stringify(notification.title)}`
    ];
  } else if (process.platform === "linux") {
    command = "notify-send";
    args = [notification.title, notification.message];
  } else {
    throw new Error(`System notifications are not supported on ${process.platform}.`);
  }

  const result = spawnSync(command, args, { encoding: "utf8", windowsHide: true });
  if (result.error !== undefined) throw result.error;
  if (result.status !== 0) {
    const detail = result.stderr.trim();
    throw new Error(`${command} exited with status ${String(result.status)}${detail.length === 0 ? "" : `: ${detail}`}.`);
  }
  return undefined;
}

function mapping(value: AgentflowYamlValue | undefined): AgentflowYamlMapping | undefined {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? value as AgentflowYamlMapping
    : undefined;
}

function stringList(value: AgentflowYamlValue | undefined): string[] {
  return Array.isArray(value)
    ? value.flatMap((entry) => {
        const normalized = nonEmptyString(entry);
        return normalized === undefined || normalized.includes("{{") || normalized.includes("}}")
          ? []
          : [normalized];
      })
    : [];
}

function nonEmptyString(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined;
  const normalized = value.trim();
  return normalized.length === 0 ? undefined : normalized;
}

function issue(code: string, path: string, message: string): AgentflowNotificationIssue {
  return { code, path, message };
}

function isPromiseLike(value: unknown): value is PromiseLike<unknown> {
  return (typeof value === "object" || typeof value === "function")
    && value !== null
    && typeof (value as PromiseLike<unknown>).then === "function";
}
