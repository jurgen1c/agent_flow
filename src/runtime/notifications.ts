import { spawnSync } from "node:child_process";
import type { AgentFlowRunStateStore, AgentFlowRunStateValue, AgentFlowRunStatus } from "./run_state";
import type { AgentFlowWorkflow, AgentFlowYamlMapping, AgentFlowYamlValue } from "./workflow";

export type AgentFlowNotificationEvent =
  | "workflow.completed"
  | "workflow.failed"
  | "workflow.paused"
  | "approval.waiting"
  | "collaboration.disagreement";

export interface AgentFlowNotification {
  runId: string;
  workflowName: string;
  event: AgentFlowNotificationEvent;
  channel: string;
  title: string;
  message: string;
  required: boolean;
  stepId?: string;
  payload?: AgentFlowRunStateValue;
}

export type AgentFlowNotificationAdapter = (notification: AgentFlowNotification) => undefined;
export type AgentFlowEmailNotificationAdapter = AgentFlowNotificationAdapter;
export type AgentFlowSlackNotificationAdapter = AgentFlowNotificationAdapter;
export type AgentFlowWebhookNotificationAdapter = AgentFlowNotificationAdapter;
export type AgentFlowCommandNotificationAdapter = AgentFlowNotificationAdapter;

export interface AgentFlowNotificationAdapters {
  terminal?: AgentFlowNotificationAdapter;
  system?: AgentFlowNotificationAdapter;
  email?: AgentFlowEmailNotificationAdapter;
  slack?: AgentFlowSlackNotificationAdapter;
  webhook?: AgentFlowWebhookNotificationAdapter;
  command?: AgentFlowCommandNotificationAdapter;
}

export interface AgentFlowNotificationContext {
  stepId?: string;
  payload?: AgentFlowRunStateValue;
  requiredRunStatus?: AgentFlowRunStatus;
}

export interface AgentFlowNotificationDeliveryResult {
  requiredFailure?: {
    channel: string;
    event: AgentFlowNotificationEvent;
    message: string;
  };
  attempts?: Array<{
    type: "notification.delivered" | "notification.failed";
    stepId?: string;
    payload: {
      channel: string;
      event: AgentFlowNotificationEvent;
      message?: string;
      required: boolean;
      stepId?: string;
    };
  }>;
}

export interface AgentFlowNotificationIssue {
  code: string;
  message: string;
  path: string;
}

export class AgentFlowNotificationRegistry {
  private readonly adapters = new Map<string, AgentFlowNotificationAdapter>();

  register(channel: string, adapter: AgentFlowNotificationAdapter): this {
    const normalized = nonEmptyString(channel);
    if (normalized === undefined) throw new Error("Notification channel names must be non-empty strings.");
    this.adapters.set(normalized, adapter);
    return this;
  }

  get(channel: string): AgentFlowNotificationAdapter | undefined {
    return this.adapters.get(channel.trim());
  }
}

export function createAgentFlowNotificationRegistry(
  adapters: AgentFlowNotificationAdapters = {}
): AgentFlowNotificationRegistry {
  const registry = new AgentFlowNotificationRegistry()
    .register("terminal", adapters.terminal ?? terminalNotificationAdapter)
    .register("system", adapters.system ?? systemNotificationAdapter);
  for (const channel of ["email", "slack", "webhook", "command"] as const) {
    const adapter = adapters[channel];
    if (adapter !== undefined) registry.register(channel, adapter);
  }
  return registry;
}

export function validateAgentFlowNotifications(workflow: AgentFlowWorkflow): AgentFlowNotificationIssue[] {
  if (workflow.notify !== undefined && !Array.isArray(workflow.notify)) {
    return [issue(
      "workflow.notification.rules.invalid",
      "notify",
      "Workflow notifications must be a list."
    )];
  }
  const errors: AgentFlowNotificationIssue[] = [];
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
    if (event === undefined || !NOTIFICATION_EVENTS.has(event as AgentFlowNotificationEvent)) {
      errors.push(issue(
        "workflow.notification.event.unsupported",
        `${path}.on`,
        "Notification on must be workflow.completed, workflow.failed, workflow.paused, approval.waiting, or collaboration.disagreement."
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

export function deliverAgentFlowNotifications(
  store: AgentFlowRunStateStore,
  runId: string,
  workflow: AgentFlowWorkflow,
  status: AgentFlowRunStatus,
  registry: AgentFlowNotificationRegistry
): AgentFlowNotificationDeliveryResult {
  const event = notificationEvent(status);
  if (event === undefined) return {};
  return deliverAgentFlowNotificationEvent(store, runId, workflow, event, registry);
}

export function deliverAgentFlowNotificationEvent(
  store: AgentFlowRunStateStore,
  runId: string,
  workflow: AgentFlowWorkflow,
  event: AgentFlowNotificationEvent,
  registry: AgentFlowNotificationRegistry,
  context: AgentFlowNotificationContext = {}
): AgentFlowNotificationDeliveryResult {
  if (context.requiredRunStatus !== undefined) {
    return store.withRunFinalizationTransaction(runId, () =>
      deliverAgentFlowNotificationEventUnlocked(store, runId, workflow, event, registry, context)
    );
  }
  return deliverAgentFlowNotificationEventUnlocked(store, runId, workflow, event, registry, context);
}

function deliverAgentFlowNotificationEventUnlocked(
  store: AgentFlowRunStateStore,
  runId: string,
  workflow: AgentFlowWorkflow,
  event: AgentFlowNotificationEvent,
  registry: AgentFlowNotificationRegistry,
  context: AgentFlowNotificationContext
): AgentFlowNotificationDeliveryResult {
  let requiredFailure: AgentFlowNotificationDeliveryResult["requiredFailure"];
  const attempts: NonNullable<AgentFlowNotificationDeliveryResult["attempts"]> = [];
  const initialStatus = store.getRun(runId)?.status;
  if (context.requiredRunStatus !== undefined && initialStatus !== context.requiredRunStatus) return {};

  for (const value of workflow.notify ?? []) {
    const rule = mapping(value);
    if (nonEmptyString(rule?.on) !== event) continue;
    const required = rule?.required === true;
    for (const channel of stringList(rule?.channels)) {
      if (store.getRun(runId)?.status !== initialStatus) {
        return deliveryResult(requiredFailure, attempts, true);
      }
      const notification = buildNotification(runId, workflow.name, event, channel, required, context);
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
        const attempt = {
          type: "notification.delivered",
          ...(context.stepId === undefined ? {} : { stepId: context.stepId }),
          payload: {
            channel,
            event,
            required,
            ...(context.stepId === undefined ? {} : { stepId: context.stepId })
          }
        } as const;
        store.appendRunEvent(runId, attempt);
        attempts.push(attempt);
      } else {
        const attempt = {
          type: "notification.failed",
          ...(context.stepId === undefined ? {} : { stepId: context.stepId }),
          payload: {
            channel,
            event,
            message: failureMessage,
            required,
            ...(context.stepId === undefined ? {} : { stepId: context.stepId })
          }
        } as const;
        store.appendRunEvent(runId, attempt);
        attempts.push(attempt);
        if (required && requiredFailure === undefined) {
          requiredFailure = { channel, event, message: failureMessage };
        }
      }
    }
  }

  return deliveryResult(requiredFailure, attempts, store.getRun(runId)?.status !== initialStatus);
}

function deliveryResult(
  requiredFailure: AgentFlowNotificationDeliveryResult["requiredFailure"],
  attempts: NonNullable<AgentFlowNotificationDeliveryResult["attempts"]>,
  stopped: boolean
): AgentFlowNotificationDeliveryResult {
  if (requiredFailure !== undefined) return { requiredFailure, attempts };
  return stopped && attempts.length > 0 ? { attempts } : {};
}

const NOTIFICATION_EVENTS = new Set<AgentFlowNotificationEvent>([
  "workflow.completed",
  "workflow.failed",
  "workflow.paused",
  "approval.waiting",
  "collaboration.disagreement"
]);

function notificationEvent(status: AgentFlowRunStatus): AgentFlowNotificationEvent | undefined {
  if (status === "completed") return "workflow.completed";
  if (status === "failed") return "workflow.failed";
  if (status === "paused") return "workflow.paused";
  return undefined;
}

function buildNotification(
  runId: string,
  workflowName: string,
  event: AgentFlowNotificationEvent,
  channel: string,
  required: boolean,
  context: AgentFlowNotificationContext
): AgentFlowNotification {
  const subject = context.stepId === undefined ? "" : ` step ${context.stepId}`;
  const message = event.startsWith("workflow.")
    ? `Agent Flow workflow ${workflowName} run ${runId} ${event.slice("workflow.".length)}.`
    : event === "approval.waiting"
      ? `Agent Flow workflow ${workflowName} run ${runId}${subject} is waiting for approval.`
      : `Agent Flow workflow ${workflowName} run ${runId}${subject} entered disagreement handling.`;
  return {
    runId,
    workflowName,
    event,
    channel,
    title: `Agent Flow: ${workflowName}`,
    message,
    required,
    ...(context.stepId === undefined ? {} : { stepId: context.stepId }),
    ...(context.payload === undefined ? {} : { payload: context.payload })
  };
}

function terminalNotificationAdapter(notification: AgentFlowNotification): undefined {
  process.stderr.write(`${notification.message}\n`);
  return undefined;
}

function systemNotificationAdapter(notification: AgentFlowNotification): undefined {
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

function mapping(value: AgentFlowYamlValue | undefined): AgentFlowYamlMapping | undefined {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? value as AgentFlowYamlMapping
    : undefined;
}

function stringList(value: AgentFlowYamlValue | undefined): string[] {
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

function issue(code: string, path: string, message: string): AgentFlowNotificationIssue {
  return { code, path, message };
}

function isPromiseLike(value: unknown): value is PromiseLike<unknown> {
  return (typeof value === "object" || typeof value === "function")
    && value !== null
    && typeof (value as PromiseLike<unknown>).then === "function";
}
