import {
  AgentFlowSessionRequestError,
  createAgentFlowSessionProviderRegistry,
  type AgentFlowSessionProviderAdapter,
  type AgentFlowSessionProviderRequest,
  type AgentFlowSessionProviderResponse,
  type AgentFlowSessionProviderRegistry
} from "./session_request";
import {
  hashAgentFlowProviderModel,
  type AgentFlowConfiguredTarget,
  type AgentFlowProviderCatalog,
  type AgentFlowResolvedProviderBinding
} from "./provider_config";

const MAX_PROVIDER_RESPONSE_BYTES = 12 * 1024 * 1024;

export interface CreateAgentFlowConfiguredProviderRegistryOptions {
  env?: Readonly<Record<string, string | undefined>>;
}

export function createAgentFlowConfiguredProviderRegistry(
  catalog: AgentFlowProviderCatalog,
  options: CreateAgentFlowConfiguredProviderRegistryOptions = {}
): AgentFlowSessionProviderRegistry {
  const registry = createAgentFlowSessionProviderRegistry();
  for (const binding of Object.values(catalog.bindings)) {
    registry.registerConfigured({
      name: binding.alias,
      kind: binding.kind,
      target: binding.target,
      driver: binding.config.driver,
      model: binding.config.model,
      fingerprint: binding.fingerprint
    }, createAgentFlowConfiguredProviderAdapter(binding, options));
  }
  return registry;
}

export function createAgentFlowConfiguredProviderAdapter(
  binding: AgentFlowResolvedProviderBinding,
  options: CreateAgentFlowConfiguredProviderRegistryOptions = {}
): AgentFlowSessionProviderAdapter {
  const env = options.env ?? process.env;
  let adapter: AgentFlowSessionProviderAdapter;
  if (binding.config.driver === "openai-responses") {
    adapter = (request) => invokeOpenAiResponses(binding, request, env);
  } else if (binding.config.driver === "anthropic-messages") {
    adapter = (request) => invokeAnthropicMessages(binding, request, env);
  } else {
    adapter = (request) => invokeOpenAiCompatible(binding, request, env);
  }
  adapter.preflight = (request) => {
    assertApiAuthority(request, binding);
    if (binding.config.api_key_env !== undefined) requiredCredential(binding.config, env);
    assertUtf8Inputs(request);
  };
  return adapter;
}

async function invokeOpenAiResponses(
  binding: AgentFlowResolvedProviderBinding,
  request: AgentFlowSessionProviderRequest,
  env: Readonly<Record<string, string | undefined>>
): Promise<AgentFlowSessionProviderResponse> {
  assertApiAuthority(request, binding);
  const apiKey = requiredCredential(binding.config, env);
  const body = await postJson(
    "https://api.openai.com/v1/responses",
    {
      model: binding.config.model,
      input: buildProviderPrompt(request),
      store: false,
      text: { format: { type: "json_schema", name: "agent_flow_outputs", strict: true, schema: outputSchema(request) } }
    },
    { Authorization: `Bearer ${apiKey}` },
    request.signal
  );
  assertProviderCompletion(body.status, ["completed"], "OpenAI Responses");
  const responseId = optionalString(body.id);
  const content = openAiResponseText(body);
  return responseFromStructuredText(content, request, binding, undefined, responseId === undefined ? undefined : { responseId });
}

async function invokeAnthropicMessages(
  binding: AgentFlowResolvedProviderBinding,
  request: AgentFlowSessionProviderRequest,
  env: Readonly<Record<string, string | undefined>>
): Promise<AgentFlowSessionProviderResponse> {
  assertApiAuthority(request, binding);
  const apiKey = requiredCredential(binding.config, env);
  const body = await postJson(
    "https://api.anthropic.com/v1/messages",
    {
      model: binding.config.model,
      max_tokens: binding.config.max_output_tokens ?? 4096,
      messages: [{ role: "user", content: buildProviderPrompt(request) }],
      output_config: { format: { type: "json_schema", schema: outputSchema(request) } }
    },
    { "x-api-key": apiKey, "anthropic-version": "2023-06-01" },
    request.signal
  );
  assertProviderCompletion(body.stop_reason, ["end_turn", "stop_sequence"], "Anthropic Messages");
  const contentBlocks = Array.isArray(body.content) ? body.content : [];
  const text = contentBlocks
    .filter(isRecord)
    .filter((entry) => entry.type === "text" && typeof entry.text === "string")
    .map((entry) => entry.text as string)
    .join("");
  if (!text) throw providerError("Anthropic Messages response did not contain text output.");
  const responseId = optionalString(body.id);
  return responseFromStructuredText(text, request, binding, undefined, responseId === undefined ? undefined : { responseId });
}

async function invokeOpenAiCompatible(
  binding: AgentFlowResolvedProviderBinding,
  request: AgentFlowSessionProviderRequest,
  env: Readonly<Record<string, string | undefined>>
): Promise<AgentFlowSessionProviderResponse> {
  assertApiAuthority(request, binding);
  const headers: Record<string, string> = {};
  if (binding.config.api_key_env !== undefined) {
    headers.Authorization = `Bearer ${requiredCredential(binding.config, env)}`;
  }
  const endpoint = `${binding.config.base_url!.replace(/\/$/, "")}/chat/completions`;
  const body = await postJson(
    endpoint,
    {
      model: binding.config.model,
      messages: [{ role: "user", content: buildProviderPrompt(request) }],
      response_format: {
        type: "json_schema",
        json_schema: { name: "agent_flow_outputs", strict: true, schema: outputSchema(request) }
      }
    },
    headers,
    request.signal
  );
  const choices = Array.isArray(body.choices) ? body.choices : [];
  const first = choices[0];
  if (isRecord(first)) assertProviderCompletion(first.finish_reason, ["stop"], "OpenAI-compatible");
  const message = isRecord(first) && isRecord(first.message) ? first.message : undefined;
  const content = message === undefined ? undefined : message.content;
  if (typeof content !== "string") throw providerError("OpenAI-compatible response did not contain message content.");
  return responseFromStructuredText(content, request, binding);
}

function buildProviderPrompt(request: AgentFlowSessionProviderRequest): string {
  const decoder = new TextDecoder("utf-8", { fatal: true });
  const inputs = request.inputs.map((input) => {
    let content: string;
    try { content = decoder.decode(input.content); } catch {
      throw providerError(`Configured providers accept UTF-8 text inputs only; ${input.path} is not valid UTF-8.`);
    }
    return [`<agent-flow-input path=${JSON.stringify(input.path)}>`, content, "</agent-flow-input>"].join("\n");
  });
  return [
    request.prompt.content,
    "",
    ...inputs,
    "",
    "Return one JSON object that matches the supplied schema.",
    "The outputs object must contain exactly the declared paths, with complete UTF-8 file content as each value.",
    "Do not wrap the JSON in Markdown fences.",
    `Declared output paths: ${JSON.stringify(request.outputs)}`,
    ...(request.kind === "recovery"
      ? ["Also return recovery_status as either remediated or unresolved."]
      : [])
  ].join("\n");
}

function outputSchema(request: AgentFlowSessionProviderRequest): Record<string, unknown> {
  const properties: Record<string, unknown> = {
    outputs: {
      type: "object",
      properties: Object.fromEntries(request.outputs.map((output) => [output, { type: "string" }])),
      required: [...request.outputs],
      additionalProperties: false
    }
  };
  if (request.kind === "recovery") properties.recovery_status = { enum: ["remediated", "unresolved"] };
  return {
    type: "object",
    properties,
    required: request.kind === "recovery" ? ["outputs", "recovery_status"] : ["outputs"],
    additionalProperties: false
  };
}

function responseFromStructuredText(
  source: string,
  request: AgentFlowSessionProviderRequest,
  binding: AgentFlowResolvedProviderBinding,
  externalSessionId?: string,
  extraMetadata?: Record<string, string>
): AgentFlowSessionProviderResponse {
  const parsed = parseJsonObject(source, `${binding.config.driver} structured output`);
  if (!isRecord(parsed.outputs)) throw providerError(`${binding.config.driver} structured output must contain an outputs object.`);
  const parsedOutputs = parsed.outputs;
  const actual = Object.keys(parsedOutputs).sort();
  const expected = [...request.outputs].sort();
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw providerError(`${binding.config.driver} returned output paths ${JSON.stringify(actual)}; expected ${JSON.stringify(expected)}.`);
  }
  const outputs = Object.fromEntries(request.outputs.map((output) => {
    const value = parsedOutputs[output];
    if (typeof value !== "string") throw providerError(`${binding.config.driver} output ${output} must be a UTF-8 string.`);
    assertUnicodeScalarString(value, `${binding.config.driver} output ${output}`);
    return [output, value];
  })) as Record<string, string>;
  return {
    outputs,
    ...(externalSessionId === undefined ? {} : { externalSessionId }),
    metadata: {
      target: binding.target,
      driver: binding.config.driver,
      modelHash: hashAgentFlowProviderModel(binding.config.model),
      fingerprint: binding.fingerprint,
      ...(request.kind === "recovery" && (parsed.recovery_status === "remediated" || parsed.recovery_status === "unresolved")
        ? { recovery_status: parsed.recovery_status }
        : {}),
      ...extraMetadata
    }
  };
}

async function postJson(
  endpoint: string,
  body: Record<string, unknown>,
  headers: Record<string, string>,
  signal: AbortSignal
): Promise<Record<string, unknown>> {
  let response: Response;
  try {
    response = await fetch(endpoint, {
      method: "POST",
      redirect: "manual",
      headers: { "content-type": "application/json", ...headers },
      body: JSON.stringify(body),
      signal
    });
  } catch (error) {
    throw providerError(`Provider request failed: ${error instanceof Error ? error.message : String(error)}.`);
  }
  if (response.status >= 300 && response.status < 400) throw providerError("Provider endpoint redirects are not allowed.");
  if (!response.ok) throw providerError(`Provider returned HTTP ${response.status}.`);
  const source = await readBoundedResponse(response);
  return parseJsonObject(source, "Provider response");
}

async function readBoundedResponse(response: Response): Promise<string> {
  const declared = Number(response.headers.get("content-length"));
  if (Number.isFinite(declared) && declared > MAX_PROVIDER_RESPONSE_BYTES) throw providerError("Provider response exceeds the size limit.");
  if (response.body === null) return "";
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  while (true) {
    const result = await reader.read();
    if (result.done) break;
    total += result.value.byteLength;
    if (total > MAX_PROVIDER_RESPONSE_BYTES) {
      await reader.cancel();
      throw providerError("Provider response exceeds the size limit.");
    }
    chunks.push(result.value);
  }
  const combined = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) { combined.set(chunk, offset); offset += chunk.byteLength; }
  try { return new TextDecoder("utf-8", { fatal: true }).decode(combined); } catch {
    throw providerError("Provider response is not valid UTF-8.");
  }
}

function assertApiAuthority(request: AgentFlowSessionProviderRequest, binding: AgentFlowResolvedProviderBinding): void {
  if (request.canModifyFiles === true) {
    throw providerError(`Configured ${binding.config.driver} target ${binding.alias} cannot receive file-modification authority.`);
  }
  if (request.resume) {
    throw providerError(`Configured ${binding.config.driver} target ${binding.alias} does not support conversational resume.`);
  }
}

function assertUtf8Inputs(request: AgentFlowSessionProviderRequest): void {
  const decoder = new TextDecoder("utf-8", { fatal: true });
  for (const input of request.inputs) {
    try { decoder.decode(input.content); } catch {
      throw providerError(`Configured providers accept UTF-8 text inputs only; ${input.path} is not valid UTF-8.`);
    }
  }
}

function requiredCredential(
  config: AgentFlowConfiguredTarget,
  env: Readonly<Record<string, string | undefined>>
): string {
  const name = config.api_key_env;
  const candidate = name !== undefined && Object.hasOwn(env, name) ? env[name] : undefined;
  const value = typeof candidate === "string" ? candidate.trim() : undefined;
  if (name === undefined || !value) throw providerError(`Credential environment variable ${name ?? "(missing api_key_env)"} is not set.`);
  return value;
}

function assertProviderCompletion(value: unknown, allowed: readonly string[], label: string): void {
  if (typeof value !== "string" || !allowed.includes(value)) {
    throw providerError(`${label} response stopped before completing the declared outputs.`);
  }
}

function assertUnicodeScalarString(value: string, label: string): void {
  for (let index = 0; index < value.length; index += 1) {
    const unit = value.charCodeAt(index);
    if (unit >= 0xD800 && unit <= 0xDBFF) {
      const next = value.charCodeAt(index + 1);
      if (next >= 0xDC00 && next <= 0xDFFF) {
        index += 1;
        continue;
      }
      throw providerError(`${label} contains an invalid Unicode scalar sequence.`);
    }
    if (unit >= 0xDC00 && unit <= 0xDFFF) {
      throw providerError(`${label} contains an invalid Unicode scalar sequence.`);
    }
  }
}

function openAiResponseText(body: Record<string, unknown>): string {
  if (typeof body.output_text === "string") return body.output_text;
  const output = Array.isArray(body.output) ? body.output : [];
  const text = output.filter(isRecord).flatMap((item) => Array.isArray(item.content) ? item.content : [])
    .filter(isRecord)
    .filter((item) => (item.type === "output_text" || item.type === "text") && typeof item.text === "string")
    .map((item) => item.text as string)
    .join("");
  if (!text) throw providerError("OpenAI Responses response did not contain output text.");
  return text;
}

function parseJsonObject(source: string, label: string): Record<string, unknown> {
  let value: unknown;
  try { value = JSON.parse(source); } catch { throw providerError(`${label} is not valid JSON.`); }
  if (!isRecord(value)) throw providerError(`${label} must be a JSON object.`);
  return value;
}

function optionalString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function providerError(message: string): AgentFlowSessionRequestError {
  return new AgentFlowSessionRequestError(message, "AGENT_FLOW_CONFIGURED_PROVIDER");
}
