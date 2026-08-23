export const AGENT_FLOW_RUN_INSPECTION_UI_HTML = String.raw`<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="color-scheme" content="dark">
  <title>Agent Flow run inspector</title>
  <link rel="stylesheet" href="/inspection.css">
</head>
<body>
  <header class="topbar">
    <div>
      <p class="eyebrow">Agent Flow</p>
      <h1>Run inspector</h1>
    </div>
    <div class="connection" id="connection-status" role="status">Connecting</div>
  </header>
  <section class="token-panel" id="token-panel" hidden>
    <div id="token-form">
      <label for="token-input">Inspection token</label>
      <div class="token-controls">
        <input id="token-input" type="password" autocomplete="off" required>
        <button id="token-submit" type="button">Connect</button>
      </div>
      <p>The token stays in this tab and is sent only in the inspection request header.</p>
    </div>
  </section>
  <main class="shell">
    <aside class="run-rail" aria-label="Runs">
      <div class="rail-heading">
        <div>
          <p class="eyebrow">Repository</p>
          <h2>Runs</h2>
        </div>
        <button class="icon-button" id="refresh-runs" type="button" aria-label="Refresh runs">Refresh</button>
      </div>
      <label class="search-label" for="run-filter">Filter runs</label>
      <input class="search" id="run-filter" type="search" placeholder="ID, workflow, status">
      <div id="run-list" class="run-list" aria-live="polite"></div>
    </aside>
    <section class="workspace" id="workspace" aria-live="polite">
      <div class="center-state">
        <p class="eyebrow">Inspection</p>
        <h2>Select a run</h2>
        <p>Choose a run to inspect its progress, evidence, and decision trail.</p>
      </div>
    </section>
  </main>
  <template id="loading-template">
    <div class="center-state loading-state" role="status">
      <span class="spinner" aria-hidden="true"></span>
      <h2>Loading run state</h2>
      <p>Reading the latest durable inspection snapshot.</p>
    </div>
  </template>
  <script src="/inspection.js" defer></script>
</body>
</html>`;

export const AGENT_FLOW_RUN_INSPECTION_UI_CSS = String.raw`:root {
  color-scheme: dark;
  --bg: #0b0e12;
  --surface: #12171d;
  --surface-raised: #171e26;
  --line: #29323d;
  --line-strong: #3b4653;
  --text: #edf2f7;
  --muted: #98a6b7;
  --accent: #78dce8;
  --accent-soft: #17343b;
  --good: #78d6a3;
  --warn: #f2c572;
  --bad: #ff8f8f;
  --waiting: #b8a1ff;
  font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
}

* { box-sizing: border-box; }

body {
  margin: 0;
  min-width: 320px;
  min-height: 100vh;
  background: var(--bg);
  color: var(--text);
}

button, input { font: inherit; }
button { cursor: pointer; }

.topbar {
  min-height: 76px;
  padding: 14px 24px;
  border-bottom: 1px solid var(--line);
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 20px;
  background: rgba(11, 14, 18, 0.96);
}

h1, h2, h3, p { margin-top: 0; }
h1 { margin-bottom: 0; font-size: 1.2rem; letter-spacing: -0.01em; }
h2 { margin-bottom: 6px; font-size: 1.05rem; }
h3 { margin-bottom: 6px; font-size: 0.95rem; }

.eyebrow {
  margin-bottom: 4px;
  color: var(--muted);
  font-size: 0.68rem;
  font-weight: 700;
  letter-spacing: 0.13em;
  text-transform: uppercase;
}

.connection, .badge, .count {
  border: 1px solid var(--line-strong);
  border-radius: 999px;
  padding: 4px 9px;
  color: var(--muted);
  font-size: 0.72rem;
  font-weight: 700;
  white-space: nowrap;
}

.connection.connected { border-color: #275c43; color: var(--good); }
.connection.failed { border-color: #6b3636; color: var(--bad); }

.token-panel {
  padding: 14px 24px;
  border-bottom: 1px solid #6b4f23;
  background: #211b12;
}

.token-panel form { max-width: 520px; }
.token-panel label { display: block; margin-bottom: 6px; font-size: 0.8rem; font-weight: 700; }
.token-panel p { margin: 7px 0 0; color: var(--muted); font-size: 0.76rem; }
.token-controls { display: flex; gap: 8px; }
.token-controls input { flex: 1; min-width: 0; }

input {
  width: 100%;
  border: 1px solid var(--line-strong);
  border-radius: 7px;
  padding: 9px 10px;
  outline: none;
  background: #0e1318;
  color: var(--text);
}

input:focus { border-color: var(--accent); box-shadow: 0 0 0 2px rgba(120, 220, 232, 0.12); }

button {
  border: 1px solid var(--line-strong);
  border-radius: 7px;
  padding: 8px 11px;
  background: var(--surface-raised);
  color: var(--text);
}

button:hover { border-color: var(--accent); }
button:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }

.shell {
  display: grid;
  grid-template-columns: minmax(250px, 310px) minmax(0, 1fr);
  height: calc(100vh - 76px);
  min-height: 520px;
}

.run-rail {
  min-width: 0;
  padding: 18px 14px;
  overflow: hidden;
  border-right: 1px solid var(--line);
  display: flex;
  flex-direction: column;
  background: #0e1217;
}

.rail-heading, .section-heading, .run-title-row, .card-heading, .metadata-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
}

.rail-heading { margin-bottom: 14px; }
.rail-heading h2 { margin-bottom: 0; }
.icon-button, .copy-button, .load-more { color: var(--accent); font-size: 0.74rem; }
.search-label { margin-bottom: 6px; color: var(--muted); font-size: 0.72rem; font-weight: 700; }
.search { margin-bottom: 12px; }

.run-list { overflow: auto; padding-right: 3px; }
.run-card {
  width: 100%;
  margin-bottom: 7px;
  padding: 11px;
  border-color: transparent;
  text-align: left;
  background: transparent;
}
.run-card:hover { background: var(--surface); }
.run-card.selected { border-color: var(--accent); background: var(--accent-soft); }
.run-card strong, .run-card small { display: block; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.run-card strong { margin: 5px 0 3px; font-size: 0.84rem; }
.run-card small { color: var(--muted); font-size: 0.72rem; }

.workspace { min-width: 0; overflow: auto; padding: 24px; }
.center-state { max-width: 480px; margin: 18vh auto 0; text-align: center; color: var(--muted); }
.center-state h2 { color: var(--text); }
.error-state { border: 1px solid #6b3636; border-radius: 10px; padding: 24px; background: #211416; }
.error-state code { color: var(--bad); }

.spinner {
  display: inline-block;
  width: 24px;
  height: 24px;
  margin-bottom: 16px;
  border: 3px solid var(--line-strong);
  border-top-color: var(--accent);
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
}
@keyframes spin { to { transform: rotate(360deg); } }

.run-header { margin-bottom: 22px; }
.run-title-row { align-items: flex-start; }
.run-title { min-width: 0; }
.run-title h2 { margin-bottom: 4px; overflow-wrap: anywhere; font-size: 1.38rem; }
.run-title p { margin-bottom: 0; color: var(--muted); }
.run-badges { display: flex; flex-wrap: wrap; justify-content: flex-end; gap: 7px; }
.badge.status-completed, .badge.status-approved, .badge.status-succeeded { border-color: #275c43; color: var(--good); }
.badge.status-failed, .badge.status-rejected, .badge.status-cancelled { border-color: #6b3636; color: var(--bad); }
.badge.status-paused, .badge.status-waiting, .badge.status-requested, .badge.status-stale { border-color: #665788; color: var(--waiting); }
.badge.status-running { border-color: #25616a; color: var(--accent); }

.summary-grid {
  margin-top: 16px;
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 8px;
}
.summary-item, .panel, .detail-card {
  border: 1px solid var(--line);
  border-radius: 9px;
  background: var(--surface);
}
.summary-item { padding: 11px; }
.summary-item span { display: block; color: var(--muted); font-size: 0.68rem; text-transform: uppercase; letter-spacing: 0.07em; }
.summary-item strong { display: block; margin-top: 5px; overflow-wrap: anywhere; font-size: 0.82rem; }

.tabs { display: flex; gap: 4px; overflow-x: auto; border-bottom: 1px solid var(--line); }
.tab { border: 0; border-radius: 0; padding: 10px 12px; background: transparent; color: var(--muted); white-space: nowrap; }
.tab.active { box-shadow: inset 0 -2px var(--accent); color: var(--text); }
.tab-panel { padding-top: 18px; }
.section-heading { margin-bottom: 12px; align-items: baseline; }
.section-heading h3 { margin-bottom: 0; }
.count { padding: 2px 7px; }

.panel { margin-bottom: 12px; padding: 14px; }
.panel.warning { border-color: #5c4926; background: #1e1a12; }
.panel.failure { border-color: #623638; background: #1e1416; }
.panel.empty { padding: 32px 16px; text-align: center; color: var(--muted); }
.panel p:last-child { margin-bottom: 0; }

.timeline { position: relative; margin-left: 7px; padding-left: 25px; }
.timeline::before { content: ""; position: absolute; inset: 6px auto 6px 5px; width: 1px; background: var(--line-strong); }
.timeline-entry { position: relative; margin-bottom: 10px; }
.timeline-entry::before { content: ""; position: absolute; left: -24px; top: 16px; width: 9px; height: 9px; border: 2px solid var(--bg); border-radius: 50%; background: var(--accent); }
.timeline-entry .panel { margin-bottom: 0; }

.detail-card { margin-bottom: 9px; overflow: hidden; }
.detail-card summary { padding: 12px 14px; cursor: pointer; list-style-position: inside; }
.detail-card summary:hover { background: var(--surface-raised); }
.detail-card[open] summary { border-bottom: 1px solid var(--line); }
.detail-body { padding: 14px; }

.metadata-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px 18px; }
.metadata-row { align-items: baseline; border-bottom: 1px solid var(--line); padding: 6px 0; }
.metadata-row span { color: var(--muted); font-size: 0.72rem; }
.metadata-row code, .metadata-row strong { min-width: 0; overflow-wrap: anywhere; text-align: right; font-size: 0.76rem; }

.code-block { margin-top: 10px; }
.code-toolbar { display: flex; justify-content: flex-end; margin-bottom: 6px; }
.code-block pre {
  max-height: 420px;
  margin: 0;
  overflow: auto;
  border: 1px solid var(--line);
  border-radius: 7px;
  padding: 13px;
  background: #090c10;
  color: #d7e1eb;
  font: 0.75rem/1.55 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  tab-size: 2;
  white-space: pre-wrap;
  overflow-wrap: anywhere;
}
.code-block .copy-button { max-width: 100%; background: #121820; white-space: normal; text-align: left; }
.load-more { width: 100%; margin-top: 8px; }

@media (max-width: 900px) {
  .summary-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .metadata-grid { grid-template-columns: 1fr; }
}

@media (max-width: 680px) {
  .topbar { padding-inline: 16px; }
  .shell { display: block; height: auto; }
  .run-rail { height: 300px; border-right: 0; border-bottom: 1px solid var(--line); }
  .workspace { min-height: 520px; padding: 18px 14px; }
  .run-title-row { display: block; }
  .run-badges { justify-content: flex-start; margin-top: 12px; }
  .summary-grid { grid-template-columns: 1fr 1fr; }
}

@media (prefers-reduced-motion: reduce) {
  .spinner { animation: none; }
}`;

export const AGENT_FLOW_RUN_INSPECTION_UI_JAVASCRIPT = String.raw`(function () {
  "use strict";

  var TOKEN_HEADER = "x-agent-flow-token";
  var RUN_ID_HEADER = "x-agent-flow-run-id";
  var EVENT_PAGE_SIZE = 100;
  var state = {
    token: tokenFromFragment(),
    runs: [],
    selectedRunId: null,
    model: null,
    sectionViews: {},
    listRequestId: 0,
    detailRequestId: 0
  };
  var elements = {
    connection: document.getElementById("connection-status"),
    tokenPanel: document.getElementById("token-panel"),
    tokenSubmit: document.getElementById("token-submit"),
    tokenInput: document.getElementById("token-input"),
    refresh: document.getElementById("refresh-runs"),
    filter: document.getElementById("run-filter"),
    runList: document.getElementById("run-list"),
    workspace: document.getElementById("workspace"),
    loadingTemplate: document.getElementById("loading-template")
  };

  elements.tokenSubmit.addEventListener("click", connectToken);
  elements.tokenInput.addEventListener("keydown", function (event) {
    if (event.key === "Enter") connectToken();
  });

  function connectToken() {
    var token = elements.tokenInput.value;
    if (!token) return;
    state.token = token;
    location.hash = "token=" + encodeURIComponent(token);
    elements.tokenPanel.hidden = true;
    void loadRuns();
  }
  elements.refresh.addEventListener("click", function () { void loadRuns(state.selectedRunId); });
  elements.filter.addEventListener("input", renderRunList);

  if (state.token) {
    elements.tokenPanel.hidden = true;
    void loadRuns();
  } else {
    requireToken("Enter the inspection token to load run state.");
  }

  function tokenFromFragment() {
    var fragment = location.hash.slice(1);
    if (!fragment) return "";
    var params = new URLSearchParams(fragment);
    var token = params.get("token");
    if (token) return token;
    try { return decodeURIComponent(fragment); } catch (_) { return ""; }
  }

  async function api(path, headers) {
    var response = await fetch(path, {
      headers: Object.assign({}, headers, Object.fromEntries([[TOKEN_HEADER, state.token]])),
      cache: "no-store"
    });
    var body;
    try { body = await response.json(); } catch (_) { body = null; }
    if (!response.ok) {
      var error = new Error(body && body.error ? body.error : "Inspection request failed with status " + response.status + ".");
      error.status = response.status;
      error.code = body && body.code ? body.code : "INSPECTION_REQUEST_FAILED";
      throw error;
    }
    return body;
  }

  async function loadRuns(preferredRunId) {
    var requestId = ++state.listRequestId;
    state.detailRequestId += 1;
    setConnection("Connecting", "");
    renderRailState("Loading runs…");
    try {
      var body = await api("/api/runs");
      if (requestId !== state.listRequestId) return;
      state.runs = Array.isArray(body.runs) ? body.runs : [];
      setConnection("Connected", "connected");
      elements.tokenPanel.hidden = true;
      renderRunList();
      var target = preferredRunId || state.selectedRunId;
      if (target && state.runs.some(function (run) { return run.id === target; })) {
        await selectRun(target);
      } else if (state.runs.length === 0) {
        renderWorkspaceState("No runs yet", "Run a workflow, then refresh to inspect its durable state.");
      } else {
        renderWorkspaceState("Select a run", "Choose a run to inspect its progress, evidence, and decision trail.");
      }
    } catch (error) {
      if (requestId !== state.listRequestId) return;
      if (error.status === 403) {
        requireToken("The inspection token was missing or invalid.");
        return;
      }
      setConnection("Load failed", "failed");
      renderRailState("Could not load runs.");
      renderError(error, function () { void loadRuns(); });
    }
  }

  function requireToken(message) {
    setConnection("Token required", "failed");
    elements.tokenPanel.hidden = false;
    elements.tokenPanel.querySelector("p").textContent = message + " It stays in this tab and is sent only in the inspection request header.";
    elements.tokenInput.focus();
    renderRailState("Connect to load runs.");
  }

  function renderRunList() {
    elements.runList.replaceChildren();
    var query = elements.filter.value.trim().toLowerCase();
    var runs = state.runs.filter(function (run) {
      return !query || [run.id, run.workflowName, run.status, run.currentStepId]
        .filter(Boolean).join(" ").toLowerCase().includes(query);
    });
    if (runs.length === 0) {
      renderRailState(state.runs.length === 0 ? "No runs available." : "No runs match this filter.");
      return;
    }
    runs.forEach(function (run) {
      var button = node("button", "run-card" + (state.selectedRunId === run.id ? " selected" : ""));
      button.type = "button";
      button.append(statusBadge(run.status), textNode("strong", run.workflowName), textNode("small", run.id));
      button.append(textNode("small", run.currentStepId ? "Current: " + run.currentStepId : "Updated " + formatDate(run.updatedAt)));
      button.addEventListener("click", function () { void selectRun(run.id); });
      elements.runList.append(button);
    });
  }

  async function selectRun(runId) {
    var requestId = ++state.detailRequestId;
    state.selectedRunId = runId;
    state.sectionViews = {};
    renderRunList();
    elements.workspace.replaceChildren(elements.loadingTemplate.content.cloneNode(true));
    try {
      var model = await api("/api/run?section=overview", Object.fromEntries([[RUN_ID_HEADER, encodeURIComponent(runId)]]));
      if (requestId !== state.detailRequestId || state.selectedRunId !== runId) return;
      state.model = model;
      renderModel();
    } catch (error) {
      if (requestId !== state.detailRequestId || state.selectedRunId !== runId) return;
      if (error.status === 403) {
        requireToken("The inspection token is no longer valid.");
        return;
      }
      renderError(error, function () { void selectRun(runId); });
    }
  }

  function renderModel() {
    var model = state.model;
    var run = model.run;
    elements.workspace.replaceChildren();
    var header = node("header", "run-header");
    var titleRow = node("div", "run-title-row");
    var title = node("div", "run-title");
    title.append(textNode("p", run.workflowName, "eyebrow"), textNode("h2", run.id));
    title.append(textNode("p", "Workflow v" + run.workflowVersion + " · " + run.workflowStyle + " · " + run.workflowMaturity));
    var badges = node("div", "run-badges");
    badges.append(statusBadge(run.status));
    if (run.currentStepId) badges.append(textNode("span", "Step: " + run.currentStepId, "badge"));
    titleRow.append(title, badges);
    header.append(titleRow, summaryGrid(run));
    elements.workspace.append(header);

    var tabs = [
      ["timeline", "Timeline"],
      ["steps", "Steps"],
      ["artifacts", "Artifacts"],
      ["failures", "Failures"],
      ["approvals", "Approvals"],
      ["decisions", "Decisions"],
      ["state", "Run state"],
      ["warnings", "Warnings"]
    ];
    var tabBar = node("nav", "tabs");
    tabBar.setAttribute("aria-label", "Run inspection sections");
    var panels = new Map();
    tabs.forEach(function (item, index) {
      var id = item[0];
      var button = textNode("button", item[1], "tab" + (index === 0 ? " active" : ""));
      button.type = "button";
      button.dataset.tab = id;
      button.setAttribute("aria-selected", index === 0 ? "true" : "false");
      button.addEventListener("click", function () { activateTab(id, tabBar, panels); });
      tabBar.append(button);
      var panel = node("section", "tab-panel");
      panel.dataset.tab = id;
      panel.hidden = index !== 0;
      panels.set(id, panel);
      state.sectionViews[id] = { panel: panel, loaded: false, loading: false, nextOffset: 0, count: 0 };
    });
    elements.workspace.append(tabBar);
    panels.forEach(function (panel) { elements.workspace.append(panel); });
    void loadSection("timeline");
  }

  function summaryGrid(run) {
    var grid = node("div", "summary-grid");
    [
      ["Current step", run.currentStepId || "None"],
      ["Started", formatDate(run.startedAt)],
      ["Updated", formatDate(run.updatedAt)],
      ["Evidence", "Paged on demand"]
    ].forEach(function (item) {
      var cell = node("div", "summary-item");
      cell.append(textNode("span", item[0]), textNode("strong", item[1]));
      grid.append(cell);
    });
    return grid;
  }

  function activateTab(id, tabBar, panels) {
    Array.from(tabBar.children).forEach(function (button) {
      var active = button.dataset.tab === id;
      button.classList.toggle("active", active);
      button.setAttribute("aria-selected", active ? "true" : "false");
    });
    panels.forEach(function (panel, panelId) { panel.hidden = panelId !== id; });
    void loadSection(id);
  }

  async function loadSection(id) {
    var view = state.sectionViews[id];
    if (!view || view.loading || (view.loaded && view.nextOffset === null)) return;
    var requestId = state.detailRequestId;
    var runId = state.selectedRunId;
    var offset = view.loaded ? view.nextOffset : 0;
    view.loading = true;
    if (view.error) view.error.remove();
    if (!view.loaded) view.panel.replaceChildren(loadingPanel("Loading " + sectionConfiguration(id).label.toLowerCase() + "…"));
    try {
      var section = id === "timeline" ? "events" : id;
      var page = await api(
        "/api/run?section=" + encodeURIComponent(section) + "&offset=" + offset + "&limit=" + EVENT_PAGE_SIZE,
        Object.fromEntries([[RUN_ID_HEADER, encodeURIComponent(runId)]])
      );
      if (requestId !== state.detailRequestId || runId !== state.selectedRunId) return;
      if (id === "state") {
        renderState(view.panel, page.state);
        view.loaded = true;
        view.nextOffset = null;
        return;
      }
      appendSectionPage(id, view, Array.isArray(page.items) ? page.items : [], page.nextOffset);
    } catch (error) {
      if (requestId !== state.detailRequestId || runId !== state.selectedRunId) return;
      view.error = sectionError(error, function () { void loadSection(id); });
      if (view.loaded) view.panel.append(view.error);
      else view.panel.replaceChildren(view.error);
    } finally {
      if (requestId === state.detailRequestId && runId === state.selectedRunId) view.loading = false;
    }
  }

  function appendSectionPage(id, view, items, nextOffset) {
    var configuration = sectionConfiguration(id);
    if (!view.loaded) {
      view.panel.replaceChildren(sectionHeading(configuration.label, 0));
      view.container = node("div", id === "timeline" ? "timeline" : "section-records");
      view.panel.append(view.container);
      view.loaded = true;
    }
    if (view.more) view.more.remove();
    appendSectionItems(id, view.container, items);
    view.count += items.length;
    view.nextOffset = nextOffset === null ? null : Number(nextOffset);
    view.panel.querySelector(".count").textContent = String(view.count) + (view.nextOffset === null ? "" : "+");
    if (view.count === 0 && view.nextOffset === null) view.container.append(emptyPanel(configuration.empty));
    if (view.nextOffset !== null) {
      view.more = textNode("button", "Show next " + EVENT_PAGE_SIZE, "load-more");
      view.more.type = "button";
      view.more.addEventListener("click", function () { void loadSection(id); });
      view.panel.append(view.more);
    }
  }

  function sectionConfiguration(id) {
    var configurations = {
      timeline: { label: "Event timeline", empty: "No events were recorded for this run." },
      steps: { label: "Step attempts", empty: "This run has no persisted step attempts." },
      artifacts: { label: "Artifact inventory", empty: "No artifacts are registered for this run." },
      failures: { label: "Failure evidence", empty: "No failures are recorded for this run." },
      approvals: { label: "Approvals", empty: "No approval records are associated with this run." },
      decisions: { label: "Decision records", empty: "No decision records are registered for this run." },
      state: { label: "Persisted run state", empty: "No persisted run state is available." },
      warnings: { label: "Inspection warnings", empty: "No inspection warnings were reported." }
    };
    return configurations[id];
  }

  function appendSectionItems(id, container, items) {
    if (id === "timeline") return appendTimelineEntries(container, items);
    if (id === "steps") return renderSteps(container, items);
    if (id === "artifacts") return renderArtifacts(container, items);
    if (id === "failures") return renderFailures(container, items);
    if (id === "approvals") return renderApprovals(container, items);
    if (id === "decisions") return renderDecisions(container, items);
    if (id === "warnings") return renderWarnings(container, items);
  }

  function appendTimelineEntries(timeline, events) {
    events.forEach(function (event) {
      var entry = node("article", "timeline-entry");
      var body = node("div", "panel");
      var heading = node("div", "card-heading");
      heading.append(textNode("strong", event.type), textNode("span", formatDate(event.createdAt), "eyebrow"));
      body.append(heading, metadata([["Sequence", event.sequence], ["Step", event.stepId || "—"]]));
      if (event.payload !== null && event.payload !== undefined) body.append(codeBlock(event.payload, "Copy event"));
      entry.append(body);
      timeline.append(entry);
    });
  }

  function renderSteps(panel, steps) {
    steps.forEach(function (step) {
      var details = node("details", "detail-card");
      var summary = node("summary");
      summary.append(statusBadge(step.status), document.createTextNode("  " + step.stepId + " · attempt " + step.attempt));
      var body = node("div", "detail-body");
      body.append(metadata([
        ["Started", formatDate(step.startedAt)], ["Finished", formatDate(step.finishedAt)],
        ["Session", step.sessionId || "—"], ["Updated", formatDate(step.updatedAt)]
      ]));
      if (step.input !== null && step.input !== undefined) body.append(labeledCode("Input", step.input));
      if (step.output !== null && step.output !== undefined) body.append(labeledCode("Output", step.output));
      if (step.error !== null && step.error !== undefined) body.append(labeledCode("Error", step.error));
      details.append(summary, body);
      panel.append(details);
    });
  }

  function renderArtifacts(panel, artifacts) {
    artifacts.forEach(function (artifact) {
      var details = node("details", "detail-card");
      var summary = node("summary");
      summary.append(statusBadge(artifact.status), document.createTextNode("  " + artifact.declaredPath));
      var body = node("div", "detail-body");
      body.append(metadata([
        ["Kind", artifact.kind], ["Content type", artifact.contentType], ["Step", artifact.producerStepId || "—"],
        ["Bytes", artifact.sizeBytes === null ? "—" : artifact.sizeBytes], ["Checksum", artifact.checksum || "—"],
        ["Written", formatDate(artifact.writtenAt)]
      ]));
      body.append(labeledCode("Copyable metadata", artifact));
      details.append(summary, body);
      panel.append(details);
    });
  }

  function renderFailures(panel, failures) {
    failures.forEach(function (failure) {
      var card = node("article", "panel failure");
      var heading = node("div", "card-heading");
      heading.append(textNode("h3", failure.message || failure.id), statusBadge(failure.resolvedAt ? "resolved" : "unresolved"));
      card.append(heading, metadata([
        ["ID", failure.id], ["Step", failure.stepId || "—"], ["Classification", failure.classification],
        ["Retryable", String(Boolean(failure.retryable))], ["Created", formatDate(failure.createdAt)],
        ["Resolved", formatDate(failure.resolvedAt)]
      ]));
      if (failure.failurePayload) {
        var payload = failure.failurePayload.document || { error: failure.failurePayload.error };
        card.append(labeledCode("Persisted failure payload", payload));
      } else {
        card.append(textNode("p", "No readable persisted failure payload is available."));
      }
      panel.append(card);
    });
  }

  function renderApprovals(panel, approvals) {
    approvals.forEach(function (approval) {
      var details = node("details", "detail-card");
      var summary = node("summary");
      summary.append(statusBadge(approval.status), document.createTextNode("  " + approval.id));
      var body = node("div", "detail-body");
      body.append(metadata([
        ["Step", approval.stepId || "—"], ["Requested by", approval.requestedBy || "—"],
        ["Decided by", approval.decidedBy || "—"], ["Decision", approval.decision || "—"],
        ["Requested", formatDate(approval.createdAt)], ["Decided", formatDate(approval.decidedAt)]
      ]));
      if (approval.context !== null && approval.context !== undefined) body.append(labeledCode("Approval context", approval.context));
      details.append(summary, body);
      panel.append(details);
    });
  }

  function renderDecisions(panel, decisions) {
    decisions.forEach(function (decision) {
      var documentValue = decision.document;
      var details = node("details", "detail-card");
      var summary = node("summary");
      summary.append(statusBadge(decision.error ? "unavailable" : "available"));
      summary.append(document.createTextNode("  " + (documentValue && documentValue.topic ? documentValue.topic : decision.artifact.declaredPath)));
      var body = node("div", "detail-body");
      if (decision.error) body.append(textNode("p", decision.error));
      body.append(labeledCode("Copyable decision record", documentValue || decision.artifact));
      details.append(summary, body);
      panel.append(details);
    });
  }

  function renderState(panel, runState) {
    panel.replaceChildren(sectionHeading("Persisted run state", null));
    [["Inputs", runState.inputs], ["Output", runState.output], ["Error", runState.error], ["Waiting", runState.waiting]]
      .forEach(function (item) { panel.append(labeledCode(item[0], item[1])); });
  }

  function renderWarnings(panel, warnings) {
    warnings.forEach(function (warning) {
      var card = node("article", "panel warning");
      card.append(textNode("h3", warning.code), textNode("p", warning.message));
      if (warning.path) card.append(textNode("code", warning.path));
      panel.append(card);
    });
  }

  function labeledCode(label, value) {
    var wrapper = node("section", "code-section");
    wrapper.append(textNode("p", label, "eyebrow"), codeBlock(value, "Copy " + label.toLowerCase()));
    return wrapper;
  }

  function codeBlock(value, label) {
    var text = stringify(value);
    var wrapper = node("div", "code-block");
    var pre = node("pre");
    pre.append(textNode("code", text));
    var copy = textNode("button", label, "copy-button");
    copy.type = "button";
    copy.addEventListener("click", async function () {
      try {
        await navigator.clipboard.writeText(text);
        copy.textContent = "Copied";
      } catch (_) {
        copy.textContent = "Copy failed";
      }
      setTimeout(function () { copy.textContent = label; }, 1400);
    });
    var toolbar = node("div", "code-toolbar");
    toolbar.append(copy);
    wrapper.append(toolbar, pre);
    return wrapper;
  }

  function metadata(items) {
    var grid = node("div", "metadata-grid");
    items.forEach(function (item) {
      var row = node("div", "metadata-row");
      row.append(textNode("span", item[0]), textNode("code", String(item[1] === null || item[1] === undefined ? "—" : item[1])));
      grid.append(row);
    });
    return grid;
  }

  function sectionHeading(title, count) {
    var heading = node("div", "section-heading");
    heading.append(textNode("h3", title));
    if (count !== null) heading.append(textNode("span", String(count), "count"));
    return heading;
  }

  function emptyPanel(message) { return textNode("div", message, "panel empty"); }

  function loadingPanel(message) {
    var panel = node("div", "panel empty");
    panel.append(node("span", "spinner"), document.createTextNode(" " + message));
    return panel;
  }

  function sectionError(error, retry) {
    var panel = node("div", "panel failure");
    panel.append(textNode("h3", "Could not load this section"), textNode("p", error.message || String(error)));
    var button = textNode("button", "Try again");
    button.type = "button";
    button.addEventListener("click", retry);
    panel.append(button);
    return panel;
  }

  function statusBadge(status) {
    var normalized = String(status || "unknown").toLowerCase().replace(/[^a-z0-9_-]+/g, "-");
    return textNode("span", String(status || "unknown"), "badge status-" + normalized);
  }

  function renderWorkspaceState(title, message) {
    var stateNode = node("div", "center-state");
    stateNode.append(textNode("p", "Inspection", "eyebrow"), textNode("h2", title), textNode("p", message));
    elements.workspace.replaceChildren(stateNode);
  }

  function renderError(error, retry) {
    var card = node("div", "center-state error-state");
    card.append(textNode("p", "Load failed", "eyebrow"), textNode("h2", "Could not load inspection data"));
    card.append(textNode("p", error.message || String(error)));
    if (error.code) card.append(textNode("code", error.code));
    var button = textNode("button", "Try again");
    button.type = "button";
    button.addEventListener("click", retry);
    card.append(document.createElement("br"), document.createElement("br"), button);
    elements.workspace.replaceChildren(card);
  }

  function renderRailState(message) { elements.runList.replaceChildren(textNode("div", message, "panel empty")); }
  function setConnection(message, className) { elements.connection.textContent = message; elements.connection.className = "connection " + className; }
  function stringify(value) { return value === undefined ? "—" : JSON.stringify(value, null, 2); }
  function formatDate(value) {
    if (!value) return "—";
    var date = new Date(value);
    return Number.isNaN(date.getTime()) ? String(value) : date.toLocaleString();
  }
  function node(tag, className) {
    var element = document.createElement(tag);
    if (className) element.className = className;
    return element;
  }
  function textNode(tag, text, className) {
    var element = node(tag, className);
    element.textContent = text;
    return element;
  }
}());`;
