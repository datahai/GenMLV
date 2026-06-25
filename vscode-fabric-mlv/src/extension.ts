import * as fs from "node:fs/promises";
import * as path from "node:path";
import * as vscode from "vscode";
import { setTimeout as delay } from "node:timers/promises";

type MetadataRecord = {
  timestamp?: number;
  datetime?: string;
};

type MetadataMap = Record<string, MetadataRecord>;

type SqlDefinition = {
  schema: string;
  tableName: string;
  key: string;
  fullPath: string;
  relPath: string;
  folderOrder: string;
  modifiedMs: number;
  modifiedIso: string;
};

type ApiProfile = {
  id: string;
  label: string;
  description: string;
  runIdFieldPath: string;
  statusFieldPath: string;
  successStates: string[];
  failureStates: string[];
};

type GraphDirection = "LR" | "TB";
type GraphNodeKind = "mlv" | "table";
type SchemaColor = {
  fill: string;
  stroke: string;
};

const SCHEMA_PALETTE: SchemaColor[] = [
  { fill: "#1e6fbe", stroke: "#0b3a66" },
  { fill: "#9a6700", stroke: "#5a3900" },
  { fill: "#2f9e44", stroke: "#1f6f30" },
  { fill: "#0f766e", stroke: "#0b4f49" },
  { fill: "#b42318", stroke: "#7a1a12" },
  { fill: "#5f4b8b", stroke: "#3f325d" },
  { fill: "#3f4b5b", stroke: "#2b343f" }
];

const API_PROFILES: ApiProfile[] = [
  {
    id: "flat-default",
    label: "Flat Default",
    description: "runId + status at top level",
    runIdFieldPath: "runId",
    statusFieldPath: "status",
    successStates: ["succeeded", "completed"],
    failureStates: ["failed", "cancelled", "canceled"]
  },
  {
    id: "nested-data-run",
    label: "Nested data.run",
    description: "run id and state nested under data.run",
    runIdFieldPath: "data.run.id",
    statusFieldPath: "data.run.state",
    successStates: ["succeeded", "completed"],
    failureStates: ["failed", "cancelled", "canceled", "error"]
  },
  {
    id: "fabric-operation",
    label: "Operation Pattern",
    description: "operationId trigger with lifecycle state polling",
    runIdFieldPath: "operationId",
    statusFieldPath: "state.lifecycle",
    successStates: ["succeeded", "completed"],
    failureStates: ["failed", "cancelled", "canceled", "error"]
  }
];

const output = vscode.window.createOutputChannel("Fabric MLV Manager");
let mlvProvider: MlvTreeDataProvider | undefined;
let compactGraphProvider: CompactDependencyGraphViewProvider | undefined;

export function activate(context: vscode.ExtensionContext): void {
  output.appendLine("Fabric MLV Manager activated.");

  mlvProvider = new MlvTreeDataProvider();
  compactGraphProvider = new CompactDependencyGraphViewProvider();
  const treeView = vscode.window.createTreeView("fabricMlv.views", {
    treeDataProvider: mlvProvider,
    showCollapseAll: true
  });

  context.subscriptions.push(
    output,
    treeView,
    vscode.window.registerWebviewViewProvider("fabricMlv.compactGraph", compactGraphProvider),
    vscode.commands.registerCommand("fabricMlv.validateSqlDefinitions", validateSqlDefinitions),
    vscode.commands.registerCommand("fabricMlv.generateDeploymentPlan", generateDeploymentPlan),
    vscode.commands.registerCommand("fabricMlv.showDependencyGraph", showDependencyGraph),
    vscode.commands.registerCommand("fabricMlv.openDependencyGraph", showDependencyGraph),
    vscode.commands.registerCommand("fabricMlv.toggleDependencyGraphDirection", toggleDependencyGraphDirection),
    vscode.commands.registerCommand("fabricMlv.triggerNotebookRun", triggerNotebookRun),
    vscode.commands.registerCommand("fabricMlv.previewApiProfile", previewApiProfile),
    vscode.commands.registerCommand("fabricMlv.applyApiProfile", applyApiProfile),
    vscode.commands.registerCommand("fabricMlv.refreshExplorer", async () => {
      await mlvProvider?.refresh();
      await compactGraphProvider?.refresh();
    }),
    vscode.commands.registerCommand("fabricMlv.openSqlFile", async (item: MlvItem) => {
      if (!item || item.kind !== "definition" || !item.resourceUri) {
        return;
      }

      const doc = await vscode.workspace.openTextDocument(item.resourceUri);
      await vscode.window.showTextDocument(doc, { preview: false });
    }),
    vscode.workspace.onDidChangeConfiguration(async (event) => {
      if (event.affectsConfiguration("fabricMlv")) {
        await mlvProvider?.refresh();
        await compactGraphProvider?.refresh();
      }
    })
  );
}

async function applyApiProfile(): Promise<void> {
  const selected = await pickApiProfile("Select Fabric API Profile");

  if (!selected) {
    return;
  }

  const previewText = renderProfilePreview(selected.profile);
  const confirm = await vscode.window.showInformationMessage(
    `Apply profile '${selected.profile.label}'?\n\n${previewText}`,
    { modal: true },
    "Apply"
  );

  if (confirm !== "Apply") {
    return;
  }

  await applyProfileToWorkspace(selected.profile);
  output.appendLine(
    `Applied API profile '${selected.profile.label}' with run id '${selected.profile.runIdFieldPath}' and status '${selected.profile.statusFieldPath}'.`
  );
  output.show(true);
  vscode.window.showInformationMessage(`Applied Fabric API profile: ${selected.profile.label}`);
}

async function applyProfileToWorkspace(profile: ApiProfile): Promise<void> {
  const config = vscode.workspace.getConfiguration("fabricMlv");
  const target = vscode.ConfigurationTarget.Workspace;

  await config.update("fabricApi.runIdFieldPath", profile.runIdFieldPath, target);
  await config.update("fabricApi.statusFieldPath", profile.statusFieldPath, target);
  await config.update("fabricApi.successStates", profile.successStates, target);
  await config.update("fabricApi.failureStates", profile.failureStates, target);
}

async function previewApiProfile(): Promise<void> {
  const selected = await pickApiProfile("Preview Fabric API Profile");
  if (!selected) {
    return;
  }

  const previewText = renderProfilePreview(selected.profile);
  output.appendLine(`Preview for '${selected.profile.label}':`);
  output.appendLine(previewText);
  output.show(true);

  const action = await vscode.window.showInformationMessage(
    `Previewed profile '${selected.profile.label}'. See Fabric MLV Manager output for full details.`,
    "Apply This Profile"
  );

  if (action === "Apply This Profile") {
    await applyProfileToWorkspace(selected.profile);
    output.appendLine(`Applied API profile '${selected.profile.label}' from preview action.`);
    output.show(true);
    vscode.window.showInformationMessage(`Applied Fabric API profile: ${selected.profile.label}`);
  }
}

async function pickApiProfile(title: string): Promise<{ label: string; description: string; profile: ApiProfile } | undefined> {
  return vscode.window.showQuickPick(
    API_PROFILES.map((profile) => ({
      label: profile.label,
      description: profile.description,
      profile
    })),
    {
      title,
      placeHolder: "Choose a predefined parsing profile"
    }
  );
}

function renderProfilePreview(profile: ApiProfile): string {
  return [
    `fabricMlv.fabricApi.runIdFieldPath = ${profile.runIdFieldPath}`,
    `fabricMlv.fabricApi.statusFieldPath = ${profile.statusFieldPath}`,
    `fabricMlv.fabricApi.successStates = [${profile.successStates.join(", ")}]`,
    `fabricMlv.fabricApi.failureStates = [${profile.failureStates.join(", ")}]`
  ].join("\n");
}

export function deactivate(): void {
  // No-op.
}

async function validateSqlDefinitions(): Promise<void> {
  const root = getWorkspaceRoot();
  if (!root) {
    return;
  }

  const config = vscode.workspace.getConfiguration("fabricMlv");
  const sqlRoot = config.get<string>("sqlRoot", "mlv");
  const requireSchemaPrefix = config.get<boolean>("requireSchemaPrefix", true);
  const enforceSqlBodyShape = config.get<boolean>("enforceSqlBodyShape", true);

  const sqlRootPath = path.join(root, sqlRoot);
  const definitions = await findSqlDefinitions(sqlRootPath, root);

  if (definitions.length === 0) {
    vscode.window.showWarningMessage(`No SQL files found under ${sqlRoot}.`);
    return;
  }

  let issues = 0;
  output.clear();
  output.appendLine(`Validating ${definitions.length} SQL file(s) under ${sqlRoot}...`);

  for (const item of definitions) {
    const fileName = path.basename(item.relPath);
    const hasSchemaPrefix = fileName.split(".").length >= 3;

    if (requireSchemaPrefix && !hasSchemaPrefix) {
      issues += 1;
      output.appendLine(`ERROR filename: ${item.relPath} should use schema.view.sql format.`);
    }

    if (!enforceSqlBodyShape) {
      continue;
    }

    const sqlBody = (await fs.readFile(item.fullPath, "utf8")).trim();
    if (/^\s*create\s+(or\s+replace\s+)?materialized\s+lake\s+view\b/i.test(sqlBody)) {
      issues += 1;
      output.appendLine(
        `ERROR SQL shape: ${item.relPath} should not include CREATE MATERIALIZED LAKE VIEW.`
      );
    }

    const hasAsSelect = /\bAS\b[\s\S]*\bSELECT\b/i.test(sqlBody);
    if (!hasAsSelect) {
      issues += 1;
      output.appendLine(`ERROR SQL shape: ${item.relPath} is missing an AS ... SELECT body.`);
    }
  }

  output.show(true);
  await mlvProvider?.refresh();
  if (issues === 0) {
    vscode.window.showInformationMessage(`Validation passed for ${definitions.length} SQL file(s).`);
    output.appendLine("Validation passed.");
    return;
  }

  vscode.window.showErrorMessage(`Validation found ${issues} issue(s). See Fabric MLV Manager output.`);
  output.appendLine(`Validation completed with ${issues} issue(s).`);
}

async function generateDeploymentPlan(): Promise<void> {
  const root = getWorkspaceRoot();
  if (!root) {
    return;
  }

  const config = vscode.workspace.getConfiguration("fabricMlv");
  const sqlRoot = config.get<string>("sqlRoot", "mlv");
  const metadataFile = config.get<string>("metadataFile", "mlv_metadata.json");

  const sqlRootPath = path.join(root, sqlRoot);
  const metadataPath = path.join(sqlRootPath, metadataFile);

  const definitions = await findSqlDefinitions(sqlRootPath, root);
  const orderedDefinitions = await orderDefinitionsByDependency(definitions);
  const metadata = await loadMetadata(metadataPath);

  const toCreateOrReplace: SqlDefinition[] = [];

  for (const def of orderedDefinitions) {
    const previous = metadata[def.key]?.datetime;
    const previousDate = previous ? new Date(previous) : new Date(0);
    if (Number.isNaN(previousDate.getTime()) || def.modifiedMs > previousDate.getTime()) {
      toCreateOrReplace.push(def);
    }
  }

  const definitionKeys = new Set(definitions.map((d) => d.key));
  const obsoleteCandidates = Object.keys(metadata).filter((k) => !definitionKeys.has(k));

  output.clear();
  output.appendLine("Fabric MLV deployment plan");
  output.appendLine("=".repeat(36));
  output.appendLine(`SQL root: ${sqlRoot}`);
  output.appendLine(`Metadata file: ${path.relative(root, metadataPath)}`);
  output.appendLine(`Discovered definitions: ${definitions.length}`);
  output.appendLine("Create/replace order: dependency-based (not folder-based)");
  output.appendLine(`Create or replace: ${toCreateOrReplace.length}`);
  output.appendLine(`Obsolete candidates: ${obsoleteCandidates.length}`);

  if (toCreateOrReplace.length > 0) {
    output.appendLine("\nCreate or replace list:");
    for (const item of toCreateOrReplace) {
      output.appendLine(`- ${item.key} (${item.relPath})`);
    }
  }

  if (obsoleteCandidates.length > 0) {
    output.appendLine("\nObsolete candidates:");
    for (const key of obsoleteCandidates) {
      output.appendLine(`- ${key}`);
    }
  }

  output.show(true);
  await mlvProvider?.refresh();
  vscode.window.showInformationMessage(
    `Plan ready: ${toCreateOrReplace.length} create/replace, ${obsoleteCandidates.length} obsolete.`
  );
}

async function showDependencyGraph(): Promise<void> {
  const config = vscode.workspace.getConfiguration("fabricMlv");
  const direction = normalizeGraphDirection(config.get<string>("dependencyGraphDirection", "LR"));
  const graphData = await buildDependencyGraphData();
  if (!graphData) {
    return;
  }

  if (graphData.nodes.size === 0) {
    vscode.window.showWarningMessage(`No SQL files found under ${graphData.sqlRoot}.`);
    return;
  }

  const panel = vscode.window.createWebviewPanel(
    "fabricMlvDependencyGraph",
    "Fabric MLV Dependency Graph",
    vscode.ViewColumn.Active,
    {
      enableScripts: true
    }
  );

  panel.webview.html = renderDependencyGraphSvgHtml(
    graphData.nodes,
    graphData.edges,
    graphData.sqlRoot,
    direction
  );
}

async function toggleDependencyGraphDirection(): Promise<void> {
  const config = vscode.workspace.getConfiguration("fabricMlv");
  const current = normalizeGraphDirection(config.get<string>("dependencyGraphDirection", "LR"));
  const next: GraphDirection = current === "LR" ? "TB" : "LR";

  await config.update("dependencyGraphDirection", next, vscode.ConfigurationTarget.Workspace);
  vscode.window.showInformationMessage(
    `Dependency graph direction set to ${next === "LR" ? "Left to Right" : "Top to Bottom"}.`
  );
}

async function buildDependencyGraphData(): Promise<{
  sqlRoot: string;
  nodes: Map<string, GraphNodeKind>;
  edges: Set<string>;
} | undefined> {
  const root = getWorkspaceRoot();
  if (!root) {
    return undefined;
  }

  const config = vscode.workspace.getConfiguration("fabricMlv");
  const sqlRoot = config.get<string>("sqlRoot", "mlv");
  const sqlRootPath = path.join(root, sqlRoot);
  const definitions = await findSqlDefinitions(sqlRootPath, root);

  if (definitions.length === 0) {
    return { sqlRoot, nodes: new Map<string, GraphNodeKind>(), edges: new Set<string>() };
  }

  const mlvKeys = new Set(definitions.map((d) => d.key.toLowerCase()));
  const tableToMlvKey = new Map<string, string>();
  for (const def of definitions) {
    tableToMlvKey.set(def.tableName.toLowerCase(), def.key);
  }

  const edges = new Set<string>();
  const nodes = new Map<string, GraphNodeKind>();

  for (const def of definitions) {
    nodes.set(def.key, "mlv");
    const sql = await fs.readFile(def.fullPath, "utf8");
    const refs = extractSqlReferences(sql);

    for (const ref of refs) {
      const normalized = normalizeReference(ref);
      if (!normalized) {
        continue;
      }

      const resolved = resolveReference(normalized, mlvKeys, tableToMlvKey);
      if (!resolved && !normalized.includes(".")) {
        continue;
      }

      const target = resolved ?? normalized;
      const targetType: GraphNodeKind = resolved ? "mlv" : "table";
      nodes.set(target, targetType);
      edges.add(`${target}-->${def.key}`);
    }
  }

  return { sqlRoot, nodes, edges };
}

class CompactDependencyGraphViewProvider implements vscode.WebviewViewProvider {
  private view?: vscode.WebviewView;

  async resolveWebviewView(webviewView: vscode.WebviewView): Promise<void> {
    this.view = webviewView;
    webviewView.webview.options = { enableScripts: false };
    await this.refresh();
  }

  async refresh(): Promise<void> {
    if (!this.view) {
      return;
    }

    const graphData = await buildDependencyGraphData();
    if (!graphData) {
      this.view.webview.html = renderCompactDependencyGraphHtml(
        "mlv",
        new Map<string, GraphNodeKind>(),
        new Set<string>()
      );
      return;
    }

    this.view.webview.html = renderCompactDependencyGraphHtml(
      graphData.sqlRoot,
      graphData.nodes,
      graphData.edges
    );
  }
}

async function triggerNotebookRun(): Promise<void> {
  const config = vscode.workspace.getConfiguration("fabricMlv");
  const apiUrl = config.get<string>("fabricApi.url", "").trim();
  const method = config.get<string>("fabricApi.method", "POST").toUpperCase();
  const token = config.get<string>("fabricApi.authToken", "").trim();
  const payloadJson = config.get<string>("fabricApi.payloadJson", "{\"source\":\"vscode-fabric-mlv\"}");
  const statusUrlTemplate = config.get<string>("fabricApi.statusUrlTemplate", "").trim();
  const pollIntervalMs = config.get<number>("fabricApi.pollIntervalMs", 5000);
  const maxPollAttempts = config.get<number>("fabricApi.maxPollAttempts", 30);
  const runIdFieldPath = config.get<string>("fabricApi.runIdFieldPath", "runId").trim();
  const statusFieldPath = config.get<string>("fabricApi.statusFieldPath", "status").trim();
  const successStates = normalizeStates(
    config.get<string[]>("fabricApi.successStates", ["succeeded", "completed"])
  );
  const failureStates = normalizeStates(
    config.get<string[]>("fabricApi.failureStates", ["failed", "cancelled", "canceled"])
  );

  if (!apiUrl) {
    vscode.window.showErrorMessage("Set fabricMlv.fabricApi.url before triggering deployment.");
    return;
  }

  let body: unknown;
  try {
    body = JSON.parse(payloadJson);
  } catch {
    vscode.window.showErrorMessage("Invalid fabricMlv.fabricApi.payloadJson. Must be valid JSON text.");
    return;
  }

  const headers: Record<string, string> = {
    "Content-Type": "application/json"
  };

  if (token) {
    headers.Authorization = `Bearer ${token}`;
  }

  output.appendLine(`Triggering deployment via ${method} ${apiUrl}`);

  try {
    const response = await fetch(apiUrl, {
      method,
      headers,
      body: JSON.stringify(body)
    });

    const responseText = await response.text();
    output.appendLine(`Response status: ${response.status}`);
    output.appendLine(`Response body: ${responseText}`);
    output.show(true);

    if (!response.ok) {
      vscode.window.showErrorMessage(
        `Deployment trigger failed (${response.status}). See Fabric MLV Manager output.`
      );
      return;
    }

    const parsed = tryParseJson(responseText);
    const runId = extractRunId(parsed, runIdFieldPath);

    if (!statusUrlTemplate || !runId) {
      vscode.window.showInformationMessage("Deployment trigger request sent successfully.");
      return;
    }

    const statusUrl = statusUrlTemplate.replace("{runId}", encodeURIComponent(runId));
    output.appendLine(`Polling deployment status: ${statusUrl}`);
    await pollDeploymentStatus(
      statusUrl,
      method,
      headers,
      pollIntervalMs,
      maxPollAttempts,
      statusFieldPath,
      successStates,
      failureStates
    );
  } catch (error) {
    output.appendLine(`Request failed: ${String(error)}`);
    output.show(true);
    vscode.window.showErrorMessage("Failed to reach deployment endpoint. See output for details.");
  }
}

async function pollDeploymentStatus(
  statusUrl: string,
  method: string,
  headers: Record<string, string>,
  pollIntervalMs: number,
  maxPollAttempts: number,
  statusFieldPath: string,
  successStates: Set<string>,
  failureStates: Set<string>
): Promise<void> {
  const terminalStates = new Set([...successStates, ...failureStates]);

  for (let attempt = 1; attempt <= maxPollAttempts; attempt += 1) {
    await delay(pollIntervalMs);

    try {
      const response = await fetch(statusUrl, {
        method: method === "PUT" ? "PUT" : "GET",
        headers
      });
      const text = await response.text();
      const payload = tryParseJson(text);
      const state = extractState(payload, statusFieldPath) ?? "unknown";

      output.appendLine(`Status attempt ${attempt}/${maxPollAttempts}: ${state}`);

      if (!response.ok) {
        output.appendLine(`Status request failed (${response.status}): ${text}`);
        continue;
      }

      if (terminalStates.has(state.toLowerCase())) {
        output.show(true);
        if (successStates.has(state.toLowerCase())) {
          vscode.window.showInformationMessage(`Fabric deployment completed with status: ${state}.`);
        } else {
          vscode.window.showErrorMessage(`Fabric deployment finished with status: ${state}.`);
        }
        return;
      }
    } catch (error) {
      output.appendLine(`Status polling error: ${String(error)}`);
    }
  }

  output.show(true);
  vscode.window.showWarningMessage("Polling reached max attempts. Check deployment status in Fabric.");
}

function extractRunId(payload: unknown, preferredPath: string): string | undefined {
  const preferred = getByPath(payload, preferredPath);
  if (typeof preferred === "string" && preferred.trim()) {
    return preferred;
  }

  if (!payload || typeof payload !== "object") {
    return undefined;
  }

  const candidate = payload as Record<string, unknown>;
  const runId = candidate.runId ?? candidate.id ?? candidate.operationId;
  return typeof runId === "string" ? runId : undefined;
}

function extractState(payload: unknown, preferredPath: string): string | undefined {
  const preferred = getByPath(payload, preferredPath);
  if (typeof preferred === "string" && preferred.trim()) {
    return preferred;
  }

  if (!payload || typeof payload !== "object") {
    return undefined;
  }

  const candidate = payload as Record<string, unknown>;
  const state = candidate.status ?? candidate.state ?? candidate.result;
  return typeof state === "string" ? state : undefined;
}

function tryParseJson(input: string): unknown {
  try {
    return JSON.parse(input);
  } catch {
    return undefined;
  }
}

function getByPath(payload: unknown, fieldPath: string): unknown {
  if (!fieldPath) {
    return undefined;
  }

  const parts = fieldPath.split(".").map((p) => p.trim()).filter(Boolean);
  if (parts.length === 0) {
    return undefined;
  }

  let current: unknown = payload;
  for (const part of parts) {
    if (!current || typeof current !== "object") {
      return undefined;
    }

    const obj = current as Record<string, unknown>;
    current = obj[part];
  }

  return current;
}

function normalizeStates(values: string[]): Set<string> {
  return new Set(values.map((v) => v.trim().toLowerCase()).filter(Boolean));
}

function extractSqlReferences(sql: string): string[] {
  const dependencySection = getDependencySearchSection(sql);
  const sanitized = stripSqlNoise(dependencySection);
  const cteAliases = extractCteAliases(sanitized);
  const refs: string[] = [];
  const regex = /\b(?:from|join)\s+([a-zA-Z_][\w$]*(?:\.[a-zA-Z_][\w$]*)*)/gi;
  let match: RegExpExecArray | null = regex.exec(sanitized);

  while (match) {
    const candidate = match[1];
    if (!cteAliases.has(candidate.toLowerCase())) {
      refs.push(candidate);
    }
    match = regex.exec(sanitized);
  }

  return refs;
}

function getDependencySearchSection(sql: string): string {
  // Most MLV files have options (COMMENT/TBLPROPERTIES/...) followed by AS SELECT.
  // Restricting search to text after AS avoids false positives from prose in options.
  const asMatch = /\bAS\b/i.exec(sql);
  if (!asMatch || typeof asMatch.index !== "number") {
    return sql;
  }

  return sql.slice(asMatch.index + asMatch[0].length);
}

function stripSqlNoise(sql: string): string {
  let cleaned = sql;

  // Remove block comments.
  cleaned = cleaned.replace(/\/\*[\s\S]*?\*\//g, " ");
  // Remove line comments.
  cleaned = cleaned.replace(/--.*$/gm, " ");
  // Remove quoted literals to prevent matching FROM/JOIN words inside strings.
  cleaned = cleaned.replace(/'(?:''|[^'])*'/g, " ");
  cleaned = cleaned.replace(/"(?:""|[^"])*"/g, " ");

  return cleaned;
}

function extractCteAliases(sql: string): Set<string> {
  const aliases = new Set<string>();
  const cteRegex = /(?:\bwith\b|,)\s*([a-zA-Z_][\w$]*)\s+as\s*\(/gi;
  let match: RegExpExecArray | null = cteRegex.exec(sql);

  while (match) {
    aliases.add(match[1].toLowerCase());
    match = cteRegex.exec(sql);
  }

  return aliases;
}

function normalizeReference(raw: string): string | undefined {
  const trimmed = raw.trim();
  if (!trimmed || trimmed.startsWith("(")) {
    return undefined;
  }

  return trimmed.replace(/[`\[\]"]/g, "");
}

function resolveReference(
  ref: string,
  mlvKeys: Set<string>,
  tableToMlvKey: Map<string, string>
): string | undefined {
  if (ref.includes(".")) {
    return mlvKeys.has(ref.toLowerCase()) ? ref : undefined;
  }

  const maybe = tableToMlvKey.get(ref.toLowerCase());
  return maybe;
}

async function orderDefinitionsByDependency(
  definitions: SqlDefinition[]
): Promise<SqlDefinition[]> {
  const byKey = new Map(definitions.map((d) => [d.key, d]));
  const keys = [...byKey.keys()].sort((a, b) => a.localeCompare(b));

  const mlvKeys = new Set(keys.map((k) => k.toLowerCase()));
  const tableToMlvKey = new Map<string, string>();
  for (const def of definitions) {
    tableToMlvKey.set(def.tableName.toLowerCase(), def.key);
  }

  const outgoing = new Map<string, Set<string>>();
  const indegree = new Map<string, number>();
  for (const key of keys) {
    outgoing.set(key, new Set<string>());
    indegree.set(key, 0);
  }

  for (const def of definitions) {
    const sql = await fs.readFile(def.fullPath, "utf8");
    const refs = extractSqlReferences(sql);

    for (const ref of refs) {
      const normalized = normalizeReference(ref);
      if (!normalized) {
        continue;
      }

      const resolved = resolveReference(normalized, mlvKeys, tableToMlvKey);
      if (!resolved || resolved === def.key) {
        continue;
      }

      const next = outgoing.get(resolved);
      if (next && !next.has(def.key)) {
        next.add(def.key);
        indegree.set(def.key, (indegree.get(def.key) ?? 0) + 1);
      }
    }
  }

  const queue = keys.filter((k) => (indegree.get(k) ?? 0) === 0);
  const orderedKeys: string[] = [];

  while (queue.length > 0) {
    queue.sort((a, b) => a.localeCompare(b));
    const current = queue.shift();
    if (!current) {
      continue;
    }

    orderedKeys.push(current);
    const dependents = [...(outgoing.get(current) ?? new Set<string>())].sort((a, b) => a.localeCompare(b));
    for (const dependent of dependents) {
      indegree.set(dependent, (indegree.get(dependent) ?? 0) - 1);
      if ((indegree.get(dependent) ?? 0) === 0) {
        queue.push(dependent);
      }
    }
  }

  if (orderedKeys.length !== keys.length) {
    const orderedSet = new Set(orderedKeys);
    const remaining = keys.filter((k) => !orderedSet.has(k));
    output.appendLine(
      `WARNING dependency cycle/unresolved ordering detected for: ${remaining.join(", ")}`
    );
    output.appendLine("WARNING falling back to alphabetical order for remaining definitions.");
    orderedKeys.push(...remaining);
  }

  return orderedKeys.map((key) => byKey.get(key)).filter((d): d is SqlDefinition => Boolean(d));
}

function normalizeGraphDirection(input: string): GraphDirection {
  return input.toUpperCase() === "TB" ? "TB" : "LR";
}

function getNodeSchema(name: string): string {
  const trimmed = name.trim();
  const dot = trimmed.indexOf(".");
  if (dot <= 0) {
    return "unknown";
  }

  return trimmed.slice(0, dot).toLowerCase();
}

function buildSchemaColorMap(nodeNames: string[]): Map<string, SchemaColor> {
  const schemas = [...new Set(nodeNames.map((name) => getNodeSchema(name)))].sort((a, b) => a.localeCompare(b));
  const colors = new Map<string, SchemaColor>();

  for (let i = 0; i < schemas.length; i += 1) {
    colors.set(schemas[i], SCHEMA_PALETTE[i % SCHEMA_PALETTE.length]);
  }

  return colors;
}

function renderDependencyGraphSvgHtml(
  nodes: Map<string, "mlv" | "table">,
  edges: Set<string>,
  sqlRoot: string,
  direction: GraphDirection
): string {
  const sortedNodeNames = [...nodes.keys()].sort((a, b) => a.localeCompare(b));
  const schemaColors = buildSchemaColorMap(sortedNodeNames);

  const adjacency = new Map<string, string[]>();
  const indegree = new Map<string, number>();
  for (const name of sortedNodeNames) {
    adjacency.set(name, []);
    indegree.set(name, 0);
  }

  for (const edge of edges) {
    const [from, to] = edge.split("-->");
    if (!from || !to || !adjacency.has(from) || !adjacency.has(to)) {
      continue;
    }

    adjacency.get(from)?.push(to);
    indegree.set(to, (indegree.get(to) ?? 0) + 1);
  }

  for (const [name, list] of adjacency.entries()) {
    list.sort((a, b) => a.localeCompare(b));
    adjacency.set(name, list);
  }

  const queue = sortedNodeNames.filter((name) => (indegree.get(name) ?? 0) === 0);
  const levelByNode = new Map<string, number>();
  for (const name of sortedNodeNames) {
    levelByNode.set(name, 0);
  }

  while (queue.length > 0) {
    const current = queue.shift();
    if (!current) {
      continue;
    }

    const currentLevel = levelByNode.get(current) ?? 0;
    const nextNodes = adjacency.get(current) ?? [];

    for (const next of nextNodes) {
      levelByNode.set(next, Math.max(levelByNode.get(next) ?? 0, currentLevel + 1));
      indegree.set(next, (indegree.get(next) ?? 0) - 1);
      if ((indegree.get(next) ?? 0) === 0) {
        queue.push(next);
      }
    }
  }

  const levels = new Map<number, string[]>();
  let maxLevel = 0;
  for (const name of sortedNodeNames) {
    const level = levelByNode.get(name) ?? 0;
    maxLevel = Math.max(maxLevel, level);
    const list = levels.get(level) ?? [];
    list.push(name);
    levels.set(level, list);
  }

  for (const [level, list] of levels.entries()) {
    list.sort((a, b) => a.localeCompare(b));
    levels.set(level, list);
  }

  const nodeWidth = 320;
  const nodeHeight = 38;
  const laneGap = 92;
  const rowGap = 24;
  const margin = 24;
  const maxLaneSize = Math.max(1, ...[...levels.values()].map((v) => v.length));

  const width =
    direction === "LR"
      ? margin * 2 + (maxLevel + 1) * nodeWidth + maxLevel * laneGap
      : margin * 2 + maxLaneSize * nodeWidth + (maxLaneSize - 1) * rowGap;
  const height =
    direction === "TB"
      ? margin * 2 + (maxLevel + 1) * nodeHeight + maxLevel * laneGap
      : margin * 2 + maxLaneSize * nodeHeight + (maxLaneSize - 1) * rowGap;

  const position = new Map<string, { x: number; y: number }>();
  for (let level = 0; level <= maxLevel; level += 1) {
    const list = levels.get(level) ?? [];
    for (let i = 0; i < list.length; i += 1) {
      const name = list[i];
      const x = direction === "LR" ? margin + level * (nodeWidth + laneGap) : margin + i * (nodeWidth + rowGap);
      const y = direction === "TB" ? margin + level * (nodeHeight + laneGap) : margin + i * (nodeHeight + rowGap);
      position.set(name, { x, y });
    }
  }

  const escapeXml = (value: string): string =>
    value
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/\"/g, "&quot;")
      .replace(/'/g, "&#39;");

  const edgeFragments: string[] = [];
  for (const edge of [...edges].sort((a, b) => a.localeCompare(b))) {
    const [from, to] = edge.split("-->");
    const fromPos = from ? position.get(from) : undefined;
    const toPos = to ? position.get(to) : undefined;
    if (!fromPos || !toPos) {
      continue;
    }

    if (direction === "LR") {
      const fromX = fromPos.x + nodeWidth;
      const fromY = fromPos.y + nodeHeight / 2;
      const toX = toPos.x;
      const toY = toPos.y + nodeHeight / 2;
      const ctrlOffset = Math.max(34, (toX - fromX) * 0.45);
      edgeFragments.push(
        `<path d="M ${fromX} ${fromY} C ${fromX + ctrlOffset} ${fromY}, ${toX - ctrlOffset} ${toY}, ${toX} ${toY}" fill="none" stroke="#7f8c9a" stroke-width="1.2" marker-end="url(#arrow)"/>`
      );
    } else {
      const fromX = fromPos.x + nodeWidth / 2;
      const fromY = fromPos.y + nodeHeight;
      const toX = toPos.x + nodeWidth / 2;
      const toY = toPos.y;
      const ctrlOffset = Math.max(34, (toY - fromY) * 0.45);
      edgeFragments.push(
        `<path d="M ${fromX} ${fromY} C ${fromX} ${fromY + ctrlOffset}, ${toX} ${toY - ctrlOffset}, ${toX} ${toY}" fill="none" stroke="#7f8c9a" stroke-width="1.2" marker-end="url(#arrow)"/>`
      );
    }
  }

  const nodeFragments: string[] = [];
  for (const name of sortedNodeNames) {
    const nodeType = nodes.get(name) ?? "table";
    const pos = position.get(name);
    if (!pos) {
      continue;
    }

    const schema = getNodeSchema(name);
    const schemaColor = schemaColors.get(schema) ?? SCHEMA_PALETTE[0];
    const fill = schemaColor.fill;
    const stroke = schemaColor.stroke;
    const radius = nodeType === "mlv" ? 6 : 18;

    nodeFragments.push(
      `<rect x="${pos.x}" y="${pos.y}" width="${nodeWidth}" height="${nodeHeight}" rx="${radius}" ry="${radius}" fill="${fill}" stroke="${stroke}" stroke-width="1.2"/>`
    );
    nodeFragments.push(
      `<text x="${pos.x + 12}" y="${pos.y + 24}" fill="#ffffff" font-size="13" font-family="var(--vscode-font-family)">${escapeXml(name)}</text>`
    );
  }

  const escapedSqlRoot = escapeXml(sqlRoot);
  const orientationLabel = direction === "LR" ? "Left to Right" : "Top to Bottom";
  const schemaLegend = [...schemaColors.entries()]
    .map(
      ([schema, color]) =>
        `<span class="schema-chip" style="background:${escapeXml(color.fill)};border-color:${escapeXml(color.stroke)}"></span>${escapeXml(schema)}`
    )
    .join(" | ");

  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <style>
    :root {
      color-scheme: light dark;
    }

    body {
      margin: 0;
      padding: 16px;
      font-family: var(--vscode-font-family);
      color: var(--vscode-foreground);
      background: var(--vscode-editor-background);
    }

    h1 {
      margin-top: 0;
      font-size: 1.2rem;
    }

    .meta {
      color: var(--vscode-descriptionForeground);
      margin-bottom: 16px;
    }

    .toolbar {
      display: flex;
      gap: 8px;
      margin-bottom: 10px;
    }

    .toolbar button {
      border: 1px solid var(--vscode-button-border, transparent);
      background: var(--vscode-button-background);
      color: var(--vscode-button-foreground);
      border-radius: 6px;
      padding: 6px 10px;
      cursor: pointer;
      font-size: 12px;
    }

    .toolbar button:hover {
      background: var(--vscode-button-hoverBackground);
    }

    .graph-wrap {
      overflow: auto;
      border: 1px solid var(--vscode-panel-border);
      border-radius: 8px;
      padding: 12px;
      background: color-mix(in srgb, var(--vscode-editor-background) 94%, var(--vscode-foreground) 6%);
    }

    .legend {
      margin-top: 12px;
      color: var(--vscode-descriptionForeground);
      font-size: 0.9rem;
      line-height: 1.6;
    }

    .schema-chip {
      display: inline-block;
      width: 10px;
      height: 10px;
      border-radius: 2px;
      border: 1px solid;
      margin-right: 4px;
      vertical-align: middle;
    }

    .chip {
      display: inline-block;
      width: 10px;
      height: 10px;
      border-radius: 2px;
      margin-right: 6px;
      vertical-align: middle;
    }

    .chip.mlv {
      background: #1e6fbe;
    }

    .chip.table {
      background: #2f9e44;
      border-radius: 999px;
    }
  </style>
</head>
<body>
  <h1>Fabric MLV Dependency Graph</h1>
  <div class="meta">SQL root: ${escapedSqlRoot} | Direction: ${orientationLabel}</div>
  <div class="toolbar">
    <button id="zoomIn" type="button" title="Zoom in">Zoom In</button>
    <button id="zoomOut" type="button" title="Zoom out">Zoom Out</button>
    <button id="fit" type="button" title="Fit graph to window">Fit to Window</button>
  </div>
  <div class="graph-wrap">
    <svg id="graphSvg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}" preserveAspectRatio="xMidYMid meet" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Fabric MLV dependency graph">
      <defs>
        <marker id="arrow" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto" markerUnits="strokeWidth">
          <path d="M0,0 L0,6 L6,3 z" fill="#7f8c9a" />
        </marker>
      </defs>
      ${edgeFragments.join("\n      ")}
      ${nodeFragments.join("\n      ")}
    </svg>
  </div>
  <div class="legend"><span class="chip mlv"></span>Rectangle = MLV defined in SQL folder, <span class="chip table"></span>Rounded = external table or unresolved reference. Arrows point source to dependent.<br/>Color by schema: ${schemaLegend}</div>
  <script>
    (() => {
      const svg = document.getElementById("graphSvg");
      const zoomInButton = document.getElementById("zoomIn");
      const zoomOutButton = document.getElementById("zoomOut");
      const fitButton = document.getElementById("fit");

      if (!(svg instanceof SVGElement)) {
        return;
      }

      const base = { x: 0, y: 0, w: ${width}, h: ${height} };
      const minScale = 0.2;
      const maxScale = 5;
      let state = { ...base };

      const applyViewBox = () => {
        svg.setAttribute("viewBox", state.x + " " + state.y + " " + state.w + " " + state.h);
      };

      const zoom = (factor) => {
        const centerX = state.x + state.w / 2;
        const centerY = state.y + state.h / 2;

        const nextW = Math.max(base.w * minScale, Math.min(base.w * maxScale, state.w * factor));
        const nextH = Math.max(base.h * minScale, Math.min(base.h * maxScale, state.h * factor));

        state = {
          x: centerX - nextW / 2,
          y: centerY - nextH / 2,
          w: nextW,
          h: nextH
        };

        applyViewBox();
      };

      zoomInButton?.addEventListener("click", () => zoom(0.8));
      zoomOutButton?.addEventListener("click", () => zoom(1.25));
      fitButton?.addEventListener("click", () => {
        state = { ...base };
        applyViewBox();
      });
    })();
  </script>
</body>
</html>`;
}

function renderCompactDependencyGraphHtml(
  sqlRoot: string,
  nodes: Map<string, GraphNodeKind>,
  edges: Set<string>
): string {
  const sortedNodeNames = [...nodes.keys()].sort((a, b) => a.localeCompare(b));
  const schemaColors = buildSchemaColorMap(sortedNodeNames);
  const adjacency = new Map<string, string[]>();
  const indegree = new Map<string, number>();
  for (const name of sortedNodeNames) {
    adjacency.set(name, []);
    indegree.set(name, 0);
  }

  for (const edge of edges) {
    const [from, to] = edge.split("-->");
    if (!from || !to || !adjacency.has(from) || !adjacency.has(to)) {
      continue;
    }

    adjacency.get(from)?.push(to);
    indegree.set(to, (indegree.get(to) ?? 0) + 1);
  }

  for (const [name, list] of adjacency.entries()) {
    list.sort((a, b) => a.localeCompare(b));
    adjacency.set(name, list);
  }

  const queue = sortedNodeNames.filter((name) => (indegree.get(name) ?? 0) === 0);
  const levelByNode = new Map<string, number>();
  for (const name of sortedNodeNames) {
    levelByNode.set(name, 0);
  }

  while (queue.length > 0) {
    const current = queue.shift();
    if (!current) {
      continue;
    }

    const currentLevel = levelByNode.get(current) ?? 0;
    const nextNodes = adjacency.get(current) ?? [];

    for (const next of nextNodes) {
      levelByNode.set(next, Math.max(levelByNode.get(next) ?? 0, currentLevel + 1));
      indegree.set(next, (indegree.get(next) ?? 0) - 1);
      if ((indegree.get(next) ?? 0) === 0) {
        queue.push(next);
      }
    }
  }

  const levels = new Map<number, string[]>();
  let maxLevel = 0;
  for (const name of sortedNodeNames) {
    const level = levelByNode.get(name) ?? 0;
    maxLevel = Math.max(maxLevel, level);
    const list = levels.get(level) ?? [];
    list.push(name);
    levels.set(level, list);
  }

  for (const [level, list] of levels.entries()) {
    list.sort((a, b) => a.localeCompare(b));
    levels.set(level, list);
  }

  const margin = 12;
  const maxLaneSize = Math.max(1, ...[...levels.values()].map((v) => v.length));
  const width = Math.max(360, margin * 2 + (maxLevel + 1) * 64);
  const height = Math.max(220, margin * 2 + maxLaneSize * 22);
  const usableWidth = Math.max(1, width - margin * 2);
  const usableHeight = Math.max(1, height - margin * 2);
  const xStep = maxLevel > 0 ? usableWidth / maxLevel : 0;

  const positions = new Map<string, { x: number; y: number }>();
  for (let level = 0; level <= maxLevel; level += 1) {
    const list = levels.get(level) ?? [];
    const laneCount = list.length;
    for (let i = 0; i < list.length; i += 1) {
      const name = list[i];
      const y = laneCount <= 1 ? margin + usableHeight / 2 : margin + ((i + 1) * usableHeight) / (laneCount + 1);
      positions.set(name, {
        x: margin + level * xStep,
        y
      });
    }
  }

  const edgeSvg: string[] = [];
  for (const edge of [...edges].sort((a, b) => a.localeCompare(b))) {
    const [from, to] = edge.split("-->");
    if (!from || !to) {
      continue;
    }

    const p1 = positions.get(from);
    const p2 = positions.get(to);
    if (!p1 || !p2) {
      continue;
    }

    edgeSvg.push(
      `<line x1="${p1.x.toFixed(2)}" y1="${p1.y.toFixed(2)}" x2="${p2.x.toFixed(2)}" y2="${p2.y.toFixed(2)}" stroke="var(--vscode-descriptionForeground)" stroke-opacity="0.45" stroke-width="1" />`
    );
  }

  const escapeXml = (value: string): string =>
    value
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/\"/g, "&quot;")
      .replace(/'/g, "&#39;");

  const nodeSvg: string[] = [];
  for (const name of sortedNodeNames) {
    const p = positions.get(name);
    if (!p) {
      continue;
    }

    const kind = nodes.get(name) ?? "table";
    const schema = getNodeSchema(name);
    const schemaColor = schemaColors.get(schema) ?? SCHEMA_PALETTE[0];
    const radius = kind === "mlv" ? 5 : 4;
    const fill = schemaColor.fill;
    const stroke = schemaColor.stroke;

    nodeSvg.push(
      `<circle cx="${p.x.toFixed(2)}" cy="${p.y.toFixed(2)}" r="${radius}" fill="${fill}" stroke="${stroke}" stroke-width="1"><title>${escapeXml(name)}</title></circle>`
    );
  }

  const escapedSqlRoot = escapeXml(sqlRoot);
  const schemaLegend = [...schemaColors.entries()]
    .map(
      ([schema, color]) =>
        `<span class="schema-chip" style="background:${escapeXml(color.fill)};border-color:${escapeXml(color.stroke)}"></span>${escapeXml(schema)}`
    )
    .join(" | ");
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <style>
    body {
      margin: 0;
      padding: 8px;
      color: var(--vscode-foreground);
      background: var(--vscode-editor-background);
      font-family: var(--vscode-font-family);
    }

    .meta {
      font-size: 11px;
      color: var(--vscode-descriptionForeground);
      margin-bottom: 6px;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }

    .graph-wrap {
      border: 1px solid var(--vscode-panel-border);
      border-radius: 6px;
      padding: 4px;
      overflow: auto;
    }

    svg {
      width: 100%;
      height: auto;
      display: block;
      min-height: 200px;
    }

    .legend {
      margin-top: 6px;
      font-size: 11px;
      color: var(--vscode-descriptionForeground);
      line-height: 1.6;
    }

    .schema-chip {
      display: inline-block;
      width: 8px;
      height: 8px;
      border-radius: 2px;
      border: 1px solid;
      margin-right: 4px;
      vertical-align: middle;
    }
  </style>
</head>
<body>
  <div class="meta">Compact dependency map | SQL root: ${escapedSqlRoot}</div>
  <div class="graph-wrap">
    <svg viewBox="0 0 ${width} ${height}" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Compact Fabric MLV dependency graph">
      ${edgeSvg.join("\n      ")}
      ${nodeSvg.join("\n      ")}
    </svg>
  </div>
  <div class="legend">Hover a node to see full object name. Shape indicates kind (larger = MLV, smaller = external).<br/>Color by schema: ${schemaLegend}</div>
</body>
</html>`;
}

function getWorkspaceRoot(): string | undefined {
  const folders = vscode.workspace.workspaceFolders;
  if (!folders || folders.length === 0) {
    vscode.window.showErrorMessage("Open a workspace folder to use Fabric MLV Manager.");
    return undefined;
  }

  return folders[0].uri.fsPath;
}

async function loadMetadata(metadataPath: string): Promise<MetadataMap> {
  try {
    const raw = await fs.readFile(metadataPath, "utf8");
    if (!raw.trim()) {
      return {};
    }

    const parsed = JSON.parse(raw) as unknown;
    if (parsed && typeof parsed === "object") {
      return parsed as MetadataMap;
    }

    return {};
  } catch {
    return {};
  }
}

async function findSqlDefinitions(sqlRootPath: string, workspaceRoot: string): Promise<SqlDefinition[]> {
  const results: SqlDefinition[] = [];

  try {
    await fs.access(sqlRootPath);
  } catch {
    return results;
  }

  const walk = async (currentDir: string): Promise<void> => {
    const entries = await fs.readdir(currentDir, { withFileTypes: true });

    const sortedDirs = entries
      .filter((entry) => entry.isDirectory())
      .map((entry) => entry.name)
      .sort((a, b) => dirSortKey(a).localeCompare(dirSortKey(b)));

    const sortedFiles = entries
      .filter((entry) => entry.isFile() && entry.name.toLowerCase().endsWith(".sql"))
      .map((entry) => entry.name)
      .sort((a, b) => a.localeCompare(b));

    for (const fileName of sortedFiles) {
      const fullPath = path.join(currentDir, fileName);
      const stat = await fs.stat(fullPath);

      const base = fileName.slice(0, -4);
      const [schema, tableName] = base.includes(".") ? base.split(/\.(.+)/) : ["default", base];

      const relPath = path.relative(workspaceRoot, fullPath);
      const relFromSqlRoot = path.relative(sqlRootPath, fullPath);
      const folderOrder = relFromSqlRoot.split(path.sep)[0] ?? "";

      results.push({
        schema,
        tableName,
        key: `${schema}.${tableName}`,
        fullPath,
        relPath,
        folderOrder,
        modifiedMs: stat.mtimeMs,
        modifiedIso: stat.mtime.toISOString()
      });
    }

    for (const dirName of sortedDirs) {
      await walk(path.join(currentDir, dirName));
    }
  };

  await walk(sqlRootPath);
  return results;
}

function dirSortKey(dirName: string): string {
  const numeric = /^\d+$/.test(dirName);
  if (numeric) {
    return `${"0"}:${dirName.padStart(10, "0")}`;
  }

  return `${"1"}:${dirName}`;
}

type MlvItemKind = "group" | "definition" | "obsolete";

class MlvItem extends vscode.TreeItem {
  readonly kind: MlvItemKind;

  constructor(
    kind: MlvItemKind,
    label: string,
    collapsibleState: vscode.TreeItemCollapsibleState,
    readonly key?: string,
    readonly resourceUri?: vscode.Uri,
    description?: string
  ) {
    super(label, collapsibleState);
    this.kind = kind;
    this.description = description;
    this.contextValue = `fabricMlv.${kind}`;
    this.iconPath =
      kind === "group"
        ? new vscode.ThemeIcon("folder")
        : kind === "obsolete"
          ? new vscode.ThemeIcon("warning")
          : new vscode.ThemeIcon("symbol-misc");

    if (kind === "definition" && resourceUri) {
      this.command = {
        command: "fabricMlv.openSqlFile",
        title: "Open SQL File",
        arguments: [this]
      };
      this.resourceUri = resourceUri;
    }
  }
}

class MlvTreeDataProvider implements vscode.TreeDataProvider<MlvItem> {
  private readonly onDidChangeTreeDataEmitter = new vscode.EventEmitter<MlvItem | undefined>();
  readonly onDidChangeTreeData = this.onDidChangeTreeDataEmitter.event;

  private definitionsByFolder = new Map<string, SqlDefinition[]>();
  private obsoleteKeys: string[] = [];
  private workspaceRoot?: string;

  async refresh(): Promise<void> {
    this.workspaceRoot = getWorkspaceRoot();
    if (!this.workspaceRoot) {
      this.definitionsByFolder = new Map();
      this.obsoleteKeys = [];
      this.onDidChangeTreeDataEmitter.fire(undefined);
      return;
    }

    const config = vscode.workspace.getConfiguration("fabricMlv");
    const sqlRoot = config.get<string>("sqlRoot", "mlv");
    const metadataFile = config.get<string>("metadataFile", "mlv_metadata.json");
    const sqlRootPath = path.join(this.workspaceRoot, sqlRoot);
    const metadataPath = path.join(sqlRootPath, metadataFile);

    const definitions = await findSqlDefinitions(sqlRootPath, this.workspaceRoot);
    const metadata = await loadMetadata(metadataPath);
    const definitionKeys = new Set(definitions.map((d) => d.key));
    this.obsoleteKeys = Object.keys(metadata).filter((k) => !definitionKeys.has(k)).sort((a, b) => a.localeCompare(b));

    const grouped = new Map<string, SqlDefinition[]>();
    for (const definition of definitions) {
      const folder = definition.folderOrder || "root";
      const current = grouped.get(folder) ?? [];
      current.push(definition);
      grouped.set(folder, current);
    }

    const ordered = new Map<string, SqlDefinition[]>();
    const folders = [...grouped.keys()].sort((a, b) => dirSortKey(a).localeCompare(dirSortKey(b)));
    for (const folder of folders) {
      const items = grouped.get(folder) ?? [];
      items.sort((a, b) => a.key.localeCompare(b.key));
      ordered.set(folder, items);
    }

    this.definitionsByFolder = ordered;
    this.onDidChangeTreeDataEmitter.fire(undefined);
  }

  getTreeItem(element: MlvItem): vscode.TreeItem {
    return element;
  }

  async getChildren(element?: MlvItem): Promise<MlvItem[]> {
    if (!this.workspaceRoot) {
      await this.refresh();
    }

    if (!element) {
      const rootItems: MlvItem[] = [];
      for (const folder of this.definitionsByFolder.keys()) {
        const count = this.definitionsByFolder.get(folder)?.length ?? 0;
        rootItems.push(
          new MlvItem(
            "group",
            folder,
            vscode.TreeItemCollapsibleState.Collapsed,
            folder,
            undefined,
            `${count} definition(s)`
          )
        );
      }

      if (this.obsoleteKeys.length > 0) {
        rootItems.push(
          new MlvItem(
            "group",
            "obsolete",
            vscode.TreeItemCollapsibleState.Collapsed,
            "obsolete",
            undefined,
            `${this.obsoleteKeys.length} candidate(s)`
          )
        );
      }

      return rootItems;
    }

    if (element.kind !== "group" || !element.key) {
      return [];
    }

    if (element.key === "obsolete") {
      return this.obsoleteKeys.map((key) =>
        new MlvItem("obsolete", key, vscode.TreeItemCollapsibleState.None)
      );
    }

    const definitions = this.definitionsByFolder.get(element.key) ?? [];
    return definitions.map((definition) =>
      new MlvItem(
        "definition",
        definition.key,
        vscode.TreeItemCollapsibleState.None,
        definition.key,
        vscode.Uri.file(definition.fullPath),
        definition.relPath
      )
    );
  }
}
