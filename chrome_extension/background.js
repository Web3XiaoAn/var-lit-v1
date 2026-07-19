const DEBUGGER_VERSION = "1.3";
const FORWARDER_BUILD = "var-lit-v1";
const COMMAND_PROTOCOL_VERSION = "var-lit-v1-command-v1";
const MAX_QUEUE_SIZE = 1000;
const MAX_QUEUE_AGE_MS = 2000;
const AUTO_RELOAD_COOLDOWN_MS = 5000;
const TEMPLATE_CACHE_VERSION = 2;
const TEMPLATE_LOCAL_STORAGE_KEY = "variationalFetchTemplateCache";
const TEMPLATE_SESSION_STORAGE_KEY = "variationalFetchTemplateSessionCache";
const QUOTE_ORDER_ASSOCIATION_MS = 15000;
const VAR_API_ORIGIN = "https://omni.variational.io";
const VAR_PAGE_URL = `${VAR_API_ORIGIN}/perpetual/BTC`;
const RECOVERY_DELAYS_MS = [1000, 2000, 5000, 10000, 30000];
const ORDER_PATH_ALLOWLIST = new Set([
  "/api/orders/new/market",
  "/api/quotes/accept"
]);

const DEFAULT_CONFIG = {
  configVersion: 4,
  autoStart: false,
  wsEndpoint: "ws://127.0.0.1:8766",
  restEndpoint: "ws://127.0.0.1:8767",
  commandEndpoint: "ws://127.0.0.1:8768",
  domainFilter: "variational",
  restAllowlist: [
    "https://omni.variational.io/api/quotes/indicative"
  ],
  wsAllowlist: [
    "wss://omni-ws-server.prod.ap-northeast-1.variational.io/events",
    "wss://omni-ws-server.prod.ap-northeast-1.variational.io/portfolio"
  ]
};

const state = {
  active: false,
  desiredActive: false,
  recoveryAttempt: 0,
  lastRecoveryReason: null,
  attachedTabId: null,
  attachedMarket: null,
  config: { ...DEFAULT_CONFIG },
  configLoaded: false,
  pendingResponses: new Map(),
  websocketMeta: new Map(),
  lastError: null,
  lastCommandStatus: "-",
  lastAutoReloadAt: 0,
  liveReplayHeaders: {},
  orderFetchTemplates: {},
  quoteFetchTemplates: {},
  templateCacheSource: "未恢复",
  lastQuoteTemplateStatus: "未捕获",
  lastFetchTemplateStatus: "未捕获"
};

let configLoadPromise = null;
let templatePersistPromise = Promise.resolve();
let templatePersistTimer = null;
let startForwardingPromise = null;
let recoveryTimer = null;

class ForwardSocket {
  constructor(label, configKey) {
    this.label = label;
    this.configKey = configKey;
    this.ws = null;
    this.status = "disconnected";
    this.queue = [];
    this.retryTimer = null;
  }

  get endpoint() {
    return state.config[this.configKey];
  }

  connect() {
    if (!state.active) {
      return;
    }

    if (this.ws && (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING)) {
      return;
    }

    const endpoint = this.endpoint;
    if (!endpoint) {
      this.status = "disconnected";
      notifyStatus();
      return;
    }

    this.status = "connecting";
    notifyStatus();

    try {
      const socket = new WebSocket(endpoint);
      this.ws = socket;

      socket.onopen = () => {
        if (this.ws !== socket) {
          return;
        }
        this.status = "authenticating";
        this.flush();
        notifyStatus();
      };

      socket.onclose = () => {
        if (this.ws !== socket) {
          return;
        }
        this.ws = null;
        this.status = "disconnected";
        notifyStatus();
        this.scheduleReconnect();
      };

      socket.onerror = () => {
        if (this.ws !== socket) {
          return;
        }
        this.status = "error";
        notifyStatus();
      };
    } catch (error) {
      this.status = "error";
      state.lastError = `${this.label} 通道连接失败：${error.message}`;
      notifyStatus();
      this.scheduleReconnect();
    }
  }

  send(payload) {
    const data = JSON.stringify(payload);
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(data);
      return;
    }

    this.queue.push({ data, enqueuedAt: Date.now() });
    if (this.queue.length > MAX_QUEUE_SIZE) {
      this.queue.shift();
    }
    this.connect();
  }

  flush() {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      return;
    }
    while (this.queue.length > 0) {
      const item = this.queue.shift();
      if (Date.now() - item.enqueuedAt <= MAX_QUEUE_AGE_MS) {
        this.ws.send(item.data);
      }
    }
  }

  scheduleReconnect() {
    if (!state.active || this.retryTimer) {
      return;
    }
    this.retryTimer = setTimeout(() => {
      this.retryTimer = null;
      this.connect();
    }, 1000);
  }

  restart() {
    this.close();
    this.connect();
  }

  close() {
    if (this.retryTimer) {
      clearTimeout(this.retryTimer);
      this.retryTimer = null;
    }
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
    this.status = "disconnected";
    notifyStatus();
  }
}

class CommandSocket {
  constructor() {
    this.ws = null;
    this.status = "disconnected";
    this.retryTimer = null;
    this.registered = false;
  }

  get endpoint() {
    return state.config.commandEndpoint;
  }

  connect() {
    if (!state.active) {
      return;
    }
    if (this.ws && (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING)) {
      return;
    }

    const endpoint = this.endpoint;
    if (!endpoint) {
      this.status = "disconnected";
      notifyStatus();
      return;
    }

    this.status = "connecting";
    notifyStatus();

    try {
      const socket = new WebSocket(endpoint);
      this.ws = socket;

      socket.onopen = () => {
        if (this.ws !== socket) {
          return;
        }
        this.status = "connected";
        this.registered = false;
        this.send({
          type: "REGISTER",
          role: "extension",
          protocolVersion: COMMAND_PROTOCOL_VERSION,
          build: FORWARDER_BUILD,
          timestamp: nowIso()
        });
        notifyStatus();
      };

      socket.onmessage = (event) => {
        if (this.ws !== socket) {
          return;
        }
        this.handleMessage(event.data).catch((error) => {
          state.lastError = `命令处理失败：${error.message}`;
          notifyStatus();
        });
      };

      socket.onclose = () => {
        if (this.ws !== socket) {
          return;
        }
        this.ws = null;
        this.registered = false;
        this.status = "disconnected";
        notifyStatus();
        this.scheduleReconnect();
      };

      socket.onerror = () => {
        if (this.ws !== socket) {
          return;
        }
        this.status = "error";
        notifyStatus();
      };
    } catch (error) {
      this.status = "error";
      state.lastError = `命令通道连接失败：${error.message}`;
      notifyStatus();
      this.scheduleReconnect();
    }
  }

  send(payload) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(payload));
    }
  }

  async handleMessage(raw) {
    let payload;
    try {
      payload = JSON.parse(raw);
    } catch {
      return;
    }
    const type = String(payload.type || "").toUpperCase();
    if (type === "REGISTER_ACK") {
      const compatible = (
        payload.ok === true &&
        payload.role === "extension" &&
        payload.protocolVersion === COMMAND_PROTOCOL_VERSION &&
        payload.build === FORWARDER_BUILD
      );
      this.registered = compatible;
      this.status = compatible ? "connected" : "error";
      if (!compatible) {
        state.lastError = payload.error || "命令通道协议或构建版本不兼容";
        state.lastCommandStatus = "命令通道注册失败";
        notifyStatus();
        if (this.ws) {
          this.ws.close();
        }
      } else {
        state.lastCommandStatus = "命令通道已连接";
        notifyStatus();
      }
      return;
    }
    if (type === "PING") {
      this.send({ type: "PONG", timestamp: nowIso() });
      return;
    }
    if (type === "PLACE_ORDER") {
      if (!this.registered) {
        this.send({
          type: "ORDER_RESULT",
          requestId: payload.requestId,
          traceId: payload.traceId || null,
          ok: false,
          error: "Chrome extension command registration is not valid.",
          timestamp: nowIso()
        });
        return;
      }
      const result = await executePlaceOrderCommand(payload, "命令通道");
      this.send({
        type: "ORDER_RESULT",
        requestId: payload.requestId,
        traceId: payload.traceId || null,
        ok: result.ok,
        error: result.error,
        detail: result.detail,
        timestamp: nowIso()
      });
    }
  }

  scheduleReconnect() {
    if (!state.active || this.retryTimer) {
      return;
    }
    this.retryTimer = setTimeout(() => {
      this.retryTimer = null;
      this.connect();
    }, 1000);
  }

  restart() {
    this.close();
    this.connect();
  }

  close() {
    if (this.retryTimer) {
      clearTimeout(this.retryTimer);
      this.retryTimer = null;
    }
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
    this.registered = false;
    this.status = "disconnected";
    notifyStatus();
  }
}

const wsForwarder = new ForwardSocket("websocket", "wsEndpoint");
const restForwarder = new ForwardSocket("rest", "restEndpoint");
const commandSocket = new CommandSocket();

function autoReloadAttachedTab(reason) {
  if (!state.active || state.attachedTabId == null) {
    return;
  }
  const now = Date.now();
  if (now - state.lastAutoReloadAt < AUTO_RELOAD_COOLDOWN_MS) {
    return;
  }
  state.lastAutoReloadAt = now;

  chrome.tabs.reload(state.attachedTabId, {}, () => {
    const err = chrome.runtime.lastError;
    if (err) {
      state.lastError = `页面自动刷新失败（${reason}）：${err.message}`;
    } else {
      state.lastError = null;
    }
    notifyStatus();
  });
}

async function ensureConfigLoaded() {
  if (state.configLoaded) {
    return;
  }
  if (!configLoadPromise) {
    configLoadPromise = (async () => {
      const stored = await chrome.storage.local.get([
        "forwarderConfig",
        TEMPLATE_LOCAL_STORAGE_KEY
      ]);
      state.config = sanitizeConfig(stored.forwarderConfig);

      let sessionCache = null;
      try {
        const sessionStored = await chrome.storage.session.get(TEMPLATE_SESSION_STORAGE_KEY);
        sessionCache = sessionStored[TEMPLATE_SESSION_STORAGE_KEY] || null;
      } catch {
        // Older Chromium builds may not expose storage.session.
      }

      const localCache = stored[TEMPLATE_LOCAL_STORAGE_KEY] || null;
      restoreTemplateCache(localCache, sessionCache);
      state.configLoaded = true;
      if (validTemplateCache(localCache)) {
        // Rewrite caches immediately so credentials left by older builds are
        // removed from chrome.storage.local instead of merely being ignored.
        await persistTemplateCache();
      }
    })().finally(() => {
      configLoadPromise = null;
    });
  }
  await configLoadPromise;
}

function isTransientReplayHeader(name) {
  const lower = String(name || "").trim().toLowerCase();
  return (
    lower === "authorization" ||
    lower === "proxy-authorization" ||
    lower.includes("csrf") ||
    lower.includes("xsrf") ||
    lower.includes("token") ||
    lower.includes("api-key") ||
    lower.includes("apikey")
  );
}

function persistentReplayHeaders(headers = {}) {
  return Object.fromEntries(
    Object.entries(headers || {}).filter(([name]) => !isTransientReplayHeader(name))
  );
}

function templateOrigin(rawUrl) {
  try {
    return new URL(rawUrl).origin;
  } catch {
    return "";
  }
}

function bodyContainsQuoteId(body) {
  return (
    typeof body === "string" &&
    /quote[_-]?id|quoteid|quote_uuid|quoteuuid/i.test(body)
  );
}

function captureLiveReplayHeaders(request = {}) {
  const url = String(request.url || "");
  if (!url || !matchesDomainFilter(url)) {
    return;
  }
  const origin = templateOrigin(url);
  if (!origin) {
    return;
  }
  const replayHeaders = sanitizeReplayHeaders(request.headers || {});
  const transient = Object.fromEntries(
    Object.entries(replayHeaders).filter(([name]) => isTransientReplayHeader(name))
  );
  if (!Object.keys(transient).length) {
    return;
  }
  const next = {
    ...(state.liveReplayHeaders[origin] || {}),
    ...transient
  };
  if (JSON.stringify(next) === JSON.stringify(state.liveReplayHeaders[origin] || {})) {
    return;
  }
  state.liveReplayHeaders[origin] = next;
  scheduleTemplateCachePersist();
}

function cloneTemplate(template, { persistent = false } = {}) {
  if (!template || typeof template !== "object") {
    return null;
  }
  const sanitizedHeaders = sanitizeReplayHeaders(template.headers || {});
  return {
    ...template,
    requiresTransientAuth: typeof template.requiresTransientAuth === "boolean"
      ? template.requiresTransientAuth
      : (
        persistent ||
        Object.keys(sanitizedHeaders).some((name) => isTransientReplayHeader(name))
      ),
    headers: persistent
      ? persistentReplayHeaders(sanitizedHeaders)
      : sanitizedHeaders
  };
}

function sanitizeSessionReplayHeaders(raw) {
  if (!raw || typeof raw !== "object") {
    return {};
  }
  const clean = {};
  for (const [origin, headers] of Object.entries(raw)) {
    if (origin !== VAR_API_ORIGIN || !headers || typeof headers !== "object") {
      continue;
    }
    const transient = Object.fromEntries(
      Object.entries(sanitizeReplayHeaders(headers))
        .filter(([name]) => isTransientReplayHeader(name))
    );
    if (Object.keys(transient).length) {
      clean[origin] = transient;
    }
  }
  return clean;
}

function validCachedTemplate(template, kind) {
  if (!template || typeof template !== "object") {
    return false;
  }
  if (!/^[A-Z0-9_-]{1,24}$/.test(String(template.market || ""))) {
    return false;
  }
  if (!isWriteMethod(template.method) || typeof template.body !== "string" || template.body.length > 200000) {
    return false;
  }
  let parsedUrl;
  try {
    parsedUrl = new URL(template.url);
  } catch {
    return false;
  }
  if (parsedUrl.origin !== VAR_API_ORIGIN) {
    return false;
  }
  const path = parsedUrl.pathname;
  if (kind === "order") {
    return (
      ORDER_PATH_ALLOWLIST.has(path) &&
      ["open", "close"].includes(template.phase) &&
      ["BUY", "SELL"].includes(template.side) &&
      bodyContainsQuoteId(template.body)
    );
  }
  return path === "/api/quotes/indicative";
}

function sanitizeCachedTemplateMap(raw, kind, { persistent = false } = {}) {
  const clean = {};
  if (!raw || typeof raw !== "object") {
    return clean;
  }
  for (const [key, template] of Object.entries(raw)) {
    if (validCachedTemplate(template, kind)) {
      clean[key] = cloneTemplate(template, { persistent });
    }
  }
  return clean;
}

function cachePayload({ persistent = false } = {}) {
  const mapTemplates = (templates) => Object.fromEntries(
    Object.entries(templates)
      .filter(([key]) => !key.endsWith(":pending"))
      .map(([key, template]) => [key, cloneTemplate(template, { persistent })])
  );
  return {
    version: TEMPLATE_CACHE_VERSION,
    savedAt: nowIso(),
    orderFetchTemplates: mapTemplates(state.orderFetchTemplates),
    quoteFetchTemplates: mapTemplates(state.quoteFetchTemplates),
    liveReplayHeaders: persistent ? {} : { ...state.liveReplayHeaders }
  };
}

function validTemplateCache(cache) {
  return cache && cache.version === TEMPLATE_CACHE_VERSION;
}

function restoreTemplateCache(localCache, sessionCache) {
  const local = validTemplateCache(localCache) ? localCache : {};
  const session = validTemplateCache(sessionCache) ? sessionCache : {};
  const localOrders = sanitizeCachedTemplateMap(
    local.orderFetchTemplates,
    "order",
    { persistent: true }
  );
  const localQuotes = sanitizeCachedTemplateMap(
    local.quoteFetchTemplates,
    "quote",
    { persistent: true }
  );
  const sessionOrders = sanitizeCachedTemplateMap(session.orderFetchTemplates, "order");
  const sessionQuotes = sanitizeCachedTemplateMap(session.quoteFetchTemplates, "quote");

  state.orderFetchTemplates = { ...localOrders, ...sessionOrders };
  state.quoteFetchTemplates = { ...localQuotes, ...sessionQuotes };
  const sessionReplayHeaders = sanitizeSessionReplayHeaders(session.liveReplayHeaders);
  // Never restore credentials from persistent storage. Older extension builds
  // wrote live headers there, so ignoring this field also performs migration.
  state.liveReplayHeaders = { ...sessionReplayHeaders };
  const restored = Object.keys(state.orderFetchTemplates).length + Object.keys(state.quoteFetchTemplates).length;
  if (restored > 0) {
    state.templateCacheSource = Object.keys(sessionOrders).length || Object.keys(sessionQuotes).length
      ? `浏览器会话（${restored}）`
      : `本地结构模板（${restored}，等待页面授权）`;
    state.lastFetchTemplateStatus = "已恢复持久化模板";
    state.lastQuoteTemplateStatus = "已恢复持久化模板";
  } else {
    state.templateCacheSource = "无缓存";
  }
}

function persistTemplateCache() {
  const localPayload = cachePayload({ persistent: true });
  const sessionPayload = cachePayload();
  templatePersistPromise = templatePersistPromise
    .catch(() => undefined)
    .then(async () => {
      await chrome.storage.local.set({ [TEMPLATE_LOCAL_STORAGE_KEY]: localPayload });
      try {
        await chrome.storage.session.set({ [TEMPLATE_SESSION_STORAGE_KEY]: sessionPayload });
      } catch {
        // Structural templates remain usable, but live authorization must be
        // captured again from the page in the new browser session.
      }
      state.templateCacheSource = "已保存";
    })
    .catch((error) => {
      state.lastError = `模板保存失败：${error.message}`;
      notifyStatus();
    });
  return templatePersistPromise;
}

function scheduleTemplateCachePersist() {
  if (templatePersistTimer) {
    return;
  }
  templatePersistTimer = setTimeout(() => {
    templatePersistTimer = null;
    persistTemplateCache();
  }, 250);
}

function clearTransientReplayState() {
  state.liveReplayHeaders = {};
  for (const templates of [state.orderFetchTemplates, state.quoteFetchTemplates]) {
    for (const template of Object.values(templates)) {
      template.headers = persistentReplayHeaders(template.headers || {});
    }
  }
  persistTemplateCache();
}

function sanitizeConfig(incoming = {}) {
  return {
    configVersion: DEFAULT_CONFIG.configVersion,
    autoStart: incoming.autoStart === true,
    wsEndpoint: asLocalWebSocketEndpoint(
      incoming.wsEndpoint,
      DEFAULT_CONFIG.wsEndpoint,
      "8766"
    ),
    restEndpoint: asLocalWebSocketEndpoint(
      incoming.restEndpoint,
      DEFAULT_CONFIG.restEndpoint,
      "8767"
    ),
    commandEndpoint: asLocalWebSocketEndpoint(
      incoming.commandEndpoint,
      DEFAULT_CONFIG.commandEndpoint,
      "8768"
    ),
    domainFilter: asStringOrDefault(incoming.domainFilter, DEFAULT_CONFIG.domainFilter),
    restAllowlist: sanitizeRestAllowlist(incoming.restAllowlist),
    wsAllowlist: sanitizeAllowlist(incoming.wsAllowlist, DEFAULT_CONFIG.wsAllowlist)
  };
}

function asLocalWebSocketEndpoint(value, fallback, expectedPort) {
  const candidate = asStringOrDefault(value, fallback);
  try {
    const parsed = new URL(candidate);
    const localHost = parsed.hostname === "127.0.0.1" || parsed.hostname === "localhost";
    const cleanPath = parsed.pathname === "/" && !parsed.search && !parsed.hash;
    if (
      parsed.protocol === "ws:" &&
      localHost &&
      parsed.port === expectedPort &&
      cleanPath &&
      !parsed.username &&
      !parsed.password
    ) {
      return parsed.toString().replace(/\/$/, "");
    }
  } catch {
    // Fall through to the fixed local endpoint.
  }
  return fallback;
}

function asStringOrDefault(value, fallback) {
  if (typeof value !== "string") {
    return fallback;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : fallback;
}

function nowIso() {
  return new Date().toISOString();
}

function sanitizeAllowlist(value, fallback) {
  if (!Array.isArray(value)) {
    return [...fallback];
  }
  const cleaned = value
    .filter((item) => typeof item === "string")
    .map((item) => item.trim())
    .filter((item) => item.length > 0);
  if (!cleaned.length) {
    return [...fallback];
  }
  return cleaned;
}

function sanitizeRestAllowlist(value) {
  const cleaned = sanitizeAllowlist(value, DEFAULT_CONFIG.restAllowlist);
  const strict = cleaned.filter((item) => item === DEFAULT_CONFIG.restAllowlist[0]);
  if (!strict.length) {
    return [...DEFAULT_CONFIG.restAllowlist];
  }
  return strict;
}

function matchesDomainFilter(url) {
  const filter = state.config.domainFilter.trim().toLowerCase();
  if (!filter) {
    return true;
  }
  return (url || "").toLowerCase().includes(filter);
}

function normalizeUrlParts(rawUrl) {
  try {
    const parsed = new URL(rawUrl);
    return {
      originPath: `${parsed.origin}${parsed.pathname}`,
      full: parsed.toString()
    };
  } catch {
    return {
      originPath: rawUrl,
      full: rawUrl
    };
  }
}

function getMatchedRestPattern(url) {
  const patterns = state.config.restAllowlist || [];
  return getMatchedPattern(url, patterns);
}

function getMatchedWsPattern(url) {
  const patterns = state.config.wsAllowlist || [];
  return getMatchedPattern(url, patterns);
}

function getMatchedPattern(url, patterns) {
  if (!patterns.length) {
    return null;
  }

  const target = normalizeUrlParts(url);
  for (const pattern of patterns) {
    const normalizedPattern = normalizeUrlParts(pattern);
    if (target.originPath === normalizedPattern.originPath || target.full.startsWith(pattern)) {
      return pattern;
    }
  }
  return null;
}

async function debuggerAttach(tabId) {
  await new Promise((resolve, reject) => {
    chrome.debugger.attach({ tabId }, DEBUGGER_VERSION, () => {
      const err = chrome.runtime.lastError;
      if (err) {
        reject(new Error(err.message));
        return;
      }
      resolve();
    });
  });
}

async function debuggerDetach(tabId) {
  await new Promise((resolve, reject) => {
    chrome.debugger.detach({ tabId }, () => {
      const err = chrome.runtime.lastError;
      if (err) {
        reject(new Error(err.message));
        return;
      }
      resolve();
    });
  });
}

async function sendDebuggerCommand(tabId, method, params = {}) {
  return new Promise((resolve, reject) => {
    chrome.debugger.sendCommand({ tabId }, method, params, (result) => {
      const err = chrome.runtime.lastError;
      if (err) {
        reject(new Error(err.message));
        return;
      }
      resolve(result || {});
    });
  });
}

async function executePlaceOrderCommand(payload, transportName) {
  const side = String(payload.side || "").toUpperCase();
  const amount = String(payload.amount || "").trim();
  const baseQty = String(payload.baseQty || "").trim();
  state.lastCommandStatus = `${transportName}收到下单：${side || "-"} ${amount || "-"}${baseQty ? ` qty=${baseQty}` : ""}`;
  notifyStatus();
  try {
    return await placeVariationalOrder(payload);
  } catch (error) {
    const message = error?.message || String(error);
    state.lastCommandStatus = `${transportName}下单异常：${message}`;
    notifyStatus();
    return { ok: false, error: message };
  }
}

function isWriteMethod(method) {
  return ["POST", "PUT", "PATCH", "DELETE"].includes(String(method || "").toUpperCase());
}

function safeUrlPath(rawUrl) {
  try {
    const parsed = new URL(rawUrl);
    return `${parsed.pathname}${parsed.search ? "?" : ""}`;
  } catch {
    return String(rawUrl || "").slice(0, 120);
  }
}

function marketFromPageUrl(rawUrl) {
  try {
    const match = new URL(rawUrl).pathname.match(/\/perpetual\/([^/?#]+)/i);
    return match ? decodeURIComponent(match[1]).toUpperCase() : null;
  } catch {
    return null;
  }
}

async function getAttachedPageMarket() {
  if (state.attachedTabId == null) {
    return null;
  }
  try {
    const tab = await chrome.tabs.get(state.attachedTabId);
    return marketFromPageUrl(tab.url || "");
  } catch {
    return null;
  }
}

function sanitizeReplayHeaders(headers = {}) {
  const blocked = new Set([
    "accept-encoding",
    "connection",
    "content-length",
    "cookie",
    "host",
    "origin",
    "referer",
    "user-agent"
  ]);
  const replayHeaders = {};
  for (const [name, value] of Object.entries(headers || {})) {
    const key = String(name || "").trim();
    const lower = key.toLowerCase();
    if (!key || blocked.has(lower) || lower.startsWith("sec-") || lower.startsWith(":")) {
      continue;
    }
    if (
      lower === "accept" ||
      lower === "authorization" ||
      lower === "content-type" ||
      lower.startsWith("x-")
    ) {
      replayHeaders[key] = Array.isArray(value) ? value.join(", ") : String(value ?? "");
    }
  }
  return replayHeaders;
}

async function getRequestPostData(requestId, request) {
  if (typeof request?.postData === "string") {
    return request.postData;
  }
  if (!request?.hasPostData || !requestId || state.attachedTabId == null) {
    return "";
  }
  try {
    const result = await sendDebuggerCommand(state.attachedTabId, "Network.getRequestPostData", { requestId });
    return typeof result.postData === "string" ? result.postData : "";
  } catch {
    return "";
  }
}

function tryParseJson(text) {
  if (typeof text !== "string" || !text.trim()) {
    return null;
  }
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
}

function tryParseForm(text) {
  if (typeof text !== "string" || !text.includes("=")) {
    return null;
  }
  try {
    const params = new URLSearchParams(text);
    return Array.from(params.entries());
  } catch {
    return null;
  }
}

function walkJson(value, visitor) {
  const stack = [{ value, path: [] }];
  let steps = 0;
  while (stack.length && steps < 1000) {
    steps += 1;
    const item = stack.pop();
    const current = item.value;
    if (Array.isArray(current)) {
      current.forEach((child, index) => stack.push({ value: child, path: item.path.concat(String(index)) }));
      continue;
    }
    if (!current || typeof current !== "object") {
      continue;
    }
    for (const [key, child] of Object.entries(current)) {
      if (visitor(key, child, item.path.concat(key)) === false) {
        return;
      }
      if (child && typeof child === "object") {
        stack.push({ value: child, path: item.path.concat(key) });
      }
    }
  }
}

function normalizeKey(key) {
  return String(key || "").replace(/[-_\s]/g, "").toLowerCase();
}

function sideFromToken(value) {
  const token = String(value ?? "").trim().toLowerCase();
  if (["buy", "bid", "long", "b", "1"].includes(token)) {
    return "BUY";
  }
  if (["sell", "ask", "short", "s", "-1"].includes(token)) {
    return "SELL";
  }
  return null;
}

function inferOrderSide(url, body) {
  const parsed = tryParseJson(body);
  if (parsed !== null) {
    let found = null;
    walkJson(parsed, (key, value) => {
      const normalized = normalizeKey(key);
      if (["side", "orderside", "direction", "action", "positionside"].includes(normalized)) {
        found = sideFromToken(value);
      }
      if (["isbuy", "buy"].includes(normalized) && typeof value === "boolean") {
        found = value ? "BUY" : "SELL";
      }
      return found ? false : undefined;
    });
    if (found) {
      return found;
    }
  }

  const form = tryParseForm(body);
  if (form) {
    for (const [key, value] of form) {
      const normalized = normalizeKey(key);
      if (["side", "orderside", "direction", "action", "positionside"].includes(normalized)) {
        const side = sideFromToken(value);
        if (side) {
          return side;
        }
      }
    }
  }

  const text = `${url}\n${body}`;
  const patterns = [
    /"(?:side|order[_-]?side|direction|action|position[_-]?side)"\s*:\s*"(buy|sell|bid|ask|long|short)"/i,
    /(?:side|order[_-]?side|direction|action|position[_-]?side)=(buy|sell|bid|ask|long|short)/i,
    /\/(buy|sell|long|short)(?:\/|$|\?)/i
  ];
  for (const pattern of patterns) {
    const match = text.match(pattern);
    const side = match ? sideFromToken(match[1]) : null;
    if (side) {
      return side;
    }
  }
  return null;
}

function inferOrderPhase(url, body) {
  const parsed = tryParseJson(body);
  if (parsed !== null) {
    let phase = null;
    walkJson(parsed, (key, value) => {
      const normalized = normalizeKey(key);
      if (["reduceonly", "isreduceonly", "closeposition", "closeonly"].includes(normalized) && value === true) {
        phase = "close";
      }
      if (["action", "intent", "ordertype", "type"].includes(normalized)) {
        const token = String(value ?? "").toLowerCase();
        if (token.includes("close") || token.includes("reduce")) {
          phase = "close";
        }
      }
      return phase ? false : undefined;
    });
    if (phase) {
      return phase;
    }
  }

  const text = `${url}\n${body}`.toLowerCase();
  if (
    /reduce[_-]?only["']?\s*[:=]\s*true/.test(text) ||
    /close[_-]?position["']?\s*[:=]\s*true/.test(text) ||
    text.includes("/close") ||
    text.includes("close_order") ||
    text.includes("decrease")
  ) {
    return "close";
  }
  return "open";
}

function looksLikeOrderRequest(url, method, body) {
  if (!isWriteMethod(method)) {
    return false;
  }
  const lower = `${url}\n${body}`.toLowerCase();
  if (lower.includes("/api/quotes/indicative")) {
    return false;
  }
  const orderWords = [
    "order",
    "trade",
    "position",
    "execute",
    "submit",
    "perpetual",
    "reduce",
    "close"
  ];
  const hasOrderWord = orderWords.some((word) => lower.includes(word));
  const hasSide = inferOrderSide(url, body) !== null;
  const hasSizeHint = /amount|notional|quantity|qty|size|margin|collateral|instrument|symbol|market/.test(lower);
  return hasOrderWord || (hasSide && hasSizeHint);
}

function orderTemplateKey(market, phase, side) {
  return `${market}:order:${phase}:${side}`;
}

function quoteTemplateKey(market, phase, side = "neutral") {
  return `${market}:quote:${phase}:${side || "neutral"}`;
}

function pendingQuoteTemplateKey(market) {
  return `${market}:quote:pending`;
}

function oppositeSide(side) {
  return side === "BUY" ? "SELL" : "BUY";
}

function inspectPatchableBodySide(body) {
  const sides = new Set();
  let fields = 0;
  const collect = (key, value) => {
    const normalized = normalizeKey(key);
    if (["side", "orderside", "direction", "action", "positionside"].includes(normalized)) {
      const side = sideFromToken(value);
      if (side) {
        sides.add(side);
        fields += 1;
      }
    }
    if (["isbuy", "buy"].includes(normalized) && typeof value === "boolean") {
      sides.add(value ? "BUY" : "SELL");
      fields += 1;
    }
  };

  const parsed = tryParseJson(body);
  if (parsed !== null) {
    walkJson(parsed, collect);
  } else {
    const form = tryParseForm(body);
    if (form) {
      for (const [key, value] of form) {
        const normalized = normalizeKey(key);
        if (["isbuy", "buy"].includes(normalized) && ["true", "false"].includes(String(value).toLowerCase())) {
          sides.add(String(value).toLowerCase() === "true" ? "BUY" : "SELL");
          fields += 1;
        } else {
          collect(key, value);
        }
      }
    }
  }

  return {
    patchable: fields > 0 && sides.size === 1,
    side: sides.size === 1 ? Array.from(sides)[0] : null,
    fields
  };
}

function canDeriveTemplateSide(template, targetSide) {
  if (!template || !["BUY", "SELL"].includes(targetSide) || template.side === targetSide) {
    return false;
  }
  const info = inspectPatchableBodySide(template.body);
  return info.patchable && info.side === template.side;
}

function prepareTemplateForReplay(template, targetSide, { derived = false, neutral = false } = {}) {
  if (!template) {
    return null;
  }
  const origin = templateOrigin(template.url);
  return {
    ...cloneTemplate(template),
    headers: {
      ...(template.headers || {}),
      ...(state.liveReplayHeaders[origin] || {})
    },
    sourceSide: template.side || null,
    targetSide,
    requiresSidePatch: Boolean(derived),
    sideNeutral: Boolean(neutral || template.sideNeutral)
  };
}

function replayAuthenticationReady(template) {
  if (!template) {
    return false;
  }
  if (!template.requiresTransientAuth) {
    return true;
  }
  return Object.keys(template.headers || {}).some((name) => isTransientReplayHeader(name));
}

function associateRecentQuoteTemplate(market, phase, side) {
  const pendingKey = pendingQuoteTemplateKey(market);
  const pending = state.quoteFetchTemplates[pendingKey];
  if (!pending) {
    return;
  }
  const ageMs = Date.now() - Number(pending.capturedAtMs || 0);
  if (!Number.isFinite(ageMs) || ageMs < 0 || ageMs > QUOTE_ORDER_ASSOCIATION_MS) {
    delete state.quoteFetchTemplates[pendingKey];
    return;
  }

  if (pending.side && pending.side !== side) {
    delete state.quoteFetchTemplates[pendingKey];
    return;
  }

  const neutral = !pending.side;
  const associated = {
    ...pending,
    key: neutral ? `${phase}:neutral` : `${phase}:${side}`,
    phase,
    correlatedSide: side,
    sideNeutral: neutral
  };
  state.quoteFetchTemplates[quoteTemplateKey(market, phase, neutral ? "neutral" : side)] = associated;
  delete state.quoteFetchTemplates[pendingKey];
}

async function captureOrderFetchTemplate(params) {
  const request = params?.request || {};
  const url = String(request.url || "");
  const method = String(request.method || "GET").toUpperCase();
  if (!url || !matchesDomainFilter(url) || !isWriteMethod(method)) {
    return;
  }
  if (getMatchedRestPattern(url)) {
    return;
  }
  let requestPath;
  try {
    const parsedUrl = new URL(url);
    if (parsedUrl.origin !== VAR_API_ORIGIN) {
      return;
    }
    requestPath = parsedUrl.pathname;
  } catch {
    return;
  }
  if (!ORDER_PATH_ALLOWLIST.has(requestPath)) {
    return;
  }

  const body = await getRequestPostData(params.requestId, request);
  if (!looksLikeOrderRequest(url, method, body)) {
    return;
  }
  if (!bodyContainsQuoteId(body)) {
    state.lastFetchTemplateStatus = `忽略未绑定 quote_id 的下单请求 ${requestPath}`;
    notifyStatus();
    return;
  }

  const side = inferOrderSide(url, body);
  if (!side) {
    return;
  }

  const phase = inferOrderPhase(url, body);
  const market = await getAttachedPageMarket();
  if (!market) {
    return;
  }
  const key = `${phase}:${side}`;
  const headers = sanitizeReplayHeaders(request.headers || {});
  const template = {
    key,
    phase,
    side,
    market,
    url,
    urlPath: safeUrlPath(url),
    method,
    headers,
    requiresTransientAuth: Object.keys(headers).some((name) => isTransientReplayHeader(name)),
    body,
    capturedAt: nowIso(),
    capturedAtMs: Date.now(),
    bodyBytes: body.length
  };
  state.attachedMarket = market;
  state.orderFetchTemplates[orderTemplateKey(market, phase, side)] = template;
  associateRecentQuoteTemplate(market, phase, side);
  state.lastFetchTemplateStatus = `已捕获 ${key} ${method} ${template.urlPath}`;
  persistTemplateCache();
  notifyStatus();
}

async function captureQuoteFetchTemplate(params) {
  const request = params?.request || {};
  const url = String(request.url || "");
  const method = String(request.method || "GET").toUpperCase();
  if (!url || !matchesDomainFilter(url) || !isWriteMethod(method)) {
    return;
  }
  if (!getMatchedRestPattern(url)) {
    return;
  }

  const body = await getRequestPostData(params.requestId, request);
  const side = inferOrderSide(url, body);
  const phase = inferOrderPhase(url, body);
  const market = await getAttachedPageMarket();
  if (!market) {
    return;
  }
  const headers = sanitizeReplayHeaders(request.headers || {});
  const template = {
    key: side ? `${phase}:${side}` : "latest",
    phase,
    side,
    market,
    url,
    urlPath: safeUrlPath(url),
    method,
    headers,
    requiresTransientAuth: Object.keys(headers).some((name) => isTransientReplayHeader(name)),
    body,
    capturedAt: nowIso(),
    capturedAtMs: Date.now(),
    bodyBytes: body.length
  };

  state.attachedMarket = market;
  state.quoteFetchTemplates[pendingQuoteTemplateKey(market)] = template;
  state.lastQuoteTemplateStatus = side
    ? `已捕获 ${side} 报价，等待关联开仓/平仓`
    : `已捕获通用报价，等待关联开仓/平仓`;
  notifyStatus();
}

function getOrderFetchTemplate(phase, side, market) {
  const normalizedPhase = ["open", "close"].includes(phase) ? phase : "open";
  const exact = state.orderFetchTemplates[orderTemplateKey(market, normalizedPhase, side)];
  if (exact) {
    return prepareTemplateForReplay(exact, side);
  }
  const opposite = state.orderFetchTemplates[
    orderTemplateKey(market, normalizedPhase, oppositeSide(side))
  ];
  if (canDeriveTemplateSide(opposite, side)) {
    return prepareTemplateForReplay(opposite, side, { derived: true });
  }
  return null;
}

function getQuoteFetchTemplate(phase, side, market) {
  const normalizedPhase = ["open", "close"].includes(phase) ? phase : "open";
  const exact = state.quoteFetchTemplates[quoteTemplateKey(market, normalizedPhase, side)];
  if (exact) {
    return prepareTemplateForReplay(exact, side);
  }
  const neutral = state.quoteFetchTemplates[quoteTemplateKey(market, normalizedPhase, "neutral")];
  if (neutral?.sideNeutral) {
    return prepareTemplateForReplay(neutral, side, { neutral: true });
  }
  const opposite = state.quoteFetchTemplates[
    quoteTemplateKey(market, normalizedPhase, oppositeSide(side))
  ];
  if (canDeriveTemplateSide(opposite, side)) {
    return prepareTemplateForReplay(opposite, side, { derived: true });
  }
  return null;
}

function summarizeFetchTemplates() {
  const summary = {};
  for (const [key, template] of Object.entries(state.orderFetchTemplates)) {
    summary[key] = {
      method: template.method,
      path: template.urlPath,
      capturedAt: template.capturedAt,
      bodyBytes: template.bodyBytes,
      market: template.market
    };
  }
  return summary;
}

function summarizeQuoteTemplates() {
  const summary = {};
  for (const [key, template] of Object.entries(state.quoteFetchTemplates)) {
    if (key.endsWith(":pending")) {
      continue;
    }
    summary[key] = {
      method: template.method,
      path: template.urlPath,
      capturedAt: template.capturedAt,
      bodyBytes: template.bodyBytes,
      market: template.market
    };
  }
  return summary;
}

function templateReadiness(market) {
  if (!market) {
    return { market: null, actions: {} };
  }
  const actions = {};
  for (const [phase, side] of [
    ["open", "BUY"],
    ["close", "SELL"],
    ["open", "SELL"],
    ["close", "BUY"]
  ]) {
    const key = `${phase}:${side}`;
    const order = getOrderFetchTemplate(phase, side, market);
    const quote = getQuoteFetchTemplate(phase, side, market);
    const orderAuthReady = replayAuthenticationReady(order);
    const quoteAuthReady = replayAuthenticationReady(quote);
    actions[key] = {
      ready: Boolean(order && quote && orderAuthReady && quoteAuthReady),
      order: !order ? "缺失" : (order.requiresSidePatch ? "派生" : "已捕获"),
      quote: !quote ? "缺失" : (quote.sideNeutral ? "通用" : (quote.requiresSidePatch ? "派生" : "已捕获")),
      authentication: orderAuthReady && quoteAuthReady ? "实时" : "等待页面"
    };
  }
  return { market, actions };
}

function resetFetchTemplates() {
  state.orderFetchTemplates = {};
  state.quoteFetchTemplates = {};
  state.templateCacheSource = "已清除";
  state.lastFetchTemplateStatus = "未捕获";
  state.lastQuoteTemplateStatus = "未捕获";
}

async function clearFetchTemplateCache() {
  resetFetchTemplates();
  await chrome.storage.local.remove(TEMPLATE_LOCAL_STORAGE_KEY);
  try {
    await chrome.storage.session.remove(TEMPLATE_SESSION_STORAGE_KEY);
  } catch {
    // Ignore when storage.session is unavailable.
  }
  notifyStatus();
}


async function pageFetchOrder(params) {
  const traceId = String(params.traceId || "").trim() || null;
  const side = String(params.side || "").toUpperCase();
  const phase = String(params.phase || "open").toLowerCase();
  const stage = String(params.stage || "").trim().toLowerCase();
  const amount = String(params.amount || "").trim();
  const baseQty = String(params.baseQty || params.closeQty || "").trim();
  const template = params.template || {};
  const quoteTemplate = params.quoteTemplate || null;
  const firmQuote = params.firmQuote && typeof params.firmQuote === "object" ? params.firmQuote : {};
  const guard = params.guard && typeof params.guard === "object" ? params.guard : {};
  const fetchTimeoutMs = Math.max(1000, Number(params.timeoutMs || 5000) - 250);
  const fetchSignal = typeof AbortSignal?.timeout === "function"
    ? AbortSignal.timeout(fetchTimeoutMs)
    : undefined;
  const startedAt = performance.now();
  const hasPositiveNumber = (value) => Number.isFinite(Number(value)) && Number(value) > 0;
  if (!["quote", "commit"].includes(stage)) {
    return { ok: false, error: "Var fetch 阶段无效；只允许 quote 或 commit" };
  }
  const safeBaseQtyKeys = new Set([
    "assetquantity",
    "assetqty",
    "basequantity",
    "baseqty",
    "basesize",
    "positionquantity",
    "positionqty",
    "qty",
    "quantity",
    "size"
  ]);
  const normalizeBodyKey = (key) => String(key || "").replace(/[-_\s]/g, "").toLowerCase();
  const pathLooksLegRatio = (path) => {
    const normalizedPath = path.map(normalizeBodyKey);
    return (
      phase === "close" &&
      normalizedPath.includes("structure") &&
      normalizedPath.includes("legs") &&
      normalizedPath[normalizedPath.length - 1] === "ratio"
    );
  };
  const isQuoteIdKey = (key) => {
    const normalized = normalizeBodyKey(key);
    return normalized === "quoteid" || normalized === "quoteuuid" || (
      normalized.includes("quote") && normalized.includes("id")
    );
  };
  const bodyLooksQuoteBound = (bodyText) => {
    if (typeof bodyText !== "string" || !bodyText) {
      return false;
    }
    const lower = bodyText.toLowerCase();
    return /quote[_-]?id|quoteid|quote_uuid|quoteuuid/.test(lower);
  };
  const targetSideToken = (oldValue) => {
    const oldText = String(oldValue ?? "").trim();
    const lower = oldText.toLowerCase();
    let next = side;
    if (["1", "-1"].includes(lower)) {
      next = side === "BUY" ? "1" : "-1";
    } else if (["b", "s"].includes(lower)) {
      next = side === "BUY" ? "b" : "s";
      if (oldText === oldText.toUpperCase()) {
        next = next.toUpperCase();
      }
    } else if (["bid", "ask"].includes(lower)) {
      next = side === "BUY" ? "bid" : "ask";
    } else if (["long", "short"].includes(lower)) {
      next = side === "BUY" ? "long" : "short";
    } else if (lower === oldText) {
      next = side.toLowerCase();
    }
    return next;
  };
  const directionalTokenSide = (value) => {
    const token = String(value ?? "").trim().toLowerCase();
    if (["buy", "bid", "long", "b", "1"].includes(token)) {
      return "BUY";
    }
    if (["sell", "ask", "short", "s", "-1"].includes(token)) {
      return "SELL";
    }
    return null;
  };
  const preserveValueType = (oldValue, textValue) => {
    if (typeof oldValue === "number") {
      const numeric = Number(textValue);
      return Number.isFinite(numeric) ? numeric : oldValue;
    }
    return textValue;
  };
  const walk = (value, visitor) => {
    const stack = [{ value, path: [] }];
    let steps = 0;
    while (stack.length && steps < 1000) {
      steps += 1;
      const item = stack.pop();
      const current = item.value;
      if (!current || typeof current !== "object") {
        continue;
      }
      for (const [key, child] of Object.entries(current)) {
        visitor(current, key, child, item.path.concat(key));
        if (child && typeof child === "object") {
          stack.push({ value: child, path: item.path.concat(key) });
        }
      }
    }
  };
  const safeOpenNotionalKeys = new Set([
    "amount",
    "amountusd",
    "collateral",
    "collateralusd",
    "margin",
    "marginusd",
    "notional",
    "notionalusd",
    "quoteamount",
    "usdamount"
  ]);
  const extractCapturedOpenNotional = (bodyText) => {
    if (phase !== "open" || typeof bodyText !== "string" || !bodyText) {
      return null;
    }
    const candidates = [];
    const collect = (key, value) => {
      if (safeOpenNotionalKeys.has(normalizeBodyKey(key)) && hasPositiveNumber(value)) {
        candidates.push(Number(value));
      }
    };
    try {
      const parsed = JSON.parse(bodyText);
      walk(parsed, (_parent, key, value) => collect(key, value));
    } catch {
      try {
        const form = new URLSearchParams(bodyText);
        for (const [key, value] of form.entries()) {
          collect(key, value);
        }
      } catch {
        return null;
      }
    }
    const distinct = Array.from(new Set(candidates.map((value) => String(value))));
    return distinct.length === 1 ? Number(distinct[0]) : null;
  };
  const adaptJsonBody = (bodyText, options = {}) => {
    try {
      const parsed = JSON.parse(bodyText);
      let sidePatched = false;
      let baseQtyPatched = false;
      let baseQtyFieldCount = 0;
      let baseQtyFieldsVerified = true;
      let legRatioPatched = false;
      let quotePatched = false;
      walk(parsed, (parent, key, value, path) => {
        const normalized = normalizeBodyKey(key);
        if (
          ["side", "orderside", "direction", "action", "positionside"].includes(normalized) &&
          (typeof value === "string" || typeof value === "number") &&
          directionalTokenSide(value)
        ) {
          const nextToken = targetSideToken(value);
          parent[key] = typeof value === "number" ? Number(nextToken) : nextToken;
          sidePatched = sidePatched || parent[key] !== value;
          return;
        }
        if (["isbuy", "buy"].includes(normalized) && typeof value === "boolean") {
          const nextValue = side === "BUY";
          if (nextValue !== value) {
            parent[key] = nextValue;
            sidePatched = true;
          }
          return;
        }
        if (options.quoteId && isQuoteIdKey(key)) {
          const nextValue = preserveValueType(value, options.quoteId);
          if (nextValue !== value) {
            parent[key] = nextValue;
            quotePatched = true;
          }
          return;
        }
        if (phase === "close" && hasPositiveNumber(options.baseQty) && safeBaseQtyKeys.has(normalized)) {
          baseQtyFieldCount += 1;
          const nextValue = preserveValueType(value, options.baseQty);
          if (nextValue !== value) {
            parent[key] = nextValue;
            baseQtyPatched = true;
          }
          if (!hasPositiveNumber(parent[key]) || Number(parent[key]) !== Number(options.baseQty)) {
            baseQtyFieldsVerified = false;
          }
          return;
        }
        if (phase === "close" && hasPositiveNumber(options.baseQty) && pathLooksLegRatio(path)) {
          const nextValue = preserveValueType(value, "1");
          if (nextValue !== value) {
            parent[key] = nextValue;
            legRatioPatched = true;
          }
        }
      });
      return {
        body: JSON.stringify(parsed),
        format: "json",
        sidePatched,
        baseQtyPatched,
        baseQtyBound: baseQtyFieldCount > 0 && baseQtyFieldsVerified,
        legRatioPatched,
        quotePatched
      };
    } catch {
      return null;
    }
  };
  const adaptFormBody = (bodyText, options = {}) => {
    try {
      if (!bodyText.includes("=")) {
        return null;
      }
      const form = new URLSearchParams(bodyText);
      let sidePatched = false;
      let baseQtyPatched = false;
      let baseQtyFieldCount = 0;
      let baseQtyFieldsVerified = true;
      let legRatioPatched = false;
      let quotePatched = false;
      for (const [key, value] of Array.from(form.entries())) {
        const normalized = normalizeBodyKey(key);
        if (
          ["side", "orderside", "direction", "action", "positionside"].includes(normalized) &&
          directionalTokenSide(value)
        ) {
          const nextValue = targetSideToken(value);
          if (nextValue !== value) {
            form.set(key, nextValue);
            sidePatched = true;
          }
        }
        if (["isbuy", "buy"].includes(normalized) && ["true", "false"].includes(String(value).toLowerCase())) {
          const nextValue = side === "BUY" ? "true" : "false";
          if (nextValue !== value) {
            form.set(key, nextValue);
            sidePatched = true;
          }
        }
        if (options.quoteId && isQuoteIdKey(key)) {
          form.set(key, options.quoteId);
          quotePatched = true;
        }
        if (phase === "close" && hasPositiveNumber(options.baseQty) && safeBaseQtyKeys.has(normalized)) {
          baseQtyFieldCount += 1;
          form.set(key, options.baseQty);
          baseQtyPatched = baseQtyPatched || String(value) !== String(options.baseQty);
          if (!hasPositiveNumber(form.get(key)) || Number(form.get(key)) !== Number(options.baseQty)) {
            baseQtyFieldsVerified = false;
          }
        }
        if (phase === "close" && hasPositiveNumber(options.baseQty) && normalized === "ratio") {
          form.set(key, "1");
          legRatioPatched = true;
        }
      }
      return {
        body: form.toString(),
        format: "form",
        sidePatched,
        baseQtyPatched,
        baseQtyBound: baseQtyFieldCount > 0 && baseQtyFieldsVerified,
        legRatioPatched,
        quotePatched
      };
    } catch {
      return null;
    }
  };
  const adaptBody = (bodyText, options = {}) => (
    bodyText
      ? (adaptJsonBody(bodyText, options) || adaptFormBody(bodyText, options) || {
          body: bodyText,
          format: "raw",
          sidePatched: false,
          baseQtyPatched: false,
          baseQtyBound: false,
          legRatioPatched: false,
          quotePatched: false
        })
      : {
          body: "",
          format: "empty",
          sidePatched: false,
          baseQtyPatched: false,
          baseQtyBound: false,
          legRatioPatched: false,
          quotePatched: false
        }
  );
  const buildFetchInit = (fetchTemplate, adapted) => {
    const method = String(fetchTemplate.method || "POST").toUpperCase();
    const init = {
      method,
      headers: { ...(fetchTemplate.headers || {}) },
      credentials: "include",
      mode: "cors",
      cache: "no-store"
    };
    if (fetchSignal) {
      init.signal = fetchSignal;
    }
    if (!["GET", "HEAD"].includes(method) && adapted.body) {
      init.body = adapted.body;
    }
    return { method, init };
  };
  const extractQuoteId = (text) => {
    if (typeof text !== "string" || !text.trim()) {
      return null;
    }
    try {
      const parsed = JSON.parse(text);
      let found = null;
      walk(parsed, (_parent, key, value, path) => {
        if (found != null || value == null || typeof value === "object") {
          return;
        }
        const normalized = normalizeBodyKey(key);
        const pathHasQuote = path.some((part) => normalizeBodyKey(part).includes("quote"));
        if (isQuoteIdKey(key) || (normalized === "id" && pathHasQuote)) {
          found = String(value);
        }
      });
      if (found) {
        return found;
      }
    } catch {
      // Fall through to regex extraction.
    }
    const match = text.match(/["']?(?:quote_id|quoteId|quoteID|quote_uuid|quoteUuid)["']?\s*[:=]\s*["']([^"',}\s]+)["']/);
    return match ? match[1] : null;
  };
  const extractFirmPrice = (text) => {
    try {
      const parsed = JSON.parse(text);
      const targetKey = side === "BUY" ? "ask" : "bid";
      const candidates = [];
      walk(parsed, (_parent, key, value, path) => {
        if (normalizeBodyKey(key) !== targetKey || !hasPositiveNumber(value)) {
          return;
        }
        const normalizedPath = path.map(normalizeBodyKey);
        const score = normalizedPath.some((part) => part.includes("quote")) ? 2 : 1;
        candidates.push({ value: Number(value), score });
      });
      candidates.sort((a, b) => b.score - a.score);
      return candidates[0]?.value || null;
    } catch {
      return null;
    }
  };
  const extractFirmQty = (text, firmPrice, closeQuantityBound = false) => {
    const expectedCloseQty = phase === "close" && hasPositiveNumber(baseQty)
      ? Number(baseQty)
      : null;
    if (!hasPositiveNumber(firmPrice) || (phase === "close" && !expectedCloseQty)) {
      return { value: null, source: null, error: "firm quote 缺少可验证的价格或平仓数量" };
    }
    try {
      const parsed = JSON.parse(text);
      const candidates = [];
      walk(parsed, (_parent, key, value, path) => {
        const normalized = normalizeBodyKey(key);
        if (!safeBaseQtyKeys.has(normalized) || !hasPositiveNumber(value)) {
          return;
        }
        const normalizedPath = path.map(normalizeBodyKey);
        const score = normalizedPath.some((part) => part.includes("quote")) ? 2 : 1;
        candidates.push({ value: Number(value), score, path: path.join(".") || key });
      });
      candidates.sort((a, b) => b.score - a.score);
      const plausible = phase === "open"
        ? candidates[0]
        : candidates.find(
            (candidate) => Math.abs(candidate.value - expectedCloseQty) / expectedCloseQty <= 0.05
          );
      if (plausible?.value) {
        return { value: plausible.value, source: `api:${plausible.path}`, error: null };
      }
      if (phase === "close" && candidates.length === 0 && closeQuantityBound) {
        return { value: Number(baseQty), source: "bound-close-quantity", error: null };
      }
      return {
        value: null,
        source: null,
        error: candidates.length
          ? "firm quote 返回数量与实际平仓数量不匹配"
          : (
              phase === "open"
                ? "firm quote 未返回权威开仓数量"
                : "firm quote 未返回权威数量且模板未绑定平仓数量"
            )
      };
    } catch {
      return { value: null, source: null, error: "firm quote 响应无法解析权威开仓数量" };
    }
  };
  const validateFirmQuote = (firmPrice) => {
    if (guard.required !== true) {
      return { ok: true, pnl: null, lighterQuoteAgeMs: null };
    }
    const lighterBid = Number(guard.lighterBid);
    const lighterAsk = Number(guard.lighterAsk);
    const minPnl = Number(guard.minPnlUsd);
    const notional = Number(guard.notionalUsd || amount);
    const qty = Number(guard.qty || baseQty);
    const lighterQuotedAtMs = Number(guard.lighterQuotedAtMs);
    const maxAgeMs = Number(guard.maxAgeMs);
    const lighterQuoteAgeMs = Date.now() - lighterQuotedAtMs;
    if (!hasPositiveNumber(firmPrice) || !hasPositiveNumber(lighterBid) || !hasPositiveNumber(lighterAsk) || !Number.isFinite(minPnl)) {
      return { ok: false, error: "firm quote 风控数据不完整", lighterQuoteAgeMs };
    }
    if (
      !hasPositiveNumber(lighterQuotedAtMs) ||
      !hasPositiveNumber(maxAgeMs) ||
      lighterQuoteAgeMs > maxAgeMs
    ) {
      return { ok: false, error: "Lighter 报价在 firm quote 返回前已过期", lighterQuoteAgeMs };
    }
    let pnl;
    if (phase === "open") {
      if (!hasPositiveNumber(notional)) {
        return { ok: false, error: "firm quote 缺少开仓金额", lighterQuoteAgeMs };
      }
      pnl = side === "BUY"
        ? ((lighterBid - firmPrice) / firmPrice) * notional
        : ((firmPrice - lighterAsk) / firmPrice) * notional;
    } else {
      if (!hasPositiveNumber(qty)) {
        return { ok: false, error: "firm quote 缺少平仓数量", lighterQuoteAgeMs };
      }
      pnl = side === "SELL"
        ? (firmPrice - lighterAsk) * qty
        : (lighterBid - firmPrice) * qty;
    }
    return pnl >= minPnl
      ? { ok: true, pnl, lighterQuoteAgeMs }
      : { ok: false, error: `firm quote 收益 ${pnl.toFixed(4)}U 低于 ${minPnl.toFixed(4)}U`, pnl, lighterQuoteAgeMs };
  };

  if (!["BUY", "SELL"].includes(side)) {
    return { ok: false, error: "方向无效" };
  }
  if (stage === "commit" && (!template.url || !template.method)) {
    return { ok: false, error: "fetch 模板无效" };
  }
  if (stage === "quote" && (!quoteTemplate?.url || !quoteTemplate?.method)) {
    return { ok: false, error: "报价模板无效" };
  }

  const originalBody = typeof template.body === "string" ? template.body : "";
  const orderTargetNotional = extractCapturedOpenNotional(originalBody);
  const quoteTargetNotional = extractCapturedOpenNotional(
    typeof quoteTemplate?.body === "string" ? quoteTemplate.body : ""
  );
  if (
    phase === "open" &&
    hasPositiveNumber(orderTargetNotional) &&
    hasPositiveNumber(quoteTargetNotional) &&
    Number(orderTargetNotional) !== Number(quoteTargetNotional)
  ) {
    return { ok: false, error: "Var开仓报价与下单模板金额不一致" };
  }
  const targetNotionalUsd = phase === "open"
    ? (quoteTargetNotional || orderTargetNotional || null)
    : null;
  let quoteId = stage === "commit" ? String(firmQuote.quoteId || "").trim() : null;
  let quoteDetail = stage === "commit"
    ? {
        quoteId,
        firmPrice: hasPositiveNumber(firmQuote.firmPrice) ? Number(firmQuote.firmPrice) : null,
        firmQty: hasPositiveNumber(firmQuote.firmQty) ? Number(firmQuote.firmQty) : null,
        firmQtySource: typeof firmQuote.firmQtySource === "string"
          ? firmQuote.firmQtySource
          : null,
        targetNotionalUsd: hasPositiveNumber(firmQuote.targetNotionalUsd)
          ? Number(firmQuote.targetNotionalUsd)
          : targetNotionalUsd,
        guardPnl: Number.isFinite(Number(firmQuote.guardPnl)) ? Number(firmQuote.guardPnl) : null,
        guardMinPnl: Number.isFinite(Number(firmQuote.guardMinPnl)) ? Number(firmQuote.guardMinPnl) : null,
        executionReserveUsd: Number.isFinite(Number(firmQuote.executionReserveUsd))
          ? Number(firmQuote.executionReserveUsd)
          : null,
        lighterVwap: hasPositiveNumber(firmQuote.lighterVwap) ? Number(firmQuote.lighterVwap) : null,
        lighterQuoteAgeMs: Number.isFinite(Number(firmQuote.lighterQuoteAgeMs))
          ? Number(firmQuote.lighterQuoteAgeMs)
          : null
      }
    : null;

  if (stage === "quote") {
    const quoteStartedAt = performance.now();
    const quoteAdapted = adaptBody(typeof quoteTemplate.body === "string" ? quoteTemplate.body : "", {
      baseQty: phase === "close" ? baseQty : ""
    });
    if (quoteTemplate.requiresSidePatch && !quoteAdapted.sidePatched) {
      return {
        ok: false,
        error: "无法安全转换 Var 报价方向",
        detail: {
          phase,
          side,
          sourceSide: quoteTemplate.sourceSide || null,
          bodyFormat: quoteAdapted.format
        }
      };
    }
    if (phase === "close" && !quoteAdapted.baseQtyBound) {
      return {
        ok: false,
        error: "无法可靠绑定 Var 平仓报价数量",
        detail: {
          traceId,
          phase,
          side,
          stage,
          bodyFormat: quoteAdapted.format,
          baseQtyBound: false
        }
      };
    }
    const quoteFetch = buildFetchInit(quoteTemplate, quoteAdapted);
    const quoteResponse = await fetch(quoteTemplate.url, quoteFetch.init);
    const quoteText = await quoteResponse.text();
    quoteDetail = {
      method: quoteFetch.method,
      urlPath: quoteTemplate.urlPath || "",
      status: quoteResponse.status,
      elapsedMs: Math.round(performance.now() - quoteStartedAt),
      bodyFormat: quoteAdapted.format,
      sidePatched: quoteAdapted.sidePatched,
      amountMode: "captured-template",
      baseQtyPatched: quoteAdapted.baseQtyPatched,
      baseQtyBound: quoteAdapted.baseQtyBound,
      legRatioPatched: quoteAdapted.legRatioPatched
    };
    if (!quoteResponse.ok) {
      return {
        ok: false,
        error: `Var quote HTTP ${quoteResponse.status}`,
        detail: {
          phase,
          side,
          quote: quoteDetail,
          responsePreview: quoteText.slice(0, 180)
        }
      };
    }
    quoteId = extractQuoteId(quoteText);
    if (!quoteId) {
      return {
        ok: false,
        error: "Var quote 未返回 quote_id",
        detail: {
          phase,
          side,
          quote: quoteDetail,
          responsePreview: quoteText.slice(0, 180)
        }
      };
    }
    const firmPrice = extractFirmPrice(quoteText);
    const firmQtyResult = extractFirmQty(
      quoteText,
      firmPrice,
      quoteAdapted.baseQtyBound
    );
    const firmQty = firmQtyResult.value;
    const guardResult = validateFirmQuote(firmPrice);
    quoteDetail.quoteId = quoteId;
    quoteDetail.firmPrice = firmPrice;
    quoteDetail.firmQty = firmQty;
    quoteDetail.firmQtySource = firmQtyResult.source;
    quoteDetail.targetNotionalUsd = targetNotionalUsd;
    quoteDetail.firmNotionalUsd = (
      hasPositiveNumber(firmPrice) && hasPositiveNumber(firmQty)
        ? firmPrice * firmQty
        : null
    );
    quoteDetail.guardPnl = guardResult.pnl;
    quoteDetail.lighterQuoteAgeMs = guardResult.lighterQuoteAgeMs;
    quoteDetail.guardMinPnl = Number.isFinite(Number(guard.minPnlUsd)) ? Number(guard.minPnlUsd) : null;
    if (!firmQty) {
      return {
        ok: false,
        error: firmQtyResult.error || "firm quote 未返回权威开仓数量",
        detail: {
          traceId,
          phase,
          side,
          stage,
          quote: quoteDetail,
          responsePreview: quoteText.slice(0, 180)
        }
      };
    }
    if (!guardResult.ok) {
      return {
        ok: false,
        error: guardResult.error || "firm quote 未通过风控",
        detail: {
          traceId,
          phase,
          side,
          quote: quoteDetail,
          responsePreview: quoteText.slice(0, 180)
        }
      };
    }
    return {
      ok: true,
      detail: {
        traceId,
        phase,
        side,
        stage,
        elapsedMs: Math.round(performance.now() - startedAt),
        quote: quoteDetail
      }
    };
  } else if (stage === "commit") {
    if (!quoteId || !hasPositiveNumber(quoteDetail?.firmPrice) || !hasPositiveNumber(quoteDetail?.firmQty)) {
      return {
        ok: false,
        error: "firm quote 提交参数不完整",
        detail: { traceId, phase, side, stage }
      };
    }
    if (phase === "open") {
      if (!String(quoteDetail.firmQtySource || "").startsWith("api:")) {
        return {
          ok: false,
          error: "firm quote 开仓数量不是权威 API 数量",
          detail: { traceId, phase, side, stage, quote: quoteDetail }
        };
      }
    }
  }

  const adapted = adaptBody(originalBody, {
    quoteId,
    baseQty: phase === "close" ? baseQty : ""
  });
  if (template.requiresSidePatch && !adapted.sidePatched) {
    return {
      ok: false,
      error: "无法安全转换 Var 下单方向",
      detail: {
        phase,
        side,
        sourceSide: template.sourceSide || null,
        bodyFormat: adapted.format,
        quote: quoteDetail
      }
    };
  }
  // The close quote request above must bind the exact base quantity. The
  // commit request legitimately may contain only the returned quote_id; the
  // quantity is already frozen server-side by that quote.
  if (stage === "commit" && (!bodyLooksQuoteBound(originalBody) || !adapted.quotePatched)) {
    return {
      ok: false,
      error: "Var 下单模板未绑定可替换的 quote_id",
      detail: {
        phase,
        side,
        bodyFormat: adapted.format,
        quote: quoteDetail
      }
    };
  }

  const orderFetch = buildFetchInit(template, adapted);
  const response = await fetch(template.url, orderFetch.init);
  const text = await response.text();
  const elapsedMs = Math.round(performance.now() - startedAt);
  const detail = {
    traceId,
    phase,
    side,
    stage,
    method: orderFetch.method,
    urlPath: template.urlPath || "",
    status: response.status,
    elapsedMs,
    bodyFormat: adapted.format,
    sidePatched: adapted.sidePatched,
    amountMode: "captured-template",
    baseQtyPatched: adapted.baseQtyPatched,
    baseQtyBound: adapted.baseQtyBound,
    legRatioPatched: adapted.legRatioPatched,
    quotePatched: adapted.quotePatched,
    quote: quoteDetail,
    responsePreview: text.slice(0, 180)
  };
  return response.ok
    ? { ok: true, detail }
    : { ok: false, error: `Var fetch HTTP ${response.status}`, detail };
}

// The injected bridge survives individual commands, but intentionally not a
// page reload.  Commands carry only the current templates/arguments; the
// large, self-contained fetch executor is compiled in the page once.
const PAGE_ORDER_BRIDGE_NAME = "__variationalLighterOrderBridgeV1";
const PAGE_ORDER_BRIDGE_VERSION = "var-lit-v1-page-order-bridge";

function pageOrderBridgeInstallExpression() {
  return `(() => {
    const bridgeName = ${JSON.stringify(PAGE_ORDER_BRIDGE_NAME)};
    const version = ${JSON.stringify(PAGE_ORDER_BRIDGE_VERSION)};
    const executor = ${pageFetchOrder.toString()};
    globalThis[bridgeName] = { version, execute: executor };
    return { ok: true, bridgeInstalled: true, version };
  })()`;
}

function pageOrderBridgeCommandExpression(params) {
  return `(() => {
    const bridge = globalThis[${JSON.stringify(PAGE_ORDER_BRIDGE_NAME)}];
    if (!bridge || bridge.version !== ${JSON.stringify(PAGE_ORDER_BRIDGE_VERSION)} || typeof bridge.execute !== "function") {
      return { __pageBridgeMissing: true };
    }
    return bridge.execute(${JSON.stringify(params)});
  })()`;
}

function safePageOrderFallbackExpression(params) {
  return `(${pageFetchOrder.toString()})(${JSON.stringify(params)})`;
}

async function evaluatePageOrderBridge(tabId, params, timeoutMs) {
  const evaluate = async (expression) => withTimeout(
    sendDebuggerCommand(tabId, "Runtime.evaluate", {
      expression,
      awaitPromise: true,
      returnByValue: true,
      userGesture: true
    }),
    timeoutMs,
    `Var 页面执行超过 ${Math.round(timeoutMs / 1000)} 秒`
  );

  let result = await evaluate(pageOrderBridgeCommandExpression(params));
  if (result.exceptionDetails || result.result?.value?.__pageBridgeMissing !== true) {
    // Once bridge.execute was reached, an exception is an unknown execution
    // outcome. Never replay a possible Commit through the page fallback.
    return result;
  }

  const installed = await evaluate(pageOrderBridgeInstallExpression());
  if (installed.exceptionDetails || installed.result?.value?.bridgeInstalled !== true) {
    // Installation itself cannot send an order, so one page fallback is safe.
    return evaluate(safePageOrderFallbackExpression(params));
  }

  result = await evaluate(pageOrderBridgeCommandExpression(params));
  if (!result.exceptionDetails && result.result?.value?.__pageBridgeMissing === true) {
    // The page reloaded between installation and dispatch; the sentinel proves
    // the executor did not run, so this one fallback is still safe.
    return evaluate(safePageOrderFallbackExpression(params));
  }
  return result;
}

async function placeVariationalOrder(command) {
  if (state.attachedTabId == null) {
    state.lastCommandStatus = "下单失败：没有绑定 Variational 页面";
    notifyStatus();
    return { ok: false, error: "没有绑定 Variational 页面" };
  }
  const side = String(command.side || "").toUpperCase();
  const amount = String(command.amount || "").trim();
  const phase = ["open", "close"].includes(String(command.phase || "").toLowerCase())
    ? String(command.phase).toLowerCase()
    : "open";
  const timeoutMs = sanitizeOrderTimeoutMs(command.timeoutMs);
  const market = String(command.market || "").trim().toUpperCase();
  const pageMarket = await getAttachedPageMarket();
  if (!market || pageMarket !== market) {
    return {
      ok: false,
      error: `交易对不匹配：脚本=${market || "-"} 页面=${pageMarket || "-"}`
    };
  }
  const template = getOrderFetchTemplate(phase, side, market);
  const quoteTemplate = getQuoteFetchTemplate(phase, side, market);
  const baseQty = String(command.baseQty || command.closeQty || "").trim();
  const guard = command.guard && typeof command.guard === "object" ? command.guard : {};
  const fetchStage = String(command.fetchStage || "").trim().toLowerCase();
  const firmQuote = command.firmQuote && typeof command.firmQuote === "object" ? command.firmQuote : null;
  if (!["quote", "commit"].includes(fetchStage)) {
    return { ok: false, error: "Var fetch 阶段无效；只允许 quote 或 commit" };
  }

  if (fetchStage !== "quote" && !template) {
    const available = Object.keys(summarizeFetchTemplates()).sort();
    state.lastCommandStatus = `Var fetch 未完成：没有 ${phase}:${side} 模板`;
    notifyStatus();
    return {
      ok: false,
      error: "没有捕获 Var fetch 模板",
      detail: { need: `${phase}:${side}`, available }
    };
  }
  if (fetchStage !== "commit" && !quoteTemplate) {
    const available = Object.keys(summarizeQuoteTemplates()).sort();
    state.lastCommandStatus = `Var fetch 未完成：没有 ${phase}:${side} 报价模板`;
    notifyStatus();
    return {
      ok: false,
      error: "没有捕获 Var 报价模板",
      detail: { need: `${phase}:${side}`, available }
    };
  }
  const executionTemplate = fetchStage === "quote" ? quoteTemplate : template;
  if (!replayAuthenticationReady(executionTemplate)) {
    state.lastCommandStatus = "Var fetch 未完成：等待页面实时授权";
    notifyStatus();
    return {
      ok: false,
      error: "Var 页面实时授权尚未捕获，请保持登录并刷新交易页面"
    };
  }

  const pageParams = {
    side,
    amount,
    baseQty,
    phase,
    stage: fetchStage,
    template,
    quoteTemplate,
    firmQuote,
    guard,
    traceId: command.traceId || null,
    timeoutMs
  };

  try {
    state.lastCommandStatus = `正在执行 Var fetch：${phase} ${side}`;
    notifyStatus();
    const result = await evaluatePageOrderBridge(
      state.attachedTabId,
      pageParams,
      timeoutMs
    );
    if (result.exceptionDetails) {
      state.lastCommandStatus = `Var 页面执行失败：${result.exceptionDetails.text || "未知错误"}`;
      notifyStatus();
      return { ok: false, error: "Var 页面执行失败", detail: result.exceptionDetails.text };
    }
    const value = result.result?.value || { ok: false, error: "Var 页面没有返回结果" };
    if (value && typeof value === "object") {
      value.traceId = command.traceId || null;
    }
    const status = value.detail?.status ? ` HTTP ${value.detail.status}` : "";
    const responseStatus = Number(value.detail?.status || value.detail?.quote?.status || 0);
    if ([401, 403].includes(responseStatus)) {
      clearTransientReplayState();
      state.lastError = "Var 登录授权已失效，请刷新页面或重新登录";
    }
    const derived = template?.requiresSidePatch ? "（方向派生）" : "";
    state.lastCommandStatus = value.ok
      ? `Var fetch 已发送：${phase} ${side}${derived}${status}`
      : `Var fetch 未完成：${value.error || "未知错误"}`;
    notifyStatus();
    return value;
  } catch (error) {
    state.lastCommandStatus = `Var 下单失败：${error.message}`;
    notifyStatus();
    return { ok: false, error: error.message };
  }
}

function sanitizeOrderTimeoutMs(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return 19000;
  }
  return Math.min(59000, Math.max(2000, numeric - 1000));
}

function withTimeout(promise, timeoutMs, message) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(message)), timeoutMs);
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (error) => {
        clearTimeout(timer);
        reject(error);
      }
    );
  });
}

function isVariationalTradingUrl(rawUrl) {
  try {
    const parsed = new URL(rawUrl);
    return (
      parsed.origin === VAR_API_ORIGIN &&
      /^\/perpetual\/[A-Z0-9_-]+\/?$/i.test(parsed.pathname)
    );
  } catch {
    return false;
  }
}

async function getVariationalTabId(tabId = null, { createIfMissing = false } = {}) {
  if (tabId != null) {
    const tab = await chrome.tabs.get(tabId);
    if (!tab || tab.id == null || !isVariationalTradingUrl(tab.url)) {
      throw new Error("指定标签页不是 Variational 永续合约交易页");
    }
    return tab.id;
  }

  const tabs = await chrome.tabs.query({ url: `${VAR_API_ORIGIN}/perpetual/*` });
  const matched = tabs.find((tab) => tab.id != null && isVariationalTradingUrl(tab.url));
  if (matched) {
    return matched.id;
  }
  if (!createIfMissing) {
    throw new Error("没有找到 Variational 永续合约交易页");
  }
  const created = await chrome.tabs.create({ url: VAR_PAGE_URL, active: false });
  if (!created || created.id == null) {
    throw new Error("无法创建 Variational 交易页");
  }
  return created.id;
}

function cancelForwardingRecovery() {
  if (recoveryTimer) {
    clearTimeout(recoveryTimer);
    recoveryTimer = null;
  }
}

function scheduleForwardingRecovery(reason, { immediate = false } = {}) {
  if (
    !(state.desiredActive || state.config.autoStart) ||
    state.active ||
    recoveryTimer
  ) {
    return;
  }
  state.lastRecoveryReason = reason;
  const delayIndex = Math.min(state.recoveryAttempt, RECOVERY_DELAYS_MS.length - 1);
  const delayMs = immediate ? 0 : RECOVERY_DELAYS_MS[delayIndex];
  state.recoveryAttempt += 1;
  recoveryTimer = setTimeout(() => {
    recoveryTimer = null;
    startForwarding(null, { automatic: true }).catch((error) => {
      state.lastError = `自动恢复失败：${error.message}`;
      notifyStatus();
      scheduleForwardingRecovery("自动恢复重试");
    });
  }, delayMs);
  notifyStatus();
}

async function startForwarding(tabId = null, { automatic = false } = {}) {
  await ensureConfigLoaded();
  state.desiredActive = true;
  cancelForwardingRecovery();

  if (state.active) {
    return getStatus();
  }
  if (startForwardingPromise) {
    return startForwardingPromise;
  }

  startForwardingPromise = (async () => {
    const targetTabId = await getVariationalTabId(tabId, { createIfMissing: true });
    await debuggerAttach(targetTabId);

    try {
      await sendDebuggerCommand(targetTabId, "Network.enable");
    } catch (error) {
      await debuggerDetach(targetTabId);
      throw error;
    }

    state.active = true;
    state.attachedTabId = targetTabId;
    state.attachedMarket = await getAttachedPageMarket();
    state.lastError = null;
    state.lastRecoveryReason = null;
    state.recoveryAttempt = 0;
    wsForwarder.connect();
    restForwarder.connect();
    commandSocket.connect();
    autoReloadAttachedTab(automatic ? "forwarder recovered" : "forwarder started");
    notifyStatus();
    return getStatus();
  })();

  try {
    return await startForwardingPromise;
  } catch (error) {
    cleanupForwardingState();
    state.lastError = `转发器启动失败：${error.message}`;
    scheduleForwardingRecovery("启动失败");
    throw error;
  } finally {
    startForwardingPromise = null;
  }
}

async function stopForwarding() {
  state.desiredActive = false;
  state.recoveryAttempt = 0;
  state.lastRecoveryReason = null;
  cancelForwardingRecovery();
  const attachedTabId = state.attachedTabId;
  await persistTemplateCache();
  cleanupForwardingState();
  if (attachedTabId != null) {
    try {
      await debuggerDetach(attachedTabId);
    } catch (error) {
      state.lastError = `调试器解绑失败：${error.message}`;
    }
  }
  notifyStatus();
  return getStatus();
}

function cleanupForwardingState() {
  state.active = false;
  state.pendingResponses.clear();
  state.websocketMeta.clear();
  state.attachedTabId = null;
  state.attachedMarket = null;
  state.lastAutoReloadAt = 0;
  wsForwarder.close();
  restForwarder.close();
  commandSocket.close();
}

function getStatus() {
  return {
    build: FORWARDER_BUILD,
    active: state.active,
    desiredActive: state.desiredActive,
    recoveryAttempt: state.recoveryAttempt,
    lastRecoveryReason: state.lastRecoveryReason,
    attachedTabId: state.attachedTabId,
    attachedMarket: state.attachedMarket,
    config: state.config,
    sockets: {
      websocket: wsForwarder.status,
      rest: restForwarder.status,
      command: commandSocket.status
    },
    lastCommandStatus: state.lastCommandStatus,
    lastFetchTemplateStatus: state.lastFetchTemplateStatus,
    lastQuoteTemplateStatus: state.lastQuoteTemplateStatus,
    fetchTemplates: summarizeFetchTemplates(),
    quoteTemplates: summarizeQuoteTemplates(),
    templateCacheSource: state.templateCacheSource,
    templateReadiness: templateReadiness(state.attachedMarket),
    lastError: state.lastError
  };
}

function notifyStatus() {
  chrome.runtime.sendMessage({ event: "status", status: getStatus() }).catch(() => {
    // No listeners (popup closed), safe to ignore.
  });
}

function trackResponse(params) {
  if (!params?.response?.url || !matchesDomainFilter(params.response.url)) {
    return;
  }
  if (params.type !== "Fetch" && params.type !== "XHR") {
    return;
  }

  const matchedPattern = getMatchedRestPattern(params.response.url);
  if (!matchedPattern) {
    return;
  }

  state.pendingResponses.set(params.requestId, {
    requestId: params.requestId,
    url: params.response.url,
    status: params.response.status,
    statusText: params.response.statusText,
    mimeType: params.response.mimeType,
    headers: params.response.headers,
    type: params.type,
    matchedPattern,
    capturedAt: nowIso()
  });
}

async function forwardResponseBody(requestId, encodedDataLength) {
  const meta = state.pendingResponses.get(requestId);
  if (!meta || state.attachedTabId == null) {
    return;
  }
  state.pendingResponses.delete(requestId);

  try {
    const result = await sendDebuggerCommand(state.attachedTabId, "Network.getResponseBody", { requestId });
    restForwarder.send({
      kind: "rest_response",
      requestId,
      timestamp: nowIso(),
      encodedDataLength,
      ...meta,
      body: result.body ?? "",
      base64Encoded: Boolean(result.base64Encoded)
    });
  } catch (error) {
    restForwarder.send({
      kind: "rest_response_error",
      requestId,
      timestamp: nowIso(),
      ...meta,
      error: error.message
    });
  }
}

function forwardWebSocketFrame(direction, params) {
  const meta = state.websocketMeta.get(params.requestId);
  if (!meta) {
    return;
  }

  wsForwarder.send({
    kind: "ws_frame",
    direction,
    requestId: params.requestId,
    url: meta.url,
    matchedPattern: meta.matchedPattern || "",
    timestamp: nowIso(),
    opcode: params.response?.opcode,
    mask: params.response?.mask,
    payloadData: params.response?.payloadData ?? ""
  });
}

async function handleDebuggerEvent(source, method, params) {
  if (!state.active || source.tabId !== state.attachedTabId) {
    return;
  }

  if (method === "Network.requestWillBeSent") {
    captureLiveReplayHeaders(params?.request || {});
    await captureQuoteFetchTemplate(params);
    await captureOrderFetchTemplate(params);
    return;
  }

  if (method === "Network.responseReceived") {
    trackResponse(params);
    return;
  }

  if (method === "Network.loadingFinished") {
    await forwardResponseBody(params.requestId, params.encodedDataLength);
    return;
  }

  if (method === "Network.loadingFailed") {
    state.pendingResponses.delete(params.requestId);
    return;
  }

  if (method === "Network.webSocketCreated") {
    const matchedPattern = getMatchedWsPattern(params.url);
    if (matchesDomainFilter(params.url) && matchedPattern) {
      state.websocketMeta.set(params.requestId, {
        url: params.url,
        matchedPattern,
        createdAt: nowIso()
      });
    }
    return;
  }

  if (method === "Network.webSocketClosed") {
    const meta = state.websocketMeta.get(params.requestId);
    if (!meta) {
      return;
    }
    wsForwarder.send({
      kind: "ws_closed",
      requestId: params.requestId,
      url: meta.url,
      matchedPattern: meta.matchedPattern || "",
      timestamp: nowIso()
    });
    state.websocketMeta.delete(params.requestId);
    return;
  }

  if (method === "Network.webSocketFrameReceived") {
    forwardWebSocketFrame("received", params);
    return;
  }

  if (method === "Network.webSocketFrameSent") {
    forwardWebSocketFrame("sent", params);
    return;
  }

  if (method === "Network.webSocketFrameError") {
    const meta = state.websocketMeta.get(params.requestId);
    if (!meta) {
      return;
    }
    wsForwarder.send({
      kind: "ws_frame_error",
      requestId: params.requestId,
      url: meta.url,
      matchedPattern: meta.matchedPattern || "",
      timestamp: nowIso(),
      errorMessage: params.errorMessage || "未知 WebSocket 帧错误"
    });
  }
}

chrome.debugger.onEvent.addListener((source, method, params) => {
  handleDebuggerEvent(source, method, params).catch((error) => {
    state.lastError = `CDP 事件处理失败：${error.message}`;
    notifyStatus();
  });
});

chrome.debugger.onDetach.addListener((source, reason) => {
  if (source.tabId !== state.attachedTabId) {
    return;
  }
  state.lastError = `调试器已断开：${reason}`;
  cleanupForwardingState();
  notifyStatus();
  scheduleForwardingRecovery("调试器断开");
});

chrome.tabs.onRemoved.addListener((tabId) => {
  if (tabId !== state.attachedTabId) {
    return;
  }
  state.lastError = "Variational 交易页已关闭";
  cleanupForwardingState();
  notifyStatus();
  scheduleForwardingRecovery("交易页关闭");
});

chrome.tabs.onUpdated.addListener((tabId, changeInfo, tab) => {
  if (
    tabId !== state.attachedTabId ||
    !changeInfo.url ||
    isVariationalTradingUrl(tab?.url || changeInfo.url)
  ) {
    return;
  }
  const detachedTabId = state.attachedTabId;
  state.lastError = "绑定标签页已离开 Variational 永续合约交易页";
  cleanupForwardingState();
  debuggerDetach(detachedTabId).catch(() => undefined).finally(() => {
    notifyStatus();
    scheduleForwardingRecovery("交易页地址变化");
  });
});

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  (async () => {
    await ensureConfigLoaded();

    if (message.action === "getStatus") {
      return { ok: true, status: getStatus() };
    }

    if (message.action === "updateConfig") {
      const previousConfig = state.config;
      state.config = sanitizeConfig(message.config);
      await chrome.storage.local.set({ forwarderConfig: state.config });
      if (state.config.autoStart && !state.active) {
        state.desiredActive = true;
        scheduleForwardingRecovery("已启用自动启动", { immediate: true });
      } else if (!state.config.autoStart && !state.active) {
        state.desiredActive = false;
        cancelForwardingRecovery();
      }
      if (state.active) {
        const wsNeedsRestart = previousConfig.wsEndpoint !== state.config.wsEndpoint;
        const restNeedsRestart = (
          previousConfig.restEndpoint !== state.config.restEndpoint ||
          JSON.stringify(previousConfig.restAllowlist || []) !== JSON.stringify(state.config.restAllowlist || [])
        );
        const commandNeedsRestart = (
          previousConfig.commandEndpoint !== state.config.commandEndpoint
        );
        if (wsNeedsRestart) {
          wsForwarder.restart();
        }
        if (restNeedsRestart) {
          restForwarder.restart();
        }
        if (commandNeedsRestart) {
          commandSocket.restart();
        }
      }
      notifyStatus();
      return { ok: true, status: getStatus() };
    }

    if (message.action === "start") {
      const status = await startForwarding(message.tabId ?? null);
      return { ok: true, status };
    }

    if (message.action === "stop") {
      const status = await stopForwarding();
      return { ok: true, status };
    }

    if (message.action === "clearTemplates") {
      await clearFetchTemplateCache();
      return { ok: true, status: getStatus() };
    }

    return { ok: false, error: `未知操作：${message.action}` };
  })()
    .then((response) => sendResponse(response))
    .catch((error) => sendResponse({ ok: false, error: error.message }));

  return true;
});

chrome.runtime.onInstalled.addListener(() => {
  ensureConfigLoaded().catch(() => {
    // Ignore config load errors during install.
  });
});

chrome.runtime.onStartup.addListener(() => {
  ensureConfigLoaded()
    .then(() => {
      if (state.config.autoStart) {
        state.desiredActive = true;
        scheduleForwardingRecovery("Chrome 启动", { immediate: true });
      }
    })
    .catch(() => {
      // The popup can retry loading state after Chrome finishes starting.
    });
});
