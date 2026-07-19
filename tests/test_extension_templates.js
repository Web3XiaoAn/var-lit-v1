const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

const backgroundSource = fs.readFileSync(
  path.join(__dirname, "..", "chrome_extension", "background.js"),
  "utf8"
);
const popupHtmlSource = fs.readFileSync(
  path.join(__dirname, "..", "chrome_extension", "popup.html"),
  "utf8"
);
const popupJsSource = fs.readFileSync(
  path.join(__dirname, "..", "chrome_extension", "popup.js"),
  "utf8"
);
const manifest = JSON.parse(fs.readFileSync(
  path.join(__dirname, "..", "chrome_extension", "manifest.json"),
  "utf8"
));

function eventTarget() {
  const listeners = [];
  return {
    addListener(listener) { listeners.push(listener); },
    async dispatch(...args) {
      return Promise.all(listeners.map((listener) => listener(...args)));
    }
  };
}

function storageArea(data) {
  return {
    async get(keys) {
      if (typeof keys === "string") {
        return { [keys]: data[keys] };
      }
      return Object.fromEntries((keys || []).map((key) => [key, data[key]]));
    },
    async set(values) {
      Object.assign(data, values);
    },
    async remove(key) {
      delete data[key];
    }
  };
}

function createContext({ tabs = [] } = {}) {
  const local = {};
  const session = {};
  const tabState = tabs.map((tab) => ({ ...tab }));
  const events = {
    debuggerDetach: eventTarget(),
    tabsRemoved: eventTarget(),
    tabsUpdated: eventTarget(),
    runtimeStartup: eventTarget()
  };
  const context = vm.createContext({
    AbortSignal,
    URL,
    URLSearchParams,
    clearTimeout,
    console,
    crypto,
    performance,
    setTimeout,
    chrome: {
      storage: {
        local: storageArea(local),
        session: storageArea(session)
      },
      debugger: {
        onEvent: eventTarget(),
        onDetach: events.debuggerDetach,
        attach(_target, _version, callback) { callback(); },
        detach(_target, callback) { callback(); },
        sendCommand(_target, method, _params, callback) {
          callback(method === "Runtime.evaluate" ? { result: { value: "BTC" } } : {});
        }
      },
      runtime: {
        onMessage: eventTarget(),
        onInstalled: eventTarget(),
        onStartup: events.runtimeStartup,
        lastError: null,
        sendMessage: async () => {}
      },
      tabs: {
        onRemoved: events.tabsRemoved,
        onUpdated: events.tabsUpdated,
        async query() { return tabState.map((tab) => ({ ...tab })); },
        async get(tabId) { return tabState.find((tab) => tab.id === tabId) || null; },
        async create(createProperties) {
          const tab = { id: tabState.length + 100, ...createProperties };
          tabState.push(tab);
          return { ...tab };
        },
        reload(_tabId, _options, callback) { callback(); }
      }
    }
  });
  vm.runInContext(backgroundSource, context, { filename: "background.js" });
  vm.runInContext("state.config = { ...DEFAULT_CONFIG }; state.configLoaded = true; state.attachedMarket = 'BTC';", context);
  return { context, local, session, tabState, events };
}

function installLearnedRound(context) {
  vm.runInContext(`
    state.orderFetchTemplates[orderTemplateKey('BTC', 'open', 'BUY')] = {
      key: 'open:BUY', phase: 'open', side: 'BUY', market: 'BTC',
      url: 'https://omni.variational.io/api/orders/new/market',
      urlPath: '/api/orders/new/market', method: 'POST',
      headers: { 'content-type': 'application/json', authorization: 'Bearer test-token' },
      requiresTransientAuth: true,
      body: JSON.stringify({ side: 'BUY', quote_id: 'old-open', notional: 200 })
    };
    state.orderFetchTemplates[orderTemplateKey('BTC', 'close', 'SELL')] = {
      key: 'close:SELL', phase: 'close', side: 'SELL', market: 'BTC',
      url: 'https://omni.variational.io/api/quotes/accept',
      urlPath: '/api/quotes/accept', method: 'POST',
      headers: { 'content-type': 'application/json', authorization: 'Bearer test-token' },
      requiresTransientAuth: true,
      body: JSON.stringify({ action: 'SELL', quote_id: 'old-close', qty: 0.003 })
    };
    state.quoteFetchTemplates[quoteTemplateKey('BTC', 'open', 'neutral')] = {
      key: 'open:neutral', phase: 'open', side: null, sideNeutral: true, market: 'BTC',
      url: 'https://omni.variational.io/api/quotes/indicative',
      urlPath: '/api/quotes/indicative', method: 'POST',
      headers: { 'content-type': 'application/json', authorization: 'Bearer test-token' },
      requiresTransientAuth: true,
      body: JSON.stringify({ instrument: 'BTC', notional: 200 })
    };
    state.quoteFetchTemplates[quoteTemplateKey('BTC', 'close', 'neutral')] = {
      key: 'close:neutral', phase: 'close', side: null, sideNeutral: true, market: 'BTC',
      url: 'https://omni.variational.io/api/quotes/indicative',
      urlPath: '/api/quotes/indicative', method: 'POST',
      headers: { 'content-type': 'application/json', authorization: 'Bearer test-token' },
      requiresTransientAuth: true,
      body: JSON.stringify({ instrument: 'BTC', qty: 0.003, structure: { legs: [{ ratio: 1 }] } })
    };
  `, context);
}

test("command socket requires the fixed extension handshake before orders", async () => {
  const { context } = createContext();
  const sockets = [];
  class FakeSocket {
    static OPEN = 1;

    constructor(url) {
      this.url = url;
      this.readyState = FakeSocket.OPEN;
      this.sent = [];
      this.closed = false;
      sockets.push(this);
    }

    send(raw) {
      this.sent.push(JSON.parse(raw));
    }

    close() {
      this.closed = true;
      this.readyState = 3;
    }
  }
  context.WebSocket = FakeSocket;
  vm.runInContext("state.active = true;", context);

  vm.runInContext("commandSocket.connect()", context);
  const socket = sockets[0];
  socket.onopen();
  assert.deepEqual(
    {
      type: socket.sent[0].type,
      role: socket.sent[0].role,
      protocolVersion: socket.sent[0].protocolVersion,
      build: socket.sent[0].build
    },
    {
      type: "REGISTER",
      role: "extension",
      protocolVersion: "var-lit-v1-command-v1",
      build: "var-lit-v1"
    }
  );

  context.commandMessage = JSON.stringify({ type: "PLACE_ORDER", requestId: "before-register" });
  await vm.runInContext("commandSocket.handleMessage(commandMessage)", context);
  assert.equal(socket.sent.at(-1).type, "ORDER_RESULT");
  assert.equal(socket.sent.at(-1).ok, false);
  assert.match(socket.sent.at(-1).error, /registration is not valid/);

  context.commandMessage = JSON.stringify({
    type: "REGISTER_ACK",
    ok: true,
    role: "extension",
    protocolVersion: "var-lit-v1-command-v1",
    build: "var-lit-v1"
  });
  await vm.runInContext("commandSocket.handleMessage(commandMessage)", context);
  assert.equal(vm.runInContext("commandSocket.registered", context), true);
  vm.runInContext("executePlaceOrderCommand = async () => ({ ok: true, detail: 'accepted' });", context);
  context.commandMessage = JSON.stringify({ type: "PLACE_ORDER", requestId: "after-register" });
  await vm.runInContext("commandSocket.handleMessage(commandMessage)", context);
  assert.equal(socket.sent.at(-1).type, "ORDER_RESULT");
  assert.equal(socket.sent.at(-1).ok, true);
});

async function executeOrder(context, phase, side, amount, baseQty) {
  const calls = [];
  context.fetch = async (url, init) => {
    calls.push({ url, body: init.body });
    const isQuote = String(url).includes("/api/quotes/indicative");
    const firmPrice = side === "BUY" ? 101 : 99;
    const firmQty = phase === "close" ? Number(baseQty) : Number(amount) / firmPrice;
    return {
      ok: true,
      status: 200,
      async text() {
        return isQuote
          ? JSON.stringify({ quote_id: `fresh-${phase}-${side}`, bid: 99, ask: 101, qty: firmQty })
          : "{}";
      }
    };
  };
  context.testParams = vm.runInContext(`({
    stage: 'quote', phase: '${phase}', side: '${side}', amount: '${amount}', baseQty: '${baseQty}', timeoutMs: 5000,
    template: getOrderFetchTemplate('${phase}', '${side}', 'BTC'),
    quoteTemplate: getQuoteFetchTemplate('${phase}', '${side}', 'BTC'),
    guard: { required: false }
  })`, context);
  const quoteResult = await vm.runInContext("pageFetchOrder(testParams)", context);
  assert.equal(quoteResult.ok, true, JSON.stringify(quoteResult));
  assert.equal(calls.length, 1);
  context.firmQuote = quoteResult.detail.quote;
  context.testParams = vm.runInContext(`({
    stage: 'commit', phase: '${phase}', side: '${side}', amount: '${amount}', baseQty: '${baseQty}', timeoutMs: 5000,
    template: getOrderFetchTemplate('${phase}', '${side}', 'BTC'),
    quoteTemplate: null,
    firmQuote,
    guard: { required: false }
  })`, context);
  const commitResult = await vm.runInContext("pageFetchOrder(testParams)", context);
  assert.equal(commitResult.ok, true, JSON.stringify(commitResult));
  assert.equal(calls.length, 2);
  return JSON.parse(calls[1].body);
}

test("one learned round safely derives all four Var actions", async () => {
  const { context } = createContext();
  installLearnedRound(context);

  const readiness = vm.runInContext("templateReadiness('BTC')", context);
  for (const action of Object.values(readiness.actions)) {
    assert.equal(action.ready, true);
  }

  const openSell = await executeOrder(context, "open", "SELL", 200, "");
  assert.equal(openSell.side, "SELL");
  assert.equal(openSell.quote_id, "fresh-open-SELL");

  const closeBuy = await executeOrder(context, "close", "BUY", "", 0.003);
  assert.equal(closeBuy.action, "BUY");
  assert.equal(closeBuy.quote_id, "fresh-close-BUY");
});

test("captured quote and order pairs are associated with open and close phases", async () => {
  const { context } = createContext({
    tabs: [{ id: 1, url: "https://omni.variational.io/perpetual/BTC" }]
  });
  vm.runInContext("state.active = true; state.attachedTabId = 1;", context);

  async function capture(url, body) {
    context.captureParams = {
      requestId: `request-${Math.random()}`,
      request: {
        url,
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: "Bearer current-session"
        },
        postData: JSON.stringify(body)
      }
    };
    vm.runInContext("captureLiveReplayHeaders(captureParams.request)", context);
    await vm.runInContext("captureQuoteFetchTemplate(captureParams)", context);
    await vm.runInContext("captureOrderFetchTemplate(captureParams)", context);
  }

  await capture("https://omni.variational.io/api/quotes/indicative", { instrument: "BTC", notional: 200 });
  await capture("https://omni.variational.io/api/orders/new/market", {
    side: "BUY",
    quote_id: "open-quote",
    notional: 200
  });
  await capture("https://omni.variational.io/api/quotes/indicative", {
    instrument: "BTC",
    qty: 0.003,
    structure: { legs: [{ ratio: 1 }] }
  });
  await capture("https://omni.variational.io/api/quotes/accept", {
    action: "SELL",
    reduce_only: true,
    quote_id: "close-quote",
    qty: 0.003
  });
  await vm.runInContext("templatePersistPromise", context);

  const readiness = vm.runInContext("templateReadiness('BTC')", context);
  for (const action of Object.values(readiness.actions)) {
    assert.equal(action.ready, true, JSON.stringify(readiness));
  }
  assert.equal(
    vm.runInContext("Boolean(state.quoteFetchTemplates[quoteTemplateKey('BTC', 'open', 'neutral')])", context),
    true
  );
  assert.equal(
    vm.runInContext("Boolean(state.quoteFetchTemplates[quoteTemplateKey('BTC', 'close', 'neutral')])", context),
    true
  );
});

test("persistent cache strips credentials and session cache restores live authorization", () => {
  const { context } = createContext();
  installLearnedRound(context);

  const localCache = vm.runInContext("cachePayload({ persistent: true })", context);
  const sessionCache = vm.runInContext("cachePayload()", context);
  localCache.liveReplayHeaders = {
    "https://omni.variational.io": { authorization: "Bearer legacy-local-token" }
  };
  assert.equal(JSON.stringify(localCache).includes("test-token"), false);
  assert.equal(JSON.stringify(sessionCache).includes("test-token"), true);

  vm.runInContext("resetFetchTemplates()", context);
  context.localCache = localCache;
  vm.runInContext("restoreTemplateCache(localCache, null)", context);
  const localReadiness = vm.runInContext("templateReadiness('BTC')", context);
  for (const action of Object.values(localReadiness.actions)) {
    assert.equal(action.ready, false);
    assert.equal(action.authentication, "等待页面");
  }
  assert.equal(
    vm.runInContext("getOrderFetchTemplate('open', 'BUY', 'BTC').headers.authorization", context),
    undefined
  );

  context.sessionCache = sessionCache;
  vm.runInContext("restoreTemplateCache(localCache, sessionCache)", context);
  const sessionReadiness = vm.runInContext("templateReadiness('BTC')", context);
  for (const action of Object.values(sessionReadiness.actions)) {
    assert.equal(action.ready, true);
    assert.equal(action.authentication, "实时");
  }
});

test("fetch quote and commit are separate stages using the same firm quote", async () => {
  const { context } = createContext();
  installLearnedRound(context);
  const calls = [];
  context.fetch = async (url, init) => {
    calls.push({ url, body: init.body });
    const isQuote = String(url).includes("/api/quotes/indicative");
    return {
      ok: true,
      status: 200,
      async text() {
        return isQuote
          ? JSON.stringify({ quote_id: "fresh-two-stage", bid: 99, ask: 101, qty: 1.98 })
          : "{}";
      }
    };
  };

  context.quoteParams = vm.runInContext(`({
    stage: 'quote', phase: 'open', side: 'BUY', amount: '200', baseQty: '', timeoutMs: 5000,
    template: getOrderFetchTemplate('open', 'BUY', 'BTC'),
    quoteTemplate: getQuoteFetchTemplate('open', 'BUY', 'BTC'),
    guard: { required: false }
  })`, context);
  const quoteResult = await vm.runInContext("pageFetchOrder(quoteParams)", context);
  assert.equal(quoteResult.ok, true, JSON.stringify(quoteResult));
  assert.equal(calls.length, 1, "quote stage must not submit an order");
  assert.equal(quoteResult.detail.stage, "quote");
  assert.equal(quoteResult.detail.quote.quoteId, "fresh-two-stage");
  assert.equal(quoteResult.detail.quote.firmPrice, 101);
  assert.equal(quoteResult.detail.quote.firmQty, 1.98);

  context.commitParams = vm.runInContext(`({
    stage: 'commit', phase: 'open', side: 'BUY', amount: '200', baseQty: '', timeoutMs: 5000,
    template: getOrderFetchTemplate('open', 'BUY', 'BTC'),
    quoteTemplate: null,
    firmQuote: {
      quoteId: 'fresh-two-stage', firmPrice: 101, firmQty: 1.98, firmQtySource: 'api:qty'
    },
    guard: { required: false }
  })`, context);
  const commitResult = await vm.runInContext("pageFetchOrder(commitParams)", context);
  assert.equal(commitResult.ok, true, JSON.stringify(commitResult));
  assert.equal(calls.length, 2, "commit stage must not request another quote");
  assert.equal(commitResult.detail.stage, "commit");
  assert.equal(JSON.parse(calls[1].body).quote_id, "fresh-two-stage");
});

test("captured open amount is preserved instead of rewritten", async () => {
  const { context } = createContext();
  installLearnedRound(context);
  vm.runInContext(`
    state.quoteFetchTemplates[quoteTemplateKey('BTC', 'open', 'neutral')].body =
      JSON.stringify({ instrument: 'BTC', opaque_value: 500 });
    state.orderFetchTemplates[orderTemplateKey('BTC', 'open', 'BUY')].body =
      JSON.stringify({ side: 'BUY', quote_id: 'old-exact-quote', opaque_value: 500 });
  `, context);
  const calls = [];
  context.fetch = async (url, init) => {
    calls.push({ url, body: init.body });
    const isQuote = String(url).includes("/api/quotes/indicative");
    return {
      ok: true,
      status: 200,
      async text() {
        return isQuote
          ? JSON.stringify({ quote_id: "stale-500", bid: 99, ask: 100, qty: 5 })
          : "{}";
      }
    };
  };
  context.exactAmountParams = vm.runInContext(`({
    stage: 'quote', phase: 'open', side: 'BUY', amount: '200', baseQty: '', timeoutMs: 5000,
    template: getOrderFetchTemplate('open', 'BUY', 'BTC'),
    quoteTemplate: getQuoteFetchTemplate('open', 'BUY', 'BTC'),
    guard: { required: false }
  })`, context);

  const quoteResult = await vm.runInContext("pageFetchOrder(exactAmountParams)", context);

  assert.equal(quoteResult.ok, true, JSON.stringify(quoteResult));
  assert.equal(JSON.parse(calls[0].body).opaque_value, 500);
  assert.equal(quoteResult.detail.quote.firmNotionalUsd, 500);
  context.firmQuote = quoteResult.detail.quote;
  context.capturedCommitParams = vm.runInContext(`({
    stage: 'commit', phase: 'open', side: 'BUY', amount: '200', baseQty: '', timeoutMs: 5000,
    template: getOrderFetchTemplate('open', 'BUY', 'BTC'), quoteTemplate: null,
    firmQuote, guard: { required: false }
  })`, context);
  const commitResult = await vm.runInContext("pageFetchOrder(capturedCommitParams)", context);
  assert.equal(commitResult.ok, true, JSON.stringify(commitResult));
  assert.equal(JSON.parse(calls[1].body).opaque_value, 500);
});

test("known captured template notional is reported without rewriting it", async () => {
  const { context } = createContext();
  installLearnedRound(context);
  vm.runInContext(`
    state.quoteFetchTemplates[quoteTemplateKey('BTC', 'open', 'neutral')].body =
      JSON.stringify({ instrument: 'BTC', notional: 500 });
    state.orderFetchTemplates[orderTemplateKey('BTC', 'open', 'BUY')].body =
      JSON.stringify({ side: 'BUY', quote_id: 'old-target-quote', notional: 500 });
  `, context);
  const calls = [];
  context.fetch = async (url, init) => {
    calls.push({ url, body: init.body });
    return {
      ok: true,
      status: 200,
      async text() {
        return JSON.stringify({ quote_id: "target-500", bid: 99, ask: 100, qty: 5.006 });
      }
    };
  };
  context.templateTargetParams = vm.runInContext(`({
    stage: 'quote', phase: 'open', side: 'BUY', amount: '200', baseQty: '', timeoutMs: 5000,
    template: getOrderFetchTemplate('open', 'BUY', 'BTC'),
    quoteTemplate: getQuoteFetchTemplate('open', 'BUY', 'BTC'),
    guard: { required: false }
  })`, context);

  const quoteResult = await vm.runInContext("pageFetchOrder(templateTargetParams)", context);

  assert.equal(quoteResult.ok, true, JSON.stringify(quoteResult));
  assert.equal(quoteResult.detail.quote.targetNotionalUsd, 500);
  assert.equal(JSON.parse(calls[0].body).notional, 500);
});

test("open quote requires an authoritative Firm quantity", async () => {
  const { context } = createContext();
  installLearnedRound(context);
  const calls = [];
  context.fetch = async (url, init) => {
    calls.push({ url, body: init.body });
    return {
      ok: true,
      status: 200,
      async text() {
        return JSON.stringify({ quote_id: "missing-qty", bid: 99, ask: 100 });
      }
    };
  };
  context.mismatchParams = vm.runInContext(`({
    stage: 'quote', phase: 'open', side: 'BUY', amount: '200', baseQty: '', timeoutMs: 5000,
    template: getOrderFetchTemplate('open', 'BUY', 'BTC'),
    quoteTemplate: getQuoteFetchTemplate('open', 'BUY', 'BTC'),
    guard: { required: false }
  })`, context);

  const result = await vm.runInContext("pageFetchOrder(mismatchParams)", context);

  assert.equal(result.ok, false, JSON.stringify(result));
  assert.match(result.error, /未返回权威开仓数量/);
  assert.equal(result.detail.quote.firmQty, null);
  assert.equal(calls.length, 1, "only the Firm Quote may be requested");
});

test("an opaque captured Commit amount is never rewritten", async () => {
  const { context } = createContext();
  installLearnedRound(context);
  vm.runInContext(`
    state.orderFetchTemplates[orderTemplateKey('BTC', 'open', 'BUY')].body =
      JSON.stringify({ side: 'BUY', quote_id: 'old-exact-quote', opaque_value: 500 });
  `, context);
  const calls = [];
  context.fetch = async (url, init) => {
    calls.push({ url, body: init.body });
    return {
      ok: true,
      status: 200,
      async text() {
        return JSON.stringify({ quote_id: "valid-500", bid: 99, ask: 100, qty: 5 });
      }
    };
  };
  context.quoteParams = vm.runInContext(`({
    stage: 'quote', phase: 'open', side: 'BUY', amount: '200', baseQty: '', timeoutMs: 5000,
    template: getOrderFetchTemplate('open', 'BUY', 'BTC'),
    quoteTemplate: getQuoteFetchTemplate('open', 'BUY', 'BTC'),
    guard: { required: false }
  })`, context);
  const quoteResult = await vm.runInContext("pageFetchOrder(quoteParams)", context);
  assert.equal(quoteResult.ok, true, JSON.stringify(quoteResult));
  context.firmQuote = quoteResult.detail.quote;
  context.commitParams = vm.runInContext(`({
    stage: 'commit', phase: 'open', side: 'BUY', amount: '200', baseQty: '', timeoutMs: 5000,
    template: getOrderFetchTemplate('open', 'BUY', 'BTC'), quoteTemplate: null,
    firmQuote, guard: { required: false }
  })`, context);

  const commitResult = await vm.runInContext("pageFetchOrder(commitParams)", context);

  assert.equal(commitResult.ok, true, JSON.stringify(commitResult));
  assert.equal(JSON.parse(calls[1].body).opaque_value, 500);
  assert.equal(calls.length, 2);
});

test("open Commit rejects a quantity without authoritative API provenance", async () => {
  const { context } = createContext();
  installLearnedRound(context);
  context.fetchCalls = 0;
  context.fetch = async () => {
    context.fetchCalls += 1;
    return { ok: true, status: 200, async text() { return "{}"; } };
  };
  context.commitParams = vm.runInContext(`({
    stage: 'commit', phase: 'open', side: 'BUY', amount: '200', baseQty: '', timeoutMs: 5000,
    template: getOrderFetchTemplate('open', 'BUY', 'BTC'), quoteTemplate: null,
    firmQuote: { quoteId: 'untrusted-qty', firmPrice: 100, firmQty: 2 },
    guard: { required: false }
  })`, context);

  const result = await vm.runInContext("pageFetchOrder(commitParams)", context);

  assert.equal(result.ok, false, JSON.stringify(result));
  assert.match(result.error, /权威 API 数量/);
  assert.equal(context.fetchCalls, 0);
});

test("missing, unknown and full fetch stages fail before network access", async () => {
  const { context } = createContext();
  installLearnedRound(context);
  context.fetchCalls = 0;
  context.fetch = async () => {
    context.fetchCalls += 1;
    return { ok: true, status: 200, async text() { return "{}"; } };
  };

  for (const stage of [undefined, "unknown", "full"]) {
    context.invalidStage = stage;
    context.invalidStageParams = vm.runInContext(`({
      stage: invalidStage,
      phase: 'open', side: 'BUY', amount: '200', baseQty: '', timeoutMs: 5000,
      template: getOrderFetchTemplate('open', 'BUY', 'BTC'),
      quoteTemplate: getQuoteFetchTemplate('open', 'BUY', 'BTC'),
      guard: { required: false }
    })`, context);
    const result = await vm.runInContext("pageFetchOrder(invalidStageParams)", context);
    assert.equal(result.ok, false, String(stage));
    assert.match(result.error, /quote.*commit/);
  }
  assert.equal(context.fetchCalls, 0);
});

test("commit rejects an order template that cannot bind the firm quote id", async () => {
  const { context } = createContext();
  context.fetchCalls = 0;
  context.fetch = async () => {
    context.fetchCalls += 1;
    return { ok: true, status: 200, async text() { return "{}"; } };
  };
  context.unboundCommit = vm.runInContext(`({
    stage: 'commit', phase: 'open', side: 'BUY', amount: '200', baseQty: '', timeoutMs: 5000,
    template: {
      key: 'open:BUY', phase: 'open', side: 'BUY', market: 'BTC',
      url: 'https://omni.variational.io/api/orders/new/market',
      urlPath: '/api/orders/new/market', method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ side: 'BUY', notional: 200 })
    },
    quoteTemplate: null,
    firmQuote: {
      quoteId: 'fresh-required', firmPrice: 101, firmQty: 1.98, firmQtySource: 'api:qty'
    },
    guard: { required: false }
  })`, context);

  const result = await vm.runInContext("pageFetchOrder(unboundCommit)", context);

  assert.equal(result.ok, false);
  assert.match(result.error, /quote_id/);
  assert.equal(context.fetchCalls, 0);
});

test("close quote rejects an authoritative quantity mismatch", async () => {
  const { context } = createContext();
  installLearnedRound(context);
  context.fetch = async () => ({
    ok: true,
    status: 200,
    async text() {
      return JSON.stringify({ quote_id: "close-qty", bid: 99, ask: 101, qty: 99 });
    }
  });
  context.closeQuoteParams = vm.runInContext(`({
    stage: 'quote', phase: 'close', side: 'SELL', amount: '200', baseQty: '0.003', timeoutMs: 5000,
    traceId: 'trace-close-123',
    template: getOrderFetchTemplate('close', 'SELL', 'BTC'),
    quoteTemplate: getQuoteFetchTemplate('close', 'SELL', 'BTC'),
    guard: { required: false }
  })`, context);

  const result = await vm.runInContext("pageFetchOrder(closeQuoteParams)", context);
  assert.equal(result.ok, false, JSON.stringify(result));
  assert.match(result.error, /数量.*不匹配/);
});

test("close commit may contain only the fresh quote id after quantity-bound quote", async () => {
  const { context } = createContext();
  installLearnedRound(context);
  vm.runInContext(`
    state.orderFetchTemplates[orderTemplateKey('BTC', 'close', 'SELL')].body =
      JSON.stringify({ action: 'SELL', quote_id: 'old-close' });
  `, context);

  const committed = await executeOrder(context, "close", "SELL", "", 0.003);

  assert.equal(committed.action, "SELL");
  assert.equal(committed.quote_id, "fresh-close-SELL");
  assert.equal(Object.hasOwn(committed, "qty"), false);
});

test("an opaque close template cannot execute a frozen quantity", async () => {
  const { context } = createContext();
  installLearnedRound(context);
  vm.runInContext(`
    state.quoteFetchTemplates[quoteTemplateKey('BTC', 'close', 'neutral')].body =
      JSON.stringify({ opaque_quantity: 0.003 });
    state.orderFetchTemplates[orderTemplateKey('BTC', 'close', 'SELL')].body =
      JSON.stringify({ action: 'SELL', quote_id: 'old-close', opaque_quantity: 0.003 });
    globalThis.closeFetchCount = 0;
    globalThis.fetch = async () => {
      globalThis.closeFetchCount += 1;
      return { ok: true, status: 200, text: async () => '{}' };
    };
    globalThis.opaqueCloseParams = ({
      stage: 'quote', phase: 'close', side: 'SELL', amount: '200', baseQty: '0.002', timeoutMs: 5000,
      template: getOrderFetchTemplate('close', 'SELL', 'BTC'),
      quoteTemplate: getQuoteFetchTemplate('close', 'SELL', 'BTC'),
      guard: { required: false }
    });
  `, context);

  const result = await vm.runInContext("pageFetchOrder(opaqueCloseParams)", context);
  assert.equal(result.ok, false, JSON.stringify(result));
  assert.match(result.error, /绑定.*平仓.*数量/);
  assert.equal(vm.runInContext("closeFetchCount", context), 0);
});

test("persistent page bridge injects once and accepts compact commands", async () => {
  const { context } = createContext();
  installLearnedRound(context);
  vm.runInContext("globalThis.window = globalThis;", context);
  const calls = [];
  context.fetch = async (url, init) => {
    calls.push({ url, body: init.body });
    const quote = String(url).includes("/api/quotes/indicative");
    return {
      ok: true,
      status: 200,
      async text() {
        return quote
          ? JSON.stringify({ quote_id: "bridge-firm", bid: 99, ask: 101, qty: 1.98 })
          : "{}";
      }
    };
  };
  context.bridgeParams = vm.runInContext(`({
    stage: 'quote', phase: 'open', side: 'BUY', amount: '200', baseQty: '', timeoutMs: 5000,
    template: getOrderFetchTemplate('open', 'BUY', 'BTC'),
    quoteTemplate: getQuoteFetchTemplate('open', 'BUY', 'BTC'), guard: { required: false }
  })`, context);

  const installExpression = vm.runInContext("pageOrderBridgeInstallExpression()", context);
  const installed = vm.runInContext(installExpression, context);
  assert.equal(installed.bridgeInstalled, true);
  const compactExpression = vm.runInContext(
    "pageOrderBridgeCommandExpression(bridgeParams)",
    context
  );
  assert.equal(compactExpression.includes("const executor"), false);
  const result = await vm.runInContext(compactExpression, context);
  assert.equal(result.ok, true, JSON.stringify(result));
  assert.equal(calls.length, 1);

  vm.runInContext("delete globalThis[PAGE_ORDER_BRIDGE_NAME];", context);
  const missing = await vm.runInContext(compactExpression, context);
  assert.equal(missing.__pageBridgeMissing, true);
});

test("page bridge execution exception never falls back to a second order command", async () => {
  const { context } = createContext();
  vm.runInContext(`
    globalThis.bridgeCalls = 0;
    sendDebuggerCommand = async () => {
      globalThis.bridgeCalls += 1;
      return { exceptionDetails: { text: 'page executor failed after possible fetch' } };
    };
  `, context);

  const result = await vm.runInContext(
    "evaluatePageOrderBridge(1, { stage: 'commit', template: {} }, 1000)",
    context
  );
  assert.equal(context.bridgeCalls, 1);
  assert.equal(result.exceptionDetails.text, "page executor failed after possible fetch");
});

test("page bridge install failure may use exactly one safe page fallback", async () => {
  const { context } = createContext();
  vm.runInContext(`
    globalThis.bridgeCalls = [];
    sendDebuggerCommand = async (_tabId, _method, params) => {
      globalThis.bridgeCalls.push(params.expression);
      if (globalThis.bridgeCalls.length === 1) {
        return { result: { value: { __pageBridgeMissing: true } } };
      }
      if (globalThis.bridgeCalls.length === 2) {
        return { exceptionDetails: { text: 'bridge installation rejected' } };
      }
      return { result: { value: { ok: true, pageFallback: true } } };
    };
  `, context);

  const result = await vm.runInContext(
    "evaluatePageOrderBridge(1, { stage: 'commit', template: {} }, 1000)",
    context
  );
  assert.equal(context.bridgeCalls.length, 3);
  assert.equal(result.result.value.pageFallback, true);
});

test("extension contains only the browser fetch execution path", () => {
  assert.equal(backgroundSource.includes("async function pagePlaceOrder"), false);
  assert.equal(backgroundSource.includes("executionMode"), false);
  assert.equal(backgroundSource.includes("fillAmount"), false);
});

test("configuration defaults to manual start and only accepts an explicit boolean", () => {
  const { context } = createContext();
  assert.equal(vm.runInContext("sanitizeConfig({}).autoStart", context), false);
  assert.equal(vm.runInContext("sanitizeConfig({ autoStart: true }).autoStart", context), true);
  assert.equal(vm.runInContext("sanitizeConfig({ autoStart: 'true' }).autoStart", context), false);
});

test("tab selection only accepts an exact Variational perpetual page", async () => {
  const { context } = createContext({
    tabs: [
      { id: 1, url: "https://example.com/perpetual/BTC" },
      { id: 2, url: "https://omni.variational.io/perpetual/BTC" }
    ]
  });
  assert.equal(await vm.runInContext("getVariationalTabId()", context), 2);
  await assert.rejects(
    vm.runInContext("getVariationalTabId(1)", context),
    /不是 Variational/
  );
});

test("auto start creates a background Variational page and attaches once", async () => {
  const { context, tabState, events } = createContext();
  vm.runInContext("state.config.autoStart = true;", context);
  await events.runtimeStartup.dispatch();
  await new Promise((resolve) => setTimeout(resolve, 20));
  assert.equal(tabState.length, 1);
  assert.equal(tabState[0].url, "https://omni.variational.io/perpetual/BTC");
  assert.equal(tabState[0].active, false);
  assert.equal(vm.runInContext("state.active", context), true);
  await vm.runInContext("stopForwarding()", context);
});

test("manifest limits access to the Variational origin", () => {
  assert.deepEqual(manifest.host_permissions, ["https://omni.variational.io/*"]);
  assert.equal(manifest.minimum_chrome_version, "116");
  assert.equal(manifest.permissions.includes("activeTab"), false);
  assert.equal(manifest.host_permissions.includes("<all_urls>"), false);
});

test("popup starts with current fields and has no redundant save button", () => {
  assert.equal(popupHtmlSource.includes('id="saveConfig"'), false);
  assert.equal(popupJsSource.includes('$("saveConfig")'), false);
  assert.equal(popupHtmlSource.includes('id="start"'), true);
  assert.equal(popupHtmlSource.includes('id="commandAuthToken"'), false);
  assert.equal(popupHtmlSource.includes('id="autoStart"'), true);
});
