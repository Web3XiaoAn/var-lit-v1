const $ = (id) => document.getElementById(id);

const inputs = {
  autoStart: $("autoStart"),
  domainFilter: $("domainFilter"),
  wsEndpoint: $("wsEndpoint"),
  restEndpoint: $("restEndpoint"),
  commandEndpoint: $("commandEndpoint"),
  restAllowlist: $("restAllowlist")
};

const statusEl = $("status");

function toStatusText(status) {
  return [
    `后台版本：${status.build || "旧版/未加载"}`,
    `转发状态：${status.active ? "已启动" : "已停止"}`,
    `自动启动：${status.config.autoStart ? "已启用" : "未启用"}`,
    `恢复状态：${status.desiredActive && !status.active ? `等待重连（第 ${status.recoveryAttempt || 0} 次）` : "-"}`,
    `绑定标签页：${status.attachedTabId ?? "-"}`,
    `域名过滤：${status.config.domainFilter}`,
    `WS 通道（${status.config.wsEndpoint}）：${socketStatusText(status.sockets.websocket)}`,
    `REST 通道（${status.config.restEndpoint}）：${socketStatusText(status.sockets.rest)}`,
    `命令通道（${status.config.commandEndpoint}）：${socketStatusText(status.sockets.command)}`,
    `REST 白名单数量：${(status.config.restAllowlist || []).length}`,
    `模板缓存：${status.templateCacheSource || "-"}`,
    `自动下单准备：${formatTemplateReadiness(status)}`,
    `报价模板：${formatQuoteTemplates(status)}`,
    `Fetch 模板：${formatFetchTemplates(status)}`,
    `最后命令：${status.lastCommandStatus || "-"}`,
    `最后错误：${status.lastError || "-"}`
  ].join("\n");
}

function formatTemplateReadiness(status) {
  const readiness = status.templateReadiness || {};
  const actions = readiness.actions || {};
  const labels = {
    "open:BUY": "做多 Var 开仓",
    "close:SELL": "做多 Var 平仓",
    "open:SELL": "做空 Var 开仓",
    "close:BUY": "做空 Var 平仓"
  };
  if (!readiness.market) {
    return "尚未绑定交易页面";
  }
  const lines = Object.entries(labels).map(([key, label]) => {
    const action = actions[key] || {};
    const stateText = action.ready ? "就绪" : "未就绪";
    return `${label}：${stateText}（订单${action.order || "缺失"}，报价${action.quote || "缺失"}，授权${action.authentication || "未知"}）`;
  });
  return `${readiness.market}\n${lines.join("\n")}`;
}

function formatQuoteTemplates(status) {
  const templates = status.quoteTemplates || {};
  const keys = Object.keys(templates).sort();
  if (!keys.length) {
    return status.lastQuoteTemplateStatus || "-";
  }
  return keys
    .map((key) => {
      const template = templates[key] || {};
      return `${key} ${template.method || ""} ${template.path || ""}`.trim();
    })
    .join("；");
}

function formatFetchTemplates(status) {
  const templates = status.fetchTemplates || {};
  const keys = Object.keys(templates).sort();
  if (!keys.length) {
    return status.lastFetchTemplateStatus || "-";
  }
  return keys
    .map((key) => {
      const template = templates[key] || {};
      return `${key} ${template.method || ""} ${template.path || ""}`.trim();
    })
    .join("；");
}

function socketStatusText(status) {
  const names = {
    connected: "已连接",
    authenticating: "认证中",
    connecting: "连接中",
    disconnected: "未连接",
    error: "错误"
  };
  return names[status] || status || "-";
}

function updateFormFromStatus(status) {
  inputs.autoStart.checked = status.config.autoStart === true;
  inputs.domainFilter.value = status.config.domainFilter || "";
  inputs.wsEndpoint.value = status.config.wsEndpoint || "";
  inputs.restEndpoint.value = status.config.restEndpoint || "";
  inputs.commandEndpoint.value = status.config.commandEndpoint || "";
  inputs.restAllowlist.value = (status.config.restAllowlist || []).join("\n");
}

function updateStatus(status) {
  statusEl.textContent = toStatusText(status);
}

async function send(action, payload = {}) {
  const response = await chrome.runtime.sendMessage({
    action,
    ...payload
  });
  if (!response?.ok) {
    throw new Error(response?.error || "未知插件错误");
  }
  return response.status;
}

function readConfig() {
  const restAllowlist = inputs.restAllowlist.value
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0);

  return {
    autoStart: inputs.autoStart.checked,
    domainFilter: inputs.domainFilter.value.trim(),
    wsEndpoint: inputs.wsEndpoint.value.trim(),
    restEndpoint: inputs.restEndpoint.value.trim(),
    commandEndpoint: inputs.commandEndpoint.value.trim(),
    restAllowlist
  };
}

async function refreshStatus() {
  const status = await send("getStatus");
  updateFormFromStatus(status);
  updateStatus(status);
}

$("start").addEventListener("click", async () => {
  try {
    await send("updateConfig", { config: readConfig() });
    const status = await send("start");
    updateStatus(status);
  } catch (error) {
    statusEl.textContent = `启动失败：${error.message}`;
  }
});

$("stop").addEventListener("click", async () => {
  try {
    const status = await send("stop");
    updateStatus(status);
  } catch (error) {
    statusEl.textContent = `停止失败：${error.message}`;
  }
});

$("clearTemplates").addEventListener("click", async () => {
  if (!confirm("确定清除已学习的 Var 下单模板吗？清除后需要重新完成一次开仓和平仓学习。")) {
    return;
  }
  try {
    const status = await send("clearTemplates");
    updateStatus(status);
  } catch (error) {
    statusEl.textContent = `清除失败：${error.message}`;
  }
});

chrome.runtime.onMessage.addListener((message) => {
  if (message.event === "status" && message.status) {
    updateStatus(message.status);
  }
});

refreshStatus().catch((error) => {
  statusEl.textContent = `加载状态失败：${error.message}`;
});
