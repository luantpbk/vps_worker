// vps_worker.js
process.env.TZ = "Asia/Ho_Chi_Minh";
require("dotenv").config();
const { io: ClientIO } = require("socket.io-client");
const customParser = require("socket.io-msgpack-parser");
const { TikTokLiveConnection } = require("tiktok-live-connector");
const HttpsProxyAgent = require("https-proxy-agent");
const axios = require("axios");
const { gotScraping } = require("got-scraping");
const fs = require("fs");
const EventEmitter = require("events");

const CONFIG_FILE = "vps_config.json";

let config = {
  masterUrl: "http://localhost:3001",
  workerName: `Worker_01`,
  proxyCount: 5,
  useLocalNetwork: false,
  loadPerProxy: 10,
  localLoad: 50,
  activeLibrary: "tiktok-live-connector",
};
let currentDynamicMaxLoad = 0;

let globalConnectTokens = config.proxyCount + (config.useLocalNetwork ? 1 : 0);
let lastRefill = Date.now();
setInterval(() => {
  const now = Date.now();
  const elapsed = now - lastRefill;

  if (elapsed >= 2000) {
    globalConnectTokens = config.proxyCount + (config.useLocalNetwork ? 1 : 0);
    lastRefill = now;
  }
}, 500);

function loadConfig() {
  if (fs.existsSync(CONFIG_FILE)) {
    try {
      config = {
        ...config,
        ...JSON.parse(fs.readFileSync(CONFIG_FILE, "utf8")),
      };
    } catch (e) {}
  } else {
    fs.writeFileSync(CONFIG_FILE, JSON.stringify(config, null, 4));
  }
}
loadConfig();

let watchTimeout = null;
fs.watch(CONFIG_FILE, (eventType) => {
  if (eventType === "change") {
    if (watchTimeout) clearTimeout(watchTimeout);
    watchTimeout = setTimeout(() => {
      try {
        const newConfig = JSON.parse(fs.readFileSync(CONFIG_FILE, "utf8"));
        let isChanged = false;

        if (
          newConfig.localLoad !== undefined &&
          config.localLoad !== newConfig.localLoad
        ) {
          config.localLoad = newConfig.localLoad;
          isChanged = true;
        }
        if (
          newConfig.loadPerProxy !== undefined &&
          config.loadPerProxy !== newConfig.loadPerProxy
        ) {
          config.loadPerProxy = newConfig.loadPerProxy;
          isChanged = true;
        }
        if (
          newConfig.useLocalNetwork !== undefined &&
          config.useLocalNetwork !== newConfig.useLocalNetwork
        ) {
          config.useLocalNetwork = newConfig.useLocalNetwork;
          isChanged = true;
        }
        if (
          newConfig.proxyCount !== undefined &&
          config.proxyCount !== newConfig.proxyCount
        ) {
          config.proxyCount = newConfig.proxyCount;
          isChanged = true;
        }

        if (isChanged && masterSocket?.connected) {
          logWarn(`⚙️ Cập nhật cấu hình nóng. Đang cân bằng lại hệ thống...`);
          checkProxyHealth();
        }
      } catch (err) {}
    }, 2000);
  }
});

let activeConnections = {};
let assignedProxies = {};
let proxyUsage = {};
let proxyFailCount = {};
let proxyCooldown = {};
let proxyStrikeCount = {};
let proxyHealth = {};

let pendingChecks = new Map();
let connectionLocks = new Map();

let masterSocket = null;
let workerPausedUntil = 0;
let hasIPv6Support = false;

let dynamicProxies = [];
let zombieProxies = {};
const agentCache = {};
let localTaskQueue = [];

let proxyGeoData = {};

function getGeoParams(countryCode) {
  const geoMap = {
    VN: { lang: "vi-VN", region: "VN" },
    US: { lang: "en-US", region: "US" },
    TH: { lang: "th-TH", region: "TH" },
    ID: { lang: "id-ID", region: "ID" },
    MY: { lang: "ms-MY", region: "MY" },
    PH: { lang: "en-PH", region: "PH" },
    SG: { lang: "en-SG", region: "SG" },
    JP: { lang: "ja-JP", region: "JP" },
    KR: { lang: "ko-KR", region: "KR" },
    TW: { lang: "zh-TW", region: "TW" },
    RU: { lang: "ru-RU", region: "RU" },
    BR: { lang: "pt-BR", region: "BR" },
  };
  return geoMap[countryCode?.toUpperCase()] || { lang: "en-US", region: "US" };
}

if (config.useLocalNetwork) proxyUsage["local"] = 0;

function getShortProxy(p) {
  if (!p) return "Unknown";
  if (p === "local") return "Mạng VPS (Local)";
  if (typeof p === "string") return p.split("@").pop();
  return String(p);
}

const ENABLE_DEBUG = process.env.DEBUG || process.env.DEBUG === "true";
function logInfo(msg) {
  if (ENABLE_DEBUG) console.log(`[ℹ️] ${msg}`);
}
function logSuccess(msg) {
  console.log(`[✅] ${msg}`);
}
function logWarn(msg) {
  console.warn(`[⚠️] ${msg}`);
}
function logError(msg) {
  console.error(`[❌] ${msg}`);
}

function retireProxy(proxyStr) {
  const isManaged =
    dynamicProxies.includes(proxyStr) || zombieProxies[proxyStr];
  if (!isManaged) return;

  dynamicProxies = dynamicProxies.filter((p) => p !== proxyStr);

  if ((proxyUsage[proxyStr] || 0) > 0) {
    if (!zombieProxies[proxyStr]) {
      logWarn(
        `🧟 Proxy [${getShortProxy(proxyStr)}] bị phế truất nhưng đang gánh ${proxyUsage[proxyStr]} tải. Chuyển sang ZOMBIE.`,
      );
      zombieProxies[proxyStr] = true;
    }
  } else {
    logWarn(
      `🗑️ Proxy [${getShortProxy(proxyStr)}] bị phế truất (Tải = 0). Dọn dẹp RAM ngay!`,
    );
    cleanupProxyData(proxyStr);
    delete zombieProxies[proxyStr];
  }
}

function sendWorkerStatus() {
  if (masterSocket && masterSocket.connected) {
    const activeNames = Object.keys(activeConnections);
    let allPending = [
      ...Array.from(pendingChecks.keys()),
      ...localTaskQueue.map((c) => c.username),
      ...socketConnectionQueue.map((item) => item.channel.username),
      ...Array.from(connectionLocks.keys()),
    ].filter((uname) => !activeNames.includes(uname));
    allPending = [...new Set(allPending)];
    masterSocket.emit("worker_status", {
      currentLoad: activeNames.length + allPending.length,
      runningChannels: activeNames,
      pendingChannels: allPending,
      proxyUsage: proxyUsage,
    });
  }
}
setInterval(sendWorkerStatus, 10000);

setInterval(
  () => {
    if (masterSocket && masterSocket.connected) {
      let heldAssets = [...dynamicProxies];
      if (heldAssets.length > 0)
        masterSocket.emit("worker_heartbeat", heldAssets);
    }
  },
  2 * 60 * 1000,
);

async function checkProxyHealth() {
  let checkList = [...dynamicProxies];
  if (config.useLocalNetwork) checkList.unshift("local");
  let currentHealth = {};

  const chunkSize = 5;
  for (let i = 0; i < checkList.length; i += chunkSize) {
    const chunk = checkList.slice(i, i + chunkSize);

    await Promise.all(
      chunk.map(async (p) => {
        if (proxyCooldown[p] && Date.now() < proxyCooldown[p]) {
          let remain = Math.ceil((proxyCooldown[p] - Date.now()) / 1000);
          currentHealth[p] = { status: `ĐANG NGHỈ (${remain}s)` };
          return;
        }

        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 8000);

        let options = { signal: controller.signal };

        if (p !== "local") {
          const proxyAgent = getCachedAgent(p);
          options.httpAgent = proxyAgent;
          options.httpsAgent = proxyAgent;
        }

        try {
          if (p !== "local" && !proxyGeoData[p]) {
            try {
              const geoRes = await axios.get(
                "http://ip-api.com/json/?fields=countryCode",
                options,
              );
              if (geoRes.data && geoRes.data.countryCode) {
                proxyGeoData[p] = geoRes.data.countryCode;
              } else {
                proxyGeoData[p] = "VN";
              }
            } catch (geoErr) {
              proxyGeoData[p] = "UNKNOWN";
            }
          }

          const healthRes = await axios.get(
            "https://clients3.google.com/generate_204",
            options,
          );
          clearTimeout(timeoutId);

          if (healthRes.status === 200 || healthRes.status === 204) {
            currentHealth[p] = {
              status: "SẴN SÀNG",
              country: proxyGeoData[p] || "VN",
            };
            proxyFailCount[p] = 0;
          } else {
            throw new Error(`HTTP Lỗi ${healthRes.status}`);
          }
        } catch (e) {
          clearTimeout(timeoutId);

          currentHealth[p] = { status: "MẤT KẾT NỐI" };

          if (p !== "local") {
            if (proxyFailCount[p] < 0) return;

            proxyFailCount[p] = (proxyFailCount[p] || 0) + 1;
            const errMsg = e.message || "";

            if (
              errMsg.includes("402") ||
              errMsg.includes("407") ||
              errMsg.includes("Payment")
            ) {
              proxyFailCount[p] = 3;
            }

            logWarn(
              `[PING LỖI] Proxy [${getShortProxy(p)}] đứt mạng (Lần ${proxyFailCount[p]}/3). Lỗi: ${errMsg}`,
            );

            if (proxyFailCount[p] >= 3) {
              currentHealth[p].status = "BÁO LỖI";

              if (masterSocket?.connected) {
                masterSocket.emit("worker_report_dead_proxy", {
                  proxy: p,
                  workerName: config.workerName,
                });
              }

              retireProxy(p);
              proxyFailCount[p] = -9999;
            } else {
              proxyCooldown[p] = Date.now() + 20000;
            }
          } else {
            proxyCooldown["local"] = Date.now() + 20000;
          }
        }
      }),
    );
  }

  proxyHealth = currentHealth;

  let aliveCount =
    config.useLocalNetwork && proxyHealth["local"]?.status === "SẴN SÀNG"
      ? config.localLoad || 0
      : 0;

  for (let p of dynamicProxies) {
    if (proxyHealth[p]?.status === "SẴN SÀNG") {
      aliveCount += config.loadPerProxy || 0;
    }
  }

  currentDynamicMaxLoad = aliveCount;

  if (masterSocket?.connected) {
    masterSocket.emit("worker_update_capacity", {
      maxLoad: currentDynamicMaxLoad,
    });
  }
}
setInterval(checkProxyHealth, 45000);
setTimeout(checkProxyHealth, 2000);

function getNextAvailableProxy() {
  let allProxies = config.useLocalNetwork
    ? ["local", ...dynamicProxies]
    : [...dynamicProxies];
  let available = allProxies.filter((p) => {
    if (proxyCooldown[p] && Date.now() < proxyCooldown[p]) return false;
    if (p !== "local" && (proxyStrikeCount[p] || 0) >= 4) return false;
    const isReady = p === "local" || proxyHealth[p]?.status === "SẴN SÀNG";
    const limit = p === "local" ? config.localLoad : config.loadPerProxy;
    return isReady && (proxyUsage[p] || 0) < limit;
  });
  if (available.length === 0) return null;
  available.sort((a, b) => (proxyUsage[a] || 0) - (proxyUsage[b] || 0));
  return available[0];
}

function formatProxyUrl(rawProxy) {
  if (!rawProxy || typeof rawProxy !== "string") return null;
  rawProxy = rawProxy.trim();

  if (
    rawProxy.startsWith("http://") ||
    rawProxy.startsWith("https://") ||
    rawProxy.startsWith("socks")
  ) {
    return rawProxy;
  }

  if (rawProxy.includes("@")) {
    return `http://${rawProxy}`;
  }

  const parts = rawProxy.split(":");

  if (parts.length === 4 && !rawProxy.includes("[")) {
    return `http://${parts[2]}:${parts[3]}@${parts[0]}:${parts[1]}`;
  } else if (parts.length === 2 && !rawProxy.includes("[")) {
    return `http://${parts[0]}:${parts[1]}`;
  }

  if (parts.length >= 6) {
    const pass = parts.pop();
    const user = parts.pop();
    const port = parts.pop();
    const ipv6Raw = parts.join(":");

    const safeIpv6 = ipv6Raw.startsWith("[") ? ipv6Raw : `[${ipv6Raw}]`;
    return `http://${user}:${pass}@${safeIpv6}:${port}`;
  }

  return `http://${rawProxy}`;
}
function getCachedAgent(proxyStr) {
  if (!proxyStr || proxyStr === "local") return undefined;
  const proxyUrl = formatProxyUrl(proxyStr);
  if (!agentCache[proxyUrl]) {
    agentCache[proxyUrl] = new HttpsProxyAgent(proxyUrl, {
      keepAlive: true,
      keepAliveMsecs: 60000,
      rejectUnauthorized: false,
    });
  }
  return agentCache[proxyUrl];
}

function cleanupProxyData(proxy) {
  const proxyUrl = formatProxyUrl(proxy);
  if (proxyUrl && agentCache[proxyUrl]) {
    try {
      agentCache[proxyUrl].destroy();
    } catch (e) {}
    delete agentCache[proxyUrl];
  }

  delete proxyGeoData[proxy];
  delete proxyHealth[proxy];
  delete proxyUsage[proxy];
  delete proxyFailCount[proxy];
  delete proxyCooldown[proxy];
  delete proxyStrikeCount[proxy];
}

function safeEmitRadarResult({ channel, status, proxy }) {
  if (masterSocket?.connected)
    masterSocket.emit("radar_result", { channel, status, proxy });
}

let disconnectTimer = null;
function connectToMaster() {
  if (masterSocket) masterSocket.disconnect();
  masterSocket = ClientIO(config.masterUrl, {
    auth: { token: process.env.SOCKET_SECRET },
    reconnection: true,
    reconnectionDelay: 1000,
    transports: ["websocket"],
    parser: customParser,
  });
  masterSocket.on("connect_error", (err) => {
    logError(
      `LỖI KẾT NỐI MASTER: ${err.message}. Hãy kiểm tra IP hoặc Mật khẩu!`,
    );
  });
  masterSocket.on("connect", () => {
    logSuccess("Đã kết nối tới Master Hub (Pure Dispatcher)!");
    if (disconnectTimer) {
      clearTimeout(disconnectTimer);
      disconnectTimer = null;
    }

    masterSocket.emit("worker_ready", {
      name: config.workerName,
      type: "vps_proxy",
      maxLoad: currentDynamicMaxLoad,
      localLoad: config.localLoad,
      loadPerProxy: config.loadPerProxy,
      proxyCount: config.proxyCount,
      useLocalNetwork: config.useLocalNetwork,
      runningChannels: Object.keys(activeConnections),
      pendingChannels: Array.from(pendingChecks.keys()),
      heldProxies: dynamicProxies,
      activeLibrary: "tiktok-live-connector",
      supportIPv6: hasIPv6Support,
    });

    const neededProxies = Math.max(
      0,
      config.proxyCount - dynamicProxies.length,
    );
    if (neededProxies > 0)
      masterSocket.emit("worker_request_proxies", {
        count: neededProxies,
        workerName: config.workerName,
        supportIPv6: hasIPv6Support,
      });
  });

  masterSocket.on("worker_receive_proxies", (assignedProxiesArr) => {
    assignedProxiesArr.forEach((p) => {
      const proxyStr = typeof p === "string" ? p : p.proxy;
      if (!dynamicProxies.includes(proxyStr)) {
        dynamicProxies.push(proxyStr);
        proxyStrikeCount[proxyStr] = 0;
        proxyCooldown[proxyStr] = 0;
        if (!proxyHealth) proxyHealth = {};
        proxyHealth[proxyStr] = { status: "SẴN SÀNG" };
      }
    });
    logSuccess(`📥 Đã nhận ${assignedProxiesArr.length} Proxy từ Master.`);
    checkProxyHealth();
  });

  masterSocket.on("worker_proxy_replacement", (data) => {
    const { deadProxy, newProxy } = data;

    retireProxy(deadProxy);

    if (newProxy) {
      const pStr = typeof newProxy === "string" ? newProxy : newProxy.proxy;
      if (zombieProxies[pStr]) delete zombieProxies[pStr];
      if (!dynamicProxies.includes(pStr)) dynamicProxies.push(pStr);

      proxyStrikeCount[pStr] = 0;
      proxyCooldown[pStr] = 0;
      if (!proxyHealth) proxyHealth = {};
      proxyHealth[pStr] = { status: "SẴN SÀNG" };
      logSuccess(
        `🔄 Đổi máu: Phế truất [${getShortProxy(deadProxy)}] -> Nạp mới [${getShortProxy(pStr)}]`,
      );
    }
    checkProxyHealth();
  });

  masterSocket.on("worker_proxy_removed", (proxyStr) => {
    retireProxy(proxyStr);
    logWarn(`🗑️ Lệnh từ Admin: Thu hồi Proxy [${getShortProxy(proxyStr)}]`);
    checkProxyHealth();
  });

  masterSocket.on("cmd_pause_worker", () => {
    logWarn(
      "⏸️ Nhận lệnh TẠM DỪNG từ Master. Ngừng nhận kênh mới, chờ xả tải dần...",
    );
    workerPausedUntil = Infinity;
  });

  masterSocket.on("cmd_resume_worker", () => {
    logSuccess("▶️ Nhận lệnh TIẾP TỤC từ Master. Bắt đầu nhận kênh mới!");
    workerPausedUntil = 0;
  });

  masterSocket.on("cmd_stop_worker", () => {
    logWarn(
      "⏹️ Nhận lệnh DỪNG HẲN (STOP) từ Master. Rút điện toàn bộ hệ thống!",
    );
    workerPausedUntil = Infinity;

    localTaskQueue = [];
    socketConnectionQueue = [];
    const runningUsers = Object.keys(activeConnections);
    runningUsers.forEach((user) => {
      safeEmitRadarResult({ channel: { username: user }, status: "REQUEUE" });
      stopWebcast(user);
    });

    pendingChecks.clear();
    connectionLocks.clear();
  });

  masterSocket.on("process_task", (channel) => {
    if (!channel || !channel.username) return;
    const maxAllowedQueue = Math.max(20, currentDynamicMaxLoad * 2);
    if (localTaskQueue.length >= maxAllowedQueue)
      return safeEmitRadarResult({ channel, status: "REQUEUE" });
    if (
      !localTaskQueue.some((c) => c.username === channel.username) &&
      !pendingChecks.has(channel.username) &&
      !activeConnections[channel.username] &&
      !connectionLocks.has(channel.username) &&
      !socketConnectionQueue.some((item) => item.channel.username === channel.username)
    ) {
      localTaskQueue.push(channel);
    }
  });

  masterSocket.on("force_update_config", (newCfg) => {
    if (newCfg.useLocalNetwork !== undefined)
      config.useLocalNetwork =
        newCfg.useLocalNetwork === true || newCfg.useLocalNetwork === "true";
    if (newCfg.proxyCount !== undefined)
      config.proxyCount = parseInt(newCfg.proxyCount);
    if (newCfg.localLoad !== undefined)
      config.localLoad = parseInt(newCfg.localLoad);
    if (newCfg.loadPerProxy !== undefined)
      config.loadPerProxy = parseInt(newCfg.loadPerProxy);

    fs.writeFileSync(CONFIG_FILE, JSON.stringify(config, null, 4));
    sendWorkerStatus();
    checkProxyHealth();
    balanceResources();
  });

  masterSocket.on("disconnect", (reason) => {
    if (reason === "io server disconnect") masterSocket.connect();
    else {
      disconnectTimer = setTimeout(() => {
        localTaskQueue = [];
        socketConnectionQueue = [];
        for (let username in activeConnections) stopWebcast(username);
        activeConnections = {};
        assignedProxies = {};
        pendingChecks.clear();
        connectionLocks.clear();
        disconnectTimer = null;
      }, 600000);
    }
  });

  masterSocket.on("cmd_stop_all", () => {
    for (let username in activeConnections) stopWebcast(username);
  });
}

let isProcessingQueue = false;
setInterval(async () => {
  if (
    isProcessingQueue ||
    Date.now() < workerPausedUntil ||
    localTaskQueue.length === 0
  )
    return;
  isProcessingQueue = true;
  try {
    const currentActive = Object.keys(activeConnections).length;
    const totalIntendedLoad =
      currentActive + socketConnectionQueue.length + pendingChecks.size;

    if (
      totalIntendedLoad >= currentDynamicMaxLoad &&
      currentDynamicMaxLoad > 0
    ) {
      while (localTaskQueue.length > 0)
        safeEmitRadarResult({
          channel: localTaskQueue.shift(),
          status: "REQUEUE",
        });
      return;
    }

    const maxConcurrentChecks =
      (dynamicProxies.length + (config.useLocalNetwork ? 1 : 0)) * 2;
    const availableCheckSlots = maxConcurrentChecks - pendingChecks.size;
    const loadShortage = currentDynamicMaxLoad - totalIntendedLoad;
    const actualSlots = Math.min(availableCheckSlots, loadShortage);

    if (actualSlots <= 0) return;

    const tasksToProcess = Math.min(actualSlots, localTaskQueue.length);
    // 💡 VÁ LỖI: Tăng delay check HTTP để tránh DDOS TikTok
    // Giãn cách tối thiểu 1-2 giây giữa các nhịp check mới
    const delayStep = Math.max(1500, 3000 / Math.max(1, tasksToProcess));

    for (let i = 0; i < tasksToProcess; i++) {
      const channel = localTaskQueue.splice(0, 1)[0];
      pendingChecks.set(channel.username, Date.now());

      setTimeout(
        () => {
          executeTask(channel);
        },
        i * delayStep + Math.floor(Math.random() * 500),
      );
    }
  } finally {
    isProcessingQueue = false;
  }
}, 1000);

async function checkLiveStatus(username, proxy) {
  let proxyUrlGot = proxy === "local" ? undefined : formatProxyUrl(proxy);
  const urlUsername = username.startsWith("@") ? username : `@${username}`;
  let retries = 1;
  while (retries >= 0) {
    try {
      const currentCountry = proxyGeoData[proxy] || "VN";
      const geo = getGeoParams(currentCountry);
      const fetchPromise = gotScraping({
        url: `https://www.tiktok.com/${urlUsername}/live`,
        proxyUrl: proxyUrlGot,
        timeout: { request: 12000 },
        throwHttpErrors: false,
        http2: true,
        retry: { limit: 0 },
        headerGeneratorOptions: {
          browsers: [{ name: "chrome", minVersion: 120 }],
          devices: ["desktop"],
          locales: [geo.lang, "en-US"],
        },
      });

      let timeoutHandle;
      const hardTimeout = new Promise((_, r) => {
        timeoutHandle = setTimeout(() => r(new Error("HARD_TIMEOUT")), 15000);
      });

      const res = await Promise.race([fetchPromise, hardTimeout]);
      clearTimeout(timeoutHandle);

      if ([403, 429].includes(res.statusCode)) {
        logWarn(
          `[TIKTOK BLOCK] HTTP ${res.statusCode} chặn kết nối - Proxy: ${getShortProxy(proxy)}`,
        );
        return "RATE_LIMIT";
      }

      if (
        [407, 502, 503, 504].includes(res.statusCode) ||
        res.statusCode >= 500
      ) {
        logWarn(
          `[PROXY ERROR] HTTP ${res.statusCode} Proxy chết yếu - Proxy: ${getShortProxy(proxy)}`,
        );
        return "PROXY_ERR";
      }

      if (res.statusCode === 404) return "NOT_FOUND";

      const finalUrl = (res.url || "").toLowerCase();
      if (
        finalUrl.includes("login") ||
        finalUrl.includes("verify") ||
        finalUrl.includes("captcha")
      ) {
        logWarn(
          `[TIKTOK CAPTCHA] Bị ép giải Captcha URL - Proxy: ${getShortProxy(proxy)}`,
        );
        return "CAPTCHA";
      }

      const html =
        typeof res.body === "string"
          ? res.body
          : JSON.stringify(res.body || "");

      if (
        html.includes("Just a moment...") ||
        html.includes("Challenge Validation") ||
        html.includes("cf-browser-verification")
      ) {
        logWarn(
          `[CLOUDFLARE WAF] Bị chặn ngầm (Bot Detect) - Proxy: ${getShortProxy(proxy)}`,
        );
        return "RATE_LIMIT";
      }

      if (
        html.includes('"statusCode":10000') ||
        html.includes("webapp.not-found")
      )
        return "NOT_FOUND";

      const isLiveFlag =
        html.includes('"status":2') || html.includes('"isLive":true');
      const roomMatch = html.match(/"(?:roomId|room_id)"\s*:\s*"?([1-9]\d+)"?/);
      if (roomMatch && roomMatch[1] && isLiveFlag) return "LIVE";

      return "OFFLINE";
    } catch (error) {
      retries--;
      if (retries < 0) {
        if (error.message === "HARD_TIMEOUT") {
          logWarn(
            `[HARD TIMEOUT] Treo kết nối quá 15s - Proxy: ${getShortProxy(proxy)}`,
          );
          return "NETWORK_ERR";
        }

        logWarn(
          `[NETWORK/TIMEOUT] Lỗi ngầm - Proxy: ${getShortProxy(proxy)} | Lỗi: ${error.message}`,
        );

        if (
          error.message.includes("402") ||
          error.message.includes("407") ||
          error.message.includes("Payment")
        ) {
          return "FATAL_PROXY_BILLING";
        }
        return "NETWORK_ERR";
      }
      await new Promise((r) => setTimeout(r, 1000));
    }
  }
}

// ==========================================
// 💡 HÀNG ĐỢI CHỐNG SPAM (RATE LIMIT TỪNG GIÂY)
// ==========================================
let socketConnectionQueue = [];
let isConnectingSocket = false;

setInterval(async () => {
  if (
    isConnectingSocket ||
    socketConnectionQueue.length === 0 ||
    Date.now() < workerPausedUntil
  )
    return;

  isConnectingSocket = true;
  try {
    const { channel, proxy } = socketConnectionQueue.shift();

    if (!activeConnections[channel.username]) {
      if (globalConnectTokens <= 0) {
        socketConnectionQueue.unshift({ channel, proxy });
        return;
      }

      globalConnectTokens--;
      startWebcast(channel, proxy);

      // 💡 VÁ LỖI: Tốc độ cắm cho Proxy Direct (TLC)
      // Thử thách của TikTok là cắm quá nhanh sẽ bị IP Ban. 
      // Giới hạn mốc 1.5 giây cho mỗi nhịp cắm để cực kỳ an toàn.
      const dynamicDelay = 1500;
      const jitter = Math.floor(Math.random() * 800);
      await new Promise((r) => setTimeout(r, dynamicDelay + jitter));
    }
  } finally {
    isConnectingSocket = false;
  }
}, 100);

async function executeTask(channel) {
  if (
    activeConnections[channel.username] ||
    connectionLocks.has(channel.username)
  ) {
    pendingChecks.delete(channel.username);
    return;
  }

  let availableProxies = [];
  if (config.useLocalNetwork) {
    if (!proxyCooldown["local"] || Date.now() > proxyCooldown["local"]) {
      availableProxies.push("local");
    }
  }
  for (let p of dynamicProxies) {
    if (
      proxyHealth[p]?.status === "SẴN SÀNG" &&
      (!proxyCooldown[p] || Date.now() > proxyCooldown[p]) &&
      (proxyStrikeCount[p] || 0) < 4
    )
      availableProxies.push(p);
  }

  if (availableProxies.length === 0) {
    pendingChecks.delete(channel.username);
    return safeEmitRadarResult({ channel, status: "REQUEUE" });
  }

  const checkProxy =
    availableProxies[Math.floor(Math.random() * availableProxies.length)];

  try {
    const status = await checkLiveStatus(channel.username, checkProxy);
    if (!pendingChecks.has(channel.username)) {
      logWarn(
        `[GHOST HTTP] Kênh ${channel.username} check quá lâu và đã bị Requeue. Hủy lệnh cắm!`,
      );
      return;
    }
    if (status === "LIVE") {
      logInfo(`${channel.username} LIVE. Đưa vào hàng chờ cắm Socket`);
    }

    if (status === "NOT_FOUND" || status === "OFFLINE") {
      safeEmitRadarResult({ channel, status: status, proxy: checkProxy });
      return;
    }

    if (
      status === "CAPTCHA" ||
      status === "RATE_LIMIT" ||
      status === "PROXY_ERR" ||
      status === "NETWORK_ERR" ||
      status === "FATAL_PROXY_BILLING"
    ) {
      if (checkProxy !== "local") {
        if (
          !dynamicProxies.includes(checkProxy) &&
          !zombieProxies[checkProxy]
        ) {
          safeEmitRadarResult({ channel, status: "REQUEUE" });
          return;
        }

        if (status === "FATAL_PROXY_BILLING") {
          proxyStrikeCount[checkProxy] = 4;
        } else if (status === "RATE_LIMIT" || status === "CAPTCHA") {
          proxyCooldown[checkProxy] = Date.now() + 180000;
        } else {
          proxyStrikeCount[checkProxy] =
            (proxyStrikeCount[checkProxy] || 0) + 1;
        }

        if (proxyStrikeCount[checkProxy] >= 4) {
          if (masterSocket?.connected)
            masterSocket.emit("worker_report_dead_proxy", {
              proxy: checkProxy,
            });
          retireProxy(checkProxy);
        } else if (
          proxyStrikeCount[checkProxy] < 4 &&
          status !== "RATE_LIMIT" &&
          status !== "CAPTCHA"
        ) {
          proxyCooldown[checkProxy] = Date.now() + 60000;
        }
      } else {
        proxyCooldown["local"] = Date.now() + 45000;
      }
      safeEmitRadarResult({ channel, status: "ERROR" });
      return;
    }

    if (status === "LIVE") {
      const socketProxy = getNextAvailableProxy();
      if (!socketProxy) {
        safeEmitRadarResult({ channel, status: "REQUEUE" });
        return;
      }

      safeEmitRadarResult({ channel, status: "LIVE", proxy: socketProxy });
      delete proxyFailCount[socketProxy];
      delete proxyCooldown[socketProxy];
      if (proxyHealth[socketProxy])
        proxyHealth[socketProxy].status = "SẴN SÀNG";
      proxyUsage[socketProxy] = (proxyUsage[socketProxy] || 0) + 1;
      assignedProxies[channel.username] = socketProxy;

      connectionLocks.set(channel.username, Date.now());
      // 💡 VÁ LỖI: Đưa vào hàng chờ thay vì gọi startWebcast trực tiếp
      socketConnectionQueue.push({ channel, proxy: socketProxy });
    }
  } catch (e) {
    if (checkProxy !== "local") proxyCooldown[checkProxy] = Date.now() + 30000;
    else proxyCooldown["local"] = Date.now() + 30000;

    setTimeout(() => {
      safeEmitRadarResult({ channel, status: "REQUEUE" });
    }, 3000);
  } finally {
    pendingChecks.delete(channel.username);
  }
}

function startWebcast(channel, proxy) {
  if (activeConnections[channel.username]) return;
  connectionLocks.set(channel.username, Date.now());

  const currentCountry = proxyGeoData[proxy] || "VN";
  const geo = getGeoParams(currentCountry);

  let conn;
  let roomId = null;
  let pendingBoxes = [];
  let currentViewers = 0;
  let isProcessingInitial = true;

  if (!channel || !channel.username)
    throw new Error("Dữ liệu kênh bị rỗng (Undefined Username)");

  const options = {
    webClientOptions: { httpsAgent: getCachedAgent(proxy) },
    websocketOptions: { agent: getCachedAgent(proxy) },
    processInitialData: true,
    fetchRoomInfoOnConnect: true,
    clientParams: {
      browser_language: `${geo.lang}-${geo.region}`,
      app_language: geo.lang,
      webcast_language: geo.lang,
      region: geo.region,
    },
  };

  conn = new TikTokLiveConnection(channel.username, options);

  const emitChest = (data) => {
    const boxData = data?.envelopeInfo || data?.treasureBoxData || data;
    const coins = boxData?.diamondCount || boxData?.coin || boxData?.coins || 0;
    const boxes =
      boxData?.peopleCount || boxData?.totalUser || boxData?.boxes || 0;
    let boxType = "tui";
    const bType = boxData?.businessType;
    if (bType === 1 || String(bType) === "1") boxType = "ruong";
    else if (bType === 4 || String(bType) === "4") boxType = "ruong_vang";
    if (coins <= 15) return;

    const icon =
      boxType === "tui" ? "🧧" : boxType === "ruong_vang" ? "🏆" : "🎁";
    logSuccess(
      `[${channel.username}] ${icon} Phát hiện ${coins} Xu / ${boxes} Hộp qua TikTok-Live-Connector [🌐 PROXY DIRECT]`,
    );

    if (activeConnections[channel.username])
      activeConnections[channel.username].lastActive = Date.now();
    let originTimeMs = Date.now();
    if (data?.common?.createTime) {
      originTimeMs = Number(data.common.createTime);
      if (originTimeMs < 10000000000) originTimeMs *= 1000;
    } else if (data?.timestamp) {
      originTimeMs = Number(data.timestamp);
    }

    masterSocket.emit("worker_chest_raw", {
      channel,
      coins,
      boxes,
      idc:
        boxData?.envelopeId ||
        boxData?.id ||
        boxData?.treasureId ||
        boxData?.envelopeIdc ||
        "",
      workerName: config.workerName,
      liveRegion: channel.country || "unknown",
      unpackAt: boxData?.unpackAt || boxData?.openTime,
      viewers: currentViewers,
      roomId,
      workerTime: originTimeMs,
      isHanging: isProcessingInitial,
      type: boxType,
    });
  };

  const catchTreasureBox = (data) => {
    if (!roomId) {
      pendingBoxes.push(data);
      return;
    }
    emitChest(data);
  };

  conn.on("envelope", catchTreasureBox);
  conn.on("treasureBox", catchTreasureBox);
  conn.on("roomUser", (u) => {
    const views = u?.viewerCount || u?.viewer_count || u?.totalUser;
    if (views) currentViewers = views;
  });

  conn.on("streamEnd", () => {
    stopWebcast(channel.username);
    safeEmitRadarResult({ channel, status: "OFFLINE", proxy });
  });

  conn.on("disconnected", () => {
    logWarn(
      `[DISCONNECTED] Kênh ${channel.username} rớt mạng. Chuyển về REQUEUE.`,
    );
    stopWebcast(channel.username);
    safeEmitRadarResult({ channel, status: "REQUEUE", proxy });
  });

  conn.on("error", (err) => {
    logWarn(
      `[ERROR] Kênh ${channel.username} lỗi: ${err.message}. Đang phục hồi...`,
    );
  });

  let timeoutHandle;
  const timeoutPromise = new Promise((_, r) => {
    timeoutHandle = setTimeout(() => r(new Error("SOCKET_TIMEOUT")), 45000);
  });

  Promise.race([conn.connect(), timeoutPromise])
    .then((state) => {
      if (
        !connectionLocks.has(channel.username) &&
        !activeConnections[channel.username]
      ) {
        logWarn(
          `[GHOST SOCKET] Kênh ${channel.username} nối thành công nhưng quá hạn. Rút ống thở ngay!`,
        );
        clearTimeout(timeoutHandle);
        try {
          if (typeof conn.disconnect === "function") conn.disconnect();
        } catch (e) {}
        return;
      }
      roomId =
        state?.roomInfo?.roomId ||
        state?.roomInfo?.room_id ||
        state?.roomId ||
        null;

      if (roomId && pendingBoxes.length) {
        for (const data of pendingBoxes) emitChest(data);
        pendingBoxes.length = 0;
      }
      clearTimeout(timeoutHandle);
      activeConnections[channel.username] = conn;
      activeConnections[channel.username].lastActive = Date.now();

      proxyStrikeCount[proxy] = 0;
      logSuccess(
        `✅ [${channel.username}] Kết nối TikTok-Live-Connector [🌐 Proxy Direct] qua (${getShortProxy(proxy)})`,
      );

      setTimeout(() => {
        isProcessingInitial = false;
      }, 5000);
    })
    .catch(async (err) => {
      clearTimeout(timeoutHandle);
      let errMsg = String(err?.message || err).toLowerCase();

      logWarn(
        `[SOCKET LỖI] Kênh: ${channel.username} | Proxy: ${getShortProxy(proxy)} | Lỗi: ${errMsg}`,
      );

      if (
        errMsg.includes("not found") ||
        errMsg.includes("offline") ||
        errMsg.includes("ended")
      ) {
        safeEmitRadarResult({ channel, status: "OFFLINE", proxy });
      } else if (
        errMsg.includes("rate limit") ||
        errMsg.includes("captcha") ||
        errMsg.includes("403")
      ) {
        if (proxy !== "local") {
          proxyStrikeCount[proxy] = (proxyStrikeCount[proxy] || 0) + 1;
          if (proxyStrikeCount[proxy] >= 4) {
            if (masterSocket?.connected)
              masterSocket.emit("worker_report_dead_proxy", { proxy: proxy });
          } else {
            proxyCooldown[proxy] = Date.now() + 60000;
            safeEmitRadarResult({ channel, status: "ERROR" });
          }
        } else {
          proxyCooldown["local"] = Date.now() + 45000;
          safeEmitRadarResult({ channel, status: "ERROR" });
        }
      } else {
        safeEmitRadarResult({ channel, status: "ERROR" });
      }
      stopWebcast(channel.username);
    });
}

function stopWebcast(user) {
  // 💡 BỔ SUNG: Dọn hàng chờ cắm socket
  socketConnectionQueue = socketConnectionQueue.filter(
    (item) => item.channel.username !== user,
  );

  const realProxy = assignedProxies[user];
  if (realProxy) {
    proxyUsage[realProxy] = Math.max(0, (proxyUsage[realProxy] || 0) - 1);
    delete assignedProxies[user];

    if (zombieProxies[realProxy] && proxyUsage[realProxy] === 0) {
      logInfo(
        `👻 Proxy Zombie [${getShortProxy(realProxy)}] đã xả xong tải. Dọn dẹp RAM!`,
      );
      cleanupProxyData(realProxy);
      delete zombieProxies[realProxy];
    }
  }

  pendingChecks.delete(user);
  connectionLocks.delete(user);

  const conn = activeConnections[user];
  if (!conn) return;

  delete activeConnections[user];

  setImmediate(() => {
    try {
      if (typeof conn.removeAllListeners === "function")
        conn.removeAllListeners();
      if (typeof conn.disconnect === "function") conn.disconnect();
    } catch (e) {}
  });
}

setInterval(() => {
  const now = Date.now();
  for (let [user, timestamp] of pendingChecks.entries()) {
    if (now - timestamp > 45000) {
      pendingChecks.delete(user);
      stopWebcast(user);
      safeEmitRadarResult({ channel: { username: user }, status: "REQUEUE" });
    }
  }

  for (let [user, timestamp] of connectionLocks.entries()) {
    if (!activeConnections[user] && now - timestamp > 45000) {
      logWarn(`[LOCK TIMEOUT] Giải phóng kênh kẹt ${user}. Thu hồi Proxy!`);
      stopWebcast(user);
      safeEmitRadarResult({ channel: { username: user }, status: "REQUEUE" });
    }
  }

  for (let user in activeConnections) {
    const conn = activeConnections[user];

    if (now - (conn.lastActive || now) > 60 * 60 * 1000) {
      logWarn(
        `✂️ Cắt bỏ Socket Zombie [${user}] do 60 phút không có tín hiệu quà.`,
      );
      stopWebcast(user);
      safeEmitRadarResult({
        channel: { username: user },
        status: "OFFLINE",
        proxy: assignedProxies[user],
      });
    }
  }
}, 30000);

function balanceResources() {
  if (masterSocket && masterSocket.connected) {
    const targetProxyCount = parseInt(config.proxyCount, 10) || 0;
    const currentProxyCount = dynamicProxies.length;
    if (currentProxyCount < targetProxyCount) {
      const needed = targetProxyCount - currentProxyCount;
      logWarn(
        `🔄 Kho Proxy đang thiếu ${needed} cái (${currentProxyCount}/${targetProxyCount}). Xin Master cấp bù...`,
      );
      masterSocket.emit("worker_request_proxies", {
        count: needed,
        workerName: config.workerName,
        supportIPv6: hasIPv6Support,
      });
    } else if (currentProxyCount > targetProxyCount) {
      const excessCount = currentProxyCount - targetProxyCount;
      const excessProxies = [...dynamicProxies].slice(-Math.abs(excessCount));

      logWarn(
        `🗑️ Thừa ${excessCount} Proxy so với cấu hình. Đang trả lại Master...`,
      );

      masterSocket.emit("worker_return_proxies", excessProxies);
      excessProxies.forEach((p) => retireProxy(p));
      checkProxyHealth();
    }
  }
}
setInterval(balanceResources, 20000);

async function checkIPv6Capability() {
  try {
    logInfo("⏳ Đang kiểm tra kết nối IPv6 của VPS...");
    const res = await axios.get("https://api6.ipify.org", { timeout: 4000 });
    if (res.data) {
      hasIPv6Support = true;
      logSuccess(
        `🌐 VPS CÓ HỖ TRỢ IPV6 (IP: ${res.data}). Sẵn sàng gánh Proxy IPv6!`,
      );
    }
  } catch (e) {
    hasIPv6Support = false;
    logWarn(`🌐 VPS KHÔNG CÓ IPV6. Sẽ yêu cầu Master chỉ cấp Proxy IPv4.`);
  }
}

checkIPv6Capability().then(() => {
  connectToMaster();
});

async function checkLocalGeo() {
  try {
    const res = await axios.get("http://ip-api.com/json/?fields=countryCode", {
      timeout: 5000,
    });
    if (res.data && res.data.countryCode) {
      proxyGeoData["local"] = res.data.countryCode;
      logSuccess(`🌍 Đã xác định vị vị trí mạng Local: ${res.data.countryCode}`);
    } else {
      proxyGeoData["local"] = "US";
    }
  } catch (e) {
    proxyGeoData["local"] = "US";
  }
}
checkLocalGeo();

function handleShutdown(signal) {
  logWarn(`⚠️ Nhận lệnh ${signal}. Đang trả tài nguyên...`);
  if (masterSocket && masterSocket.connected) {
    let proxiesToReturn = [...dynamicProxies];
    if (proxiesToReturn.length > 0)
      masterSocket.emit("worker_return_proxies", proxiesToReturn);
  }
  setTimeout(() => {
    process.exit(0);
  }, 1000);
}

process.on("uncaughtException", (err) => {
  logError(`[CRASH PROTECT]: ${err.message}`);
});
process.on("unhandledRejection", (reason) => {
  logError(`[CRASH PROTECT]: ${reason}`);
});
process.on("SIGINT", handleShutdown);
process.on("SIGTERM", handleShutdown);
