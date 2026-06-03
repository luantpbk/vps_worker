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
const { log } = require("console");

const CONFIG_FILE = "vps_config.json";
const EULER_RATE = 4; // 1 key cho mỗi 4 proxy để tối ưu hóa hiệu suất
let config = {
  masterUrl: "http://localhost:3001",
  workerName: `Worker_01`,
  proxyCount: 5,
  useLocalNetwork: false,
  loadPerProxy: 10,
  localLoad: 50,
};

let currentDynamicMaxLoad = 0;

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
        // 💡 FIX 3: Nạp cấu hình proxyCount mới để Auto-Balancer nhận diện được sự thay đổi
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
let connectionLocks = new Set(); // 💡 FIX 4: Đã khai báo biến chống Ghost Load
let masterSocket = null;
let globalFailureCount = 0;
let workerPausedUntil = 0;

setInterval(() => {
  globalFailureCount = 0;
}, 120000);

let dynamicProxies = [];
let zombieProxies = {};
let exclusiveEulerKeys = [];
let keyIndex = 0;
const agentCache = {};
let localTaskQueue = [];

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

setInterval(() => {
  process.stdout.write("\x1Bc");
  console.log("==========================================");
  console.log(
    `🚀 WORKER: ${config.workerName} | MASTER: ${masterSocket?.connected ? "ONLINE 🟢" : "OFFLINE 🔴"}`,
  );
  console.log(
    `📊 TẢI HIỆN TẠI: ${Object.keys(activeConnections).length} / ${currentDynamicMaxLoad}`,
  );
  console.log(
    `⏳ ĐANG CHECK HTTP: ${pendingChecks.size} | TRONG HÀNG ĐỢI: ${localTaskQueue.length}`,
  );
  console.log(`🔑 EULER KEYS: ${exclusiveEulerKeys.length} key độc quyền`);
  console.log("------------------------------------------");
  console.log("📡 TRẠNG THÁI PROXY:");
  const tableData = (
    config.useLocalNetwork ? ["local", ...dynamicProxies] : dynamicProxies
  ).map((p) => ({
    Proxy: getShortProxy(p),
    "Đang cắm": `${proxyUsage[p] || 0}/${p === "local" ? config.localLoad : config.loadPerProxy}`,
    "Tình trạng": proxyHealth[p]?.status || "ĐANG KIỂM TRA",
  }));
  console.table(tableData);
  console.log("==========================================\n");
}, 15000);

// XỬ LÝ XẢ TẢI MỀM (NUÔI ZOMBIE)
function retireProxy(proxyStr) {
  // 💡 FIX: Tránh việc 1 proxy bị đem ra chém nhiều lần do Master báo về trùng lặp
  const isManaged =
    dynamicProxies.includes(proxyStr) || zombieProxies[proxyStr];
  if (!isManaged) return; // Nếu đã dọn dẹp sạch sẽ từ trước rồi thì bỏ qua luôn

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

// 💓 Heartbeat Session
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

  await Promise.all(
    checkList.map(async (p) => {
      try {
        if (proxyCooldown[p] && Date.now() < proxyCooldown[p]) {
          let remain = Math.ceil((proxyCooldown[p] - Date.now()) / 1000);
          currentHealth[p] = { status: `ĐANG NGHỈ (${remain}s)` };
          return;
        }

        let proxyUrlGot = p === "local" ? undefined : formatProxyUrl(p);
        const healthRes = await gotScraping({
          url: "https://clients3.google.com/generate_204",
          proxyUrl: proxyUrlGot,
          timeout: { request: 10000 },
          throwHttpErrors: false,
          retry: { limit: 0 },
          http2: true,
          headers: {
            "User-Agent": "curl/7.81.0", // Không cần giả lập Chrome nặng nề khi Ping
          },
        });

        if (healthRes.statusCode === 200 || healthRes.statusCode === 204) {
          currentHealth[p] = { status: "SẴN SÀNG" };
          proxyFailCount[p] = 0;
        } else {
          throw new Error(`Mã lỗi HTTP ${healthRes.statusCode}`);
        }
      } catch (e) {
        currentHealth[p] = { status: "MẤT KẾT NỐI" };
        if (p !== "local") {
          // 💡 Nếu Proxy này đã bị tử hình từ luồng check trước đó rồi thì bỏ qua
          if (proxyFailCount[p] < 0) return;

          proxyFailCount[p] = (proxyFailCount[p] || 0) + 1;

          // 💡 FAST-KILL: Phát hiện lỗi hết tiền/hết hạn băng thông khi Ping Google
          if (
            e.message.includes("402") ||
            e.message.includes("407") ||
            e.message.includes("Payment")
          ) {
            proxyFailCount[p] = 3; // Ép lên thẳng 3 gậy để tử hình luôn
          }

          logWarn(
            `[PING LỖI] Proxy [${getShortProxy(p)}] không kết nối được Google (Lần ${proxyFailCount[p]}/3). Lỗi: ${e.message}`,
          );

          // 💡 Sửa từ "=== 3" thành ">= 3" để chống nhảy cóc (Bug Bất Tử)
          if (proxyFailCount[p] >= 3) {
            currentHealth[p].status = "BÁO LỖI";
            if (masterSocket?.connected)
              masterSocket.emit("worker_report_dead_proxy", {
                proxy: p,
                workerName: config.workerName,
              });

            retireProxy(p); // Vứt ngay lập tức

            proxyFailCount[p] = -9999; // Cờ hiệu: Đã tử hình, cấm đếm tiếp!
          } else {
            proxyCooldown[p] = Date.now() + 20000;
          }
        } else {
          proxyCooldown["local"] = Date.now() + 20000;
        }
      }
    }),
  );

  proxyHealth = currentHealth;
  let aliveCount =
    config.useLocalNetwork && proxyHealth["local"]?.status === "SẴN SÀNG"
      ? config.localLoad
      : 0;
  for (let p of dynamicProxies) {
    if (proxyHealth[p]?.status === "SẴN SÀNG")
      aliveCount += config.loadPerProxy;
  }

  currentDynamicMaxLoad = aliveCount;
  if (masterSocket?.connected)
    masterSocket.emit("worker_update_capacity", {
      maxLoad: currentDynamicMaxLoad,
    });
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

function getNextEulerKey() {
  if (exclusiveEulerKeys.length === 0) return "";
  const key = exclusiveEulerKeys[keyIndex % exclusiveEulerKeys.length];
  keyIndex++;
  return key;
}

function formatProxyUrl(rawProxy) {
  if (!rawProxy || typeof rawProxy !== "string") return null;
  if (rawProxy.startsWith("http")) return rawProxy;
  const parts = rawProxy.split(":");
  if (parts.length === 4)
    return `http://${parts[2]}:${parts[3]}@${parts[0]}:${parts[1]}`;
  return `http://${parts[0]}:${parts[1]}`;
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
  // 💡 BỔ SUNG: Bắt lỗi kết nối để in ra màn hình
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
      runningChannels: Object.keys(activeConnections),
      pendingChannels: Array.from(pendingChecks.keys()),
      heldProxies: dynamicProxies,
      heldKeys: exclusiveEulerKeys,
    });

    const neededProxies = Math.max(
      0,
      config.proxyCount - dynamicProxies.length,
    );
    if (neededProxies > 0)
      masterSocket.emit("worker_request_proxies", {
        count: neededProxies,
        workerName: config.workerName,
      });

    const neededKeys = Math.max(
      0,
      Math.ceil(config.proxyCount / EULER_RATE) - exclusiveEulerKeys.length,
    );
    if (neededKeys > 0)
      masterSocket.emit("worker_request_keys", {
        count: neededKeys,
        workerName: config.workerName,
      });
  });

  masterSocket.on("worker_receive_keys", (keysList) => {
    if (Array.isArray(keysList))
      exclusiveEulerKeys = Array.from(
        new Set([...exclusiveEulerKeys, ...keysList]),
      );
  });

  masterSocket.on("worker_key_replacement", (data) => {
    exclusiveEulerKeys = exclusiveEulerKeys.filter((k) => k !== data.deadKey);
    if (data.newKey && !exclusiveEulerKeys.includes(data.newKey))
      exclusiveEulerKeys.push(data.newKey);
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

    retireProxy(deadProxy); // 💡 Xả tải mềm

    if (newProxy) {
      const pStr = typeof newProxy === "string" ? newProxy : newProxy.proxy;
      if (zombieProxies[pStr]) delete zombieProxies[pStr]; // Hồi sinh
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
    retireProxy(proxyStr); // 💡 Xả tải mềm
    logWarn(`🗑️ Lệnh từ Admin: Thu hồi Proxy [${getShortProxy(proxyStr)}]`);
    checkProxyHealth();
  });

  masterSocket.on("process_task", (channel) => {
    const maxAllowedQueue = Math.max(20, currentDynamicMaxLoad * 2);
    if (localTaskQueue.length >= maxAllowedQueue)
      return safeEmitRadarResult({ channel, status: "REQUEUE" });
    if (
      !localTaskQueue.some((c) => c.username === channel.username) &&
      !pendingChecks.has(channel.username) &&
      !activeConnections[channel.username]
    ) {
      localTaskQueue.push(channel);
    }
  });

  masterSocket.on("force_update_config", (newCfg) => {
    if (newCfg.useLocalNetwork !== undefined)
      config.useLocalNetwork = newCfg.useLocalNetwork;
    if (newCfg.localLoad) config.localLoad = parseInt(newCfg.localLoad);
    if (newCfg.loadPerProxy)
      config.loadPerProxy = parseInt(newCfg.loadPerProxy);
    fs.writeFileSync(CONFIG_FILE, JSON.stringify(config, null, 4));
    sendWorkerStatus();
    checkProxyHealth();
  });

  masterSocket.on("disconnect", (reason) => {
    if (reason === "io server disconnect") masterSocket.connect();
    else {
      disconnectTimer = setTimeout(() => {
        localTaskQueue = [];
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
    if (
      Object.keys(activeConnections).length >= currentDynamicMaxLoad &&
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
    if (availableCheckSlots <= 0) return;

    const tasksToProcess = Math.min(
      maxConcurrentChecks,
      availableCheckSlots,
      localTaskQueue.length,
    );
    for (let i = 0; i < tasksToProcess; i++) {
      const channel = localTaskQueue.splice(0, 1)[0];
      pendingChecks.set(channel.username, Date.now());
      setTimeout(() => {
        executeTask(channel);
      }, Math.random() * 2000);
    }
  } finally {
    isProcessingQueue = false;
  }
}, 1000);

async function checkLiveStatus(username, proxy) {
  let proxyUrlGot = proxy === "local" ? undefined : formatProxyUrl(proxy);
  const urlUsername = username.startsWith("@") ? username : `@${username}`;

  try {
    const fetchPromise = gotScraping({
      url: `https://www.tiktok.com/${urlUsername}/live`,
      proxyUrl: proxyUrlGot,
      timeout: { request: 12000 },
      throwHttpErrors: false,
      http2: true, // 💡 BẮT BUỘC PHẢI CÓ ĐỂ QUA MẶT TIKTOK
      retry: { limit: 0 },
      headerGeneratorOptions: {
        browsers: [{ name: "chrome", minVersion: 120 }],
        devices: ["desktop"],
        locales: ["vi-VN", "en-US"],
      },
    });

    // 💡 LỚP GIÁP 1: Ép timeout cứng 15s phòng trường hợp thư viện got bị treo ngầm
    let timeoutHandle;
    const hardTimeout = new Promise((_, r) => {
      timeoutHandle = setTimeout(() => r(new Error("HARD_TIMEOUT")), 15000);
    });

    const res = await Promise.race([fetchPromise, hardTimeout]);
    clearTimeout(timeoutHandle);

    // 💡 LỚP GIÁP 2: Bị TikTok chặn HTTP (Giới hạn request)
    if ([403, 429].includes(res.statusCode)) {
      logWarn(
        `[TIKTOK BLOCK] HTTP ${res.statusCode} chặn kết nối - Proxy: ${getShortProxy(proxy)}`,
      );
      return "RATE_LIMIT";
    }

    // 💡 LỚP GIÁP 3: Do Proxy hết hạn, lỗi mạng
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
      typeof res.body === "string" ? res.body : JSON.stringify(res.body || "");

    // 💡 LỚP GIÁP 4: Bị Cloudflare chặn ngầm (Bắt được lỗi 200 OK ảo)
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
    // 💡 LỚP GIÁP 5: Bắt lỗi treo máy hoặc hết tiền
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
}

async function executeTask(channel) {
  if (
    activeConnections[channel.username] ||
    connectionLocks.has(channel.username)
  ) {
    pendingChecks.delete(channel.username);
    return;
  }

  // 💡 CHẶN ĐỨNG LỖI SPAM LOCAL
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
    // Chỉ in log Info nếu kết quả là LIVE/OFFLINE/NOT_FOUND để tránh rác console
    if (status === "LIVE") {
      logInfo(`${channel.username} LIVE. Thực hiện cắm Socket`);
    }

    if (status === "NOT_FOUND" || status === "OFFLINE") {
      safeEmitRadarResult({ channel, status: status, proxy: checkProxy });
      return;
    }

    // 💡 Đã thêm FATAL_PROXY_BILLING vào danh sách xử lý
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

        // 💡 BỔ SUNG: Nếu hết tiền hoặc sai pass -> Đánh thẳng 4 gậy để phế truất tức khắc
        if (status === "FATAL_PROXY_BILLING") {
          proxyStrikeCount[checkProxy] = 4;
        } else {
          proxyStrikeCount[checkProxy] =
            (proxyStrikeCount[checkProxy] || 0) + 1;
        }

        if (proxyStrikeCount[checkProxy] === 4) {
          if (masterSocket?.connected)
            masterSocket.emit("worker_report_dead_proxy", {
              proxy: checkProxy,
            });
          retireProxy(checkProxy);
        } else if (proxyStrikeCount[checkProxy] < 4) {
          proxyCooldown[checkProxy] = Date.now() + 60000;
        }
      } else {
        proxyCooldown["local"] = Date.now() + 45000;
      }
      safeEmitRadarResult({ channel, status: "REQUEUE" });
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

      setTimeout(() => {
        startWebcast(channel, socketProxy);
      }, Math.random() * 3000);
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
  // 💡 BỌC THÉP: Chặn 2 luồng chui vào cùng 1 username
  if (
    activeConnections[channel.username] ||
    connectionLocks.has(channel.username)
  )
    return;
  connectionLocks.add(channel.username);

  const key = getNextEulerKey();
  let conn = new TikTokLiveConnection(channel.username, {
    signApiKey: key,
    webClientOptions: { httpsAgent: getCachedAgent(proxy) },
    websocketOptions: { agent: getCachedAgent(proxy) },
    processInitialData: false,
    fetchRoomInfoOnConnect: true,
    enableExtendedGiftInfo: false,
    clientParams: {
      device_platform: "web",
      browser_language: "vi-VN",
      browser_name: "Mozilla",
      browser_version: "124.0.0.0", // 💡 Chuẩn hóa version cho API
    },
  });

  let currentViewers = 0;

  const checkAndReportDeadKey = (errObj, targetKey) => {
    if (!targetKey) return false;
    let errText =
      typeof errObj === "string"
        ? errObj
        : errObj?.message || JSON.stringify(errObj);
    const msg = String(errText).toLowerCase();

    // 💡 BỔ SUNG: Các từ khóa nhận diện Key hết hạn/hết limit của EulerStream
    const isDeadKey =
      msg.includes("balance") ||
      msg.includes("quota") ||
      msg.includes("invalid api key") ||
      msg.includes("unauthorized") ||
      msg.includes("sign error") ||
      msg.includes("401") ||
      msg.includes("rate limit") ||
      msg.includes("upgrade") ||
      msg.includes("too many connections");

    if (isDeadKey) {
      logWarn(
        `🔑 [DEAD KEY] Phát hiện Key Euler [${targetKey.substring(0, 10)}...] đã kiệt sức. Yêu cầu đổi mới!`,
      );
      if (masterSocket?.connected)
        masterSocket.emit("worker_report_dead_key", {
          key: targetKey,
          workerName: config.workerName,
        });
      return true; // Trả về true để báo là lỗi do Key
    }
    return false;
  };

  // 💡 FIX 5: Xóa rác Timer của Promise Race tránh Memory Leak
  let timeoutHandle;
  const timeoutPromise = new Promise((_, r) => {
    timeoutHandle = setTimeout(() => r(new Error("SOCKET_TIMEOUT")), 45000);
  });

  Promise.race([conn.connect(), timeoutPromise])
    .then((state) => {
      clearTimeout(timeoutHandle);
      activeConnections[channel.username] = conn;
      activeConnections[channel.username].lastActive = Date.now();
      proxyStrikeCount[proxy] = 0;
      logSuccess(
        `✅ [${channel.username}] Đã kết nối WebSocket (${getShortProxy(proxy)})`,
      );
      conn.on("warn", (err) => checkAndReportDeadKey(err, key));
      conn.on("error", (err) => checkAndReportDeadKey(err, key));

      conn.on("roomUser", (userData) => {
        if (activeConnections[channel.username])
          activeConnections[channel.username].lastActive = Date.now();
        if (userData?.viewerCount) currentViewers = userData.viewerCount;
      });

      conn.on("envelope", (data) => {
        if (activeConnections[channel.username])
          activeConnections[channel.username].lastActive = Date.now();
        if (data?.envelopeInfo?.diamondCount > 0) {
          masterSocket.emit("worker_chest_raw", {
            channel,
            coins: data.envelopeInfo.diamondCount,
            boxes: data.envelopeInfo.peopleCount,
            idc: data.envelopeInfo.envelopeIdc,
            workerName: config.workerName,
            liveRegion:
              state?.roomInfo?.owner?.region || channel.country || "unknown",
            unpackAt: data.envelopeInfo.unpackAt,
            viewers: currentViewers,
            roomId: state?.roomId || state?.roomInfo?.room_id || "",
            workerTime: Date.now(),
          });
        }
      });

      conn.on("streamEnd", () => {
        stopWebcast(channel.username);
        safeEmitRadarResult({ channel, status: "OFFLINE", proxy });
      });
      conn.on("disconnected", () => {
        stopWebcast(channel.username);
        safeEmitRadarResult({ channel, status: "OFFLINE", proxy });
      });
    })
    .catch((err) => {
      clearTimeout(timeoutHandle);
      // 💡 Lấy kết quả xem có phải lỗi do Key không
      const isKeyDead = checkAndReportDeadKey(err, key);
      let errMsg = String(err?.message || err).toLowerCase();

      // 💡 FIX QUAN TRỌNG: Nếu lỗi là do Key, TRẮNG ÁN CHO PROXY.
      // Không được để code chạy xuống dưới vì chữ "rate limit" sẽ làm Proxy bị phạt oan!
      if (isKeyDead) {
        safeEmitRadarResult({ channel, status: "REQUEUE" });
        stopWebcast(channel.username);
        return;
      }

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
            safeEmitRadarResult({ channel, status: "REQUEUE" });
          }
        } else {
          proxyCooldown["local"] = Date.now() + 45000;
          safeEmitRadarResult({ channel, status: "REQUEUE" });
        }
      } else {
        safeEmitRadarResult({ channel, status: "REQUEUE" });
      }
      stopWebcast(channel.username);
    });
}

function stopWebcast(user) {
  // 1. GIẢI PHÓNG PROXY & KHÓA NGAY LẬP TỨC
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
  connectionLocks.delete(user); // 💡 Rút khóa chống đúp

  // 2. Dọn dẹp Connection
  const conn = activeConnections[user];
  if (!conn) return;
  delete activeConnections[user];

  setImmediate(() => {
    try {
      conn.removeAllListeners();
      conn.disconnect();
      if (conn.client) {
        conn.client.removeAllListeners();
        if (conn.client.ws) conn.client.ws.terminate();
      }
    } catch (e) {}
  });
}

setInterval(() => {
  const now = Date.now();
  for (let [user, timestamp] of pendingChecks.entries()) {
    if (now - timestamp > 45000) {
      pendingChecks.delete(user);
      connectionLocks.delete(user);
      safeEmitRadarResult({ channel: { username: user }, status: "REQUEUE" });
    }
  }

  for (let user in activeConnections) {
    const conn = activeConnections[user];
    if (now - (conn.lastActive || now) > 30 * 60 * 1000) {
      stopWebcast(user);
      safeEmitRadarResult({ channel: { username: user }, status: "REQUEUE" });
    }
  }
}, 30000);

// ========================================================
// 💡 AUTO-BALANCER: TỰ ĐỘNG BÙ/TRẢ PROXY & KEYS HOÀN HẢO
// ========================================================
setInterval(() => {
  if (masterSocket && masterSocket.connected) {
    const targetProxyCount = config.proxyCount || 0;
    const currentProxyCount = dynamicProxies.length;

    if (currentProxyCount < targetProxyCount) {
      const needed = targetProxyCount - currentProxyCount;
      logWarn(
        `🔄 Kho Proxy đang thiếu ${needed} cái (${currentProxyCount}/${targetProxyCount}). Xin Master cấp bù...`,
      );
      masterSocket.emit("worker_request_proxies", {
        count: needed,
        workerName: config.workerName,
      });
    } else if (currentProxyCount > targetProxyCount) {
      const excessCount = currentProxyCount - targetProxyCount;
      // 💡 FIX 2: Không được cắt trực tiếp mảng bằng splice, tạo bản sao (slice) và dùng retireProxy để xả mềm
      const excessProxies = [...dynamicProxies].slice(-excessCount);
      logWarn(
        `🗑️ Thừa ${excessCount} Proxy so với cấu hình. Đang trả lại Master...`,
      );

      masterSocket.emit("worker_return_proxies", excessProxies);
      excessProxies.forEach((p) => retireProxy(p));
      checkProxyHealth();
    }

    // 💡 FIX 1: Dùng đúng EULER_RATE (4) để cân bằng Key
    const targetKeyCount = Math.ceil(targetProxyCount / EULER_RATE);
    const currentKeyCount = exclusiveEulerKeys.length;

    if (currentKeyCount < targetKeyCount) {
      const neededKeys = targetKeyCount - currentKeyCount;
      masterSocket.emit("worker_request_keys", {
        count: neededKeys,
        workerName: config.workerName,
      });
    } else if (currentKeyCount > targetKeyCount) {
      const countToDrop = currentKeyCount - targetKeyCount;
      const excessKeys = exclusiveEulerKeys.splice(-countToDrop);
      masterSocket.emit("worker_return_keys", excessKeys);
    }
  }
}, 20000);

connectToMaster();

function handleShutdown(signal) {
  logWarn(`⚠️ Nhận lệnh ${signal}. Đang trả tài nguyên...`);
  if (masterSocket && masterSocket.connected) {
    let proxiesToReturn = [...dynamicProxies];
    if (proxiesToReturn.length > 0)
      masterSocket.emit("worker_return_proxies", proxiesToReturn);
    if (exclusiveEulerKeys.length > 0)
      masterSocket.emit("worker_return_keys", exclusiveEulerKeys);
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
