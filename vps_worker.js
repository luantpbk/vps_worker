// vps_worker.js
process.env.TZ = "Asia/Ho_Chi_Minh";
require("dotenv").config();
const { io: ClientIO } = require("socket.io-client");
const customParser = require("socket.io-msgpack-parser");
const { WebcastPushConnection } = require("tiktok-live-connector");
const HttpsProxyAgent = require("https-proxy-agent");
const axios = require("axios");
const fs = require("fs");
const crypto = require("crypto");

const CONFIG_FILE = "vps_config.json";

// Cấu hình mặc định
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
    } catch (e) {
      console.error("Lỗi đọc file cấu hình, dùng mặc định.");
    }
  } else {
    fs.writeFileSync(CONFIG_FILE, JSON.stringify(config, null, 4));
  }
}
loadConfig();

// State quản lý
let hubConfig = { userAgents: [] };
let activeConnections = {};
let assignedProxies = {};
let proxyUsage = {};
let proxyFailCount = {};
let proxyCooldown = {};
let proxyHealth = {};
let pendingChecks = new Map(); // 💡 FIX: Dùng Map lưu Timestamp để chống kẹt vĩnh viễn
let masterSocket = null;

let dynamicProxies = [];
let exclusiveEulerKeys = [];

let proxyIndex = 0,
  uaIndex = 0,
  keyIndex = 0;
const agentCache = {};
let proxyGeoData = {};

let localTaskQueue = [];

// 💡 TỪ ĐIỂN TỰ ĐỘNG MAP QUỐC GIA SANG NGÔN NGỮ
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

// ==========================================
// HỆ THỐNG LOG & GIAO DIỆN CONSOLE
// ==========================================
const ENABLE_DEBUG = process.env.DEBUG === "true";

// 💡 FIX: Hàm bắn Log lên Master Dashboard
function sendMasterLog(msg) {
  if (masterSocket && masterSocket.connected) {
    masterSocket.emit("worker_log", `[${config.workerName}] ${msg}`);
  }
}

function logInfo(msg) {
  if (ENABLE_DEBUG) console.log(`[ℹ️] ${msg}`);
}
function logSuccess(msg) {
  console.log(`[✅] ${msg}`);
  sendMasterLog(`✅ ${msg}`);
}
function logWarn(msg) {
  console.warn(`[⚠️] ${msg}`);
  sendMasterLog(`⚠️ ${msg}`);
}
function logError(msg) {
  console.error(`[❌] ${msg}`);
  sendMasterLog(`❌ ${msg}`);
}

// Bảng điều khiển Console Realtime
setInterval(() => {
  console.clear();
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
    Proxy: p === "local" ? "Mạng VPS (Local)" : p.split("@").pop() || p,
    "Đang cắm": `${proxyUsage[p] || 0}/${p === "local" ? config.localLoad || config.loadPerProxy : config.loadPerProxy}`,
    "Tình trạng": proxyHealth[p]?.status || "ĐANG KIỂM TRA",
    IP: proxyHealth[p]?.ip || "N/A",
  }));
  console.table(tableData);
  console.log("==========================================\n");
}, 15000);

// ==========================================
// ĐỒNG BỘ VÀ KIỂM TRA SỨC KHỎE PROXY
// ==========================================
function sendWorkerStatus() {
  if (masterSocket && masterSocket.connected) {
    const queuedUsernames = localTaskQueue.map((c) => c.username);
    const allPending = [
      ...Array.from(pendingChecks.keys()),
      ...queuedUsernames,
    ];

    masterSocket.emit("worker_status", {
      currentLoad: Object.keys(activeConnections).length + allPending.length,
      runningChannels: Object.keys(activeConnections),
      pendingChannels: allPending,
    });
  }
}
setInterval(sendWorkerStatus, 30000);

async function checkProxyHealth() {
  let checkList = [...dynamicProxies];
  if (config.useLocalNetwork) checkList.unshift("local");
  let currentHealth = {};

  const chunkSize = 5;
  for (let i = 0; i < checkList.length; i += chunkSize) {
    const chunk = checkList.slice(i, i + chunkSize);
    await Promise.all(
      chunk.map(async (p) => {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 5000);

        try {
          let options = { signal: controller.signal };

          if (p !== "local") {
            const proxyAgent = getCachedAgent(p);
            options.httpAgent = proxyAgent;
            options.httpsAgent = proxyAgent;
          }

          if (!proxyGeoData[p]) {
            const geoRes = await axios.get(
              "http://ip-api.com/json/?fields=countryCode",
              options,
            );
            if (geoRes.data && geoRes.data.countryCode)
              proxyGeoData[p] = geoRes.data.countryCode;
            else proxyGeoData[p] = "VN";
          }

          const healthRes = await axios.get(
            "https://clients3.google.com/generate_204",
            options,
          );
          clearTimeout(timeoutId);

          if (healthRes.status === 204) {
            if (proxyCooldown[p] && Date.now() < proxyCooldown[p]) {
              currentHealth[p] = {
                status: `NGHỈ LẦN ${proxyFailCount[p] || 1}`,
                ip: "Đã ẩn để tiết kiệm",
              };
            } else {
              currentHealth[p] = {
                status: "SẴN SÀNG",
                ip: "Đã ẩn để tiết kiệm",
              };
            }
          } else {
            throw new Error("Lỗi Ping");
          }
        } catch (e) {
          clearTimeout(timeoutId);
          currentHealth[p] = { status: "MẤT KẾT NỐI", ip: "N/A" };

          if (p !== "local") {
            proxyFailCount[p] = (proxyFailCount[p] || 0) + 1;
            if (proxyFailCount[p] >= 3) {
              logError(
                `🚫 Proxy ${p.split("@").pop()} đứt Ping 3 lần. Xin Master đổi mới.`,
              );
              currentHealth[p].status = "BÁO LỖI";
              if (masterSocket && masterSocket.connected) {
                masterSocket.emit("worker_report_dead_proxy", {
                  proxy: p,
                  workerName: config.workerName,
                });
              }
              dynamicProxies = dynamicProxies.filter((dp) => dp !== p);
              delete proxyHealth[p];
              delete proxyUsage[p];
              delete proxyFailCount[p];
              delete proxyCooldown[p];
            } else {
              proxyCooldown[p] = Date.now() + 60000;
            }
          }
        }
      }),
    );
  }

  proxyHealth = currentHealth;

  let aliveProxiesCount = 0;
  let isLocalAlive = false;
  for (let p in proxyHealth) {
    if (proxyHealth[p].status === "SẴN SÀNG") {
      if (p === "local") isLocalAlive = true;
      else aliveProxiesCount++;
    }
  }
  currentDynamicMaxLoad =
    aliveProxiesCount * config.loadPerProxy +
    (isLocalAlive ? config.localLoad || config.loadPerProxy : 0);
  if (masterSocket && masterSocket.connected) {
    masterSocket.emit("worker_update_capacity", {
      maxLoad: currentDynamicMaxLoad,
    });
  }
}
setInterval(checkProxyHealth, 45000);
setTimeout(checkProxyHealth, 2000);

// ==========================================
// HELPER FUNCTIONS (Tài nguyên)
// ==========================================
function getNextAvailableProxy() {
  let allProxies = [...dynamicProxies];
  const now = Date.now();
  if (config.useLocalNetwork) allProxies.unshift("local");
  if (allProxies.length === 0) return null;

  for (let i = 0; i < allProxies.length; i++) {
    let p = allProxies[proxyIndex];
    proxyIndex = (proxyIndex + 1) % allProxies.length;

    const isCoolingDown = proxyCooldown[p] && now < proxyCooldown[p];
    if (isCoolingDown) continue;

    if (proxyCooldown[p] && now >= proxyCooldown[p]) {
      delete proxyCooldown[p];
      if (proxyHealth[p]) proxyHealth[p].status = "SẴN SÀNG";
      proxyUsage[p] = 0;
      logInfo(`[Proxy] 🟢 ${p.split("@").pop()} hết thời gian phạt nghỉ!`);
      checkProxyHealth();
    }
    const limitForThisNetwork =
      p === "local"
        ? config.localLoad || config.loadPerProxy
        : config.loadPerProxy;
    if (
      proxyHealth[p]?.status === "SẴN SÀNG" &&
      (proxyUsage[p] || 0) < limitForThisNetwork
    ) {
      return p;
    }
  }
  return null;
}

function getNextUA() {
  if (!hubConfig.userAgents || hubConfig.userAgents.length === 0)
    return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36";
  const ua = hubConfig.userAgents[uaIndex];
  uaIndex = (uaIndex + 1) % hubConfig.userAgents.length;
  return ua;
}

function getNextEulerKey() {
  if (exclusiveEulerKeys.length === 0) {
    logWarn(
      "⚠️ Chưa có Euler Key độc quyền! Đang cắm Socket rủi ro không Token...",
    );
    return "";
  }
  const key = exclusiveEulerKeys[keyIndex % exclusiveEulerKeys.length];
  keyIndex++;
  return key;
}

function getCachedAgent(proxyUrl) {
  if (!proxyUrl.startsWith("http")) {
    const parts = proxyUrl.split(":");
    if (parts.length === 4)
      proxyUrl = `http://${parts[2]}:${parts[3]}@${parts[0]}:${parts[1]}`;
    else if (parts.length === 2) proxyUrl = `http://${parts[0]}:${parts[1]}`;
    else proxyUrl = `http://${proxyUrl}`;
  }
  if (!agentCache[proxyUrl]) {
    agentCache[proxyUrl] = new HttpsProxyAgent(proxyUrl, {
      keepAlive: true,
      ciphers:
        "TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256:TLS_AES_128_GCM_SHA256:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES256-GCM-SHA384",
      secureProtocol: "TLS_client_method",
      secureOptions: crypto.constants.SSL_OP_LEGACY_SERVER_CONNECT,
    });
  }
  return agentCache[proxyUrl];
}

// ==========================================
// KẾT NỐI TỔNG BỘ (MASTER)
// ==========================================
let disconnectTimer = null;

function connectToMaster() {
  if (masterSocket) masterSocket.disconnect();
  masterSocket = ClientIO(config.masterUrl, {
    auth: { token: process.env.SOCKET_SECRET },
    reconnection: true,
    reconnectionAttempts: Infinity,
    reconnectionDelay: 1000,
    reconnectionDelayMax: 5000,
    randomizationFactor: 0.5,
    parser: customParser,
    transports: ["websocket"],
  });

  masterSocket.on("connect", () => {
    logSuccess("Đã kết nối tới Master Hub!");
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
      (config.proxyCount || 0) - dynamicProxies.length,
    );
    if (neededProxies > 0) {
      masterSocket.emit("worker_request_proxies", {
        count: neededProxies,
        workerName: config.workerName,
      });
    }

    const targetKeyCount = Math.ceil(config.proxyCount / 2);
    const neededKeys = Math.max(0, targetKeyCount - exclusiveEulerKeys.length);
    if (neededKeys > 0) {
      logInfo(`🔑 Đang thiếu ${neededKeys} Euler Key, tiến hành xin Master...`);
      masterSocket.emit("worker_request_keys", {
        count: neededKeys,
        workerName: config.workerName,
      });
    }
  });

  masterSocket.on("worker_receive_keys", (keysList) => {
    if (keysList.length > 0) {
      exclusiveEulerKeys = Array.from(
        new Set([...exclusiveEulerKeys, ...keysList]),
      );
      logSuccess(`🔑 Đã nhận ${keysList.length} Euler Keys độc quyền.`);
    }
  });

  masterSocket.on("worker_key_replacement", (data) => {
    const { deadKey, newKey } = data;
    exclusiveEulerKeys = exclusiveEulerKeys.filter((k) => k !== deadKey);
    if (newKey && !exclusiveEulerKeys.includes(newKey))
      exclusiveEulerKeys.push(newKey);
    logWarn(
      `🔄 Đã đổi Key Euler: [${newKey ? newKey.substring(0, 6) + "..." : "HẾT KEY DỰ TRỮ"}]`,
    );
  });

  masterSocket.on("worker_receive_proxies", (proxiesList) => {
    if (proxiesList.length > 0) {
      logSuccess(`📡 Đã nhận thêm ${proxiesList.length} proxies từ Master.`);
      dynamicProxies = Array.from(new Set([...dynamicProxies, ...proxiesList]));
      dynamicProxies.forEach((p) => {
        if (proxyUsage[p] === undefined) proxyUsage[p] = 0;
      });
      checkProxyHealth();
    }
  });

  masterSocket.on("worker_proxy_replacement", (data) => {
    const { deadProxy, newProxy } = data;
    dynamicProxies = dynamicProxies.filter((p) => p !== deadProxy);
    delete proxyHealth[deadProxy];
    delete proxyUsage[deadProxy];
    delete proxyGeoData[deadProxy];
    delete proxyFailCount[deadProxy];
    delete proxyCooldown[deadProxy];

    if (newProxy) {
      if (!dynamicProxies.includes(newProxy)) {
        dynamicProxies.push(newProxy);
        proxyUsage[newProxy] = 0;
        logWarn(`🔄 Đã nhận Proxy mới từ Master bù đắp cho proxy chết.`);
      }
    }
    checkProxyHealth();
  });

  masterSocket.on("sync_vulkan", (data) => {
    hubConfig.userAgents = data.userAgents || [];
  });

  masterSocket.on("process_task", (channel) => {
    const isAlreadyQueued = localTaskQueue.some(
      (c) => c.username === channel.username,
    );
    const isAlreadyPending = pendingChecks.has(channel.username);
    const isAlreadyLive = !!activeConnections[channel.username];

    if (!isAlreadyQueued && !isAlreadyPending && !isAlreadyLive) {
      localTaskQueue.push(channel);
    }
  });

  masterSocket.on("force_update_config", (newCfg) => {
    if (newCfg.useLocalNetwork !== undefined)
      config.useLocalNetwork = newCfg.useLocalNetwork;
    if (newCfg.localLoad) config.localLoad = parseInt(newCfg.localLoad);
    if (newCfg.loadPerProxy)
      config.loadPerProxy = parseInt(newCfg.loadPerProxy);
    logWarn(
      `⚙️ Master ép cấu hình: LocalLoad=${config.localLoad}, PerProxy=${config.loadPerProxy}`,
    );
    fs.writeFileSync(CONFIG_FILE, JSON.stringify(config, null, 4));
    sendWorkerStatus();
  });

  masterSocket.on("disconnect", (reason) => {
    if (reason === "io server disconnect") {
      logWarn("🔄 Sếp đổi ca trực! Đang kết nối lại...");
      masterSocket.connect();
    } else {
      logWarn("Mất kết nối Master! Chờ 10 phút trước khi xả tải...");
      disconnectTimer = setTimeout(() => {
        logError("Quá 10 phút không có kết nối! Tiến hành rút toàn bộ Socket.");
        localTaskQueue = [];
        for (let username in activeConnections) stopWebcast(username);
        activeConnections = {};
        assignedProxies = {};
        pendingChecks.clear();
        disconnectTimer = null;
      }, 600000);
    }
  });

  masterSocket.on("cmd_stop_all", () => {
    logWarn("Nhận lệnh Tạm Dừng từ Master! Đang rút toàn bộ Socket...");
    for (let username in activeConnections) stopWebcast(username);
  });
}

// ==========================================
// 💡 HÀNG ĐỢI & DYNAMIC RATE LIMITER
// ==========================================
setInterval(async () => {
  if (localTaskQueue.length === 0) return;

  let aliveIPs = 0;
  for (let p in proxyHealth) {
    if (proxyHealth[p].status === "SẴN SÀNG") aliveIPs++;
  }

  const totalProcessing =
    Object.keys(activeConnections).length + pendingChecks.size;
  const availableSlots = currentDynamicMaxLoad - totalProcessing;

  if (availableSlots <= 0) return;

  // 💡 FIX BỘ LỌC TẢI: Tính toán toán học chặt chẽ để không kéo dư kênh
  const speed = Math.max(1, aliveIPs * 4); // Mỗi Proxy test 4 kênh / giây
  const tasksToProcess = Math.min(speed, availableSlots, localTaskQueue.length);

  for (let i = 0; i < tasksToProcess; i++) {
    const channel = localTaskQueue.shift();
    executeTask(channel);
  }
}, 1000);

const BASE_HEADERS = {
  Accept: "text/html,application/xhtml+xml",
  "Accept-Language": "en-US,en;q=0.5",
};

async function executeTask(channel) {
  if (
    activeConnections[channel.username] ||
    pendingChecks.has(channel.username)
  )
    return;

  const totalProcessing =
    Object.keys(activeConnections).length + pendingChecks.size;
  if (totalProcessing >= currentDynamicMaxLoad) {
    return masterSocket.emit("radar_result", { channel, status: "REQUEUE" });
  }

  // 💡 FIX: Sử dụng Date.now() để Map lưu được thời gian bắt đầu
  pendingChecks.set(channel.username, Date.now());

  const proxy = getNextAvailableProxy();
  if (!proxy) {
    pendingChecks.delete(channel.username);
    return masterSocket.emit("radar_result", { channel, status: "REQUEUE" });
  }

  proxyUsage[proxy] = (proxyUsage[proxy] || 0) + 1;
  assignedProxies[channel.username] = proxy;

  const ua = getNextUA();
  let options = {
    headers: { ...BASE_HEADERS, "User-Agent": ua },
    timeout: 10000,
    validateStatus: () => true,
  };
  if (proxy !== "local") options.httpsAgent = getCachedAgent(proxy);

  try {
    await new Promise((resolve) => setTimeout(resolve, Math.random() * 300));

    const res = await axios.get(
      `https://www.tiktok.com/@${channel.username}/live`,
      options,
    );

    if (res.status === 407 || res.status === 502 || res.status === 503)
      throw new Error("PROXY_DEAD");

    if (res.status === 404) {
      masterSocket.emit("radar_result", { channel, status: "NOT_FOUND" });
      pendingChecks.delete(channel.username);
      stopWebcast(channel.username);
      return;
    }

    let status = "OFFLINE";

    if (res.status === 403 || res.status === 429 || res.status >= 500) {
      status = "BLIND_TEST";
    } else if (res.status === 200) {
      const html = res.data;
      if (
        html.includes("<title>Verification</title>") ||
        html.includes("Please confirm you are human")
      ) {
        status = "BLIND_TEST";
      } else {
        const finalUrl =
          res.request?.res?.responseUrl || res.request?.responseURL || "";
        const isLoginRedirect = finalUrl.includes("/login");
        const isAgeRestricted =
          html.includes("age_restricted") ||
          html.toLowerCase().includes("verify your age") ||
          html.toLowerCase().includes("xác nhận tuổi") ||
          html.toLowerCase().includes("log in to verify");

        if (
          html.includes('"status":2') ||
          html.includes('"roomStatus":2') ||
          html.includes('"is_live":true') ||
          html.includes('"isLive":true')
        ) {
          status = "LIVE";
        } else if (isLoginRedirect || isAgeRestricted) {
          status = "BLIND_TEST";
        }
      }
    }

    if (status === "LIVE" || status === "BLIND_TEST") {
      if (status === "LIVE") {
        masterSocket.emit("radar_result", { channel, status: "LIVE" });
      }

      delete proxyFailCount[proxy];
      delete proxyCooldown[proxy];
      if (proxyHealth[proxy]) proxyHealth[proxy].status = "SẴN SÀNG";

      startWebcast(channel, proxy, ua, status === "BLIND_TEST");
    } else {
      masterSocket.emit("radar_result", { channel, status: "OFFLINE" });
      pendingChecks.delete(channel.username);
      stopWebcast(channel.username);
    }
  } catch (e) {
    if (proxy !== "local") {
      const isNetworkError =
        e.message === "PROXY_DEAD" ||
        e.code === "ECONNREFUSED" ||
        e.code === "ETIMEDOUT" ||
        e.message.includes("timeout") ||
        e.message.includes("socket");

      if (isNetworkError) {
        proxyFailCount[proxy] = (proxyFailCount[proxy] || 0) + 1;
        if (proxyFailCount[proxy] >= 3) {
          logError(
            `🚫 Proxy ${proxy.split("@").pop()} đứt mạng 3 lần. Xin Master đổi mới...`,
          );
          if (proxyHealth[proxy]) proxyHealth[proxy].status = "BÁO LỖI";
          if (masterSocket && masterSocket.connected) {
            masterSocket.emit("worker_report_dead_proxy", {
              proxy: proxy,
              workerName: config.workerName,
            });
          }
          dynamicProxies = dynamicProxies.filter((dp) => dp !== proxy);
          delete proxyHealth[proxy];
          delete proxyUsage[proxy];
          delete proxyFailCount[proxy];
          delete proxyCooldown[proxy];
        } else {
          proxyCooldown[proxy] = Date.now() + 60000;
          if (proxyHealth[proxy])
            proxyHealth[proxy].status =
              `MẤT KẾT NỐI (${proxyFailCount[proxy]}/3)`;
        }
      } else {
        proxyCooldown[proxy] = Date.now() + 180000;
        if (proxyHealth[proxy])
          proxyHealth[proxy].status = "TikTok chặn tạm thời";
      }

      let aliveCount = 0;
      let localAlive = proxyHealth["local"]?.status === "SẴN SÀNG";
      for (let p in proxyHealth) {
        if (p !== "local" && proxyHealth[p].status === "SẴN SÀNG") aliveCount++;
      }
      currentDynamicMaxLoad =
        aliveCount * config.loadPerProxy + (localAlive ? config.localLoad : 0);
      if (masterSocket && masterSocket.connected) {
        masterSocket.emit("worker_update_capacity", {
          maxLoad: currentDynamicMaxLoad,
        });
      }
    }

    pendingChecks.delete(channel.username);
    masterSocket.emit("radar_result", { channel, status: "REQUEUE" });
    stopWebcast(channel.username);
  }
}

function startWebcast(channel, proxy, ua, isBlindTest = false) {
  const key = getNextEulerKey();
  let reqOptions = { headers: { "User-Agent": ua } };
  let wsOptions = {
    headers: {
      "User-Agent": ua,
      Origin: "https://www.tiktok.com",
      Referer: `https://www.tiktok.com/@${channel.username}/live`,
    },
  };

  if (proxy !== "local") {
    const agent = getCachedAgent(proxy);
    reqOptions.httpsAgent = agent;
    wsOptions.agent = agent;
  }

  const currentCountry = proxyGeoData[proxy] || "VN";
  const geo = getGeoParams(currentCountry);

  let conn = new WebcastPushConnection(channel.username, {
    signApiKey: key,
    requestOptions: reqOptions,
    websocketOptions: wsOptions,
    clientParams: {
      app_language: geo.lang,
      webcast_language: geo.lang,
      region: geo.region,
      sys_region: geo.region,
    },
  });

  let currentViewers = 0;

  const checkAndReportDeadKey = (errText, targetKey) => {
    if (!targetKey) return;
    const msg = String(errText).toLowerCase();
    if (
      msg.includes("limit") ||
      msg.includes("quota") ||
      msg.includes("api key") ||
      msg.includes("euler") ||
      msg.includes("balance")
    ) {
      logError(
        `🔑 Key Euler [${targetKey.substring(0, 8)}...] đã kiệt sức! Báo cáo Master đổi mới...`,
      );
      if (masterSocket && masterSocket.connected) {
        masterSocket.emit("worker_report_dead_key", {
          key: targetKey,
          workerName: config.workerName,
        });
      }
    }
  };

  conn
    .connect()
    .then((state) => {
      activeConnections[channel.username] = conn;
      pendingChecks.delete(channel.username);

      if (isBlindTest) {
        masterSocket.emit("radar_result", { channel, status: "LIVE" });
        logSuccess(
          `🔞 Đâm mù thành công kênh bị chặn web [${channel.username}]. Đã cắm Socket!`,
        );
      }

      conn.on("warn", (err) => {
        checkAndReportDeadKey(err, key);
      });
      conn.on("error", (err) => {
        checkAndReportDeadKey(err, key);
      });

      conn.on("roomUser", (userData) => {
        if (userData?.viewerCount) currentViewers = userData.viewerCount;
      });

      conn.on("envelope", (data) => {
        if (data?.envelopeInfo?.diamondCount > 0) {
          const liveRegion =
            state?.roomInfo?.owner?.region || channel.country || "unknown";
          const currentRoomId = state?.roomId || state?.roomInfo?.room_id || "";
          masterSocket.emit("worker_chest_raw", {
            channel,
            coins: data.envelopeInfo.diamondCount,
            boxes: data.envelopeInfo.peopleCount,
            idc: data.envelopeInfo.envelopeIdc,
            workerName: config.workerName,
            liveRegion: liveRegion,
            unpackAt: data.envelopeInfo.unpackAt,
            viewers: currentViewers,
            roomId: currentRoomId,
            workerTime: Date.now(),
          });
        }
      });

      conn.on("streamEnd", () => {
        masterSocket.emit("radar_result", { channel, status: "OFFLINE" });
        stopWebcast(channel.username);
      });

      conn.on("disconnected", () => {
        masterSocket.emit("radar_result", { channel, status: "OFFLINE" });
        stopWebcast(channel.username);
      });
    })
    .catch((err) => {
      checkAndReportDeadKey(err, key);
      pendingChecks.delete(channel.username);

      const errMsg = String(err).toLowerCase();
      let realStatus = "REQUEUE";

      if (
        errMsg.includes("not found") ||
        errMsg.includes("offline") ||
        errMsg.includes("ended") ||
        errMsg.includes("room_id")
      ) {
        realStatus = "OFFLINE";
      } else if (errMsg.includes("suspended") || errMsg.includes("banned")) {
        realStatus = "ERROR";
      }

      // 💡 Bắn Log lỗi thẳng lên bảng điều khiển Master để phân tích
      sendMasterLog(
        `[SOCKET ĐỨT] @${channel.username} | IP: ${proxy === "local" ? "VPS" : proxy.split("@").pop()} | Lỗi: ${err.message}`,
      );

      masterSocket.emit("radar_result", { channel, status: realStatus });
      stopWebcast(channel.username);
    });
}

function stopWebcast(user) {
  if (activeConnections[user]) {
    activeConnections[user].removeAllListeners();
    activeConnections[user].disconnect();
    delete activeConnections[user];
  }
  if (assignedProxies[user]) {
    let realProxy = assignedProxies[user];
    if (proxyUsage[realProxy] !== undefined)
      proxyUsage[realProxy] = Math.max(0, proxyUsage[realProxy] - 1);
    delete assignedProxies[user];
  }
}

let configReloadTimer = null;

fs.watchFile(CONFIG_FILE, (curr, prev) => {
  if (curr.mtime <= prev.mtime) return;

  clearTimeout(configReloadTimer);
  configReloadTimer = setTimeout(async () => {
    logWarn("📝 Phát hiện cấu hình thay đổi! Đang đồng bộ...");
    try {
      const rawData = fs.readFileSync(CONFIG_FILE, "utf8");
      if (!rawData || rawData.trim() === "") return;

      const fileData = JSON.parse(rawData);
      config.localLoad = fileData.localLoad || 50;
      config.loadPerProxy = fileData.loadPerProxy || 10;
      config.useLocalNetwork = fileData.useLocalNetwork || false;

      const newProxyCount =
        fileData.proxyCount !== undefined
          ? fileData.proxyCount
          : config.proxyCount;

      if (newProxyCount > config.proxyCount) {
        const neededProxies = newProxyCount - config.proxyCount;
        const targetKeyCount = Math.ceil(newProxyCount / 2);
        const neededKeys = Math.max(
          0,
          targetKeyCount - exclusiveEulerKeys.length,
        );

        if (masterSocket && masterSocket.connected) {
          masterSocket.emit("worker_request_proxies", {
            count: neededProxies,
            workerName: config.workerName,
          });
          if (neededKeys > 0)
            masterSocket.emit("worker_request_keys", {
              count: neededKeys,
              workerName: config.workerName,
            });
        }
        config.proxyCount = newProxyCount;
      } else if (newProxyCount < config.proxyCount) {
        const excessCount = config.proxyCount - newProxyCount;
        let sortedProxies = [...dynamicProxies].sort(
          (a, b) => (proxyUsage[a] || 0) - (proxyUsage[b] || 0),
        );
        let proxiesToReturn = sortedProxies.slice(0, excessCount);

        for (let user in assignedProxies) {
          if (proxiesToReturn.includes(assignedProxies[user])) {
            stopWebcast(user);
            if (masterSocket && masterSocket.connected) {
              masterSocket.emit("radar_result", {
                channel: { username: user },
                status: "REQUEUE",
              });
            }
          }
        }

        dynamicProxies = dynamicProxies.filter(
          (p) => !proxiesToReturn.includes(p),
        );
        proxiesToReturn.forEach((p) => {
          delete proxyHealth[p];
          delete proxyUsage[p];
          delete proxyGeoData[p];
          delete proxyFailCount[p];
          delete proxyCooldown[p];
        });

        const targetKeyCount = Math.ceil(newProxyCount / 2);
        const excessKeyCount = exclusiveEulerKeys.length - targetKeyCount;
        let keysToReturn = [];

        if (excessKeyCount > 0) {
          keysToReturn = exclusiveEulerKeys.slice(0, excessKeyCount);
          exclusiveEulerKeys = exclusiveEulerKeys.slice(excessKeyCount);
        }

        if (masterSocket && masterSocket.connected) {
          masterSocket.emit("worker_return_proxies", proxiesToReturn);
          if (keysToReturn.length > 0)
            masterSocket.emit("worker_return_keys", keysToReturn);
          logSuccess(`✅ Đã hoàn trả tài nguyên dư thừa thành công!`);
        }

        config.proxyCount = newProxyCount;
      }

      await checkProxyHealth();
    } catch (e) {
      logError(`❌ Lỗi JSON Config: ${e.message}`);
    }
  }, 500);
});

logInfo("Đang khởi động Headless Worker...");

// ==========================================
// CƠ CHẾ DỌN DẸP BÓNG MA (ZOMBIE KILLER)
// ==========================================
setInterval(() => {
  const now = Date.now();

  // 1. Dọn dẹp các Socket im lặng quá lâu
  for (let user in activeConnections) {
    const conn = activeConnections[user];
    const lastActivity =
      conn.wsClient?.lastActivity || conn.wsClient?.connectionTime || now;
    if (now - lastActivity > 180000) {
      // 3 phút
      stopWebcast(user);
      masterSocket.emit("radar_result", {
        channel: { username: user },
        status: "REQUEUE",
      });
      sendMasterLog(
        `[DỌN RÁC] 🧹 Socket @${user} chết lâm sàng. Trả kênh về Hàng Đợi.`,
      );
    }
  }

  // 2. 💡 FIX: Dọn dẹp các truy vấn HTTP / Socket Handshake kẹt (Không Resolve cũng không Reject)
  for (let [user, timestamp] of pendingChecks.entries()) {
    if (now - timestamp > 45000) {
      // 45 giây
      pendingChecks.delete(user);
      stopWebcast(user);
      masterSocket.emit("radar_result", {
        channel: { username: user },
        status: "REQUEUE",
      });
      sendMasterLog(
        `[DỌN RÁC] 🧹 Truy vấn @${user} kẹt mạng 45s. Đã reset Slot Proxy!`,
      );
    }
  }
}, 60000);

connectToMaster();
