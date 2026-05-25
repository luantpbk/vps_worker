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
      logError("Lỗi đọc file cấu hình, dùng mặc định.");
    }
  } else {
    fs.writeFileSync(CONFIG_FILE, JSON.stringify(config, null, 4));
  }
}
loadConfig();

// State quản lý
let hubConfig = { userAgents: [] }; // Đã dời eulerKeys sang quản lý độc quyền
let activeConnections = {};
let assignedProxies = {};
let proxyUsage = {};
let proxyFailCount = {};
let proxyCooldown = {};
let proxyHealth = {};
let pendingChecks = new Set();
let masterSocket = null;

let dynamicProxies = [];
let exclusiveEulerKeys = []; // 🔑 Kho Euler Key Độc quyền của Worker này

let proxyIndex = 0,
  uaIndex = 0,
  keyIndex = 0;
const agentCache = {};
let proxyGeoData = {};

let localTaskQueue = []; // 💡 HÀNG ĐỢI NỘI BỘ (Rate Limiter)

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
    `⏳ ĐANG CHECK LIVE: ${pendingChecks.size} | TRONG HÀNG ĐỢI: ${localTaskQueue.length}`,
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
    masterSocket.emit("worker_status", {
      currentLoad: Object.keys(activeConnections).length,
      runningChannels: Object.keys(activeConnections),
      pendingChannels: Array.from(pendingChecks),
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
                `🚫 Proxy ${p.split("@").pop()} đứt mạng Ping 3 lần. Báo Master đổi mới...`,
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
      logInfo(
        `[Proxy] 🟢 ${p.split("@").pop()} đã nghỉ mệt xong, test lại lần ${proxyFailCount[p] + 1}!`,
      );
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

// 🔑 LẤY EULER KEY TỪ KHO ĐỘC QUYỀN
function getNextEulerKey() {
  if (exclusiveEulerKeys.length === 0) {
    logWarn(
      "⚠️ Chưa nhận được hoặc đã cạn Euler Key độc quyền! Đang cắm Socket rủi ro...",
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
    logSuccess("Đã kết nối tới Master!");
    if (disconnectTimer) {
      clearTimeout(disconnectTimer);
      disconnectTimer = null;
      logInfo("Giữ nguyên các Socket đang cắm sau khi kết nối lại.");
    }

    // Khai báo Proxy và Keys đang giữ
    masterSocket.emit("worker_ready", {
      name: config.workerName,
      type: "vps_proxy",
      maxLoad: currentDynamicMaxLoad,
      localLoad: config.localLoad,
      loadPerProxy: config.loadPerProxy,
      runningChannels: Object.keys(activeConnections),
      pendingChannels: Array.from(pendingChecks),
      heldProxies: dynamicProxies,
      heldKeys: exclusiveEulerKeys,
    });

    // Yêu cầu bù Proxy
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

    // Yêu cầu bù Euler Key (Mỗi Worker giữ 2 key để xài dần)
    const neededKeys = Math.max(0, 2 - exclusiveEulerKeys.length);
    if (neededKeys > 0) {
      masterSocket.emit("worker_request_keys", {
        count: neededKeys,
        workerName: config.workerName,
      });
    }
  });

  // 🔑 LẮNG NGHE NHẬN KEY VÀ ĐỔI KEY
  masterSocket.on("worker_receive_keys", (keysList) => {
    if (keysList.length > 0) {
      exclusiveEulerKeys = Array.from(
        new Set([...exclusiveEulerKeys, ...keysList]),
      );
      logSuccess(`🔑 Nhận cấp phát ${keysList.length} Euler Keys độc quyền.`);
    }
  });

  masterSocket.on("worker_key_replacement", (data) => {
    const { deadKey, newKey } = data;
    exclusiveEulerKeys = exclusiveEulerKeys.filter((k) => k !== deadKey);
    if (newKey && !exclusiveEulerKeys.includes(newKey))
      exclusiveEulerKeys.push(newKey);
    logWarn(
      `🔄 Đã đổi Key Euler lỗi: Thay bằng [${newKey ? newKey.substring(0, 6) + "..." : "TRỐNG"}]`,
    );
  });

  // 🌐 LẮNG NGHE NHẬN PROXY VÀ ĐỔI PROXY
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
        logWarn(
          `🔄 Đã nhận Proxy bù đắp từ Master: ${newProxy.split("@").pop()}`,
        );
      }
    } else {
      logError(
        `⚠️ Kho Master cạn kiệt, không có Proxy thay thế cho ${deadProxy.split("@").pop()}`,
      );
    }
    checkProxyHealth();
  });

  masterSocket.on("sync_vulkan", (data) => {
    hubConfig.userAgents = data.userAgents || [];
  });

  // 💡 ĐẨY VÀO HÀNG ĐỢI RATE LIMITER (Không xử lý ngay)
  masterSocket.on("process_task", (channel) => {
    localTaskQueue.push(channel);
  });

  masterSocket.on("force_update_config", (newCfg) => {
    if (newCfg.useLocalNetwork !== undefined)
      config.useLocalNetwork = newCfg.useLocalNetwork;
    if (newCfg.localLoad) config.localLoad = parseInt(newCfg.localLoad);
    if (newCfg.loadPerProxy)
      config.loadPerProxy = parseInt(newCfg.loadPerProxy);
    logWarn(
      `⚙️ Đã áp dụng cấu hình mới: LocalLoad=${config.localLoad}, PerProxy=${config.loadPerProxy}`,
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
// 💡 HÀNG ĐỢI & RATE LIMITER (ANTI-DDOS CHẶN 403)
// ==========================================
setInterval(async () => {
  if (localTaskQueue.length === 0) return;
  const channel = localTaskQueue.shift();
  executeTask(channel);
}, 500); // Tốc độ xử lý: 2 kênh / giây

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
    return masterSocket.emit("radar_result", { channel, status: "BLOCKED" });
  }

  pendingChecks.add(channel.username);
  sendWorkerStatus();

  const proxy = getNextAvailableProxy();
  if (!proxy) {
    pendingChecks.delete(channel.username);
    sendWorkerStatus();
    return masterSocket.emit("radar_result", { channel, status: "BLOCKED" });
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
    // 💡 JITTER: Thêm độ trễ ngẫu nhiên (0 - 600ms) mô phỏng người dùng thật
    await new Promise((resolve) => setTimeout(resolve, Math.random() * 600));

    const res = await axios.get(
      `https://www.tiktok.com/${channel.username}/live`,
      options,
    );

    if (res.status === 407 || res.status === 502 || res.status === 503)
      throw new Error("PROXY_DEAD");
    if (res.status === 403 || res.status === 429 || res.status >= 500)
      throw new Error("TIKTOK_BLOCK");
    if (res.status === 404) {
      masterSocket.emit("radar_result", { channel, status: "NOT_FOUND" });
      pendingChecks.delete(channel.username);
      sendWorkerStatus();
      return;
    }

    let status = "OFFLINE";
    if (res.status === 200) {
      if (
        res.data.includes("<title>Verification</title>") ||
        res.data.includes("Please confirm you are human")
      )
        throw new Error("TIKTOK_BLOCK");
      if (
        res.data.includes('"status":2') ||
        res.data.includes('"roomStatus":2') ||
        res.data.includes('"is_live":true') ||
        res.data.includes('"isLive":true')
      )
        status = "LIVE";
    }

    masterSocket.emit("radar_result", { channel, status });

    if (status === "LIVE") {
      delete proxyFailCount[proxy];
      delete proxyCooldown[proxy];
      if (proxyHealth[proxy]) proxyHealth[proxy].status = "SẴN SÀNG";
      startWebcast(channel, proxy, ua);
    } else {
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
          if (masterSocket && masterSocket.connected)
            masterSocket.emit("worker_report_dead_proxy", {
              proxy: proxy,
              workerName: config.workerName,
            });
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
      for (let p in proxyHealth)
        if (p !== "local" && proxyHealth[p].status === "SẴN SÀNG") aliveCount++;
      currentDynamicMaxLoad =
        aliveCount * config.loadPerProxy + (localAlive ? config.localLoad : 0);
      if (masterSocket && masterSocket.connected)
        masterSocket.emit("worker_update_capacity", {
          maxLoad: currentDynamicMaxLoad,
        });
    }
    pendingChecks.delete(channel.username);
    masterSocket.emit("radar_result", { channel, status: "ERROR" });
    stopWebcast(channel.username);
  }
}

function startWebcast(channel, proxy, ua) {
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
      sendWorkerStatus();

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
        masterSocket.emit("radar_result", { channel, status: "ERROR" });
        stopWebcast(channel.username);
      });
    })
    .catch((err) => {
      checkAndReportDeadKey(err, key);
      pendingChecks.delete(channel.username);
      sendWorkerStatus();
      masterSocket.emit("radar_result", { channel, status: "ERROR" });
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
  sendWorkerStatus();
}

fs.watchFile(CONFIG_FILE, async (curr, prev) => {
  logWarn("📝 Phát hiện file cấu hình thay đổi! Đang đồng bộ...");
  try {
    const fileData = JSON.parse(fs.readFileSync(CONFIG_FILE, "utf8"));
    config.localLoad = fileData.localLoad || 50;
    config.loadPerProxy = fileData.loadPerProxy || 10;
    config.useLocalNetwork = fileData.useLocalNetwork || false;
    const newProxyCount =
      fileData.proxyCount !== undefined
        ? fileData.proxyCount
        : config.proxyCount;

    if (newProxyCount > config.proxyCount) {
      const needed = newProxyCount - config.proxyCount;
      if (masterSocket && masterSocket.connected)
        masterSocket.emit("worker_request_proxies", {
          count: needed,
          workerName: config.workerName,
        });
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
          if (masterSocket && masterSocket.connected)
            masterSocket.emit("radar_result", {
              channel: { username: user },
              status: "BLOCKED",
            });
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

      if (masterSocket && masterSocket.connected)
        masterSocket.emit("worker_return_proxies", proxiesToReturn);
      config.proxyCount = newProxyCount;
    }
    await checkProxyHealth();
  } catch (e) {
    logError("❌ Lỗi định dạng file cấu hình, không thể nạp nóng.");
  }
});

logInfo("Đang khởi động Headless Worker...");

// ==========================================
// CƠ CHẾ DỌN DẸP BÓNG MA (ZOMBIE KILLER)
// ==========================================
setInterval(() => {
  const now = Date.now();
  let cleaned = 0;
  for (let user in activeConnections) {
    const conn = activeConnections[user];
    const lastActivity =
      conn.wsClient?.lastActivity || conn.wsClient?.connectionTime || now;
    if (now - lastActivity > 180000) {
      stopWebcast(user);
      masterSocket.emit("radar_result", {
        channel: { username: user },
        status: "ERROR",
      });
      cleaned++;
    }
  }
  if (cleaned > 0) sendWorkerStatus();
}, 60000);

connectToMaster();
