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
const crypto = require("crypto");

const CONFIG_FILE = "vps_config.json";

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

let activeConnections = {};
let connectionLocks = new Set();
let assignedProxies = {};
let proxyUsage = {};
let proxyFailCount = {};
let proxyCooldown = {};
let proxyStrikeCount = {};
let proxyHealth = {};
let pendingChecks = new Map();
let masterSocket = null;
let globalFailureCount = 0;
let workerPausedUntil = 0;

setInterval(() => {
  globalFailureCount = 0;
}, 120000);

let dynamicProxies = [];
let exclusiveEulerKeys = [];
let frozenChannels = {};

// KHO LƯU TRỮ DANH TÍNH TỪ MASTER
const proxyIdentities = {};

let keyIndex = 0;
const agentCache = {};
let localTaskQueue = [];
let proxyNextSocketTime = {};
let proxyNextHttpTime = {};

if (config.useLocalNetwork) proxyUsage["local"] = 0;

// ==========================================
// HÀM BỌC THÉP CHỐNG CRASH TÊN PROXY
// ==========================================
function getShortProxy(p) {
  if (!p) return "Unknown";
  if (typeof p === "string") return p.split("@").pop();
  return String(p);
}

const ENABLE_DEBUG = process.env.DEBUG === "true";
function sendMasterLog(msg) {
  if (masterSocket && masterSocket.connected)
    masterSocket.emit("worker_log", `${msg}`);
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
  console.log("📡 TRẠNG THÁI PROXY (ECOSYSTEM):");
  const tableData = (
    config.useLocalNetwork ? ["local", ...dynamicProxies] : dynamicProxies
  ).map((p) => {
    let ecoStatus = "N/A";
    if (proxyIdentities[p] && proxyIdentities[p].subs)
      ecoStatus = `${proxyIdentities[p].subs.length} Sub-Profiles`;
    return {
      Proxy: p === "local" ? "Mạng VPS (Local)" : getShortProxy(p), // 💡 ĐÃ BỌC THÉP
      "Hệ Sinh Thái": ecoStatus,
      "Đang cắm": `${proxyUsage[p] || 0}/${p === "local" ? config.localLoad || config.loadPerProxy : config.loadPerProxy}`,
      "Tình trạng": proxyHealth[p]?.status || "ĐANG KIỂM TRA",
    };
  });
  console.table(tableData);
  console.log("==========================================\n");
}, 15000);

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

async function checkProxyHealth() {
  let checkList = [...dynamicProxies];
  if (config.useLocalNetwork) checkList.unshift("local");
  let currentHealth = {};

  await Promise.all(
    checkList.map(async (p) => {
      try {
        if (proxyCooldown[p] && Date.now() < proxyCooldown[p]) {
          currentHealth[p] = {
            status: `ĐANG NGHỈ ${proxyFailCount[p] || 1} LẦN`,
          };
          return;
        }
        let options = { timeout: 8000, validateStatus: () => true };
        if (p !== "local" && typeof p === "string") {
          const proxyAgent = getCachedAgent(p);
          options.httpAgent = proxyAgent;
          options.httpsAgent = proxyAgent;
        }
        const healthRes = await axios.get(
          "https://clients3.google.com/generate_204",
          options,
        );
        if (healthRes.status === 200 || healthRes.status === 204) {
          currentHealth[p] = { status: "SẴN SÀNG" };
          proxyFailCount[p] = 0;
        } else {
          throw new Error("Lỗi Ping");
        }
      } catch (e) {
        currentHealth[p] = { status: "MẤT KẾT NỐI" };
        if (p !== "local") {
          proxyFailCount[p] = (proxyFailCount[p] || 0) + 1;
          if (proxyFailCount[p] >= 3) {
            currentHealth[p].status = "BÁO LỖI";
            if (masterSocket && masterSocket.connected)
              masterSocket.emit("worker_report_dead_proxy", {
                proxy: p,
                workerName: config.workerName,
              });
            dynamicProxies = dynamicProxies.filter((dp) => dp !== p);
            cleanupProxyData(p);
          } else {
            proxyCooldown[p] = Date.now() + 20000;
          }
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
  if (masterSocket && masterSocket.connected)
    masterSocket.emit("worker_update_capacity", {
      maxLoad: currentDynamicMaxLoad,
    });
}
setInterval(checkProxyHealth, 45000);
setTimeout(checkProxyHealth, 2000);

function getNextSubProfile(proxy) {
  const idObj = proxyIdentities[proxy];
  if (!idObj || !idObj.subs || idObj.subs.length === 0) return null;
  const currentSub = idObj.subs[idObj.currentIndex];
  idObj.currentIndex = (idObj.currentIndex + 1) % idObj.subs.length;
  return currentSub;
}

function getNextAvailableProxy() {
  let allProxies = config.useLocalNetwork
    ? ["local", ...dynamicProxies]
    : [...dynamicProxies];
  let available = allProxies.filter((p) => {
    if (proxyCooldown[p] && Date.now() < proxyCooldown[p]) return false;
    if (p !== "local" && proxyStrikeCount[p] >= 4) return false;
    const isReady = p === "local" || proxyHealth[p]?.status === "SẴN SÀNG";
    const limit =
      p === "local"
        ? config.localLoad || config.loadPerProxy
        : config.loadPerProxy;
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

function getCachedAgent(proxyUrl) {
  if (!proxyUrl || typeof proxyUrl !== "string") return null;
  if (!proxyUrl.startsWith("http")) {
    const parts = proxyUrl.split(":");
    if (parts.length === 4)
      proxyUrl = `http://${parts[2]}:${parts[3]}@${parts[0]}:${parts[1]}`;
    else if (parts.length === 2) proxyUrl = `http://${parts[0]}:${parts[1]}`;
    else proxyUrl = `http://${proxyUrl}`;
  }
  if (!agentCache[proxyUrl]) {
    const chromeCiphers = [
      "TLS_AES_128_GCM_SHA256",
      "TLS_AES_256_GCM_SHA384",
      "TLS_CHACHA20_POLY1305_SHA256",
      "ECDHE-ECDSA-AES128-GCM-SHA256",
      "ECDHE-RSA-AES128-GCM-SHA256",
      "ECDHE-ECDSA-AES256-GCM-SHA384",
      "ECDHE-RSA-AES256-GCM-SHA384",
      "ECDHE-ECDSA-CHACHA20-POLY1305",
      "ECDHE-RSA-CHACHA20-POLY1305",
    ].join(":");
    agentCache[proxyUrl] = new HttpsProxyAgent(proxyUrl, {
      keepAlive: true,
      keepAliveMsecs: 60000,
      rejectUnauthorized: false,
      ciphers: chromeCiphers,
      minVersion: "TLSv1.2",
      maxVersion: "TLSv1.3",
      ecdhCurve: "X25519:P-256:P-384:P-521",
      secureProtocol: "TLS_client_method",
      secureOptions: crypto.constants.SSL_OP_LEGACY_SERVER_CONNECT,
    });
  }
  return agentCache[proxyUrl];
}

function cleanupProxyData(proxy) {
  if (!proxy || typeof proxy !== "string") return;
  let fmtProxy = proxy;
  if (!fmtProxy.startsWith("http")) {
    const parts = fmtProxy.split(":");
    if (parts.length === 4)
      fmtProxy = `http://${parts[2]}:${parts[3]}@${parts[0]}:${parts[1]}`;
    else fmtProxy = `http://${parts[0]}:${parts[1]}`;
  }
  if (agentCache[fmtProxy]) {
    try {
      agentCache[fmtProxy].destroy();
    } catch (e) {}
    delete agentCache[fmtProxy];
  }
  delete proxyIdentities[proxy];
  delete proxyHealth[proxy];
  delete proxyUsage[proxy];
  delete proxyFailCount[proxy];
  delete proxyCooldown[proxy];
  delete proxyStrikeCount[proxy];
  delete proxyNextSocketTime[proxy];
  delete proxyNextHttpTime[proxy];
}

function safeEmitRadarResult({ channel, status }) {
  if (!masterSocket || !masterSocket.connected) return;
  if (masterSocket.sendBuffer && masterSocket.sendBuffer.length > 50) {
    masterSocket.volatile.emit("radar_result", { channel, status });
  } else {
    masterSocket.emit("radar_result", { channel, status });
  }
}

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

    if (config.useLocalNetwork) {
      logInfo("Đang kiểm tra vị trí mạng Local trước khi xin Profile...");
      axios
        .get("http://ip-api.com/json/?fields=countryCode", { timeout: 5000 })
        .then((res) => {
          const country = res.data?.countryCode || "VN";
          masterSocket.emit("worker_request_local_profile", {
            workerName: config.workerName,
            countryCode: country,
          });
        })
        .catch((err) => {
          masterSocket.emit("worker_request_local_profile", {
            workerName: config.workerName,
            countryCode: "VN",
          });
        });
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
    if (neededProxies > 0)
      masterSocket.emit("worker_request_proxies", {
        count: neededProxies,
        workerName: config.workerName,
      });

    const targetKeyCount = Math.ceil(config.proxyCount / 2);
    const neededKeys = Math.max(0, targetKeyCount - exclusiveEulerKeys.length);
    if (neededKeys > 0)
      masterSocket.emit("worker_request_keys", {
        count: neededKeys,
        workerName: config.workerName,
      });
  });

  masterSocket.on("worker_proxy_removed", (proxyStr) => {
    logWarn(
      `🗑️ Master báo Proxy [${getShortProxy(proxyStr)}] đã bị xóa khỏi hệ thống. Tiến hành giải phóng tải khẩn cấp...`,
    );

    // Rút toàn bộ các kết nối socket đang cắm trên proxy/local profile bị xóa này và đẩy về hàng đợi
    for (let user in assignedProxies) {
      if (assignedProxies[user] === proxyStr) {
        stopWebcast(user);
        safeEmitRadarResult({ channel: { username: user }, status: "REQUEUE" });
      }
    }

    // Loại khỏi bộ nhớ đệm nội bộ của Worker
    dynamicProxies = dynamicProxies.filter((p) => p !== proxyStr);
    cleanupProxyData(proxyStr);
  });

  masterSocket.on("worker_receive_local_profile", (subProfilesStr) => {
    try {
      proxyIdentities["local"] = {
        subs: JSON.parse(subProfilesStr),
        currentIndex: 0,
      };
      logSuccess(
        `📡 Đã đồng bộ ${proxyIdentities["local"].subs.length} danh tính Local từ Master!`,
      );
    } catch (e) {
      logError("Lỗi parse Local SubProfiles");
    }
  });

  masterSocket.on("worker_receive_keys", (keysList) => {
    if (Array.isArray(keysList) && keysList.length > 0) {
      exclusiveEulerKeys = Array.from(
        new Set([...exclusiveEulerKeys, ...keysList]),
      );
    }
  });

  masterSocket.on("worker_key_replacement", (data) => {
    const { deadKey, newKey } = data;
    exclusiveEulerKeys = exclusiveEulerKeys.filter((k) => k !== deadKey);
    if (newKey && !exclusiveEulerKeys.includes(newKey))
      exclusiveEulerKeys.push(newKey);
  });

  masterSocket.on("worker_receive_proxies", (profilesList) => {
    if (Array.isArray(profilesList) && profilesList.length > 0) {
      logSuccess(`📡 Nhận thêm ${profilesList.length} Profile Ecosystem.`);
      profilesList.forEach((prof) => {
        // 💡 ĐÃ BỌC THÉP: Chống nhồi dữ liệu lỗi vào mảng dynamicProxies
        if (
          prof &&
          typeof prof.proxy === "string" &&
          !dynamicProxies.includes(prof.proxy)
        ) {
          dynamicProxies.push(prof.proxy);
          proxyUsage[prof.proxy] = 0;
          try {
            proxyIdentities[prof.proxy] = {
              subs: JSON.parse(prof.subProfiles),
              currentIndex: 0,
            };
          } catch (e) {}
        }
      });
      checkProxyHealth();
    }
  });

  masterSocket.on("worker_proxy_replacement", (data) => {
    cleanupProxyData(data.deadProxy);
    if (data.newProxy)
      masterSocket.emit("worker_request_proxies", {
        count: 1,
        workerName: config.workerName,
      });
    checkProxyHealth();
  });

  masterSocket.on("worker_proxy_rescued", async (data) => {
    const { proxy, newCookies, targetUser } = data;
    let isManaged =
      proxy === "local"
        ? config.useLocalNetwork
        : dynamicProxies.includes(proxy);
    if (
      isManaged &&
      frozenChannels[proxy] &&
      frozenChannels[proxy].username === targetUser
    ) {
      const channelToRescue = frozenChannels[proxy];
      delete frozenChannels[proxy];
      proxyStrikeCount[proxy] = 0;
      proxyCooldown[proxy] = 0;
      if (proxyHealth[proxy]) proxyHealth[proxy].status = "SẴN SÀNG";
      const subProfile = getNextSubProfile(proxy);
      if (subProfile)
        startWebcast(channelToRescue, proxy, subProfile, newCookies);
    }
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

    const now = Date.now();
    let maxQueueDelay = 0;
    for (let p in proxyNextSocketTime) {
      const delay = proxyNextSocketTime[p] - now;
      if (delay > maxQueueDelay) maxQueueDelay = delay;
    }
    if (maxQueueDelay > 10000) return;

    const proxyCount = config.useLocalNetwork
      ? dynamicProxies.length + 1
      : dynamicProxies.length;
    const maxConcurrentChecks = proxyCount * 2;
    const availableCheckSlots = maxConcurrentChecks - pendingChecks.size;

    if (availableCheckSlots <= 0) return;
    const tasksToProcess = Math.min(
      proxyCount,
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

function buildDynamicHeaders(subProfile) {
  const geo = subProfile.geo || { lang: "en-US", region: "US" };
  const referers = [
    "https://www.tiktok.com/foryou",
    "https://www.tiktok.com/explore",
    "https://www.tiktok.com/",
  ];
  let headers = {
    "User-Agent": subProfile.userAgent,
    Accept:
      "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": `${geo.lang},en-US;q=0.9,en;q=0.8`,
    Referer: referers[Math.floor(Math.random() * referers.length)],
    "Cache-Control": "max-age=0",
    "Sec-Ch-Ua": subProfile.secChUa,
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": subProfile.secChUaPlatform,
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
  };
  if (subProfile.cookies) headers["Cookie"] = subProfile.cookies;
  if (subProfile.verifyFp) headers["x-secsdk-csrf-token"] = subProfile.verifyFp;
  return headers;
}

async function checkLiveStatus(username, proxy, subProfile) {
  const geo = subProfile.geo || { lang: "vi-VN", region: "VN" };
  let proxyUrlGot = undefined;

  if (proxy && proxy !== "local" && typeof proxy === "string") {
    const parts = proxy.split(":");
    if (parts.length === 4)
      proxyUrlGot = `http://${parts[2]}:${parts[3]}@${parts[0]}:${parts[1]}`;
    else proxyUrlGot = proxy.startsWith("http") ? proxy : `http://${proxy}`;
  }

  const urlUsername =
    typeof username === "string" && username.startsWith("@")
      ? username
      : `@${username}`;
  try {
    const res = await gotScraping({
      url: `https://www.tiktok.com/${urlUsername}/live`,
      proxyUrl: proxyUrlGot,
      timeout: { request: 8000 },
      throwHttpErrors: false,
      http2: true,
      retry: { limit: 0 },
      headerGeneratorOptions: {
        browsers: [{ name: "chrome", minVersion: 120, maxVersion: 125 }],
        operatingSystems: [
          subProfile.hardware.platform === '"macOS"' ? "macos" : "windows",
        ],
        devices: ["desktop"],
        locales: subProfile.locales || [geo.lang, "en-US", "en"],
      },
      headers: buildDynamicHeaders(subProfile),
    });

    if (
      res.statusCode === 407 ||
      res.statusCode === 502 ||
      res.statusCode === 503
    )
      throw new Error("REQUEUE");
    if (res.statusCode === 404) return "NOT_FOUND";
    if (
      res.statusCode === 403 ||
      res.statusCode === 429 ||
      res.statusCode >= 500
    )
      return "ERROR";

    const finalUrl = (res.url || "").toLowerCase();
    if (
      finalUrl.includes("login") ||
      finalUrl.includes("verify") ||
      finalUrl.includes("captcha")
    )
      return "BLIND_TEST";

    const html =
      typeof res.body === "string" ? res.body : JSON.stringify(res.body || "");
    if (
      html.includes('"statusCode":10000') ||
      html.includes("webapp.not-found")
    )
      return "NOT_FOUND";
    if (
      html.includes("<title>Verification</title>") ||
      html.includes('id="verify-ele"') ||
      html.includes("age_restricted")
    )
      return "BLOCKED";

    try {
      const universalMatch = html.match(
        /<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__" type="application\/json">([^<]+)<\/script>/,
      );
      if (universalMatch) {
        const roomInfo = JSON.parse(universalMatch[1])?.["__DEFAULT_SCOPE__"]?.[
          "webapp.live-detail"
        ]?.["roomInfo"];
        if (roomInfo) return roomInfo.status === 2 ? "LIVE" : "OFFLINE";
      }
    } catch (e) {}

    let cleanHtml = html
      .split('"recommendList"')[0]
      .split('"suggested"')[0]
      .split("recommend_live")[0];
    const roomMatch = cleanHtml.match(
      /"(?:roomId|room_id)"\s*:\s*"?([1-9]\d+)"?/,
    );
    const isLiveFlag =
      cleanHtml.includes('"status":2') || cleanHtml.includes('"isLive":true');
    if (roomMatch && roomMatch[1] && isLiveFlag) return "LIVE";

    return "OFFLINE";
  } catch (error) {
    if (error.message === "REQUEUE") throw error;
    throw new Error("PROXY_DEAD");
  }
}

async function executeTask(channel) {
  if (activeConnections[channel.username]) {
    pendingChecks.delete(channel.username);
    return;
  }

  let availableProxies = config.useLocalNetwork ? ["local"] : [];
  for (let p of dynamicProxies) {
    if (
      proxyHealth[p]?.status === "SẴN SÀNG" &&
      (!proxyCooldown[p] || Date.now() > proxyCooldown[p])
    )
      availableProxies.push(p);
  }

  if (availableProxies.length === 0) {
    pendingChecks.delete(channel.username);
    return safeEmitRadarResult({ channel, status: "REQUEUE" });
  }

  const checkProxy =
    availableProxies[Math.floor(Math.random() * availableProxies.length)];
  const subProfile = getNextSubProfile(checkProxy);

  if (!subProfile) {
    pendingChecks.delete(channel.username);
    setTimeout(() => {
      safeEmitRadarResult({ channel, status: "REQUEUE" });
    }, 2000);
    return;
  }

  if (typeof proxyNextHttpTime === "undefined") global.proxyNextHttpTime = {};
  const now = Date.now();
  if (!proxyNextHttpTime[checkProxy] || proxyNextHttpTime[checkProxy] < now)
    proxyNextHttpTime[checkProxy] = now;
  const httpDelay = proxyNextHttpTime[checkProxy] - now;
  proxyNextHttpTime[checkProxy] += 3000;

  setTimeout(async () => {
    try {
      const status = await checkLiveStatus(
        channel.username,
        checkProxy,
        subProfile,
      );

      if (status === "NOT_FOUND") {
        safeEmitRadarResult({ channel, status: "NOT_FOUND" });
        stopWebcast(channel.username);
        return;
      }
      if (
        status === "BLIND_TEST" ||
        status === "BLOCKED" ||
        status === "ERROR"
      ) {
        proxyCooldown[checkProxy] = Date.now() + 45000;
        if (proxyHealth[checkProxy])
          proxyHealth[checkProxy].status = "Gặp Captcha/WAF";
        safeEmitRadarResult({ channel, status: "REQUEUE" });
        stopWebcast(channel.username);
        return;
      }

      if (status === "LIVE") {
        const socketProxy = getNextAvailableProxy();
        if (!socketProxy) {
          safeEmitRadarResult({ channel, status: "REQUEUE" });
          return;
        }

        safeEmitRadarResult({ channel, status: "LIVE" });
        delete proxyFailCount[socketProxy];
        delete proxyCooldown[socketProxy];
        if (proxyHealth[socketProxy])
          proxyHealth[socketProxy].status = "SẴN SÀNG";
        proxyUsage[socketProxy] = (proxyUsage[socketProxy] || 0) + 1;
        assignedProxies[channel.username] = socketProxy;

        const socketSubProfile =
          socketProxy === checkProxy
            ? subProfile
            : getNextSubProfile(socketProxy);

        if (!socketSubProfile) {
          stopWebcast(channel.username);
          return safeEmitRadarResult({ channel, status: "REQUEUE" });
        }

        const timeNow = Date.now();
        if (
          !proxyNextSocketTime[socketProxy] ||
          proxyNextSocketTime[socketProxy] < timeNow
        )
          proxyNextSocketTime[socketProxy] = timeNow;
        const delay = proxyNextSocketTime[socketProxy] - timeNow;
        proxyNextSocketTime[socketProxy] += 5000;

        setTimeout(() => {
          startWebcast(channel, socketProxy, socketSubProfile);
        }, delay);
      } else {
        safeEmitRadarResult({ channel, status: "OFFLINE" });
        stopWebcast(channel.username);
      }
    } catch (e) {
      if (checkProxy !== "local") {
        proxyCooldown[checkProxy] = Date.now() + 30000;
        let aliveCount = 0;
        for (let p in proxyHealth) {
          if (p !== "local" && proxyHealth[p].status === "SẴN SÀNG")
            aliveCount++;
        }
        currentDynamicMaxLoad =
          aliveCount * config.loadPerProxy +
          (proxyHealth["local"]?.status === "SẴN SÀNG" ? config.localLoad : 0);
      }
      setTimeout(
        () => {
          safeEmitRadarResult({ channel, status: "REQUEUE" });
        },
        Math.floor(Math.random() * 5000) + 3000,
      );
      stopWebcast(channel.username);
    } finally {
      pendingChecks.delete(channel.username);
    }
  }, httpDelay);
}

function startWebcast(channel, proxy, subProfile, rescueCookie = null) {
  if (
    activeConnections[channel.username] ||
    connectionLocks.has(channel.username)
  )
    return;
  connectionLocks.add(channel.username);

  const key = getNextEulerKey();
  const cleanUser =
    typeof channel.username === "string"
      ? channel.username.replace(/@/g, "")
      : channel.username;
  const geo = subProfile.geo || { lang: "en-US", region: "US" };
  const dynamicHeaders = buildDynamicHeaders(subProfile);

  let reqOptions = {
    headers: {
      ...dynamicHeaders,
      Referer: `https://www.tiktok.com/@${cleanUser}/live`,
    },
  };
  if (rescueCookie) reqOptions.headers["Cookie"] = rescueCookie;

  let wsOptions = {
    headers: {
      "User-Agent": subProfile.userAgent,
      Cookie: rescueCookie || subProfile.cookies || "",
      Origin: "https://www.tiktok.com",
      Referer: `https://www.tiktok.com/@${cleanUser}/live`,
      "Accept-Language": `${geo.lang},en-US;q=0.9`,
      "Cache-Control": "no-cache",
      Pragma: "no-cache",
      "Sec-WebSocket-Extensions": "permessage-deflate; client_max_window_bits",
    },
  };

  if (proxy !== "local" && typeof proxy === "string") {
    const agent = getCachedAgent(proxy);
    reqOptions.httpsAgent = agent;
    wsOptions.agent = agent;
  }

  let browserVersion = "124";
  const match = subProfile.userAgent.match(/Chrome\/(\d+)/);
  if (match) browserVersion = match[1];

  let conn = new TikTokLiveConnection(channel.username, {
    signApiKey: key,
    requestOptions: reqOptions,
    websocketOptions: wsOptions,
    clientParams: {
      app_language: geo.lang,
      webcast_language: geo.lang,
      region: geo.region,
      sys_region: geo.region,
      device_platform: "web",
      cookie_enabled: "true",
      browser_language: geo.lang,
      browser_name: "chrome",
      browser_version: `${browserVersion}.0.0.0`,
      browser_online: "true",
      os: subProfile.hardware.platform === '"macOS"' ? "mac" : "windows",
      screen_width: subProfile.hardware.screenWidth,
      screen_height: subProfile.hardware.screenHeight,
      device_memory: subProfile.hardware.deviceMemory,
      hardware_concurrency: subProfile.hardware.hardwareConcurrency,
      timezone_name: subProfile.timezone || "Asia/Ho_Chi_Minh",
    },
  });

  let currentViewers = 0;

  const checkAndReportDeadKey = (errObj, targetKey) => {
    if (!targetKey) return false;
    let errText = "unknown error";
    if (errObj) {
      if (typeof errObj === "string") errText = errObj;
      else if (errObj.message) errText = errObj.message;
      else {
        try {
          errText = JSON.stringify(errObj);
        } catch (e) {}
      }
    }
    const msg = String(errText).toLowerCase();
    const isDeadKey =
      msg.includes("insufficient balance") ||
      msg.includes("quota") ||
      msg.includes("invalid api key") ||
      msg.includes("unauthorized") ||
      msg.includes("key expired") ||
      msg.includes("forbidden") ||
      msg.includes("sign error") ||
      msg.includes("status 401") ||
      msg.includes("eulerstream.com") ||
      msg.includes("rate_limit_account_day");

    if (isDeadKey) {
      if (masterSocket && masterSocket.connected)
        masterSocket.emit("worker_report_dead_key", {
          key: targetKey,
          workerName: config.workerName,
        });
      return true;
    }
    return false;
  };

  Promise.race([
    conn.connect(),
    new Promise((_, r) =>
      setTimeout(() => r(new Error("SOCKET_TIMEOUT")), 60000),
    ),
  ])
    .then((state) => {
      connectionLocks.delete(channel.username);
      activeConnections[channel.username] = conn;
      activeConnections[channel.username].lastActive = Date.now();
      proxyStrikeCount[proxy] = 0;
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
        safeEmitRadarResult({ channel, status: "OFFLINE" });
        stopWebcast(channel.username);
      });
      conn.on("disconnected", () => {
        safeEmitRadarResult({ channel, status: "OFFLINE" });
        stopWebcast(channel.username);
      });

      if (rescueCookie && masterSocket && masterSocket.connected)
        masterSocket.emit("worker_rescue_success", proxy);
    })
    .catch((err) => {
      connectionLocks.delete(channel.username);
      checkAndReportDeadKey(err, key);
      let errMsg = String(err && err.message ? err.message : err).toLowerCase();

      if (
        errMsg.includes("not found") ||
        errMsg.includes("offline") ||
        errMsg.includes("ended") ||
        errMsg.includes("room_id")
      ) {
        safeEmitRadarResult({ channel, status: "OFFLINE" });
        stopWebcast(channel.username);
      } else if (errMsg.includes("suspended") || errMsg.includes("banned")) {
        safeEmitRadarResult({ channel, status: "ERROR" });
        stopWebcast(channel.username);
      } else if (
        errMsg.includes("timeout") ||
        errMsg.includes("network") ||
        errMsg.includes("socket") ||
        errMsg.includes("502") ||
        errMsg.includes("503") ||
        errMsg.includes("rate limit")
      ) {
        globalFailureCount++;
        if (globalFailureCount > Math.max(15, dynamicProxies.length * 3)) {
          workerPausedUntil = Date.now() + 60000;
          globalFailureCount = 0;
        }
        if (proxy !== "local") {
          proxyStrikeCount[proxy] = (proxyStrikeCount[proxy] || 0) + 1;
          if (proxyStrikeCount[proxy] < 4) {
            proxyCooldown[proxy] = Date.now() + 60000;
            safeEmitRadarResult({ channel, status: "REQUEUE" });
            stopWebcast(channel.username);
          } else {
            frozenChannels[proxy] = channel;
            if (masterSocket && masterSocket.connected)
              masterSocket.emit("worker_request_rescue", {
                proxy: proxy,
                userAgent: subProfile.userAgent,
                workerName: config.workerName,
                activeConnectionsCount: proxyUsage[proxy] || 0,
                targetUser: channel.username,
              });
            return;
          }
        } else {
          safeEmitRadarResult({ channel, status: "REQUEUE" });
          stopWebcast(channel.username);
        }
      } else {
        safeEmitRadarResult({ channel, status: "REQUEUE" });
        stopWebcast(channel.username);
      }
    });
}

function stopWebcast(user) {
  const conn = activeConnections[user];
  if (!conn) return;
  delete activeConnections[user];
  pendingChecks.delete(user);
  const realProxy = assignedProxies[user];
  if (realProxy) {
    proxyUsage[realProxy] = Math.max(0, (proxyUsage[realProxy] || 0) - 1);
    delete assignedProxies[user];
  }

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
  for (let proxy in frozenChannels) {
    if (now - frozenChannels[proxy].timestamp > 15 * 60 * 1000) {
      safeEmitRadarResult({
        channel: frozenChannels[proxy].channel,
        status: "REQUEUE",
      });
      delete frozenChannels[proxy];
    }
  }
  for (let user in activeConnections) {
    const conn = activeConnections[user];
    if (now - (conn.lastActive || now) > 30 * 60 * 1000) {
      stopWebcast(user);
      safeEmitRadarResult({ channel: { username: user }, status: "REQUEUE" });
    }
  }
  for (let p of dynamicProxies) {
    if (proxyStrikeCount[p] >= 4 && (proxyUsage[p] || 0) === 0) {
      if (masterSocket && masterSocket.connected)
        masterSocket.emit("worker_report_dead_proxy", {
          proxy: getShortProxy(p),
          workerName: config.workerName,
        });
      proxyStrikeCount[p] = -999;
    }
  }
}, 30000);

connectToMaster();

process.on("uncaughtException", (err) => {
  logError(`[CRASH PROTECT] Lỗi không lường trước: ${err.message}`);
});
process.on("unhandledRejection", (reason) => {
  logError(`[CRASH PROTECT] Promise bị từ chối: ${reason}`);
});
