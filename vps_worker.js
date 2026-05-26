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
let pendingChecks = new Map();
let masterSocket = null;

let dynamicProxies = [];
let exclusiveEulerKeys = [];

let proxyIndex = 0,
  uaIndex = 0,
  keyIndex = 0;
const agentCache = {};
let proxyGeoData = {};

let localTaskQueue = [];
let nextSocketConnectTime = 0;

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
    const activeNames = Object.keys(activeConnections);

    // Đảm bảo pendingChannels không chứa những kênh đã kết nối thành công (active)
    let allPending = [
      ...Array.from(pendingChecks.keys()),
      ...localTaskQueue.map((c) => c.username),
    ].filter((uname) => !activeNames.includes(uname));

    // Loại bỏ trùng lặp
    allPending = [...new Set(allPending)];

    masterSocket.emit("worker_status", {
      currentLoad: activeNames.length + allPending.length,
      runningChannels: activeNames, // Dữ liệu cho mục "Cắm Socket" trên UI
      pendingChannels: allPending, // Dữ liệu cho mục "Đang Check" trên UI
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
            ip: "Đã ẩn để tiết kiệm",
          };
          return;
        }

        // 💡 FIX: Rút ngắn timeout ping proxy xuống 4s để check nhanh hơn
        let options = { timeout: 8000, validateStatus: () => true };

        if (p !== "local") {
          const proxyAgent = getCachedAgent(p);
          options.httpAgent = proxyAgent;
          options.httpsAgent = proxyAgent;
        }

        if (!proxyGeoData[p]) {
          try {
            const geoRes = await axios.get(
              "http://ip-api.com/json/?fields=countryCode",
              options,
            );
            if (geoRes.data && geoRes.data.countryCode)
              proxyGeoData[p] = geoRes.data.countryCode;
            else proxyGeoData[p] = "VN";
          } catch (e) {
            proxyGeoData[p] = "VN"; // Default nếu check IP lỗi
          }
        }

        const healthRes = await axios.get(
          "https://clients3.google.com/generate_204",
          options,
        );

        if (healthRes.status === 204) {
          currentHealth[p] = {
            status: "SẴN SÀNG",
            ip: "Đã ẩn để tiết kiệm",
          };
          proxyFailCount[p] = 0;
        } else {
          throw new Error("Lỗi Ping");
        }
      } catch (e) {
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
            proxyCooldown[p] = Date.now() + 20000; // Nghỉ 30s
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
    if (proxyHealth[p]?.status === "SẴN SÀNG") {
      aliveCount += config.loadPerProxy;
    }
  }

  currentDynamicMaxLoad = aliveCount;
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
  let allProxies = config.useLocalNetwork
    ? ["local", ...dynamicProxies]
    : [...dynamicProxies];

  // Lọc ra các proxy/local đang Sẵn Sàng và chưa đầy tải
  let available = allProxies.filter((p) => {
    // Bỏ qua nếu đang bị phạt nghỉ
    if (proxyCooldown[p] && Date.now() < proxyCooldown[p]) return false;

    const isReady = p === "local" || proxyHealth[p]?.status === "SẴN SÀNG";
    const limit =
      p === "local"
        ? config.localLoad || config.loadPerProxy
        : config.loadPerProxy;
    const currentUsage = proxyUsage[p] || 0;

    return isReady && currentUsage < limit;
  });

  if (available.length === 0) return null;

  // Thuật toán cốt lõi: Sắp xếp ưu tiên thằng nào đang rảnh việc nhất lên làm trước
  available.sort((a, b) => (proxyUsage[a] || 0) - (proxyUsage[b] || 0));

  return available[0];
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
      // 💡 FIX CỰC QUAN TRỌNG: Thêm dấu @ giữa Pass và IP
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
    // Thêm dòng này để dọn RAM
    if (agentCache[deadProxy]) delete agentCache[deadProxy];
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
  const totalSockets = Object.keys(activeConnections).length;

  // 1. NẾU WORKER ĐÃ ĐẦY ẮP SOCKET -> Nhả toàn bộ Queue về Master cho máy khác xử lý
  if (totalSockets >= currentDynamicMaxLoad && currentDynamicMaxLoad > 0) {
    while (localTaskQueue.length > 0) {
      const c = localTaskQueue.shift();
      masterSocket.emit("radar_result", { channel: c, status: "REQUEUE" });
    }
    return;
  }
  // ========================================================
  // 💡 BỔ SUNG CẢM BIẾN KẸT XE (CHỐNG TRÀN HÀNG ĐỢI SOCKET)
  // ========================================================
  const now = Date.now();
  const socketQueueDelay = nextSocketConnectTime - now;

  // Nếu đang có quá nhiều kênh xếp hàng chờ cắm Socket (VD: Đang chờ > 8 giây ~ 4 kênh)
  // -> Ngừng bốc thêm kênh mới để đi Check HTTP, nhường CPU cho việc cắm Socket.
  if (socketQueueDelay > 8000) {
    return; // Đợi Socket giải tỏa bớt rồi mới chạy tiếp
  }
  // ========================================================
  // 2. TÍNH SỨC CHỨA CHECK HTTP ĐỘC LẬP
  // 1 Proxy gánh 2 request check cùng lúc là cực kỳ an toàn, không lo Rate Limit
  const proxyCount = config.useLocalNetwork
    ? dynamicProxies.length + 1
    : dynamicProxies.length;
  const maxConcurrentChecks = proxyCount * 2;
  const availableCheckSlots = maxConcurrentChecks - pendingChecks.size;

  if (availableCheckSlots <= 0) return;

  // 3. RẢI ĐINH REQUEST (Chống DDoS TikTok)
  // Bốc tối đa số lượng proxy kênh/giây để hệ thống luôn mượt mà
  const tasksToProcess = Math.min(
    proxyCount,
    availableCheckSlots,
    localTaskQueue.length,
  );

  for (let i = 0; i < tasksToProcess; i++) {
    const channel = localTaskQueue.splice(0, 1)[0];
    pendingChecks.set(channel.username, Date.now()); // Giữ chỗ slot HTTP

    // Rải đều thời gian bắn request ngẫu nhiên trong 2 giây để "tàng hình" trước WAF
    setTimeout(() => {
      executeTask(channel);
    }, Math.random() * 2000);
  }
}, 1000);

// 💡 FIX CỰC QUAN TRỌNG: Hàm Hard Timeout ép chết các request Axios bị treo do Proxy
const fetchWithTimeout = async (url, options, timeoutMs = 8000) => {
  const controller = new AbortController();

  const timeout = setTimeout(() => {
    controller.abort();
  }, timeoutMs);

  try {
    const response = await axios.get(url, {
      ...options,
      signal: controller.signal,
    });

    return response;
  } finally {
    clearTimeout(timeout);
  }
};

function buildDynamicHeaders(ua) {
  // Bộ Header nền tảng (Trình duyệt nào cũng gửi)
  let headers = {
    "User-Agent": ua,
    Accept:
      "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
    "Cache-Control": "max-age=0",
    Connection: "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    Range: "bytes=0-60000",
  };

  // Xác định Hệ điều hành (Platform)
  let platform = '"Windows"';
  if (ua.includes("Mac OS X")) platform = '"macOS"';
  else if (ua.includes("Linux")) platform = '"Linux"';

  // Xác định Trình duyệt & Bộ Client Hints (Sec-Ch-Ua)
  if (ua.includes("Chrome") || ua.includes("Edg")) {
    // Tách lấy chính xác version Chrome từ UA (VD: 124, 123)
    const match = ua.match(/Chrome\/(\d+)/);
    const version = match ? match[1] : "124";

    headers["Sec-Ch-Ua"] =
      `"Chromium";v="${version}", "Google Chrome";v="${version}", "Not-A.Brand";v="99"`;
    headers["Sec-Ch-Ua-Mobile"] = "?0";
    headers["Sec-Ch-Ua-Platform"] = platform;
  }
  // Firefox và Safari chuẩn không gửi Client Hints (Sec-Ch-*), nên ta không thêm vào headers.

  return headers;
}

async function checkLiveStatus(username, proxy, ua) {
  const dynamicHeaders = buildDynamicHeaders(ua);

  let options = {
    headers: dynamicHeaders,
    validateStatus: () => true,
  };

  if (proxy !== "local") options.httpsAgent = getCachedAgent(proxy);

  const urlUsername = username.startsWith("@") ? username : `@${username}`;

  // Sử dụng axios trực tiếp thay vì fetchWithTimeout để lấy được res.request.res.responseUrl chuẩn xác
  const res = await fetchWithTimeout(
    `https://www.tiktok.com/${urlUsername}/live`,
    options,
    8000,
  );

  // 1. Xử lý lỗi Proxy chết cứng
  if (res.status === 407 || res.status === 502 || res.status === 503) {
    throw new Error("PROXY_DEAD");
  }

  // 2. Kênh không tồn tại
  if (res.status === 404) return "NOT_FOUND";

  // 3. Bị chặn Rate Limit hoặc dính WAF
  if (res.status === 403 || res.status === 429 || res.status >= 500) {
    return "RATE_LIMIT";
  }

  // 4. Bắt điều hướng (Redirect)
  const finalUrl = (
    res.request?.res?.responseUrl ||
    res.config?.url ||
    ""
  ).toLowerCase();
  if (
    finalUrl.includes("login") ||
    finalUrl.includes("verify") ||
    finalUrl.includes("captcha") ||
    finalUrl.includes("sec.tiktok.com")
  ) {
    return "BLIND_TEST";
  }

  const html = res.data || "";

  // 5. Bắt Captcha trong HTML
  if (
    html.includes("<title>Verification</title>") ||
    html.includes('id="verify-ele"') ||
    html.includes("age_restricted") ||
    html.includes("Please confirm you are human")
  ) {
    return "BLIND_TEST";
  }

  // ========================================================
  // 💡 BỔ SUNG: BẮT KÊNH BỊ CẤM / RIÊNG TƯ (BLOCKED)
  // ========================================================
  const htmlLower = html.toLowerCase();
  if (
    htmlLower.includes("this account is private") ||
    htmlLower.includes("account is private") ||
    htmlLower.includes("account currently unavailable") ||
    htmlLower.includes("account has been banned") ||
    htmlLower.includes("suspended")
  ) {
    return "BLOCKED";
  }

  // 6. LOGIC TÌM LIVE CỰC CHUẨN (TỪ CHECK_IS_LIVE_FAST)
  const roomMatch = html.match(/"(?:roomId|room_id)"\s*:\s*"?([1-9]\d+)"?/);
  const isLiveFlag =
    html.includes('"status":2') ||
    html.includes('"isLive":true') ||
    html.includes('"is_live":true');

  if ((roomMatch && roomMatch[1]) || isLiveFlag) {
    return "LIVE";
  }

  return "OFFLINE";
}

async function executeTask(channel) {
  if (activeConnections[channel.username]) {
    pendingChecks.delete(channel.username);
    return;
  }

  // 💡 TỐI ƯU ĐỘT PHÁ: Lấy ngẫu nhiên 1 Proxy khỏe để Check HTTP (KHÔNG CHIẾM SLOT)
  let availableProxies = config.useLocalNetwork ? ["local"] : [];
  for (let p of dynamicProxies) {
    if (
      proxyHealth[p]?.status === "SẴN SÀNG" &&
      (!proxyCooldown[p] || Date.now() > proxyCooldown[p])
    ) {
      availableProxies.push(p);
    }
  }

  if (availableProxies.length === 0) {
    pendingChecks.delete(channel.username);
    return masterSocket.emit("radar_result", { channel, status: "REQUEUE" });
  }

  // Chọn 1 proxy ngẫu nhiên để san sẻ tải nhẹ nhàng
  const checkProxy =
    availableProxies[Math.floor(Math.random() * availableProxies.length)];
  const ua = getNextUA();

  try {
    // Gọi hàm check đã được tối ưu hóa
    const status = await checkLiveStatus(channel.username, checkProxy, ua);

    // Xử lý luồng theo trạng thái trả về
    if (status === "NOT_FOUND") {
      masterSocket.emit("radar_result", { channel, status: "NOT_FOUND" });
      stopWebcast(channel.username);
      return;
    }

    // 💡 KHÔI PHỤC TÍNH NĂNG ĐƯA VÀO HÀNG CHỜ TÁI KHÁM CỦA MASTER
    if (status === "BLIND_TEST") {
      // Trả đúng trạng thái BLIND_TEST về Master để đưa vào hàng chờ kiểm tra lại sau
      masterSocket.emit("radar_result", { channel, status: "BLIND_TEST" });
      stopWebcast(channel.username);
      return;
    }

    if (status === "LIVE") {
      // 💡 KÊNH ĐANG LIVE -> BÂY GIỜ MỚI THỰC SỰ ĐÒI SLOT ĐỂ CẮM SOCKET
      const socketProxy = getNextAvailableProxy();
      if (!socketProxy) {
        // Rủi ro cắm full tải đúng lúc phát hiện Live -> Trả kênh về Master
        masterSocket.emit("radar_result", { channel, status: "REQUEUE" });
        return;
      }

      masterSocket.emit("radar_result", { channel, status: "LIVE" }); // Báo LIVE luôn

      delete proxyFailCount[socketProxy];
      delete proxyCooldown[socketProxy];
      if (proxyHealth[socketProxy])
        proxyHealth[socketProxy].status = "SẴN SÀNG";
      // Chính thức cắm cờ ghi nhận tải cho Proxy
      proxyUsage[socketProxy] = (proxyUsage[socketProxy] || 0) + 1;
      assignedProxies[channel.username] = socketProxy;

      // ========================================================
      // 💡 TỐI ƯU: XẾP HÀNG CẮM SOCKET CHỐNG BÃO (ANTI-BURST)
      // ========================================================
      const now = Date.now();
      if (nextSocketConnectTime < now) {
        nextSocketConnectTime = now;
      }

      const delay = nextSocketConnectTime - now;

      // Cứ mỗi kênh được phát hiện Live, ép nó phải chờ thêm 2000ms (2 giây) so với kênh trước
      // -> Tốc độ cắm tối đa: 30 kênh / Phút -> Tuyệt đối an toàn trước TikTok!
      nextSocketConnectTime += 2000;

      setTimeout(() => {
        // Cắm Socket chuẩn sau khi đã chờ tới lượt
        startWebcast(channel, socketProxy, ua, false);
      }, delay);
      // ========================================================
    } else {
      masterSocket.emit("radar_result", { channel, status: "OFFLINE" });
      stopWebcast(channel.username);
    }
  } catch (e) {
    // 💡 FIX LỖI CRASH: Sử dụng biến checkProxy thay vì proxy ở khối này
    if (checkProxy !== "local") {
      const isNetworkError =
        e.message === "PROXY_DEAD" ||
        e.code === "ECONNREFUSED" ||
        e.code === "ETIMEDOUT" ||
        (e.message && e.message.includes("timeout")) ||
        (e.message && e.message.includes("socket"));

      const isRateLimit = e.message === "RATE_LIMIT";

      if (isRateLimit) {
        // Bị TikTok đánh gậy 429 -> Phạt nghỉ 45s
        proxyCooldown[checkProxy] = Date.now() + 45000;
        if (proxyHealth[checkProxy])
          proxyHealth[checkProxy].status = "TikTok chặn (Nghỉ 45s)";
      } else if (isNetworkError) {
        // Lỗi mạng thuần túy -> Phạt nhẹ 20s
        proxyCooldown[checkProxy] = Date.now() + 20000;
        if (proxyHealth[checkProxy])
          proxyHealth[checkProxy].status = "Lag mạng (Nghỉ 20s)";
      } else {
        // Lỗi khác
        proxyCooldown[checkProxy] = Date.now() + 30000;
        if (proxyHealth[checkProxy])
          proxyHealth[checkProxy].status = "Lỗi HTTP (Nghỉ 30s)";
      }

      // Cập nhật lại sức chứa
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

    masterSocket.emit("radar_result", { channel, status: "REQUEUE" });
    stopWebcast(channel.username);
  } finally {
    pendingChecks.delete(channel.username);
  }
}

function startWebcast(channel, proxy, ua) {
  const key = getNextEulerKey();
  const dynamicHeaders = buildDynamicHeaders(ua);

  // Dọn dẹp dấu @ thừa để đảm bảo URL luôn đúng chuẩn
  const cleanUser = channel.username.replace(/@/g, "");

  // 1. HTTP REQUEST (Bước lấy Token)
  let reqOptions = {
    headers: {
      ...dynamicHeaders,
      Referer: `https://www.tiktok.com/@${cleanUser}/live`,
    },
  };

  // 2. WEBSOCKET REQUEST (Chỉ dùng header cơ bản)
  let wsOptions = {
    headers: {
      "User-Agent": ua,
      Origin: "https://www.tiktok.com",
      Referer: `https://www.tiktok.com/@${cleanUser}/live`,
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

  const checkAndReportDeadKey = (errObj, targetKey) => {
    if (!targetKey) return false;

    // 💡 BÓC TÁCH LỖI CHỐNG TRỐNG CHO HÀM CHECK KEY
    let errText = "unknown error";
    if (errObj) {
      if (typeof errObj === "string") errText = errObj;
      else if (errObj.message) errText = errObj.message;
      else {
        try {
          errText = JSON.stringify(errObj);
          if (errText === "{}" || errText === "[]") errText = errObj.toString();
        } catch (e) {}
      }
    }

    const msg = String(errText).toLowerCase();

    if (
      msg.includes("rate limit") ||
      msg.includes("too many requests") ||
      msg.includes("timeout") ||
      msg.includes("socket")
    ) {
      return false;
    }

    const isDeadKey =
      msg.includes("insufficient balance") ||
      msg.includes("quota") ||
      msg.includes("invalid api key") ||
      msg.includes("unauthorized") ||
      msg.includes("key expired") ||
      msg.includes("forbidden") ||
      msg.includes("sign error") ||
      msg.includes("status 401");

    if (isDeadKey) {
      logError(
        `🔑 Key Euler [${targetKey.substring(0, 8)}...] lỗi/hết lượt. Xin Master cấp mới...`,
      );
      if (masterSocket && masterSocket.connected) {
        masterSocket.emit("worker_report_dead_key", {
          key: targetKey,
          workerName: config.workerName,
        });
      }
      return true;
    }
    return false;
  };

  const connectPromise = conn.connect();
  const timeoutPromise = new Promise((_, reject) => {
    setTimeout(() => reject(new Error("SOCKET_TIMEOUT")), 60000);
  });

  Promise.race([connectPromise, timeoutPromise])
    .then((state) => {
      activeConnections[channel.username] = conn;
      activeConnections[channel.username].lastActive = Date.now();

      logSuccess(
        `Cắm Socket thành công ${channel.username} - ${config.workerName}`,
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
      const isDeadKey = checkAndReportDeadKey(err, key);

      // 💡 BÓC TÁCH LỖI CHỐNG TRỐNG (Khắc phục lỗi "")
      let realErrorStr = "Lỗi không xác định";
      if (err) {
        if (typeof err === "string") {
          realErrorStr = err;
        } else if (err.message) {
          realErrorStr = err.message;
        } else {
          try {
            realErrorStr = JSON.stringify(err);
            if (realErrorStr === "{}" || realErrorStr === "[]")
              realErrorStr = err.toString();
          } catch (e) {
            realErrorStr = "Không thể đọc lỗi Object";
          }
        }
      }

      if (!realErrorStr || realErrorStr.trim() === "") {
        realErrorStr = "Lỗi ngầm từ thư viện proxy (Empty Error)";
      }

      const errMsg = String(realErrorStr).toLowerCase();
      let realStatus = "REQUEUE";

      if (
        errMsg.includes("not found") ||
        errMsg.includes("offline") ||
        errMsg.includes("isn't online") ||
        errMsg.includes("ended") ||
        errMsg.includes("room_id")
      ) {
        realStatus = "OFFLINE";
        logInfo(`Kênh ${channel.username} vừa tắt live hoặc ẩn danh.`);
      }
      // 💡 BỘ LỌC ĐÃ ĐƯỢC BỔ SUNG LẠI Ở ĐÂY (Sửa lỗi reading status)
      else if (
        errMsg.includes("socket_timeout") ||
        errMsg.includes("reading 'status'") ||
        errMsg.includes("properties of undefined") ||
        errMsg.includes("network") ||
        errMsg.includes("econnrefused") ||
        errMsg.includes("socket hang up") ||
        errMsg.includes("502") ||
        errMsg.includes("503") ||
        errMsg.includes("invalidresponseerror") ||
        errMsg.includes("too many connections") || // 💡 BỔ SUNG: Bắt lỗi kết nối dồn dập
        errMsg.includes("rate limited")
      ) {
        logWarn(
          `[KẸT SOCKET] ${channel.username} đứt bắt tay do Proxy lag. Trả về hàng đợi.`,
        );

        // CƠ CHẾ PHẠT PROXY NGHỈ 60 GIÂY
        if (proxy !== "local") {
          proxyCooldown[proxy] = Date.now() + 60000;
          if (proxyHealth[proxy])
            proxyHealth[proxy].status = "Nghỉ do lag Socket";

          let aliveCount = 0;
          let localAlive = proxyHealth["local"]?.status === "SẴN SÀNG";
          for (let p in proxyHealth) {
            if (p !== "local" && proxyHealth[p].status === "SẴN SÀNG")
              aliveCount++;
          }
          currentDynamicMaxLoad =
            aliveCount * config.loadPerProxy +
            (localAlive ? config.localLoad : 0);
          if (masterSocket && masterSocket.connected) {
            masterSocket.emit("worker_update_capacity", {
              maxLoad: currentDynamicMaxLoad,
            });
          }
        }
      } else if (errMsg.includes("suspended") || errMsg.includes("banned")) {
        realStatus = "ERROR";
      } else if (!isDeadKey) {
        sendMasterLog(
          `[SOCKET ĐỨT] ${channel.username}-${config.workerName}|Lỗi: ${realErrorStr}`,
        );
      }

      masterSocket.emit("radar_result", { channel, status: realStatus });
      stopWebcast(channel.username);
    });
}

function stopWebcast(user) {
  if (activeConnections[user]) {
    try {
      activeConnections[user].removeAllListeners();
      activeConnections[user].disconnect();
      // 💡 FIX: Ép clear client socket tránh leak memory khi kết nối rớt đột ngột
      if (activeConnections[user].client) {
        activeConnections[user].client.disconnect();
        activeConnections[user].client.removeAllListeners();
        if (activeConnections[user].client.ws) {
          activeConnections[user].client.ws.terminate();
        }
      }
    } catch (e) {}
    delete activeConnections[user];
  }
  if (assignedProxies[user]) {
    let realProxy = assignedProxies[user];
    if (proxyUsage[realProxy] !== undefined)
      proxyUsage[realProxy] = Math.max(0, proxyUsage[realProxy] - 1);
    delete assignedProxies[user];
  }
  pendingChecks.delete(user);
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
          // BỔ SUNG DÒNG NÀY ĐỂ DỌN TRẮNG RAM
          if (agentCache[p]) delete agentCache[p];
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

  // 1. Dọn dẹp các Socket im lặng quá lâu (30 phút không có tương tác mạng)
  for (let user in activeConnections) {
    const conn = activeConnections[user];
    const lastActivity = conn.lastActive || now;

    if (now - lastActivity > 30 * 60 * 1000) {
      stopWebcast(user);
      masterSocket.emit("radar_result", {
        channel: { username: user },
        status: "REQUEUE",
      });
      sendMasterLog(`[DỌN RÁC] 🧹 Socket ${user} chết lâm sàng > 30 phút!`);
    }
  }

  // 2. Dọn dẹp các truy vấn HTTP / Socket kẹt cứng
  for (let [user, timestamp] of pendingChecks.entries()) {
    if (now - timestamp > 30000) {
      // Quá 30s mà Axios/Webcast ko lên tiếng thì diệt
      stopWebcast(user);
      masterSocket.emit("radar_result", {
        channel: { username: user },
        status: "REQUEUE",
      });
      sendMasterLog(`[DỌN RÁC] 🧹 Truy vấn git ${user} kẹt mạng > 30s!`);
    }
  }
}, 30000);

connectToMaster();

process.on("uncaughtException", (err) => {
  logError(`[CRASH PROTECT] Lỗi không lường trước: ${err.message}`);
  console.error(err.stack);
});

process.on("unhandledRejection", (reason, promise) => {
  logError(`[CRASH PROTECT] Promise bị từ chối: ${reason}`);
});
