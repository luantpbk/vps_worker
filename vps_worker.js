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
const EulerStreamApiClient = require("@eulerstream/euler-api-sdk").default;

const CONFIG_FILE = "vps_config.json";
const EULER_PROXY_PER_KEY = 3;

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
let apiCheckCache = new Map();
let keyCooldown = {};
let keyStrikeCount = {};

const EULER_REQUESTS_PER_MINUTE = 12; // An toàn: Tối đa 12 kết nối/phút cho 1 Euler Key
let eulerRateLimiter = {};
function canUseEulerKey(key) {
  if (!key) return false;
  const now = Date.now();
  if (!eulerRateLimiter[key]) eulerRateLimiter[key] = [];

  // Chỉ giữ lại các request trong 60 giây gần nhất
  eulerRateLimiter[key] = eulerRateLimiter[key].filter((t) => now - t < 60000);

  return eulerRateLimiter[key].length < EULER_REQUESTS_PER_MINUTE;
}

function consumeEulerRequest(key) {
  if (!eulerRateLimiter[key]) eulerRateLimiter[key] = [];
  eulerRateLimiter[key].push(Date.now());
}

// Hàm tính toán chính xác số Slot Key còn trống để nhận Proxy mới
function getAvailableKeySlots() {
  const now = Date.now();
  let availableSlots = 0;
  const keyUsageCount = {};
  exclusiveEulerKeys.forEach((k) => (keyUsageCount[k] = 0));

  // Đếm số lượng Proxy đang bám vào từng Key
  for (let k of eulerKeyMap.values()) {
    if (keyUsageCount[k] !== undefined) keyUsageCount[k]++;
  }

  // Chỉ cộng dồn Slot của những Key chưa bị phạt và chưa dính Rate Limit
  exclusiveEulerKeys.forEach((k) => {
    if ((!keyCooldown[k] || now > keyCooldown[k]) && canUseEulerKey(k)) {
      availableSlots += Math.max(
        0,
        EULER_PROXY_PER_KEY - (keyUsageCount[k] || 0),
      );
    }
  });

  return availableSlots;
}

let pendingChecks = new Map();
let connectionLocks = new Map(); // 💡 FIX 4: Đã khai báo biến chống Ghost Load

let masterSocket = null;
let workerPausedUntil = 0;
let hasIPv6Support = false;

let dynamicProxies = [];
let zombieProxies = {};
let exclusiveEulerKeys = [];
let eulerKeyMap = new Map(); // 💡 BỔ SUNG: Map lưu trữ 1 Proxy -> 1 Euler Key
// ==========================================
// 💡 CƠ CHẾ KHÓA KEY (MUTEX): Đảm bảo các luồng dùng chung Key phải xếp hàng chờ nhau
// ==========================================
const keyInitMutex = {};

async function executeWithKeyMutex(key, taskFn) {
  // Nếu dùng mạng local (không có key Euler), bỏ qua khóa, cho chạy tự do
  if (!key || key === "local") return taskFn();

  // Nếu Key này chưa có hàng đợi, tạo mới
  if (!keyInitMutex[key]) keyInitMutex[key] = Promise.resolve();

  let releaseMutex;
  const nextTail = new Promise((resolve) => {
    releaseMutex = resolve;
  });
  const currentTail = keyInitMutex[key];

  // Nối luồng hiện tại vào đuôi hàng đợi
  keyInitMutex[key] = currentTail.then(() => nextTail);

  // ✋ Tạm dừng! Chờ các luồng đi trước giải quyết xong với Key này mới được chạy
  await currentTail;

  try {
    await taskFn(); // Thực thi việc kết nối
  } finally {
    releaseMutex(); // Chạy xong (dù thành công hay lỗi) đều phải nhả Khóa cho thằng sau
  }
}

const agentCache = {};
let localTaskQueue = [];

let proxyGeoData = {}; // Biến mới để lưu trữ quốc gia của từng proxy

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

  // Trả về map tương ứng, nếu quốc gia lạ thì mặc định là chuẩn Quốc Tế (US)
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

// setInterval(() => {
//   process.stdout.write("\x1Bc");
//   console.log("==========================================");
//   console.log(
//     `🚀 WORKER: ${config.workerName} | MASTER: ${masterSocket?.connected ? "ONLINE 🟢" : "OFFLINE 🔴"}`,
//   );
//   console.log(
//     `📊 TẢI HIỆN TẠI: ${Object.keys(activeConnections).length} / ${currentDynamicMaxLoad}`,
//   );
//   console.log(
//     `⏳ ĐANG CHECK HTTP: ${pendingChecks.size} | TRONG HÀNG ĐỢI: ${localTaskQueue.length}`,
//   );
//     console.log(`🔑 EULER KEYS: ${exclusiveEulerKeys.length} key độc quyền`);
//   console.log("------------------------------------------");
//   console.log("📡 TRẠNG THÁI PROXY:");
//   const tableData = (
//     config.useLocalNetwork ? ["local", ...dynamicProxies] : dynamicProxies
//   ).map((p) => ({
//     Proxy: getShortProxy(p),
//     "Đang cắm": `${proxyUsage[p] || 0}/${p === "local" ? config.localLoad : config.loadPerProxy}`,
//     "Tình trạng": proxyHealth[p]?.status || "ĐANG KIỂM TRA",
//   }));
//   console.table(tableData);
//   console.log("==========================================\n");
// }, 15000);

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
      ...eulerConnectionQueue.map((item) => item.channel.username), // 💡 Bổ sung mảng đang chờ Rate Limit
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

function updateDynamicCapacity() {
  let aliveProxyLoad =
    config.useLocalNetwork && proxyHealth["local"]?.status === "SẴN SÀNG"
      ? config.localLoad || 0
      : 0;
  for (let p of dynamicProxies) {
    if (proxyHealth[p]?.status === "SẴN SÀNG")
      aliveProxyLoad += config.loadPerProxy || 0;
  }

  const now = Date.now();
  const safeLoad = config.loadPerProxy > 0 ? config.loadPerProxy : 15;

  // SỬA ĐOẠN NÀY: Dùng tổng slot nhân với sức chứa
  let validSlots = 0;
  exclusiveEulerKeys.forEach((k) => {
    if ((!keyCooldown[k] || now > keyCooldown[k]) && canUseEulerKey(k)) {
      validSlots += EULER_PROXY_PER_KEY;
    }
  });

  const maxLoadFromKeys = validSlots * safeLoad;
  const newMaxLoad = Math.min(aliveProxyLoad, maxLoadFromKeys);

  if (newMaxLoad !== currentDynamicMaxLoad) {
    currentDynamicMaxLoad = newMaxLoad;
    if (masterSocket?.connected) {
      masterSocket.emit("worker_update_capacity", {
        maxLoad: currentDynamicMaxLoad,
      });
    }
  }
}

async function checkProxyHealth() {
  let checkList = [...dynamicProxies];
  if (config.useLocalNetwork) checkList.unshift("local");
  let currentHealth = {};

  // 💡 ƯU ĐIỂM HÀM 2: CHIA NHỎ MẢNG (CHUNKING)
  // Mỗi nhịp chỉ xử lý 5 request để bảo vệ RAM và giới hạn Socket của VPS
  const chunkSize = 5;
  for (let i = 0; i < checkList.length; i += chunkSize) {
    const chunk = checkList.slice(i, i + chunkSize);

    await Promise.all(
      chunk.map(async (p) => {
        // 💡 ƯU ĐIỂM HÀM 1: KIỂM TRA COOLDOWN ĐẦU TIÊN
        // Nếu proxy đang bị phạt nghỉ, bỏ qua luôn để đỡ tốn CPU và Băng thông
        if (proxyCooldown[p] && Date.now() < proxyCooldown[p]) {
          let remain = Math.ceil((proxyCooldown[p] - Date.now()) / 1000);
          currentHealth[p] = { status: `ĐANG NGHỈ (${remain}s)` };
          return;
        }

        // 💡 ƯU ĐIỂM HÀM 2: MÁY CHÉM THỜI GIAN STRICT TIMEOUT
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 8000); // Cho phép tối đa 8s

        let options = { signal: controller.signal };

        if (p !== "local") {
          // Xử lý Agent chuẩn xác cho cả HTTP và HTTPS
          const proxyAgent = getCachedAgent(p);
          options.httpAgent = proxyAgent;
          options.httpsAgent = proxyAgent;
        }

        try {
          // ========================================================
          // 💡 TÍNH NĂNG MỚI: LẤY VÀ LƯU COUNTRY CODE (CHỈ CHẠY 1 LẦN)
          // ========================================================
          if (p !== "local" && !proxyGeoData[p]) {
            try {
              const geoRes = await axios.get(
                "http://ip-api.com/json/?fields=countryCode",
                options,
              );
              if (geoRes.data && geoRes.data.countryCode) {
                proxyGeoData[p] = geoRes.data.countryCode;
              } else {
                proxyGeoData[p] = "VN"; // Mặc định nếu API trả về rỗng
              }
            } catch (geoErr) {
              // Gán tạm thời để lần sau không gọi lại nếu API sập, tránh treo proxy
              proxyGeoData[p] = "UNKNOWN";
            }
          }

          // ========================================================
          // 💡 PING KIỂM TRA SỨC KHỎE
          // ========================================================
          const healthRes = await axios.get(
            "https://clients3.google.com/generate_204",
            options,
          );
          clearTimeout(timeoutId); // Gỡ mìn nếu thành công

          if (healthRes.status === 200 || healthRes.status === 204) {
            currentHealth[p] = {
              status: "SẴN SÀNG",
              country: proxyGeoData[p] || "VN", // Đính kèm quốc gia vào status để đẩy lên Master
            };
            proxyFailCount[p] = 0; // Reset số lần lỗi
          } else {
            throw new Error(`HTTP Lỗi ${healthRes.status}`);
          }
        } catch (e) {
          clearTimeout(timeoutId); // Gỡ mìn nếu lỗi

          currentHealth[p] = { status: "MẤT KẾT NỐI" };

          if (p !== "local") {
            // 💡 ƯU ĐIỂM HÀM 1: BẢO VỆ CHỐNG DỌN DẸP LỖI (Bug Bất Tử)
            if (proxyFailCount[p] < 0) return;

            proxyFailCount[p] = (proxyFailCount[p] || 0) + 1;
            const errMsg = e.message || "";

            // 💡 ƯU ĐIỂM HÀM 1: FAST-KILL (TỬ HÌNH NHANH)
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

            // 💡 XỬ LÝ LỖI TRỌNG TÂM THEO HÀM 1
            if (proxyFailCount[p] >= 3) {
              currentHealth[p].status = "BÁO LỖI";

              if (masterSocket?.connected) {
                masterSocket.emit("worker_report_dead_proxy", {
                  proxy: p,
                  workerName: config.workerName,
                });
              }

              retireProxy(p); // Vứt ngay lập tức một cách an toàn
              proxyFailCount[p] = -9999; // Cờ hiệu: Đã tử hình
            } else {
              // Phạt nghỉ 20s chờ phục hồi
              proxyCooldown[p] = Date.now() + 20000;
            }
          } else {
            // Mạng Local đứt thì cho nghỉ 20s rồi thử lại
            proxyCooldown["local"] = Date.now() + 20000;
          }
        }
      }),
    );
  }

  // Cập nhật lại kho máu chung
  proxyHealth = currentHealth;

  updateDynamicCapacity();
}
setInterval(checkProxyHealth, 180000);
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

// 💡 BỔ SUNG: Hàm ghim cứng Euler Key theo Proxy để tránh lỗi spam từ nhiều IP
function getEulerKeyForProxy(proxyStr) {
  let mappingStr = proxyStr;
  if (proxyStr === "local") {
    const currentLocalUsage = proxyUsage["local"] || 0;
    const safeLoad = config.loadPerProxy > 0 ? config.loadPerProxy : 15;
    const virtualIndex = Math.floor(currentLocalUsage / safeLoad);
    mappingStr = `local_e_${virtualIndex}`;
  }

  const now = Date.now();

  if (eulerKeyMap.has(mappingStr)) {
    const mappedKey = eulerKeyMap.get(mappingStr);
    // 💡 XÓA GHIM KEY ĐỂ PROXY ĐI TÌM KEY MỚI
    if (
      (keyCooldown[mappedKey] && now < keyCooldown[mappedKey]) ||
      !canUseEulerKey(mappedKey)
    ) {
      eulerKeyMap.delete(mappingStr);
    } else {
      return mappedKey;
    }
  }

  const keyUsageCount = {};
  exclusiveEulerKeys.forEach((k) => (keyUsageCount[k] = 0));
  for (let k of eulerKeyMap.values()) {
    if (keyUsageCount[k] !== undefined) keyUsageCount[k]++;
  }

  const availableKey = exclusiveEulerKeys.find(
    (k) =>
      keyUsageCount[k] < EULER_PROXY_PER_KEY &&
      (!keyCooldown[k] || now > keyCooldown[k]) &&
      canUseEulerKey(k),
  );

  if (availableKey) {
    eulerKeyMap.set(mappingStr, availableKey);
    return availableKey;
  }
  return null;
}

function formatProxyUrl(rawProxy) {
  if (!rawProxy || typeof rawProxy !== "string") return null;
  rawProxy = rawProxy.trim();

  // 💡 Lớp 1: Nếu đã nhập đúng chuẩn URL từ Master thì trả về dùng luôn (Dùng được cho cả IPv6 đã bọc ngoặc vuông)
  if (
    rawProxy.startsWith("http://") ||
    rawProxy.startsWith("https://") ||
    rawProxy.startsWith("socks")
  ) {
    return rawProxy;
  }

  // 💡 Lớp 2: Nếu chuỗi đã chứa @ (Ví dụ: user:pass@1.2.3.4:8000)
  if (rawProxy.includes("@")) {
    return `http://${rawProxy}`;
  }

  // Tách chuỗi để xử lý định dạng dán thô
  const parts = rawProxy.split(":");

  // 💡 Lớp 3: Xử lý IPv4 hoặc Hostname Domain (VD: 1.1.1.1:8000:user:pass hoặc gate.proxy.com:8000:user:pass)
  if (parts.length === 4 && !rawProxy.includes("[")) {
    return `http://${parts[2]}:${parts[3]}@${parts[0]}:${parts[1]}`;
  } else if (parts.length === 2 && !rawProxy.includes("[")) {
    return `http://${parts[0]}:${parts[1]}`;
  }

  // 💡 Lớp 4: Xử lý IPv6 Thuần (Rất nhiều dấu hai chấm)
  // Dạng dán thô: 2001:db8::1:8000:user:pass
  if (parts.length >= 6) {
    const pass = parts.pop(); // Lấy Pass ra khỏi đuôi
    const user = parts.pop(); // Lấy User ra khỏi đuôi
    const port = parts.pop(); // Lấy Port ra khỏi đuôi
    const ipv6Raw = parts.join(":"); // Phần còn lại chính là IPv6

    // Bắt buộc phải bọc IPv6 trong ngoặc vuông [ ] theo chuẩn mạng quốc tế
    const safeIpv6 = ipv6Raw.startsWith("[") ? ipv6Raw : `[${ipv6Raw}]`;
    return `http://${user}:${pass}@${safeIpv6}:${port}`;
  }

  // Fallback an toàn
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

  if (proxy === "local") {
    for (let key of eulerKeyMap.keys()) {
      if (key.startsWith("local_e_")) eulerKeyMap.delete(key);
    }
  } else {
    eulerKeyMap.delete(proxy);
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
      proxyCount: config.proxyCount,
      useLocalNetwork: config.useLocalNetwork,
      runningChannels: Object.keys(activeConnections),
      pendingChannels: Array.from(pendingChecks.keys()),
      heldProxies: dynamicProxies,
      heldKeys: exclusiveEulerKeys,
      activeLibrary: config.activeLibrary || "tiktok-live-connector",
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

    // 3. Rẽ nhánh xin cấp Key dựa vào cấu hình thư viện
    const currentLib = config.activeLibrary;
    const safeLoad = config.loadPerProxy > 0 ? config.loadPerProxy : 15;
    const totalNodeCapacity =
      config.proxyCount +
      (config.useLocalNetwork ? config.localLoad / safeLoad : 0); // 💡 BỔ SUNG TÍNH TOÁN LOCAL

    const neededKeys = Math.max(
      0,
      Math.ceil(totalNodeCapacity / EULER_PROXY_PER_KEY) -
        exclusiveEulerKeys.length,
    );
    if (neededKeys > 0) {
      masterSocket.emit("worker_request_keys", {
        count: neededKeys,
        workerName: config.workerName,
        library: "euler",
      });
    }
    logInfo(
      `🔄 Khởi tạo luồng bằng Euler. Đang kiểm tra/xin cấp ${neededKeys} Keys...`,
    );
  });

  masterSocket.on("worker_receive_keys", (data) => {
    const keysList = Array.isArray(data) ? data : data.keys || [];
    exclusiveEulerKeys = Array.from(
      new Set([...exclusiveEulerKeys, ...keysList]),
    );
  });

  masterSocket.on("worker_key_replacement", (data) => {
    exclusiveEulerKeys = exclusiveEulerKeys.filter((k) => k !== data.deadKey);

    // 💡 BẢN VÁ: Thêm log để bạn nhìn thấy tiến trình đổi Key
    if (data.newKey && !exclusiveEulerKeys.includes(data.newKey)) {
      exclusiveEulerKeys.push(data.newKey);
      logSuccess(
        `🔄 Đổi Key: Vứt bỏ [${data.deadKey}] -> Nạp mới [${data.newKey}]`,
      );
    } else {
      logWarn(
        `⚠️ Đã vứt bỏ [${data.deadKey}] nhưng Master báo KHO ĐÃ HẾT KEY DỰ PHÒNG!`,
      );
    }

    for (let [p, k] of eulerKeyMap.entries()) {
      if (k === data.deadKey) eulerKeyMap.delete(p);
    }

    delete keyStrikeCount[data.deadKey];
    delete keyCooldown[data.deadKey];
    delete eulerRateLimiter[data.deadKey];
    delete keyInitMutex[data.deadKey]; // 💡 THÊM VÀO ĐÂY
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

  // ==========================================
  // 💡 BỔ SUNG: LẮNG NGHE LỆNH ĐIỀU KHIỂN CỤC BỘ
  // ==========================================
  masterSocket.on("cmd_pause_worker", () => {
    logWarn(
      "⏸️ Nhận lệnh TẠM DỪNG từ Master. Ngừng nhận kênh mới, chờ xả tải dần...",
    );
    workerPausedUntil = Infinity; // Khóa chức năng lấy việc
  });

  masterSocket.on("cmd_resume_worker", () => {
    logSuccess("▶️ Nhận lệnh TIẾP TỤC từ Master. Bắt đầu nhận kênh mới!");
    workerPausedUntil = 0; // Mở khóa chức năng lấy việc
  });

  masterSocket.on("cmd_stop_worker", () => {
    logWarn(
      "⏹️ Nhận lệnh DỪNG HẲN (STOP) từ Master. Rút điện toàn bộ hệ thống!",
    );
    workerPausedUntil = Infinity; // Khóa lấy việc

    // 1. Xóa sạch hàng đợi chưa kịp check
    localTaskQueue = [];
    eulerConnectionQueue = [];
    // 💡 VÁ LỖI CẤP KIẾN TRÚC 2: Rút điện TẤT CẢ các kênh đang giữ Proxy (Cả đang Live và Đang Queue)
    const usersToClean = new Set([
      ...Object.keys(activeConnections),
      ...connectionLocks.keys(),
      ...Object.keys(assignedProxies),
    ]);

    usersToClean.forEach((user) => {
      safeEmitRadarResult({ channel: { username: user }, status: "REQUEUE" });
      stopWebcast(user);
    });

    connectionLocks.clear();
  });

  masterSocket.on("process_task", (channel) => {
    // 💡 Chặn kênh lỗi (Rác) ngay từ cửa nhận việc
    if (!channel || !channel.username) return;
    const maxAllowedQueue = Math.max(20, currentDynamicMaxLoad * 2);
    if (localTaskQueue.length >= maxAllowedQueue)
      return safeEmitRadarResult({ channel, status: "REQUEUE" });
    if (
      !localTaskQueue.some((c) => c.username === channel.username) &&
      !pendingChecks.has(channel.username) &&
      !activeConnections[channel.username] &&
      !connectionLocks.has(channel.username) && // 💡 VÁ LỖI 1: Chặn kênh đang khởi tạo Socket
      !eulerConnectionQueue.some(
        (item) => item.channel.username === channel.username,
      ) // 💡 VÁ LỖI 1: Chặn kênh đang xếp hàng chờ mây
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

    // Ghi đè thư viện, các socket cũ vẫn giữ library cũ, socket mới sẽ đọc config này
    if (newCfg.activeLibrary) {
      config.activeLibrary = newCfg.activeLibrary;
      logWarn(
        `🔄 Đã chuyển đổi thư viện kết nối mới sang: ${config.activeLibrary}`,
      );
    }

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
        eulerConnectionQueue = [];
        // 💡 VÁ LỖI: Dọn sạch mọi vết tích có giữ Proxy
        const usersToClean = new Set([
          ...Object.keys(activeConnections),
          ...connectionLocks.keys(),
          ...Object.keys(assignedProxies),
        ]);
        usersToClean.forEach((user) => stopWebcast(user));
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

  if (getAvailableKeySlots() <= 0) return; // Nếu hết sạch Slot khả dụng, nhường CPU nghỉ ngơi không check HTTP nữa
  isProcessingQueue = true;

  try {
    // 💡 1. TÍNH TOÁN LẠI TỔNG TẢI DỰ KIẾN (Rất quan trọng)
    // Tổng tải = Đã cắm + Đang xếp hàng chờ cắm + Đang check HTTP
    const currentActive = Object.keys(activeConnections).length;
    const totalIntendedLoad =
      currentActive + eulerConnectionQueue.length + pendingChecks.size;

    if (
      currentDynamicMaxLoad === 0 ||
      totalIntendedLoad >= currentDynamicMaxLoad
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
    // 💡 2. KIỂM SOÁT SLOT: Chỉ check đúng số lượng kênh để lấp đầy phần tải còn thiếu
    const loadShortage = currentDynamicMaxLoad - totalIntendedLoad;
    const actualSlots = Math.min(availableCheckSlots, loadShortage);

    // Không còn slot nào thì dừng luôn vòng lặp
    if (actualSlots <= 0) return;

    const tasksToProcess = Math.min(actualSlots, localTaskQueue.length);
    // 💡 VÁ LỖI SPAM HTTP (429): Rải đều các request check HTTP ra 2 giây thay vì gọi đồng loạt cục bộ
    const delayStep = 2000 / Math.max(1, tasksToProcess);

    for (let i = 0; i < tasksToProcess; i++) {
      const channel = localTaskQueue.splice(0, 1)[0];
      pendingChecks.set(channel.username, Date.now());

      setTimeout(
        () => {
          executeTask(channel);
        },
        i * delayStep + Math.floor(Math.random() * 300),
      ); // Rải đều + Jitter nhẹ
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
      // 💡 LẤY THÔNG SỐ VÙNG MIỀN THEO PROXY (NẾU CHƯA KỊP QUÉT THÌ MẶC ĐỊNH VN)
      const currentCountry = proxyGeoData[proxy] || "VN";
      const geo = getGeoParams(currentCountry);
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
          locales: [geo.lang, "en-US"],
        },
      });

      // 💡 LỚP GIÁP 1: Ép timeout cứng 15s phòng trường hợp thư viện got bị treo ngầm
      let timeoutHandle;
      const hardTimeout = new Promise((_, r) => {
        timeoutHandle = setTimeout(() => {
          // ==========================================
          // 💡 VÁ LỖI RÒ RỈ 2: CẮT ĐỨT KẾT NỐI TCP
          // Nếu gotScraping bị treo, ép hủy Promise để trả lại tài nguyên mạng
          // ==========================================
          if (typeof fetchPromise.cancel === "function") {
            fetchPromise.cancel();
          }
          r(new Error("HARD_TIMEOUT"));
        }, 12000);
      });

      // THAY BẰNG:
      let res;
      try {
        res = await Promise.race([fetchPromise, hardTimeout]);
      } finally {
        clearTimeout(timeoutHandle); // 💡 VÁ LỖI TỬ HUYỆT: Luôn tắt đồng hồ dù thành công hay văng lỗi sớm
      }

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
        typeof res.body === "string"
          ? res.body
          : JSON.stringify(res.body || "");

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
      retries--;
      if (retries < 0) {
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
      // Nghỉ 1 giây trước khi thử lại
      await new Promise((r) => setTimeout(r, 1000));
    }
  }
}

// ========================================================
// 💡 HÀNG ĐỢI CHỐNG SPAM EULER (RATE LIMIT TỪNG GIÂY)
// ========================================================
let eulerConnectionQueue = [];
let isConnectingEuler = false;
setInterval(() => {
  // Bỏ async vì không cần await bên trong
  if (
    isConnectingEuler ||
    eulerConnectionQueue.length === 0 ||
    Date.now() < workerPausedUntil
  )
    return;

  isConnectingEuler = true; // 🔒 KHÓA LUỒNG
  try {
    // 1. Chỉ Nhìn (Peek) chứ chưa Rút vội
    const item = eulerConnectionQueue[0];
    const { channel, proxy } = item;

    // 2. Kiểm tra các điều kiện loại trừ sớm
    if (!connectionLocks.has(channel.username)) {
      eulerConnectionQueue.shift(); // Dọn rác
      stopWebcast(channel.username);
      isConnectingEuler = false; // 🔓 MỞ KHÓA
      return;
    }

    if (activeConnections[channel.username]) {
      eulerConnectionQueue.shift(); // Dọn rác
      isConnectingEuler = false; // 🔓 MỞ KHÓA
      return;
    }

    // 3. Chờ Token mạng
    if (globalConnectTokens <= 0) {
      isConnectingEuler = false; // 🔓 MỞ KHÓA (Giữ nguyên kênh trong Queue để chờ nhịp sau)
      return;
    }

    // 💡 BỔ SUNG CHỐT CHẶN BẢO VỆ: Kiểm tra xem có Key nào còn Quota phút không
    const now = Date.now();
    const hasKeyWithQuota = exclusiveEulerKeys.some(
      (k) => (!keyCooldown[k] || now > keyCooldown[k]) && canUseEulerKey(k),
    );

    if (!hasKeyWithQuota) {
      // Giữ nguyên kênh trong Queue chờ vài giây đến khi Quota phút nhả ra
      isConnectingEuler = false;
      return;
    }

    // 4. MỌI THỨ HỢP LỆ -> Bắt đầu xử lý
    eulerConnectionQueue.shift(); // Chính thức rút kênh ra
    globalConnectTokens--;
    startWebcast(channel, proxy);

    // 5. Tính toán thời gian nghỉ ngơi giữa các lần cắm
    let activeKeys = Math.max(1, exclusiveEulerKeys.length);
    let dynamicDelay = Math.max(400, 4000 / activeKeys);
    const jitter = Math.floor(Math.random() * 200);
    const totalDelay = Math.round(dynamicDelay + jitter);

    // 6. Mở khóa từ từ sau khi trễ
    setTimeout(() => {
      isConnectingEuler = false; // 🔓 MỞ KHÓA SAU DELAY
    }, totalDelay);
  } catch (err) {
    logError(`Lỗi khi khởi tạo Socket luồng chờ: ${err.message}`);
    // Đề phòng "Kênh Độc" gây lỗi, phải shift() vứt bỏ để không kẹt mãi 1 chỗ
    if (eulerConnectionQueue.length > 0) eulerConnectionQueue.shift();
    isConnectingEuler = false; // 🔓 MỞ KHÓA KHẨN CẤP
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
  // THAY BẰNG:
  if (getAvailableKeySlots() <= 0) {
    pendingChecks.delete(channel.username);
    updateDynamicCapacity();
    return safeEmitRadarResult({ channel, status: "SYSTEM_BUSY" });
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
    return safeEmitRadarResult({ channel, status: "SYSTEM_BUSY" });
  }

  const checkProxy =
    availableProxies[Math.floor(Math.random() * availableProxies.length)];

  try {
    const status = await checkLiveStatus(channel.username, checkProxy);
    // ==========================================
    // 💡 VÁ LỖI 2: CHỐNG CẮM ĐÚP DO HTTP LAG QUÁ 45 GIÂY
    // Nếu bị hàm dọn rác xóa pendingChecks và báo REQUEUE rồi thì RÚT LUI ngay!
    // ==========================================
    if (!pendingChecks.has(channel.username)) {
      logWarn(
        `[GHOST HTTP] Kênh ${channel.username} check quá lâu và đã bị Requeue. Hủy lệnh cắm!`,
      );
      return;
    }
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

        if (proxyStrikeCount[checkProxy] >= 4) {
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

      // 💡 ĐÓNG KHÓA NGAY LẬP TỨC ĐỂ TRÁNH BỊ CHECK LẠI, VÀ ĐẨY VÀO HÀNG ĐỢI
      connectionLocks.set(channel.username, Date.now());
      eulerConnectionQueue.push({ channel, proxy: socketProxy });
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

  // Ghi nhớ thư viện được khởi tạo tại thời điểm này
  let key, conn;
  let roomId = null;
  let pendingBoxes = [];
  let currentViewers = 0;
  let isProcessingInitial = true;
  // Bảo vệ an toàn: Chặn kênh lỗi dữ liệu để thư viện không crash hàm .replace()
  if (!channel || !channel.username)
    throw new Error("Dữ liệu kênh bị rỗng (Undefined Username)");

  // 💡 NÂNG CẤP: Gọi hàm ghim Key theo Proxy thay vì lấy ngẫu nhiên
  key = getEulerKeyForProxy(proxy);
  // 💡 VÁ LỖI 2: Phải gọi stopWebcast để trừ tải cho Proxy
  if (!key) {
    logInfo(
      `⏳ Thiếu Euler Key cho kênh [${channel.username}]. Đang Rate Limit hoặc hết Key...`,
    );
    stopWebcast(channel.username);
    updateDynamicCapacity(); // 💡 Cập nhật sức chứa
    setTimeout(() => {
      safeEmitRadarResult({ channel, status: "SYSTEM_BUSY" });
    }, 500);
    return;
  }
  const proxyAgent = getCachedAgent(proxy);
  // Khối else của Direct Socket...
  const eulerOptions = {
    signApiKey: key,
    webClientOptions: {
      agent: {
        http: proxyAgent,
        https: proxyAgent,
      },
    },
    websocketOptions: {
      agent: proxyAgent,
    },
    processInitialData: true,
    fetchRoomInfoOnConnect: true,
    clientParams: {
      browser_language: `${geo.lang}-${geo.region}`,
      app_language: geo.lang,
      webcast_language: geo.lang,
      region: geo.region,
    },
  };
  conn = new TikTokLiveConnection(channel.username, eulerOptions);

  const checkAndReportDeadKey = async (errObj, targetKey) => {
    if (!targetKey) return false;

    const now = Date.now();
    if (keyCooldown[targetKey] && now < keyCooldown[targetKey]) return true;

    // 💡 BẢN VÁ: Ép kiểu chuỗi siêu cứng, gom toàn bộ Object và ký tự ẩn
    let errText = String(errObj?.message || errObj).toLowerCase();

    // Bỏ qua lỗi của Proxy bẩn
    if (
      errText.includes("reading 'retry-after'") ||
      errText.includes("properties of undefined")
    ) {
      return false;
    }
    // ==========================================
    // 💡 BẢN VÁ: THÊM LẠI LỚP BẢO VỆ SERVER EULER SẬP
    // ==========================================
    if (
      errText.includes("status 500") ||
      errText.includes("status 502") ||
      errText.includes("status 503") ||
      errText.includes("status 504")
    ) {
      logWarn(
        `[⚠️] 🌐 SERVER EULER NGHẼN MẠNG (50x): Cho Key [${targetKey.substring(0, 5)}...] nghỉ 30s!`,
      );
      keyCooldown[targetKey] = now + 30000;
      return true; // Chặn đứng việc gọi check Quota dư thừa
    }
    const report_key = (key) => {
      // 💡 VÁ LỖI TỬ HUYỆT: Xóa sạch Key khỏi bộ nhớ cục bộ NGAY VÀ LUÔN
      // Không chờ đợi Master. Cắt đứt hoàn toàn liên kết của Proxy với Key này.
      exclusiveEulerKeys = exclusiveEulerKeys.filter((k) => k !== key);

      for (let [p, k] of eulerKeyMap.entries()) {
        if (k === key) eulerKeyMap.delete(p);
      }

      // Xóa án tích để tránh kẹt logic
      delete keyCooldown[key];
      delete keyStrikeCount[key];
      delete keyInitMutex[key];
      if (masterSocket?.connected) {
        masterSocket.emit("worker_report_dead_key", {
          key: key,
          workerName: config.workerName,
        });
      }
    };

    // ==========================================
    // 1. ÁN TỬ TỨC THÌ (LỖI CỨNG 100%): Báo Master đổi Key ngay
    // ==========================================
    if (
      errText.includes("insufficient balance") ||
      errText.includes("api key is invalid") ||
      errText.includes("invalid api key") ||
      errText.includes("unauthorized") ||
      errText.includes("sign server status 401") ||
      errText.includes("sign error")
    ) {
      logWarn(
        `[❌] 🔑 KEY LỖI CỨNG: Đã bắt được lỗi 401. Báo Master thu hồi Key này!`,
      );
      report_key(targetKey);
      return true;
    }

    // ==========================================
    // 2. CÁC LỖI KHÁC: CHỦ ĐỘNG GỌI CHECK RATE LIMIT
    // ==========================================
    // 💡 KHÓA TẠM THỜI (OPTIMISTIC LOCK): Phạt ngay 15s trước khi gọi API để
    // các socket khác không lao vào spam trong lúc chờ Euler phản hồi.
    keyCooldown[targetKey] = now + 15000;

    const cacheKey = `quota_${targetKey}`;

    // 💡 CHỐNG SPAM CALL API: Dù có khóa tạm thời, ta vẫn đảm bảo chỉ gọi 1 phút/lần
    if (
      apiCheckCache.has(cacheKey) &&
      now - apiCheckCache.get(cacheKey) < 60000
    ) {
      // Vẫn kill socket (return true) nhưng không gọi API để tránh dính đòn chặn IP của Euler
      return true;
    }

    apiCheckCache.set(cacheKey, now);

    try {
      const eulerClient = new EulerStreamApiClient({ apiKey: targetKey });
      const res = await eulerClient.webcast.getRateLimits();

      if (res && res.data && res.data.day) {
        const remaining = res.data.day.remaining;

        if (remaining <= 0) {
          logWarn(
            `[❌] 🔑 KEY HẾT QUOTA (Còn 0 lượt): Báo Master đổi Key mới!`,
          );
          report_key(targetKey);
          return true;
        } else {
          // Còn Quota nhưng vẫn văng lỗi (Do nghẽn mạng TikTok, IP Proxy bẩn...)
          keyStrikeCount[targetKey] = (keyStrikeCount[targetKey] || 0) + 1;

          // 💡 GIẢM TỪ 5 XUỐNG 3 THEO YÊU CẦU
          if (keyStrikeCount[targetKey] >= 3) {
            logWarn(
              `[❌] 🔑 KEY CÒN QUOTA (${remaining}) NHƯNG LỖI 3 LẦN LIÊN TIẾP: Ép Master đổi Key mới!`,
            );
            report_key(targetKey);
            return true;
          } else {
            logWarn(
              `[🛑] KEY CÒN QUOTA (${remaining}) NHƯNG BỊ LỖI (Lần ${keyStrikeCount[targetKey]}/3): Cho Key ngủ 60s!`,
            );
            keyCooldown[targetKey] = now + 60000; // Khóa đúng 60s
            return true;
          }
        }
      }
    } catch (apiErr) {
      // ==========================================
      // 💡 VÁ LỖI VÒNG LẶP: Nếu API Euler bị sập (timeout, 502...)
      // Tuyệt đối không được bỏ qua. Phải tính 1 gậy và phạt nghỉ!
      // ==========================================
      logWarn(
        `⚠️ Lỗi kiểm tra Quota Euler: ${apiErr.message}. Bắt buộc phạt Key ngủ 30s...`,
      );

      keyStrikeCount[targetKey] = (keyStrikeCount[targetKey] || 0) + 1;

      if (keyStrikeCount[targetKey] >= 3) {
        logWarn(
          `[❌] 🔑 API EULER TỪ CHỐI KẾT NỐI 3 LẦN LIÊN TIẾP. Ép đổi Key mới!`,
        );
        report_key(targetKey);
      } else {
        keyCooldown[targetKey] = now + 30000; // Phạt nhẹ 30s
      }
      return true;
    }

    return false;
  };

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
    // TLC trả về u.viewerCount, Euler JSON có thể trả về u.viewer_count
    const views = u?.viewerCount || u?.viewer_count || u?.totalUser;
    if (views) currentViewers = views;
  });
  conn.on("warn", async (err) => {
    const isDead = await checkAndReportDeadKey(err, key); // 💡 Khai báo an toàn
    if (isDead) stopWebcast(channel.username);
  });
  // 💡 VÁ LỖI CỰC KỲ QUAN TRỌNG: Requeue khi rớt mạng
  conn.on("error", async (err) => {
    const isDead = await checkAndReportDeadKey(err, key);
    if (isDead) {
      stopWebcast(channel.username);
      return;
    }

    if (activeConnections[channel.username]) {
      stopWebcast(channel.username);
      safeEmitRadarResult({ channel, status: "REQUEUE" });
    }
  });
  conn.on("streamEnd", () => {
    // 💡 VÁ LỖI
    if (activeConnections[channel.username]) {
      stopWebcast(channel.username);
      safeEmitRadarResult({ channel, status: "OFFLINE", proxy });
    }
  });
  conn.on("disconnected", () => {
    // 💡 VÁ LỖI
    if (activeConnections[channel.username]) {
      stopWebcast(channel.username);
      safeEmitRadarResult({ channel, status: "OFFLINE", proxy });
    }
  });

  let timeoutHandle;
  const timeoutPromise = new Promise((_, r) => {
    timeoutHandle = setTimeout(() => r(new Error("SOCKET_TIMEOUT")), 60000);
  });

  consumeEulerRequest(key); // 💡 Kích hoạt ghi nhận lượt dùng cho Euler Key
  // ✅ Thay bằng lệnh bọc Mutex này:
  executeWithKeyMutex(key, async () => {
    // 💡 LỚP BẢO VỆ CHÉO: Khi luồng được mở Khóa, việc đầu tiên là nhìn xem
    // Key này có vừa bị thằng đi trước tống vào viện dưỡng lão (Cooldown) hay chưa!
    if (keyCooldown[key] && Date.now() < keyCooldown[key]) {
      // Nhường CPU, im lặng hủy kết nối để dọn rác log
      safeEmitRadarResult({ channel, status: "ERROR" });
      return;
    }

    // BẮT BUỘC PHẢI CÓ 'await' Ở ĐÂY ĐỂ GIỮ KHÓA CHO ĐẾN KHI CHẠY XONG catch/then
    await Promise.race([conn.connect(), timeoutPromise])
      .then((state) => {
        // ==========================================
        // 💡 VÁ LỖI 3: CHỐNG CẮM ĐÚP DO SOCKET LAG QUÁ 60 GIÂY
        // Nếu đã bị dọn rác xóa connectionLocks, tuyệt đối không được giữ cái socket vừa nối thành công này!
        // ==========================================
        if (
          !connectionLocks.has(channel.username) &&
          !activeConnections[channel.username]
        ) {
          logWarn(
            `[GHOST SOCKET] Kênh ${channel.username} nối thành công nhưng quá hạn 120s. Rút ống thở ngay!`,
          );
          clearTimeout(timeoutHandle);
          try {
            if (typeof conn.disconnect === "function") conn.disconnect();
            if (conn.client?.ws) conn.client.ws.terminate();
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

        // 💡 ĐÃ SỬA: Đọc cờ trực tiếp từ đối tượng conn
        activeConnections[channel.username].usedKey = key;
        // ==========================================
        // 💡 RỬA TỘI: KHI KEY HOẠT ĐỘNG TỐT, XÓA MỌI ÁN PHẠT
        // ==========================================
        delete keyStrikeCount[key];
        proxyStrikeCount[proxy] = 0;
        logSuccess(
          `✅ [${channel.username}] Kết nối thành công qua (${getShortProxy(proxy)})`,
        );

        setTimeout(() => {
          isProcessingInitial = false;
        }, 5000);
      })
      .catch(async (err) => {
        clearTimeout(timeoutHandle);
        try {
          if (typeof conn.removeAllListeners === "function")
            conn.removeAllListeners();
          if (typeof conn.disconnect === "function") conn.disconnect();
          if (
            conn.client?.ws &&
            typeof conn.client.ws.terminate === "function"
          ) {
            conn.client.ws.terminate();
          }
        } catch (e) {}
        // 💡 CHIẾN THUẬT CẮT BÃO LOG (CHỐNG ẢO GIÁC)
        // Nếu Key đã bị Master thu hồi khỏi kho, các kết nối đang chạy dở sẽ tự hủy trong im lặng
        if (!exclusiveEulerKeys.includes(key)) {
          stopWebcast(channel.username);
          return;
        }
        // 💡 BẢN VÁ: XỬ LÝ ÊM ÁI LỖI TIMEOUT (Do mạng lag, Proxy chậm)
        if (errMsg.includes("socket_timeout")) {
          logWarn(
            `[⏳] Kênh ${channel.username} quá hạn kết nối 15s (Proxy chậm). Sẽ thử lại sau!`,
          );
          safeEmitRadarResult({ channel, status: "ERROR" });
          stopWebcast(channel.username);
          return; // Thoát luôn, không ném vào check Key hay phạt Proxy vì đây chỉ là lag mạng
        }
        const isKeyDead = await checkAndReportDeadKey(err, key);
        let errMsg = String(err?.message || err).toLowerCase();

        logWarn(
          `[SOCKET LỖI] Kênh: ${channel.username} | Proxy: ${getShortProxy(proxy)} | Lỗi: ${errMsg}`,
        );

        if (isKeyDead) {
          setTimeout(() => {
            safeEmitRadarResult({ channel, status: "ERROR" });
          }, 2000);
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
          errMsg.includes("reading 'retry-after'") // 💡 THÊM TỪ KHÓA NÀY ĐỂ BẮT ĐÚNG BỆNH
        ) {
          if (proxy !== "local") {
            // Tăng gậy phạt cho proxy này
            proxyStrikeCount[proxy] = (proxyStrikeCount[proxy] || 0) + 1;

            // 💡 BẢN VÁ: Giảm giới hạn chịu đựng từ 4 xuống 3 và in log thông báo số gậy
            if (proxyStrikeCount[proxy] >= 3) {
              logWarn(
                `[❌] 🗑️ Proxy [${getShortProxy(proxy)}] dính lỗi 3 lần liên tiếp. Ép đổi Proxy mới!`,
              );

              if (masterSocket?.connected) {
                masterSocket.emit("worker_report_dead_proxy", { proxy: proxy });
              }
              retireProxy(proxy); // Lệnh trảm Proxy
              delete proxyStrikeCount[proxy]; // Dọn dẹp án tích
            } else {
              logWarn(
                `[🛑] Proxy [${getShortProxy(proxy)}] bị chặn Rate Limit (Lần ${proxyStrikeCount[proxy]}/3). Cho nghỉ mát 60s!`,
              );
              proxyCooldown[proxy] = Date.now() + 60000; // Nghỉ 60 giây
              safeEmitRadarResult({ channel, status: "ERROR" });
            }
          } else {
            logWarn(`[🛑] Mạng Local bị chặn Rate Limit. Cho nghỉ 120s!`);
            proxyCooldown["local"] = Date.now() + 120000;
            safeEmitRadarResult({ channel, status: "ERROR" });
          }
        } else {
          safeEmitRadarResult({ channel, status: "ERROR" });
        }
        stopWebcast(channel.username);
      });
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
      if (typeof conn.removeAllListeners === "function")
        conn.removeAllListeners();
      if (typeof conn.disconnect === "function") conn.disconnect();

      if (conn.client) {
        if (typeof conn.client.removeAllListeners === "function") {
          conn.client.removeAllListeners();
        }
        if (conn.client.ws && typeof conn.client.ws.terminate === "function") {
          conn.client.ws.terminate();
        }
      }
    } catch (e) {}
  });
}

setInterval(() => {
  const now = Date.now();
  // ==========================================
  // 💡 VÁ LỖI RÒ RỈ RAM: Dọn dẹp rác Rate Limiter của các Key đang nghỉ
  // ==========================================
  for (const k in eulerRateLimiter) {
    eulerRateLimiter[k] = eulerRateLimiter[k].filter((t) => now - t < 60000);
    if (eulerRateLimiter[k].length === 0) delete eulerRateLimiter[k];
  }

  // 1. Dọn kẹt check HTTP
  for (let [user, timestamp] of pendingChecks.entries()) {
    if (now - timestamp > 45000) {
      pendingChecks.delete(user);
      safeEmitRadarResult({ channel: { username: user }, status: "REQUEUE" });
    }
  }

  // 2. Dọn kẹt khởi tạo Socket (Cực kỳ quan trọng để không treo tải)
  for (let [user, timestamp] of connectionLocks.entries()) {
    // Tăng lên 80000 (80s) để đảm bảo không chém nhầm Socket đang cố gắng timeout ở giây thứ 60
    if (!activeConnections[user] && now - timestamp > 80000) {
      logWarn(`[LOCK TIMEOUT] Giải phóng kênh kẹt ${user}. Thu hồi Proxy!`);
      stopWebcast(user); // 💡 Thu hồi lại load của Proxy bị chiếm dụng
      safeEmitRadarResult({ channel: { username: user }, status: "REQUEUE" });
    }
  }

  // 3. Dọn Socket Zombie (Đã nâng cấp: Chỉ cắt khi MAX TẢI THỰC SỰ)
  const currentTotalLoad =
    Object.keys(activeConnections).length +
    eulerConnectionQueue.length +
    pendingChecks.size;

  // 💡 SỬA THEO YÊU CẦU: Tính max tải thực sự dựa trên cấu hình gốc (Không tụt khi Proxy/Key lỗi)
  const safeLoad = config.loadPerProxy > 0 ? config.loadPerProxy : 15;
  const realMaxLoad =
    config.proxyCount * safeLoad +
    (config.useLocalNetwork ? config.localLoad || 0 : 0);

  const isMaxLoad = currentTotalLoad >= realMaxLoad;

  for (let user in activeConnections) {
    const conn = activeConnections[user];

    // Ngưỡng 90 phút không có rương
    if (now - (conn.lastActive || now) > 90 * 60 * 1000) {
      if (isMaxLoad) {
        logWarn(
          `✂️ Cắt bỏ Socket Zombie [${user}] (Tải đã chạm MAX thực: ${currentTotalLoad}/${realMaxLoad}) sau 90 phút không rương.`,
        );
        stopWebcast(user);
        safeEmitRadarResult({
          channel: { username: user },
          status: "COOLDOWN",
          proxy: assignedProxies[user],
        });
      } else {
        // Cố tình bỏ trống để hệ thống ngậm kênh (nuôi rương) vì tải vẫn chưa chạm trần cấu hình.
        // Reset lùi thời gian lại một chút để không check liên tục mỗi 30s.
        conn.lastActive = now - 80 * 60 * 1000;
      }
    }
  }

  // 4. [MỚI] TỰ ĐỘNG SỬA LỖI RÒ RỈ PROXY (Auto-Healing Memory Leak)
  // Nếu một kênh lọt ra ngoài sự quản lý (Không có socket, không có khóa) nhưng vẫn chiếm Proxy -> Thu hồi!
  for (let user in assignedProxies) {
    if (!activeConnections[user] && !connectionLocks.has(user)) {
      logWarn(
        `[MEM LEAK] Phát hiện kênh ${user} bốc hơi nhưng vẫn chiếm Proxy. Tự động giải phóng!`,
      );
      stopWebcast(user);
    }
  }

  // 5. [MỚI] TÁI THIẾT LẬP SINGLE SOURCE OF TRUTH CHO PROXY USAGE
  // Quét lại toàn bộ assignedProxies để tính ra Tải Thực Tế (Triệt tiêu sai số do Race Condition)
  const realUsage = {};
  if (config.useLocalNetwork) realUsage["local"] = 0;
  for (let p of dynamicProxies) realUsage[p] = 0;

  for (let user in assignedProxies) {
    const p = assignedProxies[user];
    realUsage[p] = (realUsage[p] || 0) + 1;
  }

  proxyUsage = realUsage; // Ghi đè toàn bộ số ảo bằng số thực!
}, 30000);

// ========================================================
// 💡 AUTO-BALANCER: TỰ ĐỘNG BÙ/TRẢ PROXY & KEYS HOÀN HẢO
// ========================================================
function balanceResources() {
  if (masterSocket && masterSocket.connected) {
    // 💡 FIX 1: Ép kiểu cứng về Integer để chống lỗi so sánh Chuỗi (String)
    const targetProxyCount = parseInt(config.proxyCount, 10) || 0;
    const currentProxyCount = dynamicProxies.length;
    // 💡 VÁ LỖI CRASH: Khai báo và tính toán tổng sức chứa trước khi phân nhánh
    const safeLoad = config.loadPerProxy > 0 ? config.loadPerProxy : 15;
    const totalNodeCapacity =
      targetProxyCount +
      (config.useLocalNetwork ? config.localLoad / safeLoad : 0);
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

      // 💡 FIX 2: Cắt mảng an toàn để đảm bảo luôn lấy đúng số lượng dư thừa
      const excessProxies = [...dynamicProxies].slice(-Math.abs(excessCount));

      logWarn(
        `🗑️ Thừa ${excessCount} Proxy so với cấu hình. Đang trả lại Master...`,
      );

      masterSocket.emit("worker_return_proxies", excessProxies);
      excessProxies.forEach((p) => retireProxy(p));
      checkProxyHealth();
    }

    const targetKeyCount = Math.ceil(totalNodeCapacity / EULER_PROXY_PER_KEY);
    const currentKeyCount = exclusiveEulerKeys.length;

    if (currentKeyCount < targetKeyCount) {
      masterSocket.emit("worker_request_keys", {
        count: targetKeyCount - currentKeyCount,
        workerName: config.workerName,
        library: "euler",
      });
    } else if (currentKeyCount > targetKeyCount) {
      const excessKeys = exclusiveEulerKeys.splice(
        -Math.abs(currentKeyCount - targetKeyCount),
      );
      masterSocket.emit("worker_return_keys", {
        keys: excessKeys,
        library: "euler",
      });
      excessKeys.forEach((deadKey) => {
        // 1. Xóa khỏi Map
        for (let [p, k] of eulerKeyMap.entries()) {
          if (k === deadKey) eulerKeyMap.delete(p);
        }
        // ==========================================
        // 💡 VÁ LỖI MEMORY LEAK: Rửa sạch bộ nhớ đệm
        // ==========================================
        delete keyStrikeCount[deadKey];
        delete keyCooldown[deadKey];
        delete eulerRateLimiter[deadKey];
        delete keyInitMutex[deadKey];
        // 2. 💡 RÚT ĐIỆN LUỒNG ĐANG SỬ DỤNG
        for (let user in activeConnections) {
          if (
            activeConnections[user] &&
            activeConnections[user].usedKey === deadKey
          ) {
            logWarn(`♻️ Đóng luồng [${user}] để hoàn trả Key Euler dư thừa.`);
            stopWebcast(user);
            safeEmitRadarResult({
              channel: { username: user },
              status: "REQUEUE",
            });
          }
        }
      });
    }
  }
}
// Vòng lặp định kỳ duy trì 20s
setInterval(balanceResources, 20000);

// 💡 KIỂM TRA MẠNG IPV6 TRƯỚC KHI BÁO CÁO MASTER
async function checkIPv6Capability() {
  try {
    logInfo("⏳ Đang kiểm tra kết nối IPv6 của VPS...");
    // api6.ipify.org chỉ phản hồi nếu VPS thực sự gọi ra Internet bằng IPv6
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
      logSuccess(`🌍 Đã xác định vị trí mạng Local: ${res.data.countryCode}`);
    } else {
      proxyGeoData["local"] = "US"; // An toàn hơn VN đối với IP Datacenter quốc tế
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

    // 💡 VÁ LỖI TRẢ CẢ 2 LOẠI KEY ĐÚNG ĐỊNH DẠNG
    if (exclusiveEulerKeys.length > 0)
      masterSocket.emit("worker_return_keys", {
        keys: exclusiveEulerKeys,
        library: "euler",
      });
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
