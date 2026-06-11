// vps_worker.js
process.env.TZ = "Asia/Ho_Chi_Minh";
require("dotenv").config();
const { io: ClientIO } = require("socket.io-client");
const customParser = require("socket.io-msgpack-parser");
const { TikTokLiveConnection } = require("tiktok-live-connector");
const { TikTokLive } = require("@tiktool/live");
const HttpsProxyAgent = require("https-proxy-agent");
const axios = require("axios");
const { gotScraping } = require("got-scraping");
const fs = require("fs");

const CONFIG_FILE = "vps_config.json";
const EULER_RATE = 3; // 1 key cho mỗi 3 proxy để tối ưu hóa hiệu suất

let config = {
  masterUrl: "http://localhost:3001",
  workerName: `Worker_01`,
  proxyCount: 5,
  useLocalNetwork: false,
  loadPerProxy: 10,
  localLoad: 50,
  activeLibrary: "tiktok-live-connector",
};
const MAX_EULER_INTERVAL = 2000;
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
let keyCooldown = {};
// 5 request/phút/key
const TIKTOOL_REQUESTS_PER_MINUTE = 5;
let tiktoolRateLimiter = {};
function canUseTiktoolKey(key) {
  if (!key) return false;

  const now = Date.now();

  if (!tiktoolRateLimiter[key]) {
    tiktoolRateLimiter[key] = [];
  }

  // Giữ lại request trong 60s gần nhất
  tiktoolRateLimiter[key] = tiktoolRateLimiter[key].filter(
    (t) => now - t < 60000,
  );

  return tiktoolRateLimiter[key].length < TIKTOOL_REQUESTS_PER_MINUTE;
}

function consumeTiktoolRequest(key) {
  if (!tiktoolRateLimiter[key]) {
    tiktoolRateLimiter[key] = [];
  }

  tiktoolRateLimiter[key].push(Date.now());
}

let pendingChecks = new Map();
let connectionLocks = new Set(); // 💡 FIX 4: Đã khai báo biến chống Ghost Load
let masterSocket = null;
let workerPausedUntil = 0;
let hasIPv6Support = false;

let dynamicProxies = [];
let zombieProxies = {};
let exclusiveEulerKeys = [];
let exclusiveTiktoolKeys = [];
let tiktoolKeyMap = new Map(); // Map lưu trữ 1 Proxy -> 1 Tiktool Key
let keyIndex = 0;
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
  // 💡 VÁ LỖI LOG: Tự động đổi chữ EULER hoặc TIKTOOL
  if (config.activeLibrary === "tiktool") {
    console.log(
      `🔑 TIKTOOL KEYS: ${exclusiveTiktoolKeys.length} key độc quyền`,
    );
  } else {
    console.log(`🔑 EULER KEYS: ${exclusiveEulerKeys.length} key độc quyền`);
  }
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

  // ========================================================
  // 💡 TÍNH TOÁN LẠI TỔNG TẢI (MAX LOAD) NHƯ HÀM 1
  // ========================================================
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

function getNextEulerKey() {
  if (exclusiveEulerKeys.length === 0) return null;

  const now = Date.now();
  // 💡 CHỈ LẤY CÁC KEY ĐANG KHÔNG BỊ PHẠT NGHỈ
  const availableKeys = exclusiveEulerKeys.filter(
    (k) => !keyCooldown[k] || now > keyCooldown[k],
  );

  if (availableKeys.length === 0) {
    logWarn(
      `⏳ Tất cả Euler Keys đang nghỉ 15s để chống cháy Quota. Kênh sẽ tự xếp hàng chờ...`,
    );
    return null;
  }

  // Lấy xoay vòng không giới hạn sức chứa
  const key = availableKeys[keyIndex % availableKeys.length];
  keyIndex++;
  return key;
}

function getTiktoolKeyForProxy(proxyStr) {
  // 💡 Xử lý chia tải cho Local: Tạo ra các "Proxy Ảo" để map được nhiều Key
  let mappingStr = proxyStr;
  if (proxyStr === "local") {
    const currentLocalUsage = proxyUsage["local"] || 0;
    const safeLoad = config.loadPerProxy > 0 ? config.loadPerProxy : 15;
    const virtualIndex = Math.floor(currentLocalUsage / safeLoad);
    mappingStr = `local_v_${virtualIndex}`;
  }

  // Nếu cụm ảo này đã có Key, dùng lại Key đó
  if (tiktoolKeyMap.has(mappingStr)) return tiktoolKeyMap.get(mappingStr);

  // Nếu chưa có, tìm 1 Key rảnh rỗi trong kho
  const usedKeys = Array.from(tiktoolKeyMap.values());
  const availableKey = exclusiveTiktoolKeys.find(
    (k) =>
      !usedKeys.includes(k) && (!keyCooldown[k] || Date.now() > keyCooldown[k]),
  );

  if (availableKey) {
    tiktoolKeyMap.set(mappingStr, availableKey);
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
  // 💡 Dọn dẹp triệt để toàn bộ các Key đang gán cho Proxy Ảo của Local
  if (proxy === "local") {
    for (let key of tiktoolKeyMap.keys()) {
      if (key.startsWith("local_v_")) tiktoolKeyMap.delete(key);
    }
  } else {
    tiktoolKeyMap.delete(proxy);
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
      heldTiktoolKeys: exclusiveTiktoolKeys,
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

    if (currentLib === "tiktool") {
      const neededKeys = Math.max(
        0,
        totalNodeCapacity - exclusiveTiktoolKeys.length,
      );
      if (neededKeys > 0) {
        masterSocket.emit("worker_request_keys", {
          count: neededKeys,
          workerName: config.workerName,
          library: "tiktool",
        });
      }
      logInfo(
        `🔄 Khởi tạo luồng bằng Tiktool. Đang kiểm tra/xin cấp ${neededKeys} Keys...`,
      );
    } else {
      const neededKeys = Math.max(
        0,
        Math.ceil(totalNodeCapacity / EULER_RATE) - exclusiveEulerKeys.length,
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
    }
  });

  masterSocket.on("worker_receive_keys", (data) => {
    const keysList = Array.isArray(data) ? data : data.keys || [];
    const lib = data.library || "euler";

    if (lib === "tiktool") {
      exclusiveTiktoolKeys = Array.from(
        new Set([...exclusiveTiktoolKeys, ...keysList]),
      );
    } else {
      exclusiveEulerKeys = Array.from(
        new Set([...exclusiveEulerKeys, ...keysList]),
      );
    }
  });

  masterSocket.on("worker_key_replacement", (data) => {
    const lib = data.library || "euler";
    if (lib === "tiktool") {
      exclusiveTiktoolKeys = exclusiveTiktoolKeys.filter(
        (k) => k !== data.deadKey,
      );
      if (data.newKey && !exclusiveTiktoolKeys.includes(data.newKey))
        exclusiveTiktoolKeys.push(data.newKey);
    } else {
      exclusiveEulerKeys = exclusiveEulerKeys.filter((k) => k !== data.deadKey);
      if (data.newKey && !exclusiveEulerKeys.includes(data.newKey))
        exclusiveEulerKeys.push(data.newKey);
    }
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
    // 2. Ép ngắt kết nối (Rút ống thở) TOÀN BỘ các kênh đang LIVE
    const runningUsers = Object.keys(activeConnections);
    runningUsers.forEach((user) => {
      safeEmitRadarResult({ channel: { username: user }, status: "REQUEUE" });
      stopWebcast(user);
    });

    // 3. Xóa các khóa check
    pendingChecks.clear();
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
      !activeConnections[channel.username]
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
    // 💡 1. TÍNH TOÁN LẠI TỔNG TẢI DỰ KIẾN (Rất quan trọng)
    // Tổng tải = Đã cắm + Đang xếp hàng chờ cắm + Đang check HTTP
    const currentActive = Object.keys(activeConnections).length;
    const totalIntendedLoad =
      currentActive + eulerConnectionQueue.length + pendingChecks.size;

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

setInterval(async () => {
  if (
    isConnectingEuler ||
    eulerConnectionQueue.length === 0 ||
    Date.now() < workerPausedUntil
  )
    return;

  isConnectingEuler = true;
  try {
    const { channel, proxy } = eulerConnectionQueue.shift();

    if (!activeConnections[channel.username]) {
      startWebcast(channel, proxy);

      // 💡 VÁ LỖI TỈ LỆ: Tính toán tốc độ cắm dựa trên SỐ LƯỢNG KEY thực tế
      let dynamicDelay = 2000;
      let activeKeys = 1;

      if (config.activeLibrary === "tiktool") {
        // Tiktool (Tỉ lệ 1 Key : 1 Proxy)
        // Giới hạn API: 5 request/phút/key => Cần 12 giây hồi chiêu cho mỗi Key
        activeKeys = Math.max(1, exclusiveTiktoolKeys.length);
        dynamicDelay = Math.max(500, 12000 / activeKeys);
      } else {
        // Euler (TLC) (Tỉ lệ 1 Key : 3 Proxy)
        // 1 Key phải gánh nhiều kết nối hơn, lấy mốc an toàn là 3 giây hồi chiêu cho 1 Key
        activeKeys = Math.max(1, exclusiveEulerKeys.length);
        dynamicDelay = Math.max(200, 3000 / activeKeys);
      }

      // Thêm độ nhiễu ngẫu nhiên (Jitter) từ 0-500ms để vượt qua các bộ lọc Bot tĩnh
      const jitter = Math.floor(Math.random() * 500);
      const totalDelay = Math.round(dynamicDelay + jitter);

      logInfo(
        `⏱️ Cắm [${channel.username}]. Nghỉ ${totalDelay}ms (Đang xả tải dựa trên ${activeKeys} ${config.activeLibrary === "tiktool" ? "Tiktool" : "Euler"} Keys)...`,
      );
      await new Promise((r) => setTimeout(r, totalDelay));
    }
  } finally {
    isConnectingEuler = false;
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

      // 💡 ĐÓNG KHÓA NGAY LẬP TỨC ĐỂ TRÁNH BỊ CHECK LẠI, VÀ ĐẨY VÀO HÀNG ĐỢI
      connectionLocks.add(channel.username);
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
  connectionLocks.add(channel.username);

  const currentCountry = proxyGeoData[proxy] || "VN";
  const geo = getGeoParams(currentCountry);

  // Ghi nhớ thư viện được khởi tạo tại thời điểm này
  const libraryUsed = config.activeLibrary;
  let key, conn;
  let roomId = null;
  let pendingBoxes = [];
  let currentViewers = 0;
  let isProcessingInitial = true;

  // Bảo vệ an toàn: Chặn kênh lỗi dữ liệu để thư viện không crash hàm .replace()
  if (!channel || !channel.username)
    throw new Error("Dữ liệu kênh bị rỗng (Undefined Username)");
  // PHÂN LUỒNG THƯ VIỆN & KEY
  if (libraryUsed === "tiktool") {
    key = getTiktoolKeyForProxy(proxy);
    console.log("Key được chọn cho Tiktool:", key);
    if (!key) return;
    if (!canUseTiktoolKey(key)) {
      logWarn(`[RATE LIMIT] Key ${key.slice(0, 8)}... đã đạt 5 req/phút`);

      connectionLocks.delete(channel.username);

      setTimeout(() => {
        safeEmitRadarResult({
          channel,
          status: "REQUEUE",
        });
      }, 15000);

      return;
    }
    if (!key) {
      logWarn(
        `⏳ Thiếu Tiktool Key cho proxy [${getShortProxy(proxy)}]. Xếp hàng chờ Master cấp...`,
      );
      connectionLocks.delete(channel.username);
      safeEmitRadarResult({ channel, status: "REQUEUE" });
      return;
    }

    const tiktoolOpts = {
      uniqueId: channel.username.startsWith("@")
        ? channel.username.slice(1)
        : channel.username,
      apiKey: key,
      clientParams: {
        app_language: geo.lang,
        webcast_language: geo.lang,
        region: geo.region,
      },
    };

    // 💡 VÁ LỖI CRASH: Chỉ khai báo proxy nếu không phải mạng local (Tránh truyền undefined)
    const pUrl = formatProxyUrl(proxy);
    if (proxy !== "local" && pUrl) {
      tiktoolOpts.proxy = pUrl;
    }

    conn = new TikTokLive(tiktoolOpts);
  } else {
    key = getNextEulerKey();
    conn = new TikTokLiveConnection(channel.username, {
      signApiKey: key,
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
    });
  }

  const checkAndReportDeadKey = (errObj, targetKey) => {
    if (!targetKey) return false;
    let errText =
      typeof errObj === "string"
        ? errObj
        : errObj?.message || JSON.stringify(errObj);
    const msg = String(errText).toLowerCase();

    const isFatalKey =
      msg.includes("insufficient balance") ||
      msg.includes("api key is invalid");
    const isOverloadedKey =
      msg.includes("too many connections") ||
      msg.includes("rate limit for your plan") ||
      msg.includes("rate_limit_");

    if (isFatalKey) {
      logWarn(`[❌] 🔑 PHÁT HIỆN KEY CHẾT HẲN (${libraryUsed}): ${msg}`);
      if (masterSocket?.connected) {
        masterSocket.emit("worker_report_dead_key", {
          key: targetKey,
          workerName: config.workerName,
          library: libraryUsed,
        });
      }
      return true;
    }

    if (isOverloadedKey) {
      logWarn(`[⚠️] ⏳ KEY QUÁ TẢI (${libraryUsed}): ${msg}`);
      keyCooldown[targetKey] = Date.now() + 5000;
      return true;
    }
    return false;
  };

  const emitChest = (data) => {
    const boxData = data?.envelopeInfo || data?.treasureBoxData || data;
    const coins = boxData?.diamondCount || boxData?.coin || boxData?.coins || 0;
    const boxes =
      boxData?.peopleCount || boxData?.totalUser || boxData?.boxes || 0;
    let boxType = "ruong";

    const bType = boxData?.businessType;
    const sId = boxData?.skinId;

    if (bType === 1) boxType = "ruong";
    else if (bType === 4) boxType = "ruong_vang";
    else if (
      (bType !== undefined && bType !== 1 && bType !== 4) ||
      (sId !== undefined && sId !== 0)
    )
      boxType = "tui";

    const idcStr = String(boxData?.envelopeIdc || "").toLowerCase();
    if (idcStr.includes("packet") || idcStr.includes("red")) boxType = "tui";

    if (coins <= 0) return;

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
      idc: boxData?.envelopeIdc || boxData?.id || boxData?.treasureId || "",
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
    if (activeConnections[channel.username])
      activeConnections[channel.username].lastActive = Date.now();
    if (!roomId) {
      pendingBoxes.push(data);
      return;
    }
    emitChest(data);
  };

  // Chuẩn hóa Binding sự kiện cho 2 Thư viện
  if (libraryUsed === "tiktool") {
    conn.on("chest", catchTreasureBox);
    conn.on("error", (err) => checkAndReportDeadKey(err, key));
    conn.on("disconnected", () => {
      stopWebcast(channel.username);
      safeEmitRadarResult({ channel, status: "OFFLINE", proxy });
    });
  } else {
    conn.on("envelope", catchTreasureBox);
    conn.on("treasureBox", catchTreasureBox);
    conn.on("roomUser", (u) => {
      if (u?.viewerCount) currentViewers = u.viewerCount;
    });
    conn.on("warn", (err) => checkAndReportDeadKey(err, key));
    conn.on("error", (err) => checkAndReportDeadKey(err, key));
    conn.on("streamEnd", () => {
      stopWebcast(channel.username);
      safeEmitRadarResult({ channel, status: "OFFLINE", proxy });
    });
    conn.on("disconnected", () => {
      stopWebcast(channel.username);
      safeEmitRadarResult({ channel, status: "OFFLINE", proxy });
    });
  }

  let timeoutHandle;
  const timeoutPromise = new Promise((_, r) => {
    timeoutHandle = setTimeout(() => {
      console.log("[STILL WAITING]", channel.username);
      r(new Error("SOCKET_TIMEOUT"));
    }, 30000);
  });
  if (libraryUsed === "tiktool") {
    consumeTiktoolRequest(key);
  }
  console.log("[CONNECT START]", channel.username, key.slice(0, 8));
  Promise.race([conn.connect(), timeoutPromise])
    .then((state) => {
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
      activeConnections[channel.username].isTiktool = libraryUsed === "tiktool"; // Đánh dấu thư viện dọn rác

      proxyStrikeCount[proxy] = 0;
      logSuccess(
        `✅ [${channel.username}] Đã kết nối qua ${libraryUsed} (${getShortProxy(proxy)})`,
      );

      setTimeout(() => {
        isProcessingInitial = false;
      }, 5000);
    })
    .catch((err) => {
      clearTimeout(timeoutHandle);
      const isKeyDead = checkAndReportDeadKey(err, key);
      let errMsg = String(err?.message || err).toLowerCase();

      logWarn(
        `[SOCKET LỖI] Kênh: ${channel.username} | Proxy: ${getShortProxy(proxy)} | Lỗi: ${errMsg}`,
      );

      if (isKeyDead) {
        setTimeout(() => {
          safeEmitRadarResult({ channel, status: "REQUEUE" });
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

    if (now - (conn.lastActive || now) > 5 * 60 * 1000) {
      logWarn(
        `✂️ Cắt bỏ Socket Zombie [${user}] do 5 phút không có tín hiệu mạng.`,
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

    // Xử lý Cân bằng Keys tùy theo thư viện
    if (config.activeLibrary === "tiktool") {
      // 💡 BỔ SUNG: Trả sạch Euler Keys nếu đang dùng Tiktool
      if (exclusiveEulerKeys.length > 0) {
        masterSocket.emit("worker_return_keys", {
          keys: exclusiveEulerKeys,
          library: "euler",
        });
        exclusiveEulerKeys = [];
      }
      const targetKeyCount = totalNodeCapacity; // 💡 Đã cộng gộp mạng Local
      const currentKeyCount = exclusiveTiktoolKeys.length;

      if (currentKeyCount < targetKeyCount) {
        masterSocket.emit("worker_request_keys", {
          count: targetKeyCount - currentKeyCount,
          workerName: config.workerName,
          library: "tiktool",
        });
      } else if (currentKeyCount > targetKeyCount) {
        const excessKeys = exclusiveTiktoolKeys.splice(
          -Math.abs(currentKeyCount - targetKeyCount),
        );
        masterSocket.emit("worker_return_keys", {
          keys: excessKeys,
          library: "tiktool",
        });
      }
    } else {
      // 💡 BỔ SUNG: Trả sạch Tiktool Keys nếu đang dùng Euler
      if (exclusiveTiktoolKeys.length > 0) {
        masterSocket.emit("worker_return_keys", {
          keys: exclusiveTiktoolKeys,
          library: "tiktool",
        });
        exclusiveTiktoolKeys = [];
      }
      const targetKeyCount = Math.ceil(totalNodeCapacity / EULER_RATE);
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
      }
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
    if (exclusiveTiktoolKeys.length > 0)
      masterSocket.emit("worker_return_keys", {
        keys: exclusiveTiktoolKeys,
        library: "tiktool",
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
