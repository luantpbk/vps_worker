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
const INSTANCE_ID = process.env.NODE_APP_INSTANCE || "0";
// Cấu hình mặc định
let config = {
  masterUrl: "http://localhost:3000",
  workerName: `VPS_Worker_01_${INSTANCE_ID}`,
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
      config.workerName = `${config.workerName}_${INSTANCE_ID}`;
    } catch (e) {
      logError("Lỗi đọc file cấu hình, dùng mặc định.");
    }
  } else {
    fs.writeFileSync(CONFIG_FILE, JSON.stringify(config, null, 4));
  }
}
loadConfig();

// State quản lý
let hubConfig = { eulerKeys: [], userAgents: [], deadEulerKeys: [] };
let activeConnections = {};
let assignedProxies = {};
let proxyUsage = {};
let proxyFailCount = {}; // Đếm số lần lỗi liên tiếp của từng proxy
let proxyCooldown = {}; // Thời gian ép proxy nghỉ ngơi (Timestamp)
let proxyHealth = {};
let pendingChecks = new Set();
let masterSocket = null;

let dynamicProxies = [];

let proxyIndex = 0,
  uaIndex = 0,
  keyIndex = 0;
const agentCache = {};
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

if (config.useLocalNetwork) {
  proxyUsage["local"] = 0;
}

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
} // BỔ SUNG DÒNG NÀY
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
  console.log(`⏳ ĐANG CHECK LIVE: ${pendingChecks.size} kênh`);
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
// ĐỒNG BỘ VÀ KIỂM TRA SỨC KHỎE
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

  // 💡 HÀM CHIA NHỎ MẢNG (CHUNKING)
  const chunkSize = 5; // Chỉ ping 5 proxy cùng lúc để tránh nghẽn Socket VPS
  for (let i = 0; i < checkList.length; i += chunkSize) {
    const chunk = checkList.slice(i, i + chunkSize);
    await Promise.all(
      chunk.map(async (p) => {
        // 1. TẠO MÁY CHÉM THỜI GIAN TỪ BÊN NGOÀI (5 GIÂY)
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 5000);

        try {
          let options = {
            signal: controller.signal, // 💡 Gắn máy chém vào request
          };

          if (p !== "local") {
            const proxyAgent = getCachedAgent(p);
            // 💡 LỖI CHÍ MẠNG ĐÃ SỬA: Phải gán cho cả HTTP và HTTPS
            // Vì ip-api.com (bản miễn phí) chỉ chạy http://
            options.httpAgent = proxyAgent;
            options.httpsAgent = proxyAgent;
          }

          // ========================================================
          // 💡 TỐI ƯU 1: QUÉT QUỐC GIA
          // ========================================================
          if (!proxyGeoData[p]) {
            const geoRes = await axios.get(
              "http://ip-api.com/json/?fields=countryCode",
              options,
            );
            if (geoRes.data && geoRes.data.countryCode) {
              proxyGeoData[p] = geoRes.data.countryCode;
            } else {
              proxyGeoData[p] = "VN";
            }
          }

          // ========================================================
          // 💡 TỐI ƯU 2: PING MẠNG
          // ========================================================
          // Khuyên dùng HTTPS để test proxy sát với thực tế kết nối TikTok nhất
          const healthRes = await axios.get(
            "https://clients3.google.com/generate_204",
            options,
          );

          clearTimeout(timeoutId); // Quét thành công thì gỡ bom

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
          clearTimeout(timeoutId); // Có lỗi cũng phải gỡ bom

          currentHealth[p] = { status: "MẤT KẾT NỐI", ip: "N/A" };
          logError(`[Cảnh báo] Proxy ${p} MẤT KẾT NỐI: ${e.message}`);

          // THÊM LÓT LỖI VÀO SỔ ĐỂ ĐỔI PROXY MỚI TỪ MASTER
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
              proxyCooldown[p] = Date.now() + 60000; // Nghỉ 1 phút chờ hồi sinh
            }
          }
        }
      }),
    );
  }

  proxyHealth = currentHealth;

  // ========================================================
  // TÍNH TOÁN LẠI TỔNG TẢI (MAX LOAD) TỰ ĐỘNG
  // ========================================================
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
  // Tạo danh sách mạng khả dụng dựa trên config
  let allProxies = [...dynamicProxies];
  const now = Date.now();
  // Chỉ thêm 'local' vào danh sách nếu useLocalNetwork là true
  if (config.useLocalNetwork) {
    allProxies.unshift("local");
  }
  if (allProxies.length === 0) return null;

  for (let i = 0; i < allProxies.length; i++) {
    let p = allProxies[proxyIndex];
    proxyIndex = (proxyIndex + 1) % allProxies.length;

    // 💡 SỬA LẠI: Kiểm tra thời gian nghỉ ngơi (Cooldown)
    const isCoolingDown = proxyCooldown[p] && now < proxyCooldown[p];
    if (isCoolingDown) continue; // Đang bị phạt nghỉ, tìm proxy tiếp theo

    // Hết hạn nghỉ -> Xóa án
    if (proxyCooldown[p] && now >= proxyCooldown[p]) {
      delete proxyCooldown[p];
      if (proxyHealth[p]) proxyHealth[p].status = "SẴN SÀNG";
      proxyUsage[p] = 0;
      logInfo(
        `[Proxy] 🟢 ${p.split("@").pop()} đã nghỉ mệt xong, test lại lần ${proxyFailCount[p] + 1}!`,
      );
      checkProxyHealth();
    }
    // 💡 TÁCH BIỆT GIỚI HẠN TẢI
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
  if (!hubConfig.eulerKeys || hubConfig.eulerKeys.length === 0) return "";

  // Lọc bỏ các key đã bị Master đánh dấu chết
  const deadKeys = hubConfig.deadEulerKeys || [];
  const aliveKeys = hubConfig.eulerKeys.filter((k) => !deadKeys.includes(k));

  if (aliveKeys.length === 0) {
    logWarn("⚠️ BÁO ĐỘNG: Toàn bộ Euler Keys đã cạn kiệt lượt xác thực!");
    return ""; // Dừng cấp Key để thư viện văng lỗi rõ ràng hoặc dùng key dự phòng
  }

  const key = aliveKeys[keyIndex % aliveKeys.length];
  keyIndex++;
  return key;
}

function getCachedAgent(proxyUrl) {
  // Nếu proxy chưa có chữ http:// ở đầu, ta sẽ tự động build lại cho chuẩn URL Node.js
  if (!proxyUrl.startsWith("http")) {
    const parts = proxyUrl.split(":");
    if (parts.length === 4) {
      // Định dạng: IP:PORT:USER:PASS
      proxyUrl = `http://${parts[2]}:${parts[3]}@${parts[0]}:${parts[1]}`;
    } else if (parts.length === 2) {
      // Định dạng: IP:PORT (Không có user/pass)
      proxyUrl = `http://${parts[0]}:${parts[1]}`;
    } else {
      // Trùng hợp lạ, cứ ép thêm http://
      proxyUrl = `http://${proxyUrl}`;
    }
  }
  if (!agentCache[proxyUrl]) {
    agentCache[proxyUrl] = new HttpsProxyAgent(proxyUrl, {
      keepAlive: true,
      // 💡 BỘ GIÁP CHỐNG QUÉT JA3: Ép VPS dùng Ciphers giống hệ điều hành người dùng thực
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
    reconnection: true, // Bật tính năng tự động nối lại (Mặc định là true)
    reconnectionAttempts: Infinity, // Cố gắng nối lại vô hạn lần (Không bao giờ bỏ cuộc)
    reconnectionDelay: 1000, // Đợi 1 giây trước khi thử lại lần đầu
    reconnectionDelayMax: 5000, // Giãn khoảng cách tối đa là 5 giây giữa các lần thử (Chống DDoS chính Nginx của mình)
    randomizationFactor: 0.5, // Tránh tình trạng 1000 máy cùng lao vào reconnect cùng 1 phần nghìn giây
    parser: customParser, // Chuyển toàn bộ JSON thành Nhị phân (Siêu nhẹ)
    transports: ["websocket"], // Ép kết nối bằng WebSocket thuần ngay từ đầu (bỏ qua polling) để tối ưu luồng nén Zlib
  });

  masterSocket.on("connect", () => {
    logSuccess("Đã kết nối tới Master!");
    if (disconnectTimer) {
      clearTimeout(disconnectTimer);
      disconnectTimer = null;
      logInfo("Giữ nguyên các Socket đang cắm sau khi kết nối lại.");
    }
    masterSocket.emit("worker_ready", {
      name: config.workerName,
      type: "vps_proxy",
      maxLoad: currentDynamicMaxLoad,
      localLoad: config.localLoad,
      loadPerProxy: config.loadPerProxy,
      runningChannels: Object.keys(activeConnections),
      pendingChannels: Array.from(pendingChecks),
      heldProxies: dynamicProxies,
    });
    // 💡 FIX BUG 2: Tính toán xem có thực sự cần xin thêm Proxy không
    const neededProxies = Math.max(
      0,
      (config.proxyCount || 0) - dynamicProxies.length,
    );

    if (neededProxies > 0) {
      logInfo(
        `🔄 Đang thiếu ${neededProxies} proxy so với cấu hình, tiến hành xin thêm...`,
      );
      masterSocket.emit("worker_request_proxies", {
        count: neededProxies,
        workerName: config.workerName,
      });
    } else {
      logSuccess(
        `✅ Đã có đủ ${dynamicProxies.length}/${config.proxyCount} proxy, tiếp tục làm việc, không xin thêm!`,
      );
    }
  });

  // 💡 BỔ SUNG 2: Nhận mảng Proxy Master cấp phát
  masterSocket.on("worker_receive_proxies", (proxiesList) => {
    if (proxiesList.length > 0) {
      logSuccess(`📡 Đã nhận thêm ${proxiesList.length} proxies từ Master.`);

      // 💡 Gộp proxy mới vào danh sách hiện tại (dùng Set để chống trùng lặp an toàn)
      dynamicProxies = Array.from(new Set([...dynamicProxies, ...proxiesList]));

      // Khởi tạo biến đếm usage cho các proxy mới
      dynamicProxies.forEach((p) => {
        if (proxyUsage[p] === undefined) proxyUsage[p] = 0;
      });

      checkProxyHealth(); // Ép kiểm tra sức khỏe ngay để mở rộng Max Load
    }
  });

  // 💡 BỔ SUNG 3: Nhận Proxy mới thay thế khi Proxy cũ bị lỗi
  masterSocket.on("worker_proxy_replacement", (data) => {
    const { deadProxy, newProxy } = data;

    // Xóa proxy chết khỏi sổ sách
    dynamicProxies = dynamicProxies.filter((p) => p !== deadProxy);
    delete proxyHealth[deadProxy];
    delete proxyUsage[deadProxy];
    delete proxyGeoData[deadProxy];
    delete proxyFailCount[deadProxy]; // 💡 Bổ sung
    delete proxyCooldown[deadProxy]; // 💡 Bổ sung

    if (newProxy) {
      // 💡 [ĐÃ SỬA CHỮA LỖI TRÙNG LẶP]: Kiểm tra xem proxy này đã có trên RAM chưa rồi mới nhét
      if (!dynamicProxies.includes(newProxy)) {
        dynamicProxies.push(newProxy);
        proxyUsage[newProxy] = 0;
        logWarn(
          `🔄 Đã nhận Proxy bù đắp từ Master: ${newProxy.split("@").pop()}`,
        );
      }
    } else {
      logError(
        `⚠️ Kho Master đã cạn kiệt, không có Proxy thay thế cho ${deadProxy.split("@").pop()}`,
      );
    }

    checkProxyHealth(); // Ép tính lại tổng tải (Max Load)
  });

  masterSocket.on("sync_vulkan", (data) => (hubConfig = data));
  masterSocket.on("process_task", (channel) => handleTask(channel));

  masterSocket.on("force_update_config", (newCfg) => {
    if (newCfg.useLocalNetwork !== undefined) {
      config.useLocalNetwork = newCfg.useLocalNetwork;
    }
    // 1. Cập nhật các giá trị thô
    if (newCfg.localLoad) config.localLoad = parseInt(newCfg.localLoad);
    if (newCfg.loadPerProxy)
      config.loadPerProxy = parseInt(newCfg.loadPerProxy);

    logWarn(
      `⚙️ Đã áp dụng cấu hình mới: LocalLoad=${config.localLoad}, PerProxy=${config.loadPerProxy}`,
    );
    // Lưu lại file để nếu vps crash, khi khởi động lại sẽ nhớ cấu hình này
    fs.writeFileSync(CONFIG_FILE, JSON.stringify(config, null, 4));
    // Báo ngược lại Master là tôi đã nhận và áp dụng thành công
    sendWorkerStatus();
  });

  masterSocket.on("disconnect", (reason) => {
    if (reason === "io server disconnect") {
      logWarn(
        "🔄 Sếp đổi ca trực! Đang chủ động gọi cửa lại để Nginx chuyển sang Master khác...",
      );
      masterSocket.connect();
    } else {
      logWarn("Mất kết nối Master! Chờ 10 phút trước khi xả tải...");
      disconnectTimer = setTimeout(() => {
        logError("Quá 10 phút không có kết nối! Tiến hành rút toàn bộ Socket.");
        for (let username in activeConnections) {
          stopWebcast(username);
        }
        activeConnections = {};
        assignedProxies = {};
        pendingChecks.clear();
        disconnectTimer = null;
      }, 600000);
    }
  });

  masterSocket.on("cmd_stop_all", () => {
    logWarn("Nhận lệnh Tạm Dừng từ Master! Đang rút toàn bộ Socket...");
    for (let username in activeConnections) {
      stopWebcast(username);
    }
  });
}

// ==========================================
// XỬ LÝ TASK & WEBCAST CẮM SOCKET
// ==========================================
// Thêm Header như PC_Worker
const BASE_HEADERS = {
  Accept: "text/html,application/xhtml+xml",
  "Accept-Language": "en-US,en;q=0.5",
};

async function handleTask(channel) {
  if (
    activeConnections[channel.username] ||
    pendingChecks.has(channel.username)
  )
    return;

  // CỘNG DỒN cả những luồng đang chạy và những luồng đang được soi (pendingChecks)
  const totalProcessing =
    Object.keys(activeConnections).length + pendingChecks.size;

  if (totalProcessing >= currentDynamicMaxLoad) {
    logWarn(
      `⚠️ [${channel.username}] Từ chối nhận thêm vì tổng tải đang là ${totalProcessing}/${currentDynamicMaxLoad}`,
    );
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

  // 💡 [FIX LOGIC]: ĐẶT GẠCH (RESERVE) SLOT PROXY NGAY LẬP TỨC!
  // Khóa slot này lại để các luồng khác không bị cấp trùng khi chưa check Live xong.
  proxyUsage[proxy] = (proxyUsage[proxy] || 0) + 1;
  assignedProxies[channel.username] = proxy;

  const ua = getNextUA();
  let options = {
    headers: { ...BASE_HEADERS, "User-Agent": ua },
    timeout: 10000,
    validateStatus: () => true, // Giữ nguyên để tự bắt mã lỗi HTTP
  };
  if (proxy !== "local") options.httpsAgent = getCachedAgent(proxy);

  try {
    const res = await axios.get(
      `https://www.tiktok.com/${channel.username}/live`,
      options,
    );

    // ==========================================
    // 1. TIKTOK CHẶN IP (HTTP 403, 429) -> Đẩy xuống catch
    // ==========================================
    // 🌟 THÊM ĐOẠN NÀY ĐỂ BẮT ĐÚNG BỆNH PROXY SẬP HOẶC HẾT HẠN
    if (res.status === 407 || res.status === 502 || res.status === 503) {
      throw new Error("PROXY_DEAD");
    }

    // 1. TIKTOK CHẶN IP (HTTP 403, 429) -> Đẩy xuống catch
    if (res.status === 403 || res.status === 429 || res.status >= 500) {
      throw new Error("TIKTOK_BLOCK");
    }

    // ==========================================
    // 2. KÊNH ẢO / ĐÃ XÓA (HTTP 404)
    // ==========================================
    if (res.status === 404) {
      logWarn(`🗑️ Kênh @${channel.username} không tồn tại!`);
      masterSocket.emit("radar_result", { channel, status: "NOT_FOUND" });
      pendingChecks.delete(channel.username);
      sendWorkerStatus();
      return;
    }

    // ==========================================
    // 3. KIỂM TRA TRẠNG THÁI LIVE / OFFLINE
    // ==========================================
    let status = "OFFLINE";

    if (res.status === 200) {
      // 💡 BẮT CAPTCHA CHUẨN: Chỉ bắt các đoạn text thực sự hiển thị trên trang chặn
      if (
        res.data.includes("<title>Verification</title>") ||
        res.data.includes("Please confirm you are human")
      ) {
        throw new Error("TIKTOK_BLOCK"); // Ném xuống catch để phạt Proxy
      }

      // Nếu không bị Captcha, check xem có đang Live không
      if (
        res.data.includes('"status":2') ||
        res.data.includes('"roomStatus":2') ||
        res.data.includes('"is_live":true') ||
        res.data.includes('"isLive":true')
      ) {
        status = "LIVE";
      }
    }

    masterSocket.emit("radar_result", { channel, status });

    if (status === "LIVE") {
      // Khởi động thành công -> Xóa sạch mọi án phạt cũ của Proxy này
      delete proxyFailCount[proxy];
      delete proxyCooldown[proxy];
      if (proxyHealth[proxy]) proxyHealth[proxy].status = "SẴN SÀNG";

      masterSocket.emit(
        "worker_log",
        `[${channel.username}] Đang Live! Cắm Socket...`,
      );
      logInfo(
        `[${channel.username}] Đang Live! Cắm Socket (Proxy: ${proxy === "local" ? "Local" : proxy.split("@").pop()})`,
      );

      startWebcast(channel, proxy, ua);
    } else {
      pendingChecks.delete(channel.username);
      // 💡 [FIX LOGIC]: TRẢ LẠI SLOT PROXY VÌ KÊNH OFFLINE
      stopWebcast(channel.username);
      logWarn(`💤 [${channel.username}] Đang Offline thật sự.`); // Bạn có thể mở ra nếu muốn theo dõi log
    }
  } catch (e) {
    if (proxy !== "local") {
      // Phân biệt: Lỗi Mạng (Timeout/Đứt kết nối) và Lỗi TikTok (Bị chặn 403, Captcha)
      const isNetworkError =
        e.message === "PROXY_DEAD" ||
        e.code === "ECONNREFUSED" ||
        e.code === "ETIMEDOUT" ||
        e.message.includes("timeout") ||
        e.message.includes("socket");

      if (isNetworkError) {
        // Mạng chết thật -> Tăng FailCount (Đủ 3 lần thì vứt xin mới)
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
          proxyCooldown[proxy] = Date.now() + 60000; // Nghỉ 1 phút
          if (proxyHealth[proxy])
            proxyHealth[proxy].status =
              `MẤT KẾT NỐI (${proxyFailCount[proxy]}/3)`;
        }
      } else {
        // Lỗi do TikTok chặn (TIKTOK_BLOCK / 403 / Captcha)
        // -> CHỈ phạt nghỉ 3 phút, KHÔNG tăng FailCount để tránh vứt nhầm Proxy xịn đang bị TikTok nghi ngờ tạm thời
        proxyCooldown[proxy] = Date.now() + 180000;
        if (proxyHealth[proxy])
          proxyHealth[proxy].status = "TikTok chặn tạm thời";
        logWarn(
          `⚠️ Proxy ${proxy.split("@").pop()} bị TikTok chặn tạm thời. Ép nghỉ 3 phút.`,
        );
      }

      // Tính lại sức chứa (Max Load) và báo cho Master
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
    } else {
      // KHI MẠNG LOCAL GẶP 403 HAY CAPTCHA THÌ CHỈ BÁO LOG MÀ KHÔNG GÂY SỤP ĐỔ
      logWarn(`⚠️ Mạng Local bị TikTok từ chối kết nối! Cứ lướt qua...`);
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
  // 💡 LẤY THÔNG SỐ VÙNG MIỀN THEO PROXY (NẾU CHƯA KỊP QUÉT THÌ MẶC ĐỊNH VN)
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

  // Hàm cắm cờ báo lỗi Key
  const checkAndReportDeadKey = (errText, targetKey) => {
    const msg = String(errText).toLowerCase();
    if (
      msg.includes("limit") ||
      msg.includes("quota") ||
      msg.includes("api key") ||
      msg.includes("euler") ||
      msg.includes("balance")
    ) {
      logError(
        `🔑 Key Euler [${targetKey.substring(0, 8)}...] đã kiệt sức! Báo cáo Master...`,
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

      // 💡 BỔ SUNG SỰ KIỆN NÀY ĐỂ BẮT BỆNH EULER API
      conn.on("warn", (err) => {
        checkAndReportDeadKey(err, key);
        logWarn(`⚠️ [${channel.username}] Cảnh báo nội bộ thư viện: ${err}`);
      });

      const msg = `✅ ${channel.username} cắm Socket THÀNH CÔNG!`;
      masterSocket.emit("worker_log", msg);

      conn.on("error", (err) => {
        checkAndReportDeadKey(err, key);
        console.error(
          `🚨 [${channel.username}] LỖI GIẢI MÃ/CORE THƯ VIỆN:`,
          err.message,
        );
        if (err.stack) console.error(err.stack);
      });
      conn.on("roomUser", (userData) => {
        if (userData?.viewerCount) currentViewers = userData.viewerCount;
      });

      conn.on("envelope", (data) => {
        if (data?.envelopeInfo?.diamondCount > 0) {
          logSuccess(
            `🎁 [${channel.username}] nổ rương ${data.envelopeInfo.diamondCount} xu!`,
          );

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
        logWarn(`🛑 ${channel.username} đã tắt Live.`);
        masterSocket.emit("radar_result", { channel, status: "OFFLINE" });
        stopWebcast(channel.username);
      });

      conn.on("disconnected", () => {
        logError(
          `⚠️ ${channel.username} đứt kết nối (Idol rớt mạng/Proxy sập)`,
        );
        masterSocket.emit("radar_result", { channel, status: "ERROR" });
        stopWebcast(channel.username);
      });
    })
    .catch((err) => {
      checkAndReportDeadKey(err, key);
      pendingChecks.delete(channel.username);
      sendWorkerStatus();
      logError(`❌ ${channel.username} lỗi Socket: ${err.message}`);
      masterSocket.emit("radar_result", { channel, status: "ERROR" });
      stopWebcast(channel.username);
    });
}

function stopWebcast(user) {
  if (activeConnections[user]) {
    // 🌟 SỬA ĐOẠN NÀY: Xóa sạch BẤT KỲ sự kiện nào đang bám vào kết nối này
    activeConnections[user].removeAllListeners();
    activeConnections[user].disconnect();
    delete activeConnections[user];
  }

  if (assignedProxies[user]) {
    let realProxy = assignedProxies[user];

    if (proxyUsage[realProxy] !== undefined) {
      proxyUsage[realProxy] = Math.max(0, proxyUsage[realProxy] - 1);
    }

    // 💡 Xóa sổ ngay lập tức. Nếu hàm này vô tình bị gọi lại lần 2,
    // assignedProxies[user] sẽ là undefined và không bị trừ lấn nữa.
    delete assignedProxies[user];
  }

  sendWorkerStatus();
}

fs.watchFile(CONFIG_FILE, async (curr, prev) => {
  logWarn("📝 Phát hiện file cấu hình thay đổi! Đang đồng bộ...");
  try {
    const fileData = JSON.parse(fs.readFileSync(CONFIG_FILE, "utf8"));

    // Cập nhật danh sách proxy
    config.localLoad = fileData.localLoad || 50;
    config.loadPerProxy = fileData.loadPerProxy || 10;
    config.useLocalNetwork = fileData.useLocalNetwork || false;

    const newProxyCount =
      fileData.proxyCount !== undefined
        ? fileData.proxyCount
        : config.proxyCount;

    // ==========================================
    // LOGIC 1: YÊU CẦU THÊM PROXY NẾU TĂNG COUNT
    // ==========================================
    if (newProxyCount > config.proxyCount) {
      const needed = newProxyCount - config.proxyCount;
      logInfo(
        `🔄 Cấu hình tăng Proxy: Đang xin thêm ${needed} Proxy từ Master...`,
      );
      if (masterSocket && masterSocket.connected) {
        masterSocket.emit("worker_request_proxies", {
          count: needed,
          workerName: config.workerName,
        });
      }
      config.proxyCount = newProxyCount;
    }
    // ==========================================
    // LOGIC 2: TRẢ LẠI PROXY THẤP TẢI NHẤT NẾU GIẢM COUNT
    // ==========================================
    else if (newProxyCount < config.proxyCount) {
      const excessCount = config.proxyCount - newProxyCount;
      logWarn(
        `📉 Cấu hình giảm Proxy: Đang thu hồi và trả lại ${excessCount} Proxy thấp tải nhất...`,
      );

      // 1. Chỉ lấy các Proxy được cấp (loại bỏ mạng 'local'), sắp xếp tải theo thứ tự Tăng Dần (Thấp -> Cao)
      let sortedProxies = [...dynamicProxies].sort(
        (a, b) => (proxyUsage[a] || 0) - (proxyUsage[b] || 0),
      );

      // 2. Cắt ra danh sách Proxy sẽ bị "Sa thải"
      let proxiesToReturn = sortedProxies.slice(0, excessCount);

      // 3. Quét các kênh đang dùng proxy bị sa thải -> Ép ngắt kết nối
      for (let user in assignedProxies) {
        if (proxiesToReturn.includes(assignedProxies[user])) {
          logWarn(
            `🛑 Đang ép ngắt kết nối kênh [${user}] để giải phóng Proxy trả về Master.`,
          );
          stopWebcast(user); // Rút socket nội bộ

          // 💡 Đẩy lại cho Master với trạng thái BLOCKED để Master giao cho Worker khác hoặc dùng proxy khác
          if (masterSocket && masterSocket.connected) {
            masterSocket.emit("radar_result", {
              channel: { username: user },
              status: "BLOCKED",
            });
          }
        }
      }

      // 4. Xóa các proxy này khỏi bộ nhớ Worker
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

      // 5. Gửi hàng trả về cho Master
      if (masterSocket && masterSocket.connected) {
        masterSocket.emit("worker_return_proxies", proxiesToReturn);
        logSuccess(
          `✅ Đã đóng gói và hoàn trả ${excessCount} Proxy thành công!`,
        );
      }

      config.proxyCount = newProxyCount;
    }

    await checkProxyHealth();
    logSuccess("✅ Đồng bộ cấu hình nóng thành công!");
  } catch (e) {
    logError("❌ Lỗi định dạng file cấu hình, không thể nạp nóng.");
  }
});

// Khởi động
logInfo("Đang khởi động Headless Worker...");

// ==========================================
// CƠ CHẾ DỌN DẸP BÓNG MA (ZOMBIE KILLER)
// ==========================================
setInterval(() => {
  const now = Date.now();
  let cleaned = 0;

  for (let user in activeConnections) {
    const conn = activeConnections[user];

    // Thư viện Tiktok Live Connector có một biến nội bộ lưu thời gian nhận data cuối cùng
    // Nếu quá 3 phút (180000ms) không nhận được bất kỳ byte dữ liệu nào từ Tiktok -> Socket đã chết lâm sàng
    const lastActivity =
      conn.wsClient?.lastActivity || conn.wsClient?.connectionTime || now;

    if (now - lastActivity > 180000) {
      logError(
        `💀 Phát hiện Zombie Socket [${user}] (Treo quá 3 phút). Tiến hành tiêu diệt!`,
      );
      stopWebcast(user);
      masterSocket.emit("radar_result", {
        channel: { username: user },
        status: "ERROR",
      });
      cleaned++;
    }
  }

  if (cleaned > 0) {
    logWarn(
      `🧹 Đã dọn dẹp ${cleaned} kênh bị kẹt data. Tổng tải hiện tại: ${Object.keys(activeConnections).length}`,
    );
    sendWorkerStatus(); // Ép đồng bộ lại với Master ngay lập tức
  }
}, 60000); // Quét mỗi 1 phút

connectToMaster();
