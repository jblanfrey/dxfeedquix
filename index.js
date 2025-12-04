const WebSocket = require("ws");
const dotenv = require("dotenv");

dotenv.config();

const DXFEED_WS_URL = process.env.DXFEED_WS_URL;
const DXFEED_AUTH_TOKEN = process.env.DXFEED_AUTH_TOKEN;
const SYMBOLS = process.env.SYMBOLS.split(",");

if (!DXFEED_WS_URL || !DXFEED_AUTH_TOKEN) {
    throw new Error("Missing dxFeed credentials in .env");
}

let ws;
let keepaliveInterval;

function send(msg) {
    ws.send(JSON.stringify(msg));
    console.log("➡️ SENT:", msg.type);
}

function startDxFeed() {
    console.log("🔌 Connecting to dxFeed...");
    ws = new WebSocket(DXFEED_WS_URL);

    ws.on("open", () => {
        console.log("✅ WebSocket open");

        // 1️⃣ Send SETUP
        send({
            type: "SETUP",
            channel: 0,
            keepaliveTimeout: 60,
            acceptKeepaliveTimeout: 60,
            version: "0.2-js/1.0.1"
        });
    });

    ws.on("message", (data) => {
        const raw = data.toString();
        let msg;
        try {
            msg = JSON.parse(raw);
        } catch (err) {
            console.error("❌ Invalid JSON:", raw);
            return;
        }

        handleMessage(msg);
    });

    ws.on("close", () => {
        console.warn("⚠️ dxFeed disconnected — reconnecting in 2s...");
        clearInterval(keepaliveInterval);
        setTimeout(startDxFeed, 2000);
    });

    ws.on("error", (err) => {
        console.error("❌ WS Error:", err.message);
    });
}

function handleMessage(msg) {
    switch (msg.type) {

        // ✅ SETUP ACK received → send AUTH
        case "SETUP":
            console.log("✅ SETUP ACK received — sending AUTH");
            send({
                type: "AUTH",
                channel: 0,
                token: DXFEED_AUTH_TOKEN
            });
            break;

        // ✅ AUTH OK → start KEEPALIVE + CHANNEL_REQUEST
        case "AUTH_STATE":
            if (msg.state === "AUTHORIZED") {
                console.log("✅ AUTH OK — starting KEEPALIVE and channel request");

                // start keepalive only after authorized
                keepaliveInterval = setInterval(() => {
                    send({ type: "KEEPALIVE", channel: 0 });
                }, 55_000);

                // proceed with channel request
                send({
                    type: "CHANNEL_REQUEST",
                    channel: 1,
                    service: "FEED",
                    parameters: { contract: "AUTO" }
                });

            } else {
                // just log intermediate states; don't treat as error
                console.log("ℹ️ AUTH_STATE:", msg);
            }
            break;

        // ❌ ERROR received
        case "ERROR":
            console.error("❌ SERVER ERROR:", msg);
            break;

        // ✅ FEED channel opened → send FEED_SETUP
        case "CHANNEL_OPENED":
            console.log("✅ FEED CHANNEL OPEN — sending FEED_SETUP");
            send({
                type: "FEED_SETUP",
                channel: 1,
                acceptAggregationPeriod: 10,
                acceptDataFormat: "COMPACT",
                acceptEventFields: {
                    Quote: ["eventType", "eventSymbol", "bidPrice", "askPrice", "bidSize", "askSize"],
                    Greeks: ["eventType", "eventSymbol", "eventTime", "eventFlags", "index", "time", "sequence",
                        "price", "volatility", "delta", "gamma", "theta", "rho", "vega"]
                }
            });
            break;

        // ✅ FEED_CONFIG → subscribe to symbols
        case "FEED_CONFIG":
            console.log("✅ FEED CONFIG OK — subscribing to symbols");

            const subs = SYMBOLS.map(sym => ({ symbol: sym, type: "Quote" }));
            send({
                type: "FEED_SUBSCRIPTION",
                channel: 1,
                add: subs
            });
            break;

        // ✅ FEED_DATA → live market data
        case "FEED_DATA":
            const normalized = {
                receivedAt: Date.now(),
                source: "dxfeed",
                payload: msg.data
            };

            console.log("📥 FEED DATA:", normalized);

            // 🔜 NEXT: forward to Quix Kafka topic here
            break;

        case "KEEPALIVE":
            // server heartbeat
            break;

        default:
            console.log("ℹ️ UNHANDLED EVENT:", msg.type, msg);
    }
}

startDxFeed();
