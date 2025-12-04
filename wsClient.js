const WebSocket = require("ws");
const dotenv = require("dotenv");
const { sendTick } = require("./kafkaProducer");

dotenv.config();

const DXFEED_WS_URL = process.env.DXFEED_WS_URL;
const DXFEED_AUTH_TOKEN = process.env.DXFEED_AUTH_TOKEN;
const SYMBOLS = process.env.SYMBOLS.split(",");

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
        send({
            type: "SETUP",
            channel: 0,
            keepaliveTimeout: 60,
            acceptKeepaliveTimeout: 60,
            version: "0.2-js/1.0.1"
        });
    });

    ws.on("message", async (data) => {
        let msg;
        try {
            msg = JSON.parse(data.toString());
        } catch (err) {
            console.error("❌ Invalid JSON:", data.toString());
            return;
        }

        await handleMessage(msg);
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

async function handleMessage(msg) {
    switch (msg.type) {
        case "SETUP":
            console.log("✅ SETUP ACK received — sending AUTH");
            send({ type: "AUTH", channel: 0, token: DXFEED_AUTH_TOKEN });
            break;

        case "AUTH_STATE":
            if (msg.state === "AUTHORIZED") {
                console.log("✅ AUTH OK — starting KEEPALIVE and channel request");
                keepaliveInterval = setInterval(() => {
                    send({ type: "KEEPALIVE", channel: 0 });
                }, 55_000);

                send({
                    type: "CHANNEL_REQUEST",
                    channel: 1,
                    service: "FEED",
                    parameters: { contract: "AUTO" }
                });
            } else {
                console.log("ℹ️ AUTH_STATE:", msg);
            }
            break;

        case "CHANNEL_OPENED":
            console.log("✅ FEED CHANNEL OPEN — sending FEED_SETUP");
            send({
                type: "FEED_SETUP",
                channel: 1,
                acceptAggregationPeriod: 10,
                acceptDataFormat: "COMPACT",
                acceptEventFields: {
                    Quote: ["eventType", "eventSymbol", "bidPrice", "askPrice", "bidSize", "askSize"],
                    Greeks: ["eventType", "eventSymbol", "eventTime", "eventFlags", "index", "time", "sequence", "price", "volatility", "delta", "gamma", "theta", "rho", "vega"]
                }
            });
            break;

        case "FEED_CONFIG":
            console.log("✅ FEED CONFIG OK — subscribing to symbols");
            const subs = SYMBOLS.map(sym => ({ symbol: sym, type: "Quote" }));
            send({ type: "FEED_SUBSCRIPTION", channel: 1, add: subs });
            break;

        case "FEED_DATA":
            const normalized = {
                receivedAt: Date.now(),
                source: "dxfeed",
                payload: msg.data
            };
            await sendTick(normalized);
            console.log("📥 FEED DATA sent to Quix Bronze:", normalized);
            break;

        case "KEEPALIVE":
            break;

        case "ERROR":
            console.error("❌ dxFeed ERROR:", msg);
            break;

        default:
            console.log("ℹ️ UNHANDLED EVENT:", msg.type, msg);
    }
}

module.exports = { startDxFeed };
