const { Kafka } = require("kafkajs");
const dotenv = require("dotenv");

dotenv.config();

const QUIX_BOOTSTRAP_SERVER = process.env.QUIX_BOOTSTRAP_SERVER;
const QUIX_USER = process.env.QUIX_USER;
const QUIX_PASSWORD = process.env.QUIX_PASSWORD;

if (!QUIX_BOOTSTRAP_SERVER || !QUIX_USER || !QUIX_PASSWORD) {
    throw new Error("Missing Quix credentials for SCRAM-SHA-512");
}

const kafka = new Kafka({
    clientId: "dxfeed-client",
    brokers: [QUIX_BOOTSTRAP_SERVER],
    ssl: true,
    sasl: {
        mechanism: "scram-sha-512",   // <-- must match Quix
        username: QUIX_USER,
        password: QUIX_PASSWORD
    }
});

const producer = kafka.producer();

async function startProducer() {
    await producer.connect();
    console.log("✅ Kafka producer connected with SCRAM-SHA-512");
}

async function sendTick(tick) {
    try {
        await producer.send({
            topic: "bigjboz-myproject-production-dxfeed.ticks.bronze",
            messages: [{ value: JSON.stringify(tick) }]
        });
        console.log("📤 Tick sent to Quix Bronze");
    } catch (err) {
        console.error("❌ Error sending tick to Quix:", err.message);
    }
}

module.exports = { startProducer, sendTick };
