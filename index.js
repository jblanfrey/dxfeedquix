const { startProducer } = require("./kafkaProducer");
const { startDxFeed } = require("./wsClient");

async function startApp() {
    await startProducer();
    startDxFeed();
}

startApp();
