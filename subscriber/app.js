const mqtt = require('mqtt');
const Redis = require('ioredis');

// --- Configuration ---
const MQTT_URL = process.env.MQTT_URL || 'mqtt://localhost:1883';
const REDIS_HOST = process.env.REDIS_HOST || 'localhost';
const TOPIC = 'ems/+/meters';

// --- The community unit ID needs to be mapped. IoT devices cannot transmit strings, so please clarify (Internal Configuration) ---
const DEVICE_REGISTRY = {
    "MINI-001": { community: "North Brisbane XXXX", unit: "Unit-A" },
    "MINI-002": { community: "South Brisbane XXXX", unit: "Unit-01" }
};

// --- Initialization ---
const redis = new Redis({ host: REDIS_HOST, port: 6379 });
const client = mqtt.connect(MQTT_URL);

client.on('connect', () => {
    console.log(`[System] Connected to MQTT Broker: ${MQTT_URL}`);
    client.subscribe(TOPIC, (err) => {
        if (!err) console.log(`[System] Subscribing to: ${TOPIC}`);
    });
});

client.on('message', async (topic, message) => {
    try {
        // 1. Parse raw MQTT message to JSON
        const incomingData = JSON.parse(message.toString());
        const { message_id, kwh_total, voltage, battery_v } = incomingData;

        if (!message_id) {
            console.warn('[Warn] Packet dropped: No message_id found');
            return;
        }

        // 2. Concurrency Deduplication 
        // Set key with 24h expiration only if it does not exist (NX)
        const isNew = await redis.set(`msg:${message_id}`, 'processed', 'NX', 'EX', 86400);

        if (isNew) {
            // 3. String Manipulation: Extracting actual_meter_id
            // Input: "MINI-002-A1B2-1" -> Output: "MINI-002"
            const parts = message_id.split('-');
            const actual_meter_id = parts.length > 2 ? parts.slice(0, -2).join('-') : message_id;
            
            // 4. Mapping metadata
            const deviceInfo = DEVICE_REGISTRY[actual_meter_id] || { 
                community: "Unknown Community", 
                unit: "Unknown Unit" 
            };

            // 5. Final Schema Construction 
            const internalPayload = {
                meter_id: actual_meter_id,
                community: deviceInfo.community,
                unit: deviceInfo.unit,
                timestamp: Math.floor(Date.now() / 1000),
                metrics: {
                    kwh: kwh_total ? kwh_total.toFixed(2) : '0.00',
                    volts: voltage ? voltage.toFixed(1) : '0.0',
                    batt: battery_v ? battery_v.toFixed(2) : '0.00'
                },
                raw_id: message_id
            };

            console.log(`[Process] Validated Data: [${internalPayload.community}] Device: ${internalPayload.meter_id}`);
            console.log(`Values: ${internalPayload.metrics.kwh} kWh | ${internalPayload.metrics.volts}V | Battery: ${internalPayload.metrics.batt}V`);


            // 6. TODO : insert ->  DB 

            
        } else {
            // Redis hit: this message_id has been processed already
            console.log(`[Filter] Duplicate ignored: ${message_id}`);
        }

    } catch (error) {
        console.error(`[Error] Processing failed: ${error.message}`);
    }
});

// Error Handling
redis.on('error', (err) => console.error(`[Error] Redis: ${err.message}`));
client.on('error', (err) => console.error(`[Error] MQTT: ${err.message}`));