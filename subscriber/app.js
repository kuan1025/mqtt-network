const mqtt = require('mqtt');
const Redis = require('ioredis');

// --- Configuration ---
const MQTT_URL = 'mqtt://broker.hivemq.com:1883';
const REDIS_HOST = process.env.REDIS_HOST || 'localhost';
const TOPIC = 'qut_ems_project_888/ems/zoneA/meters';

// --- The community unit ID needs to be mapped. IoT devices cannot transmit strings, so please clarify (Internal Configuration) ---

const DEVICE_MAP = {
    1: "MINI-001",
    2: "MINI-002"
};

const COMMUNITY_MAP = {
    1: "North Brisbane XXXX",
    2: "South Brisbane XXXX"
};

const UNIT_MAP = {
    1: "Unit-A",
    15: "Unit-15"
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
        // 1. (C Struct Payload 26 bytes)
        if (message.length !== 26) {
            console.warn(`[Warn] Packet dropped: Invalid size ${message.length} bytes. Expected 26 bytes.`);
            return;
        }

        const uid          = message.readUInt32LE(0);   // 4 bytes
        const seq          = message.readUInt32LE(4);   // 4 bytes
        const kwh_import   = message.readFloatLE(8);    // 4 bytes
        const kwh_export   = message.readFloatLE(12);   // 4 bytes
        const voltage      = message.readFloatLE(16);   // 4 bytes
        const battery_v    = message.readFloatLE(20);   // 4 bytes
        const community_id = message.readUInt8(24);     // 1 byte
        const unit_id      = message.readUInt8(25);     // 1 byte

        const uniqueKey = `${uid}-${seq}`;
        const isNew = await redis.set(`msg:${uniqueKey}`, 'processed', 'NX', 'EX', 86400);


       if (isNew) {
            // 4. mapping Metadata
            const deviceName = DEVICE_MAP[uid] || `Unknown-Dev-${uid}`;
            const communityName = COMMUNITY_MAP[community_id] || `Unknown-Comm-${community_id}`;
            const unitName = UNIT_MAP[unit_id] || `Unknown-Unit-${unit_id}`;

            // 5. JSON Schema 
            const internalPayload = {
                meter_id: deviceName,
                community: communityName,
                unit: unitName,
                timestamp: Math.floor(Date.now() / 1000),
                metrics: {
                    kwh_import: kwh_import.toFixed(2),
                    kwh_export: kwh_export.toFixed(2),
                    volts: voltage.toFixed(1),
                    batt: battery_v.toFixed(2)
                },
                raw_uid: uid,
                raw_seq: seq
            };

    
            console.log(`[Process] Validated Data: [${internalPayload.community}] Device: ${internalPayload.meter_id} (Unit: ${internalPayload.unit})`);
            console.log(`Values: In ${internalPayload.metrics.kwh_import} kWh | Out ${internalPayload.metrics.kwh_export} kWh | ${internalPayload.metrics.volts}V | Battery: ${internalPayload.metrics.batt}V`);

            // 6. TODO : insert ->  DB

            
        } else {
            // Redis hit: this message_id has been processed already
            console.log(`[Filter] Duplicate ignored: UID=${uid}, SEQ=${seq}`);
        }

    } catch (error) {
        console.error(`[Error] Processing failed: ${error.message}`);
    }
});

// Error Handling
redis.on('error', (err) => console.error(`[Error] Redis: ${err.message}`));
client.on('error', (err) => console.error(`[Error] MQTT: ${err.message}`));