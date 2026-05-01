const mqtt = require('mqtt');


const MQTT_URL = 'mqtt://localhost:1883';
const TOPIC = 'ems/zoneA/meters'; 

const client = mqtt.connect(MQTT_URL);

client.on('connect', () => {
    console.log(`[System] Connected to MQTT Broker: ${MQTT_URL}`);

    // 1.  26 Bytes ( C Struct)
    const payload = Buffer.alloc(26);

    // 2. Testing payload
    payload.writeUInt32LE(2, 0);       // uid: 2 
    payload.writeUInt32LE(9998, 4);    // seq: 9998
    payload.writeFloatLE(1234.56, 8);  // kwh_import: 1234.56
    payload.writeFloatLE(0.0, 12);     // kwh_export: 0.0
    payload.writeFloatLE(236.5, 16);   // voltage: 236.5V
    payload.writeFloatLE(3.85, 20);    // battery_v: 3.85V
    payload.writeUInt8(1, 24);         // community_id: 1
    payload.writeUInt8(15, 25);        // unit_id: 15

    // 3. Send Buffer
    client.publish(TOPIC, payload, { qos: 0 }, (err) => {
        if (err) {
            console.error('Fail :', err);
        } else {
            console.log('Test packet sent successfully');
            console.log(` Hex: ${payload.toString('hex')}`);
        }
        client.end();
    });
});