const express = require('express');
const { Kafka } = require('kafkajs');

const app  = express();
const PORT = process.env.PORT || 3001;

/* ══════════════════════════════════════
   Kafka Producer
══════════════════════════════════════ */

const kafka = new Kafka({
    clientId: 'website-traffic-producer',
    brokers:  ['master:9092'],
    retry: { initialRetryTime: 100, retries: 10 },
});

const producer = kafka.producer();

async function startKafka() {
    try {
        await producer.connect();
        console.log('✅ Connected to Kafka');
    } catch (err) {
        console.error('❌ Kafka Connection Failed:', err.message);
    }
}

startKafka();

producer.on(producer.events.DISCONNECT, async () => {
    console.warn('⚠️  Kafka producer disconnected — reconnecting…');
    try {
        await producer.connect();
        console.log('✅ Kafka reconnected');
    } catch (err) {
        console.error('❌ Kafka reconnect failed:', err.message);
    }
});

/* ══════════════════════════════════════
   In-memory session store
══════════════════════════════════════ */

const visitors    = {};       // userId → { first_visit, last_activity }
const activeUsers = new Set();
let   totalVisits  = 0;
let   uniqueUsers  = 0;

const SESSION_TIMEOUT = 15_000;   // 15 s

function userId(ip, userAgent) {
    return `${ip}_${userAgent.substring(0, 20)}`;
}

function pruneInactive() {
    const now = Date.now();
    for (const id of activeUsers) {
        if (visitors[id] && now - visitors[id].last_activity > SESSION_TIMEOUT) {
            activeUsers.delete(id);
        }
    }
}

setInterval(pruneInactive, 5_000);

/* ══════════════════════════════════════
   Kafka send helper
══════════════════════════════════════ */

async function sendToKafka(event, key) {
    try {
        await producer.send({
            topic:    'website_logs',
            messages: [{ key, value: JSON.stringify(event) }],
        });
    } catch (err) {
        console.error('Kafka send error:', err.message);
    }
}

/* ══════════════════════════════════════
   Tracking middleware
══════════════════════════════════════ */

const SKIP = ['/api', '.css', '.js', '.png', '.ico', 'favicon'];

app.use((req, res, next) => {
    const url = req.url;
    if (SKIP.some(s => url.includes(s))) return next();

    const ip        = (req.headers['x-forwarded-for'] || '').split(',')[0].trim()
                      || req.socket.remoteAddress
                      || 'unknown';
    const userAgent = req.get('User-Agent') || 'unknown';
    const uid       = userId(ip, userAgent);
    const now       = Date.now();

    const event = {
        ip,
        user_agent: userAgent,
        page:       url,
        timestamp:  new Date().toISOString(),
        action:     'visit',
    };

    sendToKafka(event, uid);

    if (!visitors[uid]) {
        visitors[uid] = { first_visit: now, last_activity: now };
        uniqueUsers++;
        totalVisits++;
    } else {
        visitors[uid].last_activity = now;
    }

    activeUsers.add(uid);
    next();
});

/* ══════════════════════════════════════
   Stats API
══════════════════════════════════════ */

app.get('/api/stats', (req, res) => {
    pruneInactive();
    res.json({
        current_active_users: activeUsers.size,
        total_unique_users:   uniqueUsers,
        total_visits:         totalVisits,
        last_updated:         new Date().toISOString(),
    });
});

/* ══════════════════════════════════════
   Static site
══════════════════════════════════════ */

app.use(express.static('public'));

/* ══════════════════════════════════════
   Start
══════════════════════════════════════ */

app.listen(PORT, () => {
    console.log('========================================');
    console.log('🌐 Website Traffic Monitor Started');
    console.log(`📡 Running at http://localhost:${PORT}`);
    console.log('========================================');
});
