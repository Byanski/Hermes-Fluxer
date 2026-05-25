import 'dotenv/config';
import { connect, StringCodec, NatsConnection } from 'nats';
import Redis from 'ioredis';
import axios from 'axios';
import WebSocket from 'ws';
import FormData from 'form-data';

// ==========================================
// CONFIGURATION
// ==========================================
const FLUXER_API_URL = process.env.FLUXER_API_URL || 'https://api.fluxer.app/v1';
const BOT_TOKEN = process.env.BOT_TOKEN;
const NATS_URL = process.env.NATS_URL || 'nats://localhost:4222';
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';

if (!BOT_TOKEN) {
    console.error("❌ ERROR: BOT_TOKEN is missing in your .env file!");
    process.exit(1);
}

// ==========================================
// STATE MANAGEMENT & HTTP CLIENT
// ==========================================
const redis = new Redis(REDIS_URL);
const sc = StringCodec();
const pendingEdits = new Map<string, NodeJS.Timeout>();
const processedExecutions = new Set<string>();

// Pre-configured Axios client for Fluxer's REST API
const fluxerClient = axios.create({
    baseURL: FLUXER_API_URL,
    headers: {
        'Authorization': `Bot ${BOT_TOKEN}`
    }
});

// ==========================================
// CORE INITIALIZATION
// ==========================================
async function main() {
    let nc: NatsConnection;
    try {
        nc = await connect({ servers: NATS_URL });
        console.log("🟢 Connected to local NATS message queue");
    } catch (err) {
        console.error("❌ Failed to connect to NATS:", err);
        process.exit(1);
    }

    nc.subscribe("hermes.execution.state_change", {
        callback: async (err, msg) => {
            if (err) return console.error("NATS Subscription Error:", err);
            try {
                const data = JSON.parse(sc.decode(msg.data));
                await handleStateChange(data);
            } catch (pErr) {
                console.error("Error processing worker event payload:", pErr);
            }
        }
    });

    try {
        console.log("🔍 Requesting Gateway URL from Fluxer API...");
        const response = await fluxerClient.get('/gateway/bot', {
            headers: { 'Content-Type': 'application/json' }
        }); 
        
        const GATEWAY_URL = `${response.data.url}?v=1&encoding=json`;
        connectToFluxerGateway(nc, GATEWAY_URL);
    } catch (err: any) {
        console.error("❌ Failed to fetch Gateway URL from Fluxer:");
        console.error(err.response?.data || err.message);
        process.exit(1);
    }
}

// ==========================================
// WEBSOCKET INGRESS HANDLER
// ==========================================
function connectToFluxerGateway(nc: NatsConnection, gatewayUrl: string) {
    console.log(`🔌 Connecting to Fluxer Gateway at: ${gatewayUrl}`);
    const ws = new WebSocket(gatewayUrl);
    let heartbeatInterval: NodeJS.Timeout;

    ws.on('open', () => {
        console.log("⚡ Socket opened. Waiting for server HELLO handshake...");
    });

    ws.on('message', async (rawData: string) => {
        try {
            const packet = JSON.parse(rawData);
            const { op, t, d } = packet;

            if (op === 10 || op === "HELLO") {
                const interval = d.heartbeat_interval || 41250;
                
                heartbeatInterval = setInterval(() => {
                    ws.send(JSON.stringify({ op: 1, d: null })); 
                }, interval);
                console.log(`📡 Heartbeat loop initiated every ${interval}ms`);

                const identifyPayload = {
                    op: 2, 
                    d: { 
                        token: BOT_TOKEN,
                        intents: 33280, // Guilds + Guild Messages + Message Content
                        properties: {
                            os: process.platform,
                            browser: "fluxer-hermes-adapter",
                            device: "fluxer-hermes-adapter"
                        }
                    }
                };
                ws.send(JSON.stringify(identifyPayload));
                console.log("🔐 Sent IDENTIFY payload with Intents...");
            }

            if ((op === 0 || op === "DISPATCH") && t === "READY") {
                console.log(`✅ Successfully authenticated as Bot! Ready to receive messages.`);
            }

            if ((op === 0 || op === "DISPATCH") && t === "MESSAGE_CREATE") {
                const { id: messageId, channel_id: channelId, content, author, mentions } = d;

                if (author?.bot) return;

                console.log(`[DEBUG] Incoming msg from ${author.username}: "${content}"`);

                const isMentioned = 
                    (mentions && mentions.some((user: any) => user.username.toLowerCase() === "hermes")) || 
                    (content && content.toLowerCase().includes("hermes"));

                if (isMentioned) {
                    console.log(`📥 Live prompt received in channel ${channelId} from user ${author.id}`);
                    
                    try {
                        await fluxerClient.post(`/channels/${channelId}/typing`, {}, {
                            headers: { 'Content-Type': 'application/json' }
                        });
                    } catch (e: any) {
                        console.error("Failed to trigger typing status:", e.response?.data || e.message);
                    }

                    const cleanPrompt = content.replace(/<@!?\d+>/g, "").trim();

                    const executionId = `exec_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;
                    const workerJob = {
                        event: "inference_requested",
                        metadata: { execution_id: executionId, timestamp: Date.now() },
                        payload: {
                            user_id: author.id,
                            channel_id: channelId,
                            prompt: cleanPrompt
                        }
                    };

                    nc.publish("hermes.worker.queue", sc.encode(JSON.stringify(workerJob)));
                }
            }
        } catch (err: any) {
            console.error("Failed parsing incoming WebSocket frame:", err.message);
        }
    });

    ws.on('close', (code, reason) => {
        console.log(`⚠️ Fluxer Gateway connection closed (Code: ${code}). Reconnecting in 5 seconds...`);
        if (reason) console.log(`Reason given by server: ${reason}`);
        clearInterval(heartbeatInterval);
        setTimeout(() => connectToFluxerGateway(nc, gatewayUrl), 5000);
    });

    ws.on('error', (err) => {
        console.error("WebSocket connection error:", err.message);
    });
}

// ==========================================
// REST EGRESS (STATE MACHINE UPDATES)
// ==========================================
async function handleStateChange(data: any) {
    const { execution_id } = data.metadata;
    const { display_text, is_final, channel_id, attachments } = data.payload;

    // GATEKEEPER: If we've already finalized this, ignore all future packets
    if (processedExecutions.has(execution_id)) {
        console.log(`🚫 Dropping redundant update for completed job: ${execution_id}`);
        return;
    }

    const stateKey = `hermes:execution:${execution_id}`;
    const stateStr = await redis.get(stateKey);

    // SAFETY LOCK: If the state is gone, the job is done. DO NOT process updates.
    if (!stateStr && !is_final) {
        console.log(`⚠️ Ignoring late-arriving packet for finished job: ${execution_id}`);
        return;
    }
    const state = stateStr ? JSON.parse(stateStr) : {};

    // Phase A: Create Placeholder
    if (!state.fluxer_message_id) {
        try {
            const res = await fluxerClient.post(`/channels/${channel_id}/messages`, 
                { content: display_text },
                { headers: { 'Content-Type': 'application/json' } }
            );
            
            state.fluxer_message_id = res.data.id;
            await redis.set(stateKey, JSON.stringify(state), 'EX', 300);
            console.log(`📝 Live placeholder created. ID: ${state.fluxer_message_id}`);
        } catch (err: any) {
            console.error("Failed creating placeholder:", err.response?.data || err.message);
        }
        return;
    }

    // Phase B: Debounce Updates
    if (pendingEdits.has(execution_id) && !is_final) {
        clearTimeout(pendingEdits.get(execution_id));
    }

    const editTask = setTimeout(async () => {
        try {
            let safeText = display_text.length > 4000 ? display_text.substring(0, 3990) + "\n\n...[Truncated]" : display_text;

            await fluxerClient.patch(`/channels/${channel_id}/messages/${state.fluxer_message_id}`, 
                { content: safeText },
                { headers: { 'Content-Type': 'application/json' } }
            );

            const parsedAttachments = attachments || [];
            if (is_final && parsedAttachments.length > 0) {
                const form = new FormData();
                form.append('payload_json', JSON.stringify({ content: "📎 **Generated Files:**" }));
                parsedAttachments.forEach((att: any, index: number) => {
                    form.append(`files[${index}]`, Buffer.from(att.data, 'base64'), { filename: att.filename });
                });
                await fluxerClient.post(`/channels/${channel_id}/messages`, form, { headers: form.getHeaders() });
            }

            if (is_final) {
                processedExecutions.add(execution_id);
                // Clean up the set entry after 5 minutes to prevent memory leaks
                setTimeout(() => processedExecutions.delete(execution_id), 300000);
                await redis.del(stateKey); // The Lock
                console.log(`🧹 Finalized and cleared state for: ${execution_id}`);
            }
        } catch (err: any) {
            console.error(`Patching failed:`, err.response?.data || err.message);
        }
        pendingEdits.delete(execution_id);
    }, is_final ? 0 : 2000);

    pendingEdits.set(execution_id, editTask);
}

// Boot the service
main();
