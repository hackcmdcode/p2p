// tracker/server.js - WebSocket signaling server for TCP P2P
const WebSocket = require('ws');
const http = require('http');
const crypto = require('crypto');

const server = http.createServer((req, res) => {
    if (req.url === '/health') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
            status: 'ok',
            peers: peers.size,
            senders: fileMetadata.size,
            timestamp: new Date().toISOString()
        }));
        return;
    }
    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.end('<h1>P2P Tracker Server Running</h1>');
});

const wss = new WebSocket.Server({ server });

const peers = new Map();          // peerId -> { ws, type, ip }
const fileMetadata = new Map();  // senderId -> { files, hash, totalSize }

console.log('🔍 P2P TCP Tracker started');

wss.on('connection', (ws, req) => {
    const peerId = crypto.randomBytes(16).toString('hex');
    const clientIp = req.socket.remoteAddress.replace(/^::ffff:/, '');
    let peerType = null;

    console.log(`🟢 ${peerId} connected from ${clientIp}`);

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);

            // ---- Register ----
            if (data.type === 'register') {
                peerType = data.peerType;
                peers.set(peerId, { ws, type: peerType, ip: clientIp });

                if (peerType === 'sender') {
                    fileMetadata.set(peerId, {
                        files: data.files,
                        hash: data.hash,
                        totalSize: data.totalSize,
                        timestamp: Date.now()
                    });
                    console.log(`📤 Sender registered: ${peerId}`);
                } else {
                    console.log(`📥 Receiver registered: ${peerId}`);
                    // Send list of available senders to this receiver
                    const senders = [];
                    for (const [id, meta] of fileMetadata.entries()) {
                        if (peers.has(id) && peers.get(id).type === 'sender') {
                            senders.push({
                                peerId: id,
                                files: meta.files,
                                hash: meta.hash,
                                totalSize: meta.totalSize
                            });
                        }
                    }
                    ws.send(JSON.stringify({ type: 'available_senders', senders }));
                }
                ws.send(JSON.stringify({ type: 'registered', peerId }));
            }

            // ---- Download request ----
            else if (data.type === 'request_download') {
                const sender = peers.get(data.senderId);
                if (!sender || sender.type !== 'sender') {
                    ws.send(JSON.stringify({ type: 'error', message: 'Sender not found' }));
                    return;
                }
                // Tell sender to start its TCP server
                sender.ws.send(JSON.stringify({
                    type: 'start_tcp',
                    receiverId: peerId,
                    receiverIp: clientIp
                }));
                ws.send(JSON.stringify({ type: 'download_started', senderId: data.senderId }));
            }

            // ---- TCP connection info from sender ----
            else if (data.type === 'tcp_info') {
                const receiver = peers.get(data.receiverId);
                if (receiver) {
                    receiver.ws.send(JSON.stringify({
                        type: 'tcp_info',
                        senderId: peerId,
                        host: clientIp,   // sender's IP seen by tracker
                        port: data.port
                    }));
                }
            }

            // ---- Heartbeat ----
            else if (data.type === 'heartbeat') {
                ws.send(JSON.stringify({ type: 'heartbeat_ack' }));
            }

            // ---- Disconnect ----
            else if (data.type === 'disconnect') {
                cleanupPeer(peerId);
            }

        } catch (err) {
            console.error('Error:', err);
        }
    });

    ws.on('close', () => {
        console.log(`🔴 ${peerId} disconnected`);
        cleanupPeer(peerId);
    });

    ws.on('error', (err) => {
        console.error(`WebSocket error for ${peerId}:`, err);
        cleanupPeer(peerId);
    });

    function cleanupPeer(id) {
        peers.delete(id);
        fileMetadata.delete(id);
    }
});

// Periodic cleanup of stale metadata
setInterval(() => {
    const oneHourAgo = Date.now() - 3600000;
    for (const [id, meta] of fileMetadata.entries()) {
        if (meta.timestamp < oneHourAgo) {
            fileMetadata.delete(id);
        }
    }
}, 30000);

const PORT = process.env.PORT || 8080;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`🚀 Tracker on ws://0.0.0.0:${PORT}`);
    console.log(`   Health: http://localhost:${PORT}/health`);
});
