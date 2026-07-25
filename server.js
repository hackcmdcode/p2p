// server.js – WebSocket tracker for P2P file transfer
const WebSocket = require('ws');
const http = require('http');
const crypto = require('crypto');

const server = http.createServer();
const wss = new WebSocket.Server({ server });

// Store peers and their metadata
const peers = new Map();          // peerId -> { ws, type, info, senderId? }
const fileMetadata = new Map();   // senderId -> { files, hash, totalSize }

console.log('🔍 P2P Tracker Server started');

wss.on('connection', (ws, req) => {
    const peerId = crypto.randomBytes(16).toString('hex');
    let peerType = null;
    let senderId = null; // for receiver, the sender they are connected to

    console.log(`🟢 New connection: ${peerId}`);

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            switch (data.type) {
                case 'register':
                    peerType = data.peerType;
                    if (peerType === 'sender') {
                        // Store file metadata
                        fileMetadata.set(peerId, {
                            files: data.files,
                            hash: data.hash,
                            totalSize: data.totalSize,
                            timestamp: Date.now()
                        });
                        peers.set(peerId, { ws, type: 'sender', info: data.info || {} });
                        console.log(`📤 Sender registered: ${peerId} with ${data.files.length} files`);
                        ws.send(JSON.stringify({ type: 'registered', status: 'success', peerId }));
                    } else if (peerType === 'receiver') {
                        peers.set(peerId, { ws, type: 'receiver' });
                        console.log(`📥 Receiver registered: ${peerId}`);
                        // Send list of available senders
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
                    break;

                case 'request_download':
                    // Receiver requests to download from a specific sender
                    const targetSenderId = data.senderId;
                    const senderPeer = peers.get(targetSenderId);
                    if (senderPeer && senderPeer.type === 'sender') {
                        senderId = targetSenderId;
                        const meta = fileMetadata.get(targetSenderId);
                        // Forward request to sender
                        senderPeer.ws.send(JSON.stringify({
                            type: 'download_request',
                            receiverId: peerId,
                            files: meta.files,
                            hash: meta.hash
                        }));
                        ws.send(JSON.stringify({ type: 'download_requested', status: 'pending' }));
                    } else {
                        ws.send(JSON.stringify({ type: 'error', message: 'Sender not found' }));
                    }
                    break;

                case 'signal':
                    // Relay WebRTC signaling (offer, answer, ice) between peers
                    const targetId = data.targetId;
                    const targetPeer = peers.get(targetId);
                    if (targetPeer) {
                        targetPeer.ws.send(JSON.stringify({
                            type: 'signal',
                            fromId: peerId,
                            signal: data.signal
                        }));
                    } else {
                        ws.send(JSON.stringify({ type: 'error', message: 'Target peer not found' }));
                    }
                    break;

                case 'heartbeat':
                    ws.send(JSON.stringify({ type: 'heartbeat_ack', timestamp: Date.now() }));
                    break;

                case 'disconnect':
                    console.log(`👋 Peer ${peerId} disconnecting`);
                    cleanupPeer(peerId);
                    break;

                default:
                    console.warn(`Unknown message type: ${data.type}`);
            }
        } catch (err) {
            console.error('Error processing message:', err);
            ws.send(JSON.stringify({ type: 'error', message: 'Invalid message' }));
        }
    });

    ws.on('close', () => {
        console.log(`🔴 Connection closed: ${peerId}`);
        cleanupPeer(peerId);
    });

    ws.on('error', (err) => {
        console.error(`WebSocket error for ${peerId}:`, err);
        cleanupPeer(peerId);
    });

    function cleanupPeer(id) {
        peers.delete(id);
        fileMetadata.delete(id);
        // Also remove any receiver that was connected to this sender? Not needed.
    }
});

// Periodic cleanup of stale connections
setInterval(() => {
    const now = Date.now();
    for (const [id, peer] of peers) {
        if (peer.ws.readyState === WebSocket.CLOSED) {
            peers.delete(id);
            fileMetadata.delete(id);
        }
    }
}, 30000);

const PORT = process.env.PORT || 8080;
server.listen(PORT, () => {
    console.log(`🚀 Tracker server running on ws://localhost:${PORT}`);
    console.log(`   (Replace with your deployed URL when hosting)`);
});