const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');
const dgram = require('dgram');
const net = require('net');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const PORT = process.env.PORT || 3000;

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

// Global stats
const stats = {
  totalSent: 0,
  totalReceived: 0,
  totalBytesSent: 0,
  totalBytesReceived: 0,
  errors: 0,
  startTime: Date.now()
};

// Rate tracking
let recentBytesSent = 0;
let recentPacketsSent = 0;
let currentPps = 0;
let currentBps = 0;

setInterval(() => {
  currentPps = recentPacketsSent;
  currentBps = recentBytesSent;
  recentPacketsSent = 0;
  recentBytesSent = 0;

  broadcast({
    type: 'stats_tick',
    stats: {
      ...stats,
      currentPps,
      currentBps,
      activeStreams: activeStreams.size,
      activeListeners: activeListeners.size
    }
  });
}, 1000);

// Active streaming jobs and listeners
const activeStreams = new Map();
const activeListeners = new Map();

function broadcast(data) {
  const message = JSON.stringify(data);
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  });
}

// Payload generation helper
function buildPayload({ payloadType, payloadValue, templateName, randomSize, domainName }) {
  try {
    switch (payloadType) {
      case 'hex': {
        const cleanHex = (payloadValue || '').replace(/0x|[\s,;:-]/g, '');
        if (!cleanHex) return Buffer.alloc(0);
        return Buffer.from(cleanHex, 'hex');
      }
      case 'random': {
        const size = Math.max(1, Math.min(65507, parseInt(randomSize, 10) || 64));
        return crypto.randomBytes(size);
      }
      case 'template': {
        return buildTemplate(templateName, { domainName, payloadValue });
      }
      case 'text':
      default: {
        return Buffer.from(payloadValue || 'Hello from Packet Generator!', 'utf-8');
      }
    }
  } catch (err) {
    throw new Error(`Failed to generate payload: ${err.message}`);
  }
}

// Pre-defined protocol templates
function buildTemplate(name, options = {}) {
  switch (name) {
    case 'dns_query': {
      // Standard DNS Query for A record
      const domain = options.domainName || options.payloadValue || 'example.com';
      const parts = domain.split('.').filter(Boolean);
      const queryParts = [];
      for (const part of parts) {
        queryParts.push(Buffer.from([part.length]));
        queryParts.push(Buffer.from(part, 'ascii'));
      }
      queryParts.push(Buffer.from([0])); // End of domain
      queryParts.push(Buffer.from([0x00, 0x01])); // Type A
      queryParts.push(Buffer.from([0x00, 0x01])); // Class IN

      const questionBuf = Buffer.concat(queryParts);
      const txId = crypto.randomBytes(2);
      const flags = Buffer.from([0x01, 0x00]); // Standard query with recursion desired
      const qdcount = Buffer.from([0x00, 0x01]); // 1 question
      const ancount = Buffer.from([0x00, 0x00]);
      const nscount = Buffer.from([0x00, 0x00]);
      const arcount = Buffer.from([0x00, 0x00]);

      return Buffer.concat([txId, flags, qdcount, ancount, nscount, arcount, questionBuf]);
    }

    case 'http_get': {
      const host = options.domainName || options.payloadValue || 'localhost';
      const request = `GET / HTTP/1.1\r\nHost: ${host}\r\nUser-Agent: NetPulse-PacketGen/1.0\r\nAccept: */*\r\nConnection: close\r\n\r\n`;
      return Buffer.from(request, 'utf-8');
    }

    case 'syslog': {
      // RFC 5424 Syslog
      const now = new Date().toISOString();
      const message = options.payloadValue || 'NetPulse system event: test diagnostic packet';
      const syslogStr = `<34>1 ${now} netpulse-host packetgen 9999 ID47 - ${message}`;
      return Buffer.from(syslogStr, 'utf-8');
    }

    case 'ntp_request': {
      // 48-byte NTP client request packet
      const ntpBuf = Buffer.alloc(48);
      // LI = 0, VN = 4 (IPv4), Mode = 3 (Client) -> 0x23
      ntpBuf[0] = 0x23;
      // Add transmit timestamp bytes
      const nowSec = Math.floor(Date.now() / 1000) + 2208988800; // Epoch offset to 1900
      ntpBuf.writeUInt32BE(nowSec, 40);
      return ntpBuf;
    }

    case 'echo_ping':
    default: {
      const ts = Date.now();
      const str = `PING:NETPULSE:SEQ=${Math.floor(Math.random() * 100000)}:TIME=${ts}:DATA=${options.payloadValue || 'ECHO'}`;
      return Buffer.from(str, 'utf-8');
    }
  }
}

// Convert buffer to hex dump structure
function formatHexDump(buf, maxBytes = 256) {
  const length = Math.min(buf.length, maxBytes);
  const hex = [];
  const ascii = [];
  for (let i = 0; i < length; i++) {
    const b = buf[i];
    hex.push(b.toString(16).padStart(2, '0'));
    ascii.push(b >= 32 && b <= 126 ? String.fromCharCode(b) : '.');
  }
  return {
    rawHex: buf.toString('hex'),
    formattedHex: hex.join(' '),
    ascii: ascii.join(''),
    length: buf.length,
    truncated: buf.length > maxBytes
  };
}

// UDP Sender
function sendUdpPacket({ targetIp, targetPort, payloadBuf, listenResponse = true, responseTimeout = 3000, localPort = 0 }) {
  return new Promise((resolve, reject) => {
    const socket = dgram.createSocket('udp4');
    const sendTime = Date.now();
    let isResolved = false;
    let timer = null;

    const cleanup = () => {
      if (timer) clearTimeout(timer);
      try {
        socket.removeAllListeners();
        socket.close();
      } catch (e) {}
    };

    socket.on('error', (err) => {
      if (!isResolved) {
        isResolved = true;
        cleanup();
        stats.errors++;
        reject(err);
      }
    });

    if (listenResponse) {
      socket.on('message', (msg, rinfo) => {
        const latency = Date.now() - sendTime;
        stats.totalReceived++;
        stats.totalBytesReceived += msg.length;

        broadcast({
          type: 'packet_received',
          data: {
            timestamp: Date.now(),
            protocol: 'UDP',
            remoteIp: rinfo.address,
            remotePort: rinfo.port,
            size: msg.length,
            latency,
            hexDump: formatHexDump(msg)
          }
        });
      });

      timer = setTimeout(() => {
        cleanup();
      }, responseTimeout);
    }

    const doSend = () => {
      socket.send(payloadBuf, 0, payloadBuf.length, targetPort, targetIp, (err) => {
        if (err) {
          if (!isResolved) {
            isResolved = true;
            cleanup();
            stats.errors++;
            return reject(err);
          }
        }
        stats.totalSent++;
        stats.totalBytesSent += payloadBuf.length;
        recentPacketsSent++;
        recentBytesSent += payloadBuf.length;

        if (!isResolved) {
          isResolved = true;
          if (!listenResponse) {
            cleanup();
          }
          resolve({
            timestamp: sendTime,
            protocol: 'UDP',
            targetIp,
            targetPort,
            size: payloadBuf.length,
            payloadPreview: formatHexDump(payloadBuf, 64)
          });
        }
      });
    };

    if (localPort && localPort > 0) {
      socket.bind(localPort, () => {
        doSend();
      });
    } else {
      doSend();
    }
  });
}

// TCP Sender
function sendTcpPacket({ targetIp, targetPort, payloadBuf, listenResponse = true, timeout = 4000 }) {
  return new Promise((resolve, reject) => {
    const socket = new net.Socket();
    const sendTime = Date.now();
    let isResolved = false;
    let connectTime = 0;

    socket.setTimeout(timeout);

    const cleanup = () => {
      try {
        socket.removeAllListeners();
        socket.destroy();
      } catch (e) {}
    };

    socket.on('connect', () => {
      connectTime = Date.now() - sendTime;
      socket.write(payloadBuf, (err) => {
        if (err) {
          if (!isResolved) {
            isResolved = true;
            cleanup();
            stats.errors++;
            return reject(err);
          }
        }
        stats.totalSent++;
        stats.totalBytesSent += payloadBuf.length;
        recentPacketsSent++;
        recentBytesSent += payloadBuf.length;

        if (!listenResponse) {
          if (!isResolved) {
            isResolved = true;
            cleanup();
            resolve({
              timestamp: sendTime,
              protocol: 'TCP',
              targetIp,
              targetPort,
              size: payloadBuf.length,
              connectLatency: connectTime,
              payloadPreview: formatHexDump(payloadBuf, 64)
            });
          }
        }
      });
    });

    socket.on('data', (data) => {
      const latency = Date.now() - sendTime;
      stats.totalReceived++;
      stats.totalBytesReceived += data.length;

      broadcast({
        type: 'packet_received',
        data: {
          timestamp: Date.now(),
          protocol: 'TCP',
          remoteIp: targetIp,
          remotePort: targetPort,
          size: data.length,
          latency,
          hexDump: formatHexDump(data)
        }
      });

      if (!isResolved) {
        isResolved = true;
        resolve({
          timestamp: sendTime,
          protocol: 'TCP',
          targetIp,
          targetPort,
          size: payloadBuf.length,
          connectLatency: connectTime,
          responseLatency: latency,
          payloadPreview: formatHexDump(payloadBuf, 64)
        });
      }
    });

    socket.on('timeout', () => {
      cleanup();
      if (!isResolved) {
        isResolved = true;
        resolve({
          timestamp: sendTime,
          protocol: 'TCP',
          targetIp,
          targetPort,
          size: payloadBuf.length,
          connectLatency: connectTime,
          note: 'Connection/Response timed out',
          payloadPreview: formatHexDump(payloadBuf, 64)
        });
      }
    });

    socket.on('error', (err) => {
      cleanup();
      if (!isResolved) {
        isResolved = true;
        stats.errors++;
        reject(err);
      }
    });

    socket.on('close', () => {
      cleanup();
      if (!isResolved) {
        isResolved = true;
        resolve({
          timestamp: sendTime,
          protocol: 'TCP',
          targetIp,
          targetPort,
          size: payloadBuf.length,
          connectLatency: connectTime,
          payloadPreview: formatHexDump(payloadBuf, 64)
        });
      }
    });

    socket.connect(targetPort, targetIp);
  });
}

// Continuous stream manager
function startStream(config) {
  const streamId = config.id || `stream_${Date.now()}_${Math.random().toString(36).substr(2, 5)}`;
  const pps = Math.max(1, Math.min(5000, parseInt(config.pps, 10) || 10));
  const totalCount = parseInt(config.count, 10) || 0; // 0 = continuous
  const intervalMs = Math.max(1, Math.floor(1000 / pps));
  let sentInStream = 0;

  const timer = setInterval(async () => {
    if (totalCount > 0 && sentInStream >= totalCount) {
      stopStream(streamId);
      broadcast({ type: 'stream_completed', streamId, totalSent: sentInStream });
      return;
    }

    try {
      const payloadBuf = buildPayload(config);
      if (config.protocol === 'UDP') {
        await sendUdpPacket({
          targetIp: config.targetIp,
          targetPort: parseInt(config.targetPort, 10),
          payloadBuf,
          listenResponse: Boolean(config.listenResponse),
          localPort: parseInt(config.localPort, 10) || 0
        });
      } else {
        await sendTcpPacket({
          targetIp: config.targetIp,
          targetPort: parseInt(config.targetPort, 10),
          payloadBuf,
          listenResponse: Boolean(config.listenResponse)
        });
      }
      sentInStream++;
    } catch (err) {
      broadcast({
        type: 'stream_error',
        streamId,
        error: err.message
      });
    }
  }, intervalMs);

  activeStreams.set(streamId, { timer, config, startedAt: Date.now(), sentCount: () => sentInStream });

  broadcast({
    type: 'stream_started',
    streamId,
    config: {
      protocol: config.protocol,
      targetIp: config.targetIp,
      targetPort: config.targetPort,
      pps,
      totalCount
    }
  });

  return streamId;
}

function stopStream(streamId) {
  if (activeStreams.has(streamId)) {
    const stream = activeStreams.get(streamId);
    clearInterval(stream.timer);
    activeStreams.delete(streamId);
    broadcast({ type: 'stream_stopped', streamId });
    return true;
  }
  return false;
}

// Local Listener / Echo Server
function startLocalListener({ protocol, port, autoEcho = false }) {
  const listenerKey = `${protocol}_${port}`;
  if (activeListeners.has(listenerKey)) {
    throw new Error(`Listener already running on ${protocol} port ${port}`);
  }

  if (protocol === 'UDP') {
    const socket = dgram.createSocket('udp4');
    socket.on('message', (msg, rinfo) => {
      stats.totalReceived++;
      stats.totalBytesReceived += msg.length;

      broadcast({
        type: 'packet_received',
        data: {
          timestamp: Date.now(),
          protocol: 'UDP (Listener)',
          remoteIp: rinfo.address,
          remotePort: rinfo.port,
          listenerPort: port,
          size: msg.length,
          hexDump: formatHexDump(msg)
        }
      });

      if (autoEcho) {
        socket.send(msg, 0, msg.length, rinfo.port, rinfo.address);
      }
    });

    socket.on('error', (err) => {
      broadcast({ type: 'listener_error', listenerKey, error: err.message });
      stopLocalListener(listenerKey);
    });

    socket.bind(port, () => {
      activeListeners.set(listenerKey, {
        protocol,
        port,
        close: () => socket.close(),
        autoEcho
      });
      broadcast({ type: 'listener_started', protocol, port, autoEcho });
    });
  } else {
    // TCP Listener
    const tcpServer = net.createServer((cSocket) => {
      cSocket.on('data', (data) => {
        stats.totalReceived++;
        stats.totalBytesReceived += data.length;

        broadcast({
          type: 'packet_received',
          data: {
            timestamp: Date.now(),
            protocol: 'TCP (Listener)',
            remoteIp: cSocket.remoteAddress,
            remotePort: cSocket.remotePort,
            listenerPort: port,
            size: data.length,
            hexDump: formatHexDump(data)
          }
        });

        if (autoEcho) {
          cSocket.write(data);
        }
      });
    });

    tcpServer.on('error', (err) => {
      broadcast({ type: 'listener_error', listenerKey, error: err.message });
      stopLocalListener(listenerKey);
    });

    tcpServer.listen(port, () => {
      activeListeners.set(listenerKey, {
        protocol,
        port,
        close: () => tcpServer.close(),
        autoEcho
      });
      broadcast({ type: 'listener_started', protocol, port, autoEcho });
    });
  }
}

function stopLocalListener(listenerKey) {
  if (activeListeners.has(listenerKey)) {
    const listener = activeListeners.get(listenerKey);
    try {
      listener.close();
    } catch (e) {}
    activeListeners.delete(listenerKey);
    broadcast({ type: 'listener_stopped', listenerKey });
    return true;
  }
  return false;
}

// WebSocket handler
wss.on('connection', (ws) => {
  ws.send(JSON.stringify({
    type: 'init_state',
    stats: {
      ...stats,
      currentPps,
      currentBps,
      activeStreams: activeStreams.size,
      activeListeners: activeListeners.size
    },
    activeListeners: Array.from(activeListeners.entries()).map(([k, v]) => ({
      key: k,
      protocol: v.protocol,
      port: v.port,
      autoEcho: v.autoEcho
    }))
  }));

  ws.on('message', async (rawMessage) => {
    try {
      const data = JSON.parse(rawMessage);

      switch (data.action) {
        case 'preview_payload': {
          try {
            const buf = buildPayload(data);
            ws.send(JSON.stringify({
              type: 'payload_preview',
              hexDump: formatHexDump(buf)
            }));
          } catch (err) {
            ws.send(JSON.stringify({ type: 'error', message: err.message }));
          }
          break;
        }

        case 'send_single': {
          try {
            const payloadBuf = buildPayload(data);
            let result;
            if (data.protocol === 'UDP') {
              result = await sendUdpPacket({
                targetIp: data.targetIp,
                targetPort: parseInt(data.targetPort, 10),
                payloadBuf,
                listenResponse: Boolean(data.listenResponse),
                localPort: parseInt(data.localPort, 10) || 0
              });
            } else {
              result = await sendTcpPacket({
                targetIp: data.targetIp,
                targetPort: parseInt(data.targetPort, 10),
                payloadBuf,
                listenResponse: Boolean(data.listenResponse)
              });
            }

            broadcast({
              type: 'packet_sent',
              data: result
            });
          } catch (err) {
            ws.send(JSON.stringify({ type: 'send_error', error: err.message }));
          }
          break;
        }

        case 'send_burst': {
          const count = Math.max(1, Math.min(1000, parseInt(data.burstCount, 10) || 10));
          const delayMs = Math.max(0, parseInt(data.burstDelayMs, 10) || 10);
          const payloadBuf = buildPayload(data);

          let sentCount = 0;
          for (let i = 0; i < count; i++) {
            try {
              if (data.protocol === 'UDP') {
                sendUdpPacket({
                  targetIp: data.targetIp,
                  targetPort: parseInt(data.targetPort, 10),
                  payloadBuf,
                  listenResponse: Boolean(data.listenResponse)
                }).then((res) => {
                  broadcast({ type: 'packet_sent', data: res });
                }).catch(() => {});
              } else {
                sendTcpPacket({
                  targetIp: data.targetIp,
                  targetPort: parseInt(data.targetPort, 10),
                  payloadBuf,
                  listenResponse: Boolean(data.listenResponse)
                }).then((res) => {
                  broadcast({ type: 'packet_sent', data: res });
                }).catch(() => {});
              }
              sentCount++;
              if (delayMs > 0 && i < count - 1) {
                await new Promise(r => setTimeout(r, delayMs));
              }
            } catch (err) {
              stats.errors++;
            }
          }
          ws.send(JSON.stringify({ type: 'burst_completed', count: sentCount }));
          break;
        }

        case 'start_stream': {
          startStream(data);
          break;
        }

        case 'stop_stream': {
          stopStream(data.streamId);
          break;
        }

        case 'stop_all_streams': {
          for (const [id] of activeStreams) {
            stopStream(id);
          }
          break;
        }

        case 'start_listener': {
          try {
            startLocalListener({
              protocol: data.protocol,
              port: parseInt(data.port, 10),
              autoEcho: Boolean(data.autoEcho)
            });
          } catch (err) {
            ws.send(JSON.stringify({ type: 'listener_error', error: err.message }));
          }
          break;
        }

        case 'stop_listener': {
          stopLocalListener(data.listenerKey);
          break;
        }

        case 'clear_stats': {
          stats.totalSent = 0;
          stats.totalReceived = 0;
          stats.totalBytesSent = 0;
          stats.totalBytesReceived = 0;
          stats.errors = 0;
          broadcast({ type: 'stats_cleared' });
          break;
        }
      }
    } catch (err) {
      ws.send(JSON.stringify({ type: 'error', message: `Malformed request: ${err.message}` }));
    }
  });
});

// REST API endpoints for automation / scripting
app.post('/api/send', async (req, res) => {
  try {
    const { protocol, targetIp, targetPort, payloadType, payloadValue, templateName, randomSize } = req.body;
    if (!targetIp || !targetPort || !protocol) {
      return res.status(400).json({ error: 'targetIp, targetPort, and protocol are required' });
    }
    const payloadBuf = buildPayload({ payloadType, payloadValue, templateName, randomSize });
    let result;
    if (protocol.toUpperCase() === 'UDP') {
      result = await sendUdpPacket({
        targetIp,
        targetPort: parseInt(targetPort, 10),
        payloadBuf,
        listenResponse: false
      });
    } else {
      result = await sendTcpPacket({
        targetIp,
        targetPort: parseInt(targetPort, 10),
        payloadBuf,
        listenResponse: false
      });
    }
    return res.json({ success: true, result });
  } catch (err) {
    return res.status(500).json({ success: false, error: err.message });
  }
});

app.get('/api/stats', (req, res) => {
  res.json({
    ...stats,
    currentPps,
    currentBps,
    activeStreams: activeStreams.size,
    activeListeners: activeListeners.size
  });
});

server.listen(PORT, () => {
  console.log(`🚀 NetPulse Packet Generator Server running on http://localhost:${PORT}`);
});
