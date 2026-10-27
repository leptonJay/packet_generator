# NetPulse - UDP & TCP Packet Generator & Traffic Analyzer

A modern, high-performance web-based network utility for generating, transmitting, and inspecting UDP and TCP packets.

![NetPulse Banner](https://img.shields.io/badge/NetPulse-PacketGen-00f2fe?style=for-the-badge&logo=wireguard)

---

## ⚡ Features

- **Multi-Protocol Support**:
  - **UDP (Datagrams)**: Fire-and-forget or reply-listening modes with configurable ephemeral or fixed source binding.
  - **TCP (Stream)**: Full connection handshake, TTFB/connect latency measurement, data push, and reply capture.
- **Transmission Modes**:
  - **Single Shot**: Individual packet dispatch for precise probes.
  - **Burst Mode**: Transmit batches (e.g. 10 to 1,000 packets) with custom inter-packet delay intervals (ms).
  - **Continuous Stream**: High-throughput packet streaming with configurable PPS (packets-per-second, up to 5,000 PPS) and limit caps.
- **Payload Composer & Real-time Hex Inspection**:
  - **Plaintext (UTF-8)**: Type any string with live byte count.
  - **Raw Hex Stream**: Provide hex values (`48 65 6c 6c 6f` or continuous `deadbeef`).
  - **Random Noise Generator**: Specify random byte buffers from 8 to 65,507 bytes with one-click regeneration.
  - **Protocol Templates**:
    - **DNS Query**: Valid RFC 1035 DNS Query packet for custom domains.
    - **HTTP GET Request**: HTTP/1.1 formatted request with custom host headers.
    - **Syslog (RFC 5424)**: Automated syslog facility/severity messages with ISO timestamps.
    - **NTP Request**: 48-byte network time protocol client datagram.
    - **Echo Ping**: High-precision timestamp probe.
- **Built-in Local Receiver / Echo Server**:
  - Test packet transmission locally on your own machine without external tools (e.g. Wireshark/Netcat).
  - Open UDP or TCP listener sockets on any port (e.g. `5005`) with an optional **Auto-Echo** toggle to send replies back to the sender.
- **Real-time Live Telemetry & Inspector**:
  - Live PPS (Packets Per Second) and Bandwidth (KB/s).
  - Interactive packet feed table with direction tags (TX/RX), protocol badges, latency, and previews.
  - Deep-dive Hex Dump and ASCII sidebar inspector for any selected packet.
- **REST API**:
  - Send packets programmatically via `POST /api/send`.
  - Fetch telemetry via `GET /api/stats`.

---

## 🚀 Quick Start

### 1. Install dependencies
```bash
npm install
```

### 2. Start the server
```bash
npm start
```

### 3. Open in Browser
Open your browser and navigate to:
```
http://localhost:3000
```

---

## 🔌 REST API Endpoints

### Send Packet
```http
POST /api/send
Content-Type: application/json

{
  "protocol": "UDP",
  "targetIp": "127.0.0.1",
  "targetPort": 5005,
  "payloadType": "text",
  "payloadValue": "Test message from API"
}
```

### Get Telemetry Stats
```http
GET /api/stats
```

---

## 🛡️ License
ISC
