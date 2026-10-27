// NetPulse Client Application Logic
(function () {
  'use strict';

  // State Management
  const state = {
    protocol: 'UDP', // 'UDP' | 'TCP'
    mode: 'single',  // 'single' | 'burst' | 'stream'
    payloadType: 'text', // 'text' | 'hex' | 'random' | 'template'
    currentStreamId: null,
    activeListeners: new Map(),
    packets: [],
    filter: 'all', // 'all' | 'sent' | 'received'
    selectedPacket: null,
    autoScroll: true
  };

  // DOM Elements
  const el = {
    // Header telemetry
    metricTotalSent: document.getElementById('metricTotalSent'),
    metricTotalReceived: document.getElementById('metricTotalReceived'),
    metricLivePps: document.getElementById('metricLivePps'),
    metricLiveBps: document.getElementById('metricLiveBps'),
    metricErrors: document.getElementById('metricErrors'),
    wsStatus: document.getElementById('wsStatus'),
    btnResetStats: document.getElementById('btnResetStats'),

    // Target Config
    btnProtoUDP: document.getElementById('btnProtoUDP'),
    btnProtoTCP: document.getElementById('btnProtoTCP'),
    inputTargetIp: document.getElementById('inputTargetIp'),
    inputTargetPort: document.getElementById('inputTargetPort'),
    checkListenResponse: document.getElementById('checkListenResponse'),
    udpLocalPortWrap: document.getElementById('udpLocalPortWrap'),
    inputLocalPort: document.getElementById('inputLocalPort'),

    // Modes
    tabModeSingle: document.getElementById('tabModeSingle'),
    tabModeBurst: document.getElementById('tabModeBurst'),
    tabModeStream: document.getElementById('tabModeStream'),
    modeConfigBurst: document.getElementById('modeConfigBurst'),
    modeConfigStream: document.getElementById('modeConfigStream'),
    inputBurstCount: document.getElementById('inputBurstCount'),
    inputBurstDelay: document.getElementById('inputBurstDelay'),
    inputStreamPps: document.getElementById('inputStreamPps'),
    inputStreamLimit: document.getElementById('inputStreamLimit'),
    btnTriggerSend: document.getElementById('btnTriggerSend'),
    btnTriggerLabel: document.getElementById('btnTriggerLabel'),
    btnStopStream: document.getElementById('btnStopStream'),

    // Payload Composer
    wrapInputText: document.getElementById('wrapInputText'),
    wrapInputHex: document.getElementById('wrapInputHex'),
    wrapInputRandom: document.getElementById('wrapInputRandom'),
    wrapInputTemplate: document.getElementById('wrapInputTemplate'),
    inputPayloadText: document.getElementById('inputPayloadText'),
    inputPayloadHex: document.getElementById('inputPayloadHex'),
    rangeRandomSize: document.getElementById('rangeRandomSize'),
    inputRandomSize: document.getElementById('inputRandomSize'),
    btnRegenerateRandom: document.getElementById('btnRegenerateRandom'),
    selectTemplate: document.getElementById('selectTemplate'),
    wrapTemplateParam: document.getElementById('wrapTemplateParam'),
    labelTemplateParam: document.getElementById('labelTemplateParam'),
    inputTemplateParam: document.getElementById('inputTemplateParam'),
    badgeByteSize: document.getElementById('badgeByteSize'),
    badgeHexByteSize: document.getElementById('badgeHexByteSize'),
    previewHex: document.getElementById('previewHex'),
    previewAscii: document.getElementById('previewAscii'),
    previewLengthInfo: document.getElementById('previewLengthInfo'),

    // Local Receiver
    selectListenerProto: document.getElementById('selectListenerProto'),
    inputListenerPort: document.getElementById('inputListenerPort'),
    checkAutoEcho: document.getElementById('checkAutoEcho'),
    btnToggleListener: document.getElementById('btnToggleListener'),
    btnListenerText: document.getElementById('btnListenerText'),
    listenerStatusBadge: document.getElementById('listenerStatusBadge'),
    activeListenersBox: document.getElementById('activeListenersBox'),

    // Console & Table
    packetFeedBody: document.getElementById('packetFeedBody'),
    rowEmptyPlaceholder: document.getElementById('rowEmptyPlaceholder'),
    btnClearFeed: document.getElementById('btnClearFeed'),
    checkAutoScroll: document.getElementById('checkAutoScroll'),
    tableContainer: document.querySelector('.table-container'),
    packetDetailPanel: document.getElementById('packetDetailPanel'),
    detailBodyContent: document.getElementById('detailBodyContent'),
    btnCloseDetail: document.getElementById('btnCloseDetail'),
    toastContainer: document.getElementById('toastContainer')
  };

  // WebSocket Connection
  let ws = null;
  let reconnectTimer = null;

  function initWebSocket() {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${window.location.host}`;

    el.wsStatus.className = 'connection-status';
    el.wsStatus.querySelector('.status-text').textContent = 'CONNECTING';

    try {
      ws = new WebSocket(wsUrl);

      ws.onopen = () => {
        el.wsStatus.className = 'connection-status online';
        el.wsStatus.querySelector('.status-text').textContent = 'ONLINE';
        if (reconnectTimer) clearInterval(reconnectTimer);
        requestPayloadPreview();
      };

      ws.onclose = () => {
        el.wsStatus.className = 'connection-status offline';
        el.wsStatus.querySelector('.status-text').textContent = 'OFFLINE';
        scheduleReconnect();
      };

      ws.onerror = () => {
        el.wsStatus.className = 'connection-status offline';
        el.wsStatus.querySelector('.status-text').textContent = 'ERROR';
      };

      ws.onmessage = (event) => {
        try {
          const msg = JSON.parse(event.data);
          handleServerMessage(msg);
        } catch (err) {
          console.error('Error handling WebSocket message:', err);
        }
      };
    } catch (e) {
      scheduleReconnect();
    }
  }

  function scheduleReconnect() {
    if (!reconnectTimer) {
      reconnectTimer = setInterval(() => {
        if (!ws || ws.readyState === WebSocket.CLOSED) {
          initWebSocket();
        }
      }, 3000);
    }
  }

  function sendWS(data) {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(data));
      return true;
    } else {
      showToast('Backend disconnected. Reconnecting...', 'error');
      return false;
    }
  }

  // Handle incoming WebSocket messages
  function handleServerMessage(msg) {
    switch (msg.type) {
      case 'init_state':
      case 'stats_tick': {
        updateStats(msg.stats);
        if (msg.activeListeners) {
          renderActiveListeners(msg.activeListeners);
        }
        break;
      }

      case 'packet_sent': {
        addPacketToFeed({
          type: 'TX',
          timestamp: msg.data.timestamp,
          protocol: msg.data.protocol,
          address: `${msg.data.targetIp}:${msg.data.targetPort}`,
          length: msg.data.size,
          latency: msg.data.connectLatency ? `${msg.data.connectLatency} ms (conn)` : '-',
          preview: msg.data.payloadPreview ? msg.data.payloadPreview.ascii : '',
          hexDump: msg.data.payloadPreview
        });
        break;
      }

      case 'packet_received': {
        addPacketToFeed({
          type: 'RX',
          timestamp: msg.data.timestamp,
          protocol: msg.data.protocol,
          address: `${msg.data.remoteIp}:${msg.data.remotePort}`,
          length: msg.data.size,
          latency: msg.data.latency ? `${msg.data.latency} ms` : '-',
          preview: msg.data.hexDump ? msg.data.hexDump.ascii : '',
          hexDump: msg.data.hexDump
        });
        break;
      }

      case 'payload_preview': {
        renderPayloadPreview(msg.hexDump);
        break;
      }

      case 'stream_started': {
        state.currentStreamId = msg.streamId;
        el.btnTriggerSend.style.display = 'none';
        el.btnStopStream.style.display = 'inline-flex';
        showToast(`Streaming started (${msg.config.pps} PPS)`, 'info');
        break;
      }

      case 'stream_stopped':
      case 'stream_completed': {
        state.currentStreamId = null;
        el.btnTriggerSend.style.display = 'inline-flex';
        el.btnStopStream.style.display = 'none';
        showToast(msg.type === 'stream_completed' ? `Continuous stream finished (${msg.totalSent} sent)` : 'Stream stopped', 'info');
        break;
      }

      case 'listener_started': {
        showToast(`Listener active on ${msg.protocol} port ${msg.port}`, 'success');
        break;
      }

      case 'listener_stopped': {
        showToast(`Listener stopped`, 'info');
        break;
      }

      case 'listener_error':
      case 'send_error':
      case 'stream_error':
      case 'error': {
        showToast(msg.error || msg.message, 'error');
        break;
      }

      case 'burst_completed': {
        showToast(`Burst transmission completed: ${msg.count} packets sent`, 'success');
        break;
      }
    }
  }

  // Update Telemetry Metrics
  function updateStats(stats) {
    if (!stats) return;
    el.metricTotalSent.textContent = stats.totalSent.toLocaleString();
    el.metricTotalReceived.textContent = stats.totalReceived.toLocaleString();
    el.metricLivePps.textContent = stats.currentPps.toLocaleString();
    
    // Format Bps to KB/s or MB/s
    const kbps = (stats.currentBps / 1024).toFixed(1);
    el.metricLiveBps.textContent = `${kbps} KB/s`;

    el.metricErrors.textContent = stats.errors.toLocaleString();
  }

  // Payload Preview
  function requestPayloadPreview() {
    const config = getPayloadConfig();
    sendWS({
      action: 'preview_payload',
      ...config
    });
  }

  function getPayloadConfig() {
    const type = state.payloadType;
    let payloadValue = '';
    let randomSize = 128;
    let templateName = '';
    let domainName = '';

    if (type === 'text') {
      payloadValue = el.inputPayloadText.value;
    } else if (type === 'hex') {
      payloadValue = el.inputPayloadHex.value;
    } else if (type === 'random') {
      randomSize = parseInt(el.inputRandomSize.value, 10) || 128;
    } else if (type === 'template') {
      templateName = el.selectTemplate.value;
      domainName = el.inputTemplateParam.value;
      payloadValue = domainName;
    }

    return {
      payloadType: type,
      payloadValue,
      randomSize,
      templateName,
      domainName
    };
  }

  function renderPayloadPreview(hexDump) {
    if (!hexDump) return;
    el.previewLengthInfo.textContent = `Length: ${hexDump.length} bytes`;
    el.previewHex.textContent = hexDump.formattedHex || '-';
    el.previewAscii.textContent = hexDump.ascii || '-';

    if (state.payloadType === 'text') {
      el.badgeByteSize.textContent = `${hexDump.length} bytes`;
    } else if (state.payloadType === 'hex') {
      el.badgeHexByteSize.textContent = `${hexDump.length} bytes`;
    }
  }

  // Packet Feed Management
  function addPacketToFeed(packet) {
    state.packets.push(packet);
    if (state.packets.length > 500) {
      state.packets.shift();
    }

    // Check filter
    if (state.filter === 'sent' && packet.type !== 'TX') return;
    if (state.filter === 'received' && packet.type !== 'RX') return;

    if (el.rowEmptyPlaceholder) {
      el.rowEmptyPlaceholder.style.display = 'none';
    }

    const tr = document.createElement('tr');
    tr.className = 'packet-row';
    const timeStr = new Date(packet.timestamp).toLocaleTimeString();
    const protoLower = packet.protocol.toLowerCase().includes('udp') ? 'udp' : 'tcp';
    const dirClass = packet.type === 'TX' ? 'tx' : 'rx';

    tr.innerHTML = `
      <td>${timeStr}</td>
      <td><span class="badge-dir ${dirClass}">${packet.type}</span></td>
      <td><span class="badge-proto ${protoLower}">${packet.protocol.replace(' (Listener)', '')}</span></td>
      <td>${escapeHtml(packet.address)}</td>
      <td>${packet.length} B</td>
      <td>${packet.latency}</td>
      <td title="${escapeHtml(packet.preview)}">${escapeHtml(packet.preview.substring(0, 32))}${packet.preview.length > 32 ? '...' : ''}</td>
    `;

    tr.addEventListener('click', () => {
      document.querySelectorAll('.packet-row.selected').forEach(r => r.classList.remove('selected'));
      tr.classList.add('selected');
      showPacketDetail(packet);
    });

    el.packetFeedBody.appendChild(tr);

    if (state.autoScroll) {
      el.tableContainer.scrollTop = el.tableContainer.scrollHeight;
    }
  }

  function refreshPacketTable() {
    el.packetFeedBody.innerHTML = '';
    const filtered = state.packets.filter(p => {
      if (state.filter === 'sent') return p.type === 'TX';
      if (state.filter === 'received') return p.type === 'RX';
      return true;
    });

    if (filtered.length === 0) {
      el.packetFeedBody.innerHTML = `
        <tr class="empty-row" id="rowEmptyPlaceholder">
          <td colspan="7">
            <div class="empty-state">
              <span>No packets match the current filter.</span>
            </div>
          </td>
        </tr>
      `;
      return;
    }

    filtered.forEach(p => {
      const tr = document.createElement('tr');
      tr.className = 'packet-row';
      const timeStr = new Date(p.timestamp).toLocaleTimeString();
      const protoLower = p.protocol.toLowerCase().includes('udp') ? 'udp' : 'tcp';
      const dirClass = p.type === 'TX' ? 'tx' : 'rx';

      tr.innerHTML = `
        <td>${timeStr}</td>
        <td><span class="badge-dir ${dirClass}">${p.type}</span></td>
        <td><span class="badge-proto ${protoLower}">${p.protocol.replace(' (Listener)', '')}</span></td>
        <td>${escapeHtml(p.address)}</td>
        <td>${p.length} B</td>
        <td>${p.latency}</td>
        <td title="${escapeHtml(p.preview)}">${escapeHtml(p.preview.substring(0, 32))}${p.preview.length > 32 ? '...' : ''}</td>
      `;

      tr.addEventListener('click', () => {
        document.querySelectorAll('.packet-row.selected').forEach(r => r.classList.remove('selected'));
        tr.classList.add('selected');
        showPacketDetail(p);
      });

      el.packetFeedBody.appendChild(tr);
    });
  }

  // Packet Inspection Sidebar
  function showPacketDetail(packet) {
    const timeStr = new Date(packet.timestamp).toISOString();
    const hex = packet.hexDump ? packet.hexDump.formattedHex : 'N/A';
    const ascii = packet.hexDump ? packet.hexDump.ascii : 'N/A';

    el.detailBodyContent.innerHTML = `
      <div class="detail-meta-grid">
        <div class="detail-meta-item">
          <span class="detail-meta-label">TIMESTAMP</span>
          <span class="detail-meta-value">${timeStr}</span>
        </div>
        <div class="detail-meta-item">
          <span class="detail-meta-label">DIRECTION / TYPE</span>
          <span class="detail-meta-value">${packet.type} (${packet.protocol})</span>
        </div>
        <div class="detail-meta-item">
          <span class="detail-meta-label">ENDPOINT</span>
          <span class="detail-meta-value">${escapeHtml(packet.address)}</span>
        </div>
        <div class="detail-meta-item">
          <span class="detail-meta-label">PAYLOAD SIZE</span>
          <span class="detail-meta-value">${packet.length} bytes</span>
        </div>
      </div>

      <div class="section-label mt-2">Hex Dump</div>
      <div class="raw-dump-box">${hex}</div>

      <div class="section-label mt-2">ASCII Decoded</div>
      <div class="raw-dump-box" style="color: var(--text-main);">${escapeHtml(ascii)}</div>
    `;
  }

  // Active Listeners UI
  function renderActiveListeners(listeners) {
    if (!listeners || listeners.length === 0) {
      el.activeListenersBox.innerHTML = '<span class="text-subtle">No active listeners. Click above to test locally.</span>';
      el.listenerStatusBadge.textContent = 'INACTIVE';
      el.listenerStatusBadge.className = 'badge-status';
      return;
    }

    el.listenerStatusBadge.textContent = `${listeners.length} ACTIVE`;
    el.listenerStatusBadge.className = 'badge-status active';

    el.activeListenersBox.innerHTML = '';
    listeners.forEach(l => {
      const item = document.createElement('div');
      item.className = 'listener-item';
      item.innerHTML = `
        <div>
          <span class="listener-tag">${l.protocol}:${l.port}</span>
          ${l.autoEcho ? '<span class="badge-tag" style="margin-left:6px;">Auto-Echo</span>' : ''}
        </div>
        <button class="btn btn-ghost btn-sm" style="color:var(--accent-danger);" data-stop-key="${l.key}">Stop</button>
      `;

      item.querySelector('button').addEventListener('click', () => {
        sendWS({ action: 'stop_listener', listenerKey: l.key });
      });

      el.activeListenersBox.appendChild(item);
    });
  }

  // Toast Notification helper
  function showToast(message, type = 'info') {
    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    toast.innerHTML = `<span>${escapeHtml(message)}</span>`;
    el.toastContainer.appendChild(toast);

    setTimeout(() => {
      toast.style.opacity = '0';
      toast.style.transform = 'translateY(10px)';
      setTimeout(() => toast.remove(), 300);
    }, 3500);
  }

  function escapeHtml(str) {
    if (!str) return '';
    return str
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#039;');
  }

  // Setup Event Listeners
  function setupEvents() {
    // Protocol toggle
    el.btnProtoUDP.addEventListener('click', () => {
      state.protocol = 'UDP';
      el.btnProtoUDP.classList.add('active');
      el.btnProtoTCP.classList.remove('active');
      el.udpLocalPortWrap.style.display = 'flex';
      updateActionLabel();
    });

    el.btnProtoTCP.addEventListener('click', () => {
      state.protocol = 'TCP';
      el.btnProtoTCP.classList.add('active');
      el.btnProtoUDP.classList.remove('active');
      el.udpLocalPortWrap.style.display = 'none';
      updateActionLabel();
    });

    // Preset IP Chips
    document.querySelectorAll('[data-preset-ip]').forEach(chip => {
      chip.addEventListener('click', () => {
        el.inputTargetIp.value = chip.dataset.presetIp;
      });
    });

    // Preset Port Chips
    document.querySelectorAll('[data-preset-port]').forEach(chip => {
      chip.addEventListener('click', () => {
        el.inputTargetPort.value = chip.dataset.presetPort;
      });
    });

    // Transmission Modes
    [el.tabModeSingle, el.tabModeBurst, el.tabModeStream].forEach(tab => {
      tab.addEventListener('click', () => {
        [el.tabModeSingle, el.tabModeBurst, el.tabModeStream].forEach(t => t.classList.remove('active'));
        tab.classList.add('active');
        state.mode = tab.dataset.mode;

        el.modeConfigBurst.style.display = state.mode === 'burst' ? 'block' : 'none';
        el.modeConfigStream.style.display = state.mode === 'stream' ? 'block' : 'none';
        updateActionLabel();
      });
    });

    function updateActionLabel() {
      if (state.mode === 'single') {
        el.btnTriggerLabel.textContent = `Send Single ${state.protocol} Packet`;
      } else if (state.mode === 'burst') {
        el.btnTriggerLabel.textContent = `Transmit Burst (${el.inputBurstCount.value} ${state.protocol} Pkts)`;
      } else {
        el.btnTriggerLabel.textContent = `Start Continuous ${state.protocol} Stream`;
      }
    }

    el.inputBurstCount.addEventListener('input', updateActionLabel);

    // Payload Type Switcher
    document.querySelectorAll('.type-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('.type-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        state.payloadType = btn.dataset.type;

        el.wrapInputText.style.display = state.payloadType === 'text' ? 'block' : 'none';
        el.wrapInputHex.style.display = state.payloadType === 'hex' ? 'block' : 'none';
        el.wrapInputRandom.style.display = state.payloadType === 'random' ? 'block' : 'none';
        el.wrapInputTemplate.style.display = state.payloadType === 'template' ? 'block' : 'none';

        requestPayloadPreview();
      });
    });

    // Payload inputs change
    el.inputPayloadText.addEventListener('input', requestPayloadPreview);
    el.inputPayloadHex.addEventListener('input', requestPayloadPreview);

    el.rangeRandomSize.addEventListener('input', () => {
      el.inputRandomSize.value = el.rangeRandomSize.value;
      requestPayloadPreview();
    });
    el.inputRandomSize.addEventListener('input', () => {
      el.rangeRandomSize.value = el.inputRandomSize.value;
      requestPayloadPreview();
    });
    el.btnRegenerateRandom.addEventListener('click', requestPayloadPreview);

    el.selectTemplate.addEventListener('change', () => {
      const val = el.selectTemplate.value;
      if (val === 'dns_query') {
        el.wrapTemplateParam.style.display = 'block';
        el.labelTemplateParam.textContent = 'Domain to Query:';
        el.inputTemplateParam.value = 'google.com';
      } else if (val === 'http_get') {
        el.wrapTemplateParam.style.display = 'block';
        el.labelTemplateParam.textContent = 'Host header:';
        el.inputTemplateParam.value = 'localhost';
      } else if (val === 'syslog') {
        el.wrapTemplateParam.style.display = 'block';
        el.labelTemplateParam.textContent = 'Syslog message:';
        el.inputTemplateParam.value = 'NetPulse test syslog event';
      } else if (val === 'ntp_request') {
        el.wrapTemplateParam.style.display = 'none';
      } else {
        el.wrapTemplateParam.style.display = 'block';
        el.labelTemplateParam.textContent = 'Echo payload:';
        el.inputTemplateParam.value = 'PING';
      }
      requestPayloadPreview();
    });

    el.inputTemplateParam.addEventListener('input', requestPayloadPreview);

    // Send Trigger Button
    el.btnTriggerSend.addEventListener('click', () => {
      const targetIp = el.inputTargetIp.value.trim();
      const targetPort = parseInt(el.inputTargetPort.value, 10);

      if (!targetIp) {
        showToast('Please specify a valid Target IP or Hostname', 'error');
        return;
      }
      if (!targetPort || targetPort < 1 || targetPort > 65535) {
        showToast('Please specify a valid Target Port (1 - 65535)', 'error');
        return;
      }

      const payloadConfig = getPayloadConfig();
      const listenResponse = el.checkListenResponse.checked;
      const localPort = parseInt(el.inputLocalPort.value, 10) || 0;

      if (state.mode === 'single') {
        sendWS({
          action: 'send_single',
          protocol: state.protocol,
          targetIp,
          targetPort,
          listenResponse,
          localPort,
          ...payloadConfig
        });
      } else if (state.mode === 'burst') {
        const count = parseInt(el.inputBurstCount.value, 10) || 10;
        const delay = parseInt(el.inputBurstDelay.value, 10) || 20;
        sendWS({
          action: 'send_burst',
          protocol: state.protocol,
          targetIp,
          targetPort,
          burstCount: count,
          burstDelayMs: delay,
          listenResponse,
          ...payloadConfig
        });
      } else if (state.mode === 'stream') {
        const pps = parseInt(el.inputStreamPps.value, 10) || 20;
        const count = parseInt(el.inputStreamLimit.value, 10) || 0;
        sendWS({
          action: 'start_stream',
          protocol: state.protocol,
          targetIp,
          targetPort,
          pps,
          count,
          listenResponse,
          localPort,
          ...payloadConfig
        });
      }
    });

    // Stop Stream Button
    el.btnStopStream.addEventListener('click', () => {
      if (state.currentStreamId) {
        sendWS({ action: 'stop_stream', streamId: state.currentStreamId });
      } else {
        sendWS({ action: 'stop_all_streams' });
      }
    });

    // Local Listener Trigger
    el.btnToggleListener.addEventListener('click', () => {
      const protocol = el.selectListenerProto.value;
      const port = parseInt(el.inputListenerPort.value, 10);
      const autoEcho = el.checkAutoEcho.checked;

      if (!port || port < 1 || port > 65535) {
        showToast('Enter valid listener port (1 - 65535)', 'error');
        return;
      }

      sendWS({
        action: 'start_listener',
        protocol,
        port,
        autoEcho
      });
    });

    // Clear Feed
    el.btnClearFeed.addEventListener('click', () => {
      state.packets = [];
      refreshPacketTable();
      showToast('Activity feed cleared', 'info');
    });

    // Reset Stats
    el.btnResetStats.addEventListener('click', () => {
      sendWS({ action: 'clear_stats' });
      showToast('Stats counters reset', 'info');
    });

    // Auto-scroll toggle
    el.checkAutoScroll.addEventListener('change', (e) => {
      state.autoScroll = e.target.checked;
    });

    // Filter Buttons
    document.querySelectorAll('.filter-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        state.filter = btn.dataset.filter;
        refreshPacketTable();
      });
    });

    // Close Detail Sidebar
    el.btnCloseDetail.addEventListener('click', () => {
      el.detailBodyContent.innerHTML = '<p class="text-subtle select-prompt">Click any packet row on the left to inspect its complete raw byte headers and payload.</p>';
      document.querySelectorAll('.packet-row.selected').forEach(r => r.classList.remove('selected'));
    });
  }

  // Initialize
  window.addEventListener('DOMContentLoaded', () => {
    setupEvents();
    initWebSocket();
  });
})();
