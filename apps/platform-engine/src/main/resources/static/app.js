const state = {
  username: localStorage.getItem('eventdriven.username') || 'admin',
  password: localStorage.getItem('eventdriven.password') || 'local-dev-password'
};

const $ = (id) => document.getElementById(id);
$('username').value = state.username;
$('password').value = state.password;

function authHeaders() {
  return {
    Authorization: `Basic ${btoa(`${state.username}:${state.password}`)}`,
    'Content-Type': 'application/json'
  };
}

async function request(path, options = {}) {
  const response = await fetch(path, {
    ...options,
    headers: { ...authHeaders(), ...(options.headers || {}) }
  });
  const correlationId = response.headers.get('X-Correlation-Id');
  const traceId = response.headers.get('X-Trace-Id');
  if (correlationId) $('correlationId').textContent = correlationId;
  if (traceId) $('traceId').textContent = traceId;
  if (!response.ok) throw new Error(`${response.status} ${response.statusText}`);
  const contentType = response.headers.get('content-type') || '';
  return contentType.includes('json') ? response.json() : response.text();
}

function show(id, value) {
  $(id).textContent = typeof value === 'string' ? value : JSON.stringify(value, null, 2);
}

async function refreshHealth() {
  try {
    const health = await fetch('/actuator/health');
    $('healthStatus').textContent = health.ok ? '● SYSTEM HEALTHY' : '● HEALTH DEGRADED';
    $('healthStatus').style.color = health.ok ? 'var(--mint)' : 'var(--red)';
  } catch (error) {
    $('healthStatus').textContent = '● APP UNREACHABLE';
    $('healthStatus').style.color = 'var(--red)';
  }
}

async function refreshEvidence() {
  try {
    const [outbox, hash, ledger] = await Promise.all([
      request('/api/v1/outbox/status'),
      request('/api/v1/ledger/latest-hash'),
      request('/api/v1/ledger')
    ]);
    show('outboxResult', outbox);
    show('hashResult', hash);
    show('ledgerResult', ledger);
  } catch (error) {
    show('outboxResult', `Unable to load evidence: ${error.message}`);
  }
}

$('saveCredentials').addEventListener('click', () => {
  state.username = $('username').value;
  state.password = $('password').value;
  localStorage.setItem('eventdriven.username', state.username);
  localStorage.setItem('eventdriven.password', state.password);
  refreshEvidence();
});

$('submitLoan').addEventListener('click', async () => {
  try {
    const result = await request('/api/v1/loans', {
      method: 'POST',
      body: JSON.stringify({
        applicantId: $('applicantId').value,
        amount: Number($('loanAmount').value),
        termMonths: Number($('termMonths').value)
      })
    });
    show('loanResult', result);
    refreshEvidence();
  } catch (error) {
    show('loanResult', `Loan failed: ${error.message}`);
  }
});

$('sendBurst').addEventListener('click', async () => {
  const count = Number($('transactionCount').value);
  const merchantId = $('merchantId').value;
  const amount = Number($('transactionAmount').value);
  try {
    for (let index = 1; index <= count; index += 1) {
      await request('/api/v1/transactions', {
        method: 'POST',
        body: JSON.stringify({
          transactionId: `ui-burst-${Date.now()}-${index}`,
          amount,
          currency: 'USD',
          merchantId,
          correlationId: `ui-burst-${index}`,
          settlementStatus: 'PENDING'
        })
      });
    }
    show('transactionResult', `Submitted ${count} transactions for ${merchantId}.`);
    refreshEvidence();
  } catch (error) {
    show('transactionResult', `Burst failed: ${error.message}`);
  }
});

async function sendTelemetry(value) {
  const deviceId = $('deviceId').value;
  try {
    await request('/api/v1/telemetry', {
      method: 'POST',
      body: JSON.stringify({
        deviceId,
        timestamp: new Date().toISOString(),
        logLevel: 'INFO',
        rawPayload: JSON.stringify({ cpu_load: value, mem_usage: 42.0 })
      })
    });
    show('telemetryResult', `Submitted cpu_load=${value} for ${deviceId}.`);
    refreshEvidence();
  } catch (error) {
    show('telemetryResult', `Telemetry failed: ${error.message}`);
  }
}

$('sendBaseline').addEventListener('click', async () => {
  for (const value of [42, 43, 41, 44, 42, 43]) {
    await sendTelemetry(value);
  }
});

$('sendOutlier').addEventListener('click', () => sendTelemetry(99));

$('refreshEvidence').addEventListener('click', refreshEvidence);
refreshHealth();
refreshEvidence();
