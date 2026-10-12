const state = {
  username: localStorage.getItem('eventdriven.username') || 'admin',
  password: '',
  timelineActive: false
};

const $ = (id) => document.getElementById(id);
$('username').value = state.username;
$('password').value = '';

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

function renderRecentEvents(events) {
  const body = $('recentEventsBody');
  if (!events.length) {
    const row = document.createElement('tr');
    const cell = document.createElement('td');
    cell.colSpan = 8;
    cell.textContent = 'No outbox events yet.';
    row.append(cell);
    body.replaceChildren(row);
    return;
  }

  const rows = events.map((event) => {
    const row = document.createElement('tr');
    const values = [
      event.eventType,
      event.aggregateType,
      event.id,
      event.correlationId || '—',
      event.createdAt ? new Date(event.createdAt).toLocaleString() : 'Unknown',
      event.processed ? 'Published' : event.lastAttemptFailed ? 'Retry pending' : 'Pending',
      String(event.retryCount)
    ];
    for (const value of values) {
      const cell = document.createElement('td');
      cell.textContent = value;
      row.append(cell);
    }
    row.dataset.processed = String(event.processed);
    const actionCell = document.createElement('td');
    const action = document.createElement('button');
    action.type = 'button';
    action.className = 'button small';
    action.textContent = 'Open';
    action.setAttribute('aria-label', `Open timeline for ${event.eventType}`);
    action.addEventListener('click', () => loadTimeline({ eventId: event.id }));
    actionCell.append(action);
    row.append(actionCell);
    return row;
  });
  body.replaceChildren(...rows);
}

function timelineLink(traceId) {
  const query = {
    datasource: 'Tempo',
    queries: [{ refId: 'A', queryType: 'traceql', query: `{ trace:id = "${traceId}" }` }],
    range: { from: 'now-7d', to: 'now' }
  };
  return `http://${window.location.hostname}:3000/explore?left=${encodeURIComponent(JSON.stringify(query))}`;
}

function renderTimeline(timeline) {
  const list = $('workflowTimeline');
  const events = timeline.events || [];
  if (!events.length) {
    list.replaceChildren();
    $('timelineStatus').textContent = `No workflow events found for ${timeline.lookupType} ${timeline.lookupKey}.`;
    $('timelineStatus').dataset.state = 'empty';
    return;
  }

  const items = [];
  for (const event of events) {
    const group = document.createElement('li');
    group.className = 'workflow-event';
    const heading = document.createElement('h4');
    heading.textContent = `${event.eventType} · ${event.eventId}`;
    group.append(heading);
    const metadata = document.createElement('p');
    metadata.textContent = `Correlation: ${event.correlationId || 'not recorded'} · Retries: ${event.retryCount}`;
    group.append(metadata);
    const steps = document.createElement('ol');
    steps.className = 'workflow-steps';
    for (const step of event.steps) {
      const item = document.createElement('li');
      item.dataset.state = step.status;
      const title = document.createElement('strong');
      title.textContent = step.name.replaceAll('-', ' ');
      const detail = document.createElement('span');
      detail.textContent = `${step.status}: ${step.detail || 'No additional detail'}`;
      item.append(title, detail);
      if (step.occurredAt) {
        const time = document.createElement('time');
        time.dateTime = step.occurredAt;
        time.textContent = new Date(step.occurredAt).toLocaleString();
        item.append(time);
      }
      steps.append(item);
    }
    group.append(steps);
    if (event.traceId) {
      const trace = document.createElement('a');
      trace.href = timelineLink(event.traceId);
      trace.target = '_blank';
      trace.rel = 'noopener noreferrer';
      trace.textContent = `Open trace ${event.traceId} in Grafana`;
      group.append(trace);
    }
    items.push(group);
  }
  list.replaceChildren(...items);
  $('timelineStatus').textContent = `Showing ${events.length} workflow event${events.length === 1 ? '' : 's'} for ${timeline.lookupType} ${timeline.lookupKey}.`;
  const hasPendingStep = events.some((event) =>
    event.steps.some((step) => ['pending', 'retrying', 'failed'].includes(step.status)));
  $('timelineStatus').dataset.state = hasPendingStep ? 'pending' : 'loaded';
}

async function loadTimeline(lookup) {
  state.timelineActive = true;
  const eventId = lookup?.eventId ?? $('timelineEventId').value.trim();
  const correlationId = lookup?.correlationId ?? $('timelineCorrelationId').value.trim();
  if (Boolean(eventId) === Boolean(correlationId)) {
    $('timelineStatus').textContent = 'Provide exactly one event ID or correlation ID.';
    $('timelineStatus').dataset.state = 'error';
    $('workflowTimeline').replaceChildren();
    return;
  }
  if (lookup?.eventId) {
    $('timelineEventId').value = lookup.eventId;
    $('timelineCorrelationId').value = '';
  }
  $('timelineStatus').textContent = 'Loading workflow evidence...';
  $('timelineStatus').dataset.state = 'loading';
  $('workflowTimeline').setAttribute('aria-busy', 'true');
  $('workflowTimeline').replaceChildren();
  try {
    const key = eventId ? `eventId=${encodeURIComponent(eventId)}` : `correlationId=${encodeURIComponent(correlationId)}`;
    renderTimeline(await request(`/api/v1/outbox/timeline?${key}`));
  } catch (error) {
    $('timelineStatus').textContent = `Unable to load workflow evidence: ${error.message}`;
    $('timelineStatus').dataset.state = 'error';
  } finally {
    $('workflowTimeline').setAttribute('aria-busy', 'false');
  }
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
  $('outboxResult').textContent = 'Loading outbox status...';
  $('hashResult').textContent = 'Loading ledger head...';
  $('ledgerResult').textContent = 'Loading ledger verification...';
  if (!state.timelineActive) {
    $('timelineStatus').textContent = 'Loading recent evidence...';
    $('timelineStatus').dataset.state = 'loading';
    $('workflowTimeline').replaceChildren();
  }
  try {
    const [outbox, hash, verification, recentEvents] = await Promise.all([
      request('/api/v1/outbox/status'),
      request('/api/v1/ledger/latest-hash'),
      request('/api/v1/ledger/verify?recent=100'),
      request('/api/v1/outbox/recent')
    ]);
    show('outboxResult', outbox);
    show('hashResult', hash);
    show('ledgerResult', {
      valid: verification.valid,
      eventsChecked: verification.eventsChecked,
      scope: verification.scope
    });
    renderRecentEvents(recentEvents);
    if (!state.timelineActive) {
      $('timelineStatus').textContent = recentEvents.length
        ? 'Choose a recent event or enter an event ID or correlation ID.'
        : 'No workflow events yet.';
      $('timelineStatus').dataset.state = recentEvents.length ? 'loaded' : 'empty';
    }
  } catch (error) {
    show('outboxResult', `Unable to load evidence: ${error.message}`);
    const row = document.createElement('tr');
    const cell = document.createElement('td');
    cell.colSpan = 8;
    cell.textContent = `Unable to load recent events: ${error.message}`;
    row.append(cell);
    $('recentEventsBody').replaceChildren(row);
    if (!state.timelineActive) {
      $('timelineStatus').textContent = `Unable to load evidence: ${error.message}`;
      $('timelineStatus').dataset.state = 'error';
    }
  }
}

$('saveCredentials').addEventListener('click', () => {
  state.username = $('username').value;
  state.password = $('password').value;
  localStorage.setItem('eventdriven.username', state.username);
  localStorage.removeItem('eventdriven.password');
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
$('loadTimeline').addEventListener('click', () => loadTimeline());
refreshHealth();
refreshEvidence();
