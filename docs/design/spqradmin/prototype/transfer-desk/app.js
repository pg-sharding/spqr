(() => {
  const page = document.body.dataset.page;
  const demo = window.SPQRDemo.create();
  const selected = demo.selectedRangeIndex;
  const state = demo.rangeState(selected);
  const shardColor = (index) => `hsl(${Math.round(index * 137.508) % 360} 62% 86%)`;

  document.querySelectorAll('[data-distribution-picker]').forEach((select) => {
    select.innerHTML = Array.from({ length: demo.distributionCount }, (_, index) => `<option value="${index}">${demo.distribution(index).id}</option>`).join('');
  });
  demo.bindControls();
  document.querySelectorAll('[data-distribution-name]').forEach((node) => { node.textContent = demo.distribution().id; });
  document.querySelectorAll('[data-range-total]').forEach((node) => { node.textContent = demo.krCount.toLocaleString('en-US'); });
  document.querySelectorAll('[data-shard-total]').forEach((node) => { node.textContent = demo.shardCount.toLocaleString('en-US'); });
  document.querySelectorAll('[data-presentation]').forEach((button) => button.addEventListener('click', () => {
    document.body.classList.toggle('presentation');
    button.textContent = document.body.classList.contains('presentation') ? 'Exit presentation' : 'Present';
  }));

  const moves = () => demo.moves();
  const renderDesk = (filter = 'all') => {
    const headers = document.querySelector('[data-shard-headers]');
    const list = document.querySelector('[data-move-list]');
    if (!headers || !list) return;
    const involved = [...new Set(moves().flatMap((move) => [move.source, move.destination]))];
    const fallback = Array.from({ length: Math.min(demo.shardCount, 8) }, (_, index) => index);
    const visibleShards = (involved.length ? involved : fallback).slice(0, 8);
    headers.style.gridTemplateColumns = `repeat(${Math.max(1, visibleShards.length)}, minmax(110px, 1fr))`;
    headers.innerHTML = visibleShards.map((shardIndex) => `<a href="${demo.url('shard.html', { shard: demo.shardId(shardIndex) })}"><span class="shard-dot" style="background:${shardColor(shardIndex)}"></span><strong>${demo.shardId(shardIndex)}</strong><small>${demo.shardRangeCount(shardIndex)} ranges</small></a>`).join('');
    const visible = moves().filter((move) => filter === 'all' || (filter === 'attention' ? move.state === 'ERROR' : move.state.toLowerCase() === filter));
    list.innerHTML = visible.length ? visible.map((move) => `<a class="move-row ${move.state.toLowerCase()}" href="${demo.url(move.state === 'ERROR' ? 'task.html' : 'range.html', { id: move.rangeId })}">
      <div class="move-origin"><span style="background:${shardColor(move.source)}">${demo.shardId(move.source)}</span></div>
      <div class="move-flow"><span class="move-state">${move.state}</span><strong>${move.rangeId}</strong><small>${move.id} · ${move.phase}</small><div class="flow-line"><i></i><b>→</b></div><p>${move.state === 'ERROR' ? 'Metadata update stopped after data copy' : move.state === 'RUNNING' ? 'Transfer is in progress' : 'Queued for execution'}</p></div>
      <div class="move-destination"><span style="background:${shardColor(move.destination)}">${demo.shardId(move.destination)}</span></div>
    </a>`).join('') : '<div class="empty-state">No moves match this filter.</div>';
    const counts = { ERROR: 0, RUNNING: 0, PLANNED: 0 }; moves().forEach((move) => { counts[move.state] += 1; });
    document.querySelectorAll('[data-stat="error"]').forEach((node) => { node.textContent = counts.ERROR; });
    document.querySelectorAll('[data-stat="running"]').forEach((node) => { node.textContent = counts.RUNNING; });
    document.querySelectorAll('[data-stat="planned"]').forEach((node) => { node.textContent = counts.PLANNED; });
    const queue = document.querySelector('[data-attention-queue]');
    if (queue) {
      const ordered = [...moves()].sort((a, b) => ({ ERROR: 0, RUNNING: 1, PLANNED: 2 }[a.state] - { ERROR: 0, RUNNING: 1, PLANNED: 2 }[b.state]));
      queue.innerHTML = ordered.slice(0, 3).map((move) => `<a class="queue-card ${move.state.toLowerCase()}" href="${demo.url(move.state === 'ERROR' ? 'task.html' : 'range.html', { id: move.rangeId })}"><span class="queue-state">${move.state}</span><strong>${move.id}</strong><p>${move.phase}</p><small>${move.rangeId} · ${demo.shardId(move.source)} → ${demo.shardId(move.destination)}</small></a>`).join('') || '<p class="empty-copy">No active moves.</p>';
    }
  };

  if (page === 'overview') {
    const filter = document.querySelector('[data-filter]'); filter?.addEventListener('change', () => renderDesk(filter.value)); renderDesk();
  }

  if (page === 'range') {
    const rangeId = demo.rangeId(selected);
    const currentShard = demo.shardIndexFor(selected);
    const move = moves().find((item) => item.rangeIndex === selected);
    const destination = move?.destination ?? (currentShard + 1) % demo.shardCount;
    document.querySelector('[data-page-range-id]').textContent = rangeId;
    document.querySelector('[data-range-subtitle]').textContent = `${demo.distribution().id} · range context`;
    const rangeChip = document.querySelector('[data-range-state]');
    rangeChip.textContent = state.locked ? 'LOCKED' : 'UNLOCKED';
    rangeChip.className = `state-chip ${state.locked ? 'locked' : 'unlocked'}`;
    document.querySelector('[data-current-shard]').textContent = demo.shardId(currentShard);
    document.querySelector('[data-destination-shard]').textContent = demo.shardId(destination);
    document.querySelector('[data-route-phase]').textContent = state.transfer;
    document.querySelector('[data-back-to-desk]').href = demo.url('index.html');
    const taskLink = document.querySelector('[data-task-link]');
    if (move?.state === 'ERROR') taskLink.href = demo.url('task.html', { id: rangeId }); else taskLink.hidden = true;
    const context = document.querySelector('[data-move-context]');
    if (!move) context.innerHTML = '<div class="panel-heading"><div><h2>No linked move</h2><p>Selection does not change Coordinator state.</p></div></div><p class="empty-copy">This range has no active task in the demo scenario.</p>';
    else if (move.state !== 'ERROR') context.innerHTML = `<div class="panel-heading"><div><h2>Move ${move.state.toLowerCase()}</h2><p>Task ${move.id}</p></div></div><div class="compact-timeline"><div class="done"><i></i><strong>LOCKED</strong><span>Range frozen</span></div><div class="${move.phase === 'DATA_MOVED' ? 'done' : ''}"><i></i><strong>DATA_MOVED</strong><span>${move.phase}</span></div><div><i></i><strong>COMPLETE</strong><span>Not reached</span></div></div>`;
    document.querySelector('[data-range-facts]').innerHTML = `<div class="panel-heading"><div><h2>Routing facts</h2><p>KeyRangeService response</p></div></div><dl class="fact-list"><div><dt>ID</dt><dd>${rangeId}</dd></div><div><dt>Bounds</dt><dd>[${selected * 1000}, ${(selected + 1) * 1000})</dd></div><div><dt>Distribution</dt><dd>${demo.distribution().id}</dd></div><div><dt>Current shard</dt><dd>${demo.shardId(currentShard)}</dd></div><div><dt>Key range</dt><dd>${state.locked ? 'LOCKED' : 'UNLOCKED'}</dd></div><div><dt>Task group</dt><dd>${state.taskState || '—'}</dd></div></dl>`;
    document.querySelector('[data-raw-json]').textContent = JSON.stringify({ krid: rangeId, shardId: demo.shardId(currentShard), distributionId: demo.distribution().id, locked: state.locked, bound: { values: [String(selected * 1000)] } }, null, 2);
  }

  if (page === 'task') {
    const move = moves().find((item) => item.rangeIndex === selected) || moves()[0];
    if (move) {
      document.querySelectorAll('[data-task-id]').forEach((node) => { node.textContent = move.id; });
      document.querySelectorAll('[data-task-range]').forEach((node) => { node.textContent = move.rangeId; node.href = demo.url('range.html', { id: move.rangeId }); });
      document.querySelectorAll('[data-task-source]').forEach((node) => { node.textContent = demo.shardId(move.source); });
      document.querySelectorAll('[data-task-destination]').forEach((node) => { node.textContent = demo.shardId(move.destination); });
      document.querySelectorAll('[data-task-distribution]').forEach((node) => { node.textContent = demo.distribution(move.distributionIndex).id; });
      document.querySelector('[data-back-to-desk]').href = demo.url('index.html');
    }
  }

  demo.decorateLinks();
})();
