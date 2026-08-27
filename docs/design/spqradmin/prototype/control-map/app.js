(() => {
  const page = document.body.dataset.page;
  const demo = window.SPQRDemo.create();
  const total = demo.krCount;
  let selected = demo.selectedRangeIndex;
  const viewportSize = total <= 24 ? total : 18;
  let viewportStart = total <= 24 ? 0 : Math.max(0, Math.min(total - viewportSize, selected - 8));
  const shardColor = (index) => `hsl(${Math.round(index * 137.508) % 360} 62% 86%)`;
  const css = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();

  document.querySelectorAll('[data-distribution-picker]').forEach((select) => { select.innerHTML = Array.from({ length: demo.distributionCount }, (_, index) => `<option value="${index}">${demo.distribution(index).id}</option>`).join(''); });
  demo.bindControls();
  document.querySelectorAll('[data-range-total]').forEach((node) => { node.textContent = total.toLocaleString('en-US'); });
  document.querySelectorAll('[data-shard-total]').forEach((node) => { node.textContent = demo.shardCount.toLocaleString('en-US'); });
  document.querySelectorAll('[data-distribution-name]').forEach((node) => { node.textContent = demo.distribution().id; });
  document.querySelectorAll('[data-presentation]').forEach((button) => button.addEventListener('click', () => { document.body.classList.toggle('presentation'); button.textContent = document.body.classList.contains('presentation') ? 'Exit presentation' : 'Present'; }));

  const drawMap = () => {
    const canvas = document.querySelector('[data-map-canvas]'); if (!canvas) return;
    const rect = canvas.parentElement.getBoundingClientRect(); const ratio = Math.min(2, devicePixelRatio || 1);
    canvas.width = Math.max(1, Math.floor(rect.width * ratio)); canvas.height = Math.max(1, Math.floor(rect.height * ratio));
    const context = canvas.getContext('2d'); context.scale(ratio, ratio); const width = rect.width; const bins = Math.min(total, Math.max(1, Math.floor(width)));
    for (let bin = 0; bin < bins; bin += 1) { const index = Math.floor(bin * total / bins); const left = bin * width / bins; context.fillStyle = shardColor(demo.shardIndexFor(index)); context.fillRect(left, 16, Math.ceil((bin + 1) * width / bins - left), 34); }
    const viewLeft = viewportStart / total * width; const viewWidth = Math.max(9, viewportSize / total * width); context.strokeStyle = css('--primary'); context.lineWidth = 2; context.strokeRect(Math.max(1, viewLeft), 8, Math.min(width - 2, viewWidth), 50);
    demo.lockedIndices().forEach((index) => { const x = (index + .5) / total * width; context.strokeStyle = css('--warning'); context.lineWidth = 2; context.beginPath(); context.moveTo(x, 12); context.lineTo(x, 54); context.stroke(); });
    if (demo.rangeState(demo.incidentIndex).taskState === 'ERROR') { const errorX = (demo.incidentIndex + .5) / total * width; context.fillStyle = css('--danger'); context.beginPath(); context.arc(errorX, 7, 4, 0, Math.PI * 2); context.fill(); }
  };

  const selectRange = (index) => { selected = Math.max(0, Math.min(total - 1, index)); if (selected < viewportStart || selected >= viewportStart + viewportSize) viewportStart = Math.max(0, Math.min(total - viewportSize, selected - 8)); renderControlMap(); };
  const renderRanges = () => {
    const target = document.querySelector('[data-range-strip]'); if (!target) return; target.innerHTML = '';
    for (let index = viewportStart; index < Math.min(total, viewportStart + viewportSize); index += 1) {
      const state = demo.rangeState(index); const shardIndex = demo.shardIndexFor(index); const button = document.createElement('button'); button.type = 'button'; button.className = `range-tile${state.locked ? ' locked' : ''}${selected === index ? ' selected' : ''}`; button.style.backgroundColor = shardColor(shardIndex); button.innerHTML = `<strong>${demo.rangeId(index)}</strong><span>${demo.shardId(shardIndex)}</span>${state.locked ? '<em>LOCKED</em>' : ''}`; button.addEventListener('click', () => selectRange(index)); target.append(button);
    }
    document.querySelector('[data-window-label]').textContent = `Ranges ${viewportStart + 1}–${Math.min(total, viewportStart + viewportSize)}`;
    document.querySelector('[data-density-label]').textContent = total > 900 ? 'Binned overview with an exact 18-range viewport.' : 'Every overview mark represents one key range.';
  };
  const renderSelected = () => {
    const target = document.querySelector('[data-selected-card]'); if (!target) return; const state = demo.rangeState(selected);
    target.innerHTML = `<div><p class="eyebrow">SELECTED</p><h2>${demo.rangeId(selected)}</h2><span class="state-chip ${state.locked ? 'locked' : 'unlocked'}">${state.locked ? 'LOCKED' : 'UNLOCKED'}</span></div><dl><div><dt>Bounds</dt><dd>[${selected * 1000}, ${(selected + 1) * 1000})</dd></div><div><dt>Routes to</dt><dd>${demo.shardId(demo.shardIndexFor(selected))}</dd></div><div><dt>Linked task</dt><dd class="${state.taskState === 'ERROR' ? 'danger-text' : ''}">${state.task}</dd></div></dl><a href="${demo.url('range.html', { id: demo.rangeId(selected) })}">Inspect range →</a>`;
  };
  const renderActivity = () => {
    const target = document.querySelector('[data-activity-list]'); if (!target) return; target.innerHTML = '';
    demo.moves().forEach((move) => { const button = document.createElement('button'); button.type = 'button'; button.className = `activity-item ${move.state.toLowerCase()}${move.rangeIndex === selected ? ' selected' : ''}`; button.innerHTML = `<span class="activity-state">${move.state}</span><strong>${move.id}</strong><p>${move.rangeId} · ${demo.shardId(move.source)} → ${demo.shardId(move.destination)}</p><small>${move.phase}</small>`; button.addEventListener('click', () => selectRange(move.rangeIndex)); target.append(button); });
    if (!demo.moves().length) target.innerHTML = '<p class="empty-copy">No active moves.</p>';
    const counts = { ERROR: 0, RUNNING: 0, PLANNED: 0 }; demo.moves().forEach((move) => { counts[move.state] += 1; });
    document.querySelectorAll('[data-activity-errors]').forEach((node) => { node.textContent = counts.ERROR; }); document.querySelectorAll('[data-activity-running]').forEach((node) => { node.textContent = counts.RUNNING; }); document.querySelectorAll('[data-activity-planned]').forEach((node) => { node.textContent = counts.PLANNED; });
  };
  const renderControlMap = () => { renderRanges(); renderSelected(); renderActivity(); requestAnimationFrame(drawMap); };

  if (page === 'overview') {
    document.querySelector('[data-jump-button]')?.addEventListener('click', () => { const index = Number(document.querySelector('[data-jump]').value.replace(/\D/g, '')) - 1; if (Number.isInteger(index) && index >= 0 && index < total) selectRange(index); });
    renderControlMap(); new ResizeObserver(drawMap).observe(document.querySelector('[data-map-canvas]').parentElement);
  }

  if (page === 'range') {
    const rangeId = demo.rangeId(selected); const shardIndex = demo.shardIndexFor(selected); const shardId = demo.shardId(shardIndex); const state = demo.rangeState(selected); const move = demo.moves().find((item) => item.rangeIndex === selected);
    document.querySelector('[data-range-id]').textContent = rangeId; document.querySelector('[data-range-subtitle]').textContent = `${demo.distribution().id} · ordered context and operational state`; document.querySelector('[data-position-label]').textContent = `${selected + 1} of ${total.toLocaleString('en-US')}`; document.querySelector('[data-back-map]').href = demo.url('index.html', { id: rangeId });
    const chip = document.querySelector('[data-range-state]'); chip.textContent = state.locked ? 'LOCKED' : 'UNLOCKED'; chip.className = `state-chip ${state.locked ? 'locked' : 'unlocked'}`;
    const neighbors = document.querySelector('[data-neighbor-strip]'); for (let index = Math.max(0, selected - 2); index <= Math.min(total - 1, selected + 2); index += 1) { const neighborShard = demo.shardIndexFor(index); const link = document.createElement('a'); link.href = demo.url('range.html', { id: demo.rangeId(index) }); link.className = index === selected ? 'current' : ''; link.style.backgroundColor = shardColor(neighborShard); link.innerHTML = `<strong>${demo.rangeId(index)}</strong><span>${demo.shardId(neighborShard)}</span><small>[${index * 1000}, ${(index + 1) * 1000})</small>`; neighbors.append(link); }
    document.querySelector('[data-range-facts]').innerHTML = `<div><dt>ID</dt><dd>${rangeId}</dd></div><div><dt>Bounds</dt><dd>[${selected * 1000}, ${(selected + 1) * 1000})</dd></div><div><dt>Distribution</dt><dd>${demo.distribution().id}</dd></div><div><dt>Shard</dt><dd>${shardId}</dd></div><div><dt>Key range</dt><dd>${state.locked ? 'LOCKED' : 'UNLOCKED'}</dd></div><div><dt>Move journal</dt><dd>${state.move}</dd></div>`;
    const card = document.querySelector('[data-linked-move]');
    if (move?.state === 'ERROR') card.innerHTML = `<span class="task-status">TASK GROUP · ERROR</span><h2>Move stopped after data copy</h2><p>Routing still points to <b>${shardId}</b>. Destination: ${demo.shardId(move.destination)}.</p><div class="mini-phase"><span class="done">LOCKED</span><span class="done">DATA_MOVED</span><span class="failed">COORD_META_UPDATED</span></div><a href="${demo.url('task.html', { id: rangeId })}">Open move evidence →</a>`;
    else if (move) { card.classList.add('running'); card.innerHTML = `<span class="task-status">TASK GROUP · ${move.state}</span><h2>Move is active</h2><p>${demo.shardId(move.source)} → ${demo.shardId(move.destination)} · ${move.phase}</p>`; }
    else { card.classList.add('idle'); card.innerHTML = '<span class="task-status">NO LINKED TASK</span><h2>No active move for this range</h2><p>Selection changes focus only; Coordinator state is unchanged.</p>'; }
    document.querySelector('[data-raw-json]').textContent = JSON.stringify({ krid: rangeId, shardId, distributionId: demo.distribution().id, locked: state.locked, bound: { values: [String(selected * 1000)] } }, null, 2);
  }

  if (page === 'task') {
    const move = demo.moves().find((item) => item.rangeIndex === selected) || demo.moves()[0]; if (move) { document.querySelectorAll('[data-task-id]').forEach((node) => { node.textContent = move.id; }); document.querySelectorAll('[data-task-range]').forEach((node) => { node.textContent = move.rangeId; node.href = demo.url('range.html', { id: move.rangeId }); }); document.querySelectorAll('[data-task-source]').forEach((node) => { node.textContent = demo.shardId(move.source); }); document.querySelectorAll('[data-task-destination]').forEach((node) => { node.textContent = demo.shardId(move.destination); }); document.querySelectorAll('[data-task-distribution]').forEach((node) => { node.textContent = demo.distribution(move.distributionIndex).id; }); document.querySelector('[data-back-map]').href = demo.url('index.html', { id: move.rangeId }); }
  }
  demo.decorateLinks();
})();
