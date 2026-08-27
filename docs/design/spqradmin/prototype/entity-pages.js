(() => {
  const page = document.body.dataset.page;
  if (!['cluster', 'shard'].includes(page)) return;
  const demo = window.SPQRDemo.create();
  const format = (value) => value.toLocaleString('en-US');
  const shardColor = (index) => `hsl(${Math.round(index * 137.508) % 360} 62% 82%)`;
  const paths = {
    distribution: document.body.dataset.distributionPath || 'index.html',
    range: document.body.dataset.rangePath || 'range.html',
    shard: document.body.dataset.shardPath || 'shard.html',
    move: document.body.dataset.movePath || 'task.html'
  };

  demo.bindControls();
  document.querySelectorAll('[data-presentation]').forEach((button) => button.addEventListener('click', () => {
    document.body.classList.toggle('presentation');
    button.textContent = document.body.classList.contains('presentation') ? 'Exit presentation' : 'Present';
  }));

  const moveMarkup = (move) => `<a class="cluster-move ${move.state.toLowerCase()}" href="${demo.url(move.state === 'ERROR' ? paths.move : paths.range, { distribution: move.distributionIndex, id: move.rangeId })}">
    <span class="state-chip ${move.state === 'ERROR' ? 'error' : 'running'}">${move.state}</span><strong>${move.id}</strong>
    <span>${demo.distribution(move.distributionIndex).id}</span><code>${move.rangeId} · ${demo.shardId(move.source)} → ${demo.shardId(move.destination)}</code><small>${move.phase}</small>
  </a>`;

  if (page === 'cluster') {
    const summaries = Array.from({ length: demo.distributionCount }, (_, index) => demo.distributionSummary(index));
    const moves = demo.allMoves();
    const lockedTotal = summaries.reduce((sum, item) => sum + item.lockedCount, 0);
    const stats = [[demo.distributionCount, 'distributions'], [demo.shardCount, 'shards'], [demo.distributionCount * demo.krCount, 'key ranges'], [lockedTotal, 'locked KR'], [moves.filter((move) => move.state === 'RUNNING').length, 'running moves'], [moves.filter((move) => move.state === 'ERROR').length, 'errors']];
    document.querySelector('[data-cluster-stats]').innerHTML = stats.map(([value, label]) => `<div><strong>${format(value)}</strong><span>${label}</span></div>`).join('');
    document.querySelector('[data-distribution-rows]').innerHTML = summaries.map((item) => `<tr><td><a class="table-link" href="${demo.url(paths.distribution, { distribution: item.index, id: null })}">${item.id}</a><small>${item.relationCount} relations · ${item.columnType}</small></td><td>${format(item.rangeCount)}</td><td>${item.shardsUsed} / ${demo.shardCount}</td><td>${item.lockedCount}</td><td>${item.errorMoves ? `<b class="danger-text">${item.errorMoves} error</b>` : `${item.runningMoves} running`}</td></tr>`).join('');
    document.querySelector('[data-shard-cloud]').innerHTML = Array.from({ length: demo.shardCount }, (_, shardIndex) => `<a href="${demo.url(paths.shard, { shard: demo.shardId(shardIndex) })}" style="--shard-color:${shardColor(shardIndex)}"><strong>${demo.shardId(shardIndex)}</strong><span>${demo.shardDistributions(shardIndex).length} distributions</span></a>`).join('');
    const firstShard = document.querySelector('[data-first-shard]');
    if (firstShard) firstShard.href = demo.url(paths.shard, { shard: demo.shardId(0) });
    const priority = { ERROR: 0, RUNNING: 1, PLANNED: 2 };
    const sorted = [...moves].sort((a, b) => priority[a.state] - priority[b.state]);
    document.querySelector('[data-cluster-moves]').innerHTML = sorted.length ? sorted.slice(0, 12).map(moveMarkup).join('') + (sorted.length > 12 ? `<p class="aggregation-note">Showing 12 of ${sorted.length} move tasks.</p>` : '') : '<p class="empty-copy">No active moves in this scenario.</p>';
  }

  if (page === 'shard') {
    const shardIndex = demo.selectedShardIndex;
    const picker = document.querySelector('[data-shard-picker]');
    picker.innerHTML = Array.from({ length: demo.shardCount }, (_, index) => `<option value="${demo.shardId(index)}">${demo.shardId(index)}</option>`).join('');
    picker.value = demo.shardId(shardIndex);
    picker.addEventListener('change', () => { location.href = demo.changeUrl({ shard: picker.value, id: null }); });
    const distributions = demo.shardDistributions(shardIndex);
    const rangeTotal = distributions.reduce((sum, distIndex) => sum + demo.shardRangeCount(shardIndex, distIndex), 0);
    document.querySelector('[data-shard-id]').textContent = demo.shardId(shardIndex);
    document.querySelector('[data-shard-distribution-total]').textContent = format(distributions.length);
    document.querySelector('[data-shard-range-total]').textContent = format(rangeTotal);
    document.querySelector('[data-shard-distribution-rows]').innerHTML = distributions.map((distIndex) => {
      const lockedCount = demo.lockedIndices(distIndex).filter((rangeIndex) => demo.shardIndexFor(rangeIndex, distIndex) === shardIndex).length;
      return `<tr><td><a class="table-link" href="${demo.url(paths.distribution, { distribution: distIndex, id: null })}">${demo.distribution(distIndex).id}</a></td><td>${demo.shardRangeCount(shardIndex, distIndex)}</td><td>${lockedCount}</td></tr>`;
    }).join('') || '<tr><td colspan="3">This shard is unused by the selected scenario.</td></tr>';
    const moves = demo.allMoves().filter((move) => move.source === shardIndex || move.destination === shardIndex);
    document.querySelector('[data-shard-moves]').innerHTML = moves.length ? moves.map(moveMarkup).join('') : '<p class="empty-copy">No incoming or outgoing moves.</p>';
  }

  demo.decorateLinks();
})();
