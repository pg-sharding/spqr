(() => {
  const page = document.body.dataset.page;
  const demo = window.SPQRDemo.create();
  const total = demo.krCount;
  let selected = demo.selectedRangeIndex;
  const windowSize = total <= 32 ? total : 24;
  let windowStart = total <= 32 ? 0 : Math.max(0, Math.min(total - windowSize, selected - 11));

  const shardColor = (index) => `hsl(${Math.round(index * 137.508) % 360} 62% 86%)`;
  const shardFor = (index) => demo.shardIndexFor(index);
  const idFor = (index) => demo.rangeId(index);
  const stateFor = (index) => demo.rangeState(index);
  const locked = () => demo.lockedIndices();
  const css = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  const format = (value) => value.toLocaleString("en-US");

  const populateDistributionPickers = () => document.querySelectorAll("[data-distribution-picker]").forEach((select) => {
    select.innerHTML = Array.from({ length: demo.distributionCount }, (_, index) => `<option value="${index}">${demo.distribution(index).id}</option>`).join("");
  });
  populateDistributionPickers();
  demo.bindControls();

  document.querySelectorAll("[data-range-total]").forEach((node) => { node.textContent = format(total); });
  document.querySelectorAll("[data-shard-total]").forEach((node) => { node.textContent = format(demo.shardCount); });
  document.querySelectorAll("[data-distribution-total]").forEach((node) => { node.textContent = format(demo.distributionCount); });
  document.querySelectorAll("[data-distribution-name]").forEach((node) => { node.textContent = demo.distribution().id; });
  document.querySelectorAll("[data-presentation]").forEach((button) => button.addEventListener("click", () => {
    document.body.classList.toggle("presentation");
    button.textContent = document.body.classList.contains("presentation") ? "Exit presentation" : "Present";
  }));

  const moveMarkup = (move) => `<a class="cluster-move ${move.state.toLowerCase()}" href="${demo.url("task.html", { distribution: move.distributionIndex, id: move.rangeId })}">
    <span class="state-chip ${move.state === "ERROR" ? "error" : "running"}">${move.state}</span>
    <strong>${move.id}</strong><span>${demo.distribution(move.distributionIndex).id}</span>
    <code>${move.rangeId} · ${demo.shardId(move.source)} → ${demo.shardId(move.destination)}</code><small>${move.phase}</small>
  </a>`;

  const selectRange = (index) => {
    selected = index;
    if (selected < windowStart || selected >= windowStart + windowSize) windowStart = Math.max(0, Math.min(total - windowSize, selected - 11));
    renderOverview();
  };

  const renderSelection = () => {
    const panel = document.querySelector("[data-selection-panel]");
    if (!panel) return;
    const state = stateFor(selected);
    panel.innerHTML = `<div class="selection-kicker">SELECTED KEY RANGE</div>
      <h2>${idFor(selected)}</h2><span class="state-chip ${state.locked ? "locked" : "unlocked"}">${state.locked ? "LOCKED" : "UNLOCKED"}</span>
      <dl class="detail-list">
        <div class="detail-row"><dt>Interval</dt><dd>[${selected * 1000}, ${(selected + 1) * 1000})</dd></div>
        <div class="detail-row"><dt>Routes to</dt><dd>${demo.shardId(shardFor(selected))}</dd></div>
        <div class="detail-row"><dt>Task group</dt><dd>${state.task}</dd></div>
        <div class="detail-row"><dt>Move journal</dt><dd>${state.move}</dd></div>
        <div class="detail-row"><dt>Transfer</dt><dd>${state.transfer}</dd></div>
      </dl><a class="panel-link" href="${demo.url("range.html", { id: idFor(selected) })}">Open exact range detail →</a>`;
  };

  const drawCanvas = () => {
    const canvas = document.querySelector("[data-overview-canvas]");
    if (!canvas) return;
    const rect = canvas.parentElement.getBoundingClientRect();
    const ratio = Math.min(2, devicePixelRatio || 1);
    canvas.width = Math.max(1, Math.floor(rect.width * ratio));
    canvas.height = Math.max(1, Math.floor(rect.height * ratio));
    const context = canvas.getContext("2d");
    context.scale(ratio, ratio);
    const width = rect.width;
    const bins = Math.min(total, Math.max(1, Math.floor(width)));
    for (let bin = 0; bin < bins; bin += 1) {
      const index = Math.floor(bin * total / bins);
      const left = bin * width / bins;
      context.fillStyle = shardColor(shardFor(index));
      context.fillRect(left, 15, Math.ceil((bin + 1) * width / bins - left), 28);
    }
    const viewX = windowStart / total * width;
    const viewWidth = Math.max(8, windowSize / total * width);
    context.strokeStyle = css("--primary"); context.lineWidth = 2;
    context.strokeRect(Math.max(1, viewX), 7, Math.min(width - 2, viewWidth), 44);
    locked().forEach((index) => {
      const x = (index + .5) / total * width;
      context.strokeStyle = css("--warning"); context.lineWidth = 2;
      context.beginPath(); context.moveTo(x, 11); context.lineTo(x, 47); context.stroke();
    });
    if (stateFor(demo.incidentIndex).taskState === "ERROR") {
      const errorX = (demo.incidentIndex + .5) / total * width;
      context.fillStyle = css("--danger"); context.beginPath(); context.moveTo(errorX, 2); context.lineTo(errorX - 5, 9); context.lineTo(errorX + 5, 9); context.fill();
    }
  };

  const renderViewport = () => {
    const viewport = document.querySelector("[data-range-viewport]");
    if (!viewport) return;
    viewport.innerHTML = "";
    for (let index = windowStart; index < Math.min(total, windowStart + windowSize); index += 1) {
      const state = stateFor(index);
      const shardIndex = shardFor(index);
      const button = document.createElement("button");
      button.type = "button";
      button.className = `range-cell${state.locked ? " locked" : ""}${index === selected ? " selected" : ""}`;
      button.style.backgroundColor = shardColor(shardIndex);
      button.dataset.index = String(index);
      button.setAttribute("aria-label", `${idFor(index)}, ${demo.shardId(shardIndex)}, ${state.locked ? "LOCKED" : "UNLOCKED"}`);
      button.innerHTML = `<strong>${idFor(index)}</strong><span>${demo.shardId(shardIndex)}</span>${state.locked ? "<em>LOCKED</em>" : ""}`;
      button.addEventListener("click", () => selectRange(index));
      button.addEventListener("keydown", (event) => {
        if (!['ArrowLeft', 'ArrowRight'].includes(event.key)) return;
        event.preventDefault(); selectRange(Math.max(0, Math.min(total - 1, index + (event.key === "ArrowRight" ? 1 : -1))));
        requestAnimationFrame(() => document.querySelector(`[data-index="${selected}"]`)?.focus());
      });
      viewport.append(button);
    }
    document.querySelector("[data-window-title]").textContent = `Ranges ${windowStart + 1}–${Math.min(total, windowStart + windowSize)} of ${format(total)}`;
    document.querySelector("[data-density-label]").textContent = total > 900 ? "Binned ownership layer · exceptions stay exact" : "One ordered mark per key range";
  };

  const renderLegend = () => {
    const legend = document.querySelector("[data-shard-legend]");
    if (!legend) return;
    const used = [...new Set(Array.from({ length: total }, (_, index) => shardFor(index)))];
    const visible = used.slice(0, demo.shardCount <= 8 ? 8 : 4);
    legend.innerHTML = visible.map((index) => `<span><i style="background:${shardColor(index)}"></i>${demo.shardId(index)}</span>`).join("") +
      (used.length > visible.length ? `<span>+${used.length - visible.length} more</span>` : "") + `<span><i class="locked-mark"></i>LOCKED</span>`;
  };

  const renderAllocation = () => {
    const target = document.querySelector("[data-allocation-bars]");
    if (!target) return;
    const counts = Array.from({ length: demo.shardCount }, (_, shardIndex) => ({ shardIndex, count: demo.shardRangeCount(shardIndex) })).filter((item) => item.count > 0);
    const visible = counts.slice(0, 12);
    const max = Math.max(1, ...counts.map((item) => item.count));
    target.innerHTML = visible.map(({ shardIndex, count }) => `<a class="allocation-row" href="${demo.url("shard.html", { shard: demo.shardId(shardIndex) })}"><strong>${demo.shardId(shardIndex)}</strong><div class="allocation-track"><div class="allocation-fill" style="width:${count / max * 100}%;background:${shardColor(shardIndex)}"></div></div><span class="allocation-count">${count} ranges</span></a>`).join("") + (counts.length > visible.length ? `<p class="aggregation-note">Showing 12 of ${counts.length} participating shards. Use shard search for an exact shard.</p>` : "");
  };

  const renderOverview = () => { renderViewport(); renderSelection(); renderLegend(); renderAllocation(); requestAnimationFrame(drawCanvas); };

  if (page === "overview") {
    document.querySelector("[data-jump-button]")?.addEventListener("click", () => {
      const index = Number(document.querySelector("[data-jump]").value.replace(/\D/g, "")) - 1;
      if (Number.isInteger(index) && index >= 0 && index < total) selectRange(index);
    });
    renderOverview();
    new ResizeObserver(drawCanvas).observe(document.querySelector("[data-overview-canvas]").parentElement);
  }

  if (page === "range") {
    const state = stateFor(selected);
    const rangeId = idFor(selected);
    document.querySelector("[data-page-range-id]").textContent = rangeId;
    document.querySelector("[data-range-subtitle]").textContent = `${demo.distribution().id} · exact Coordinator state`;
    document.querySelector("[data-back-to-map]").href = demo.url("index.html", { id: rangeId });
    const taskLink = state.taskState === "ERROR" ? `<a class="panel-link" href="${demo.url("task.html", { id: rangeId })}">Explain failed move →</a>` : "";
    document.querySelector("[data-range-detail]").innerHTML = `<h2>Routing and lock</h2><p>Facts from KeyRangeService and matching move journals.</p><span class="state-chip ${state.locked ? "locked" : "unlocked"}">${state.locked ? "LOCKED" : "UNLOCKED"}</span><dl class="detail-list">
      <div class="detail-row"><dt>ID</dt><dd>${rangeId}</dd></div><div class="detail-row"><dt>Distribution</dt><dd>${demo.distribution().id}</dd></div><div class="detail-row"><dt>Lower bound</dt><dd>${selected * 1000}</dd></div><div class="detail-row"><dt>Upper bound</dt><dd>${(selected + 1) * 1000}</dd></div><div class="detail-row"><dt>Shard</dt><dd>${demo.shardId(shardFor(selected))}</dd></div><div class="detail-row"><dt>Move journal</dt><dd>${state.move}</dd></div><div class="detail-row"><dt>Transfer</dt><dd>${state.transfer}</dd></div></dl>${taskLink}`;
    const neighbors = document.querySelector("[data-neighbors]");
    for (let index = Math.max(0, selected - 2); index <= Math.min(total - 1, selected + 2); index += 1) {
      const shardIndex = shardFor(index); const link = document.createElement("a"); link.className = `context-range${index === selected ? " current" : ""}`; link.href = demo.url("range.html", { id: idFor(index) }); link.style.backgroundColor = shardColor(shardIndex); link.innerHTML = `<strong>${idFor(index)}</strong><span>${demo.shardId(shardIndex)} · [${index * 1000}, ${(index + 1) * 1000})</span>`; neighbors.append(link);
    }
    const relations = document.querySelector("[data-relations-body]");
    relations.innerHTML = Array.from({ length: demo.distribution().relationCount }, (_, index) => `<tr><td>public.${demo.distribution().id.split("_by_")[0]}${index ? `_${index + 1}` : ""}</td><td><code>${demo.distribution().id.split("_by_")[1] || "id"}</code></td><td>${demo.distribution().columnType}</td></tr>`).join("");
    document.querySelector("[data-raw-json]").textContent = JSON.stringify({ krid: rangeId, shardId: demo.shardId(shardFor(selected)), distributionId: demo.distribution().id, locked: state.locked, bound: { values: [String(selected * 1000)] } }, null, 2);
  }

  if (page === "cluster") {
    const summaries = Array.from({ length: demo.distributionCount }, (_, index) => demo.distributionSummary(index));
    const moves = demo.allMoves();
    const lockedTotal = summaries.reduce((sum, item) => sum + item.lockedCount, 0);
    const stats = [[demo.distributionCount, "distributions"], [demo.shardCount, "shards"], [demo.distributionCount * total, "key ranges"], [lockedTotal, "locked KR"], [moves.filter((move) => move.state === "RUNNING").length, "running moves"], [moves.filter((move) => move.state === "ERROR").length, "errors"]];
    document.querySelector("[data-cluster-stats]").innerHTML = stats.map(([value, label]) => `<div><strong>${format(value)}</strong><span>${label}</span></div>`).join("");
    document.querySelector("[data-distribution-rows]").innerHTML = summaries.map((item) => `<tr><td><a class="table-link" href="${demo.url("index.html", { distribution: item.index, id: null })}">${item.id}</a><small>${item.relationCount} relations · ${item.columnType}</small></td><td>${format(item.rangeCount)}</td><td>${item.shardsUsed} / ${demo.shardCount}</td><td>${item.lockedCount}</td><td>${item.errorMoves ? `<b class="danger-text">${item.errorMoves} error</b>` : `${item.runningMoves} running`}</td></tr>`).join("");
    const cloud = document.querySelector("[data-shard-cloud]");
    cloud.innerHTML = Array.from({ length: demo.shardCount }, (_, shardIndex) => {
      const distCount = demo.shardDistributions(shardIndex).length;
      return `<a href="${demo.url("shard.html", { shard: demo.shardId(shardIndex) })}" style="--shard-color:${shardColor(shardIndex)}"><strong>${demo.shardId(shardIndex)}</strong><span>${distCount} distributions</span></a>`;
    }).join("");
    document.querySelector("[data-first-shard]").href = demo.url("shard.html", { shard: demo.shardId(0) });
    const sortedMoves = [...moves].sort((a, b) => ({ ERROR: 0, RUNNING: 1, PLANNED: 2 }[a.state] - ({ ERROR: 0, RUNNING: 1, PLANNED: 2 }[b.state])));
    document.querySelector("[data-cluster-moves]").innerHTML = sortedMoves.length ? sortedMoves.slice(0, 12).map(moveMarkup).join("") + (sortedMoves.length > 12 ? `<p class="aggregation-note">Showing 12 of ${sortedMoves.length} move tasks.</p>` : "") : `<p class="empty-copy">No active moves in this scenario.</p>`;
  }

  if (page === "shard") {
    const shardIndex = demo.selectedShardIndex;
    const picker = document.querySelector("[data-shard-picker]");
    picker.innerHTML = Array.from({ length: demo.shardCount }, (_, index) => `<option value="${demo.shardId(index)}">${demo.shardId(index)}</option>`).join(""); picker.value = demo.shardId(shardIndex);
    picker.addEventListener("change", () => { location.href = demo.changeUrl({ shard: picker.value, id: null }); });
    const distributions = demo.shardDistributions(shardIndex);
    const rangeTotal = distributions.reduce((sum, distIndex) => sum + demo.shardRangeCount(shardIndex, distIndex), 0);
    document.querySelector("[data-shard-id]").textContent = demo.shardId(shardIndex);
    document.querySelector("[data-shard-distribution-total]").textContent = format(distributions.length);
    document.querySelector("[data-shard-range-total]").textContent = format(rangeTotal);
    document.querySelector("[data-shard-distribution-rows]").innerHTML = distributions.map((distIndex) => {
      const lockedCount = demo.lockedIndices(distIndex).filter((rangeIndex) => demo.shardIndexFor(rangeIndex, distIndex) === shardIndex).length;
      return `<tr><td><a class="table-link" href="${demo.url("index.html", { distribution: distIndex, id: null })}">${demo.distribution(distIndex).id}</a></td><td>${demo.shardRangeCount(shardIndex, distIndex)}</td><td>${lockedCount}</td></tr>`;
    }).join("") || `<tr><td colspan="3">This shard is unused by the selected scenario.</td></tr>`;
    const moves = demo.allMoves().filter((move) => move.source === shardIndex || move.destination === shardIndex);
    document.querySelector("[data-shard-moves]").innerHTML = moves.length ? moves.map(moveMarkup).join("") : `<p class="empty-copy">No incoming or outgoing moves.</p>`;
  }

  if (page === "task") {
    const move = demo.moves().find((item) => item.rangeIndex === selected) || demo.moves()[0];
    if (move) {
      document.querySelectorAll("[data-task-id]").forEach((node) => { node.textContent = move.id; });
      document.querySelectorAll("[data-task-range]").forEach((node) => { node.textContent = move.rangeId; node.href = demo.url("range.html", { id: move.rangeId }); });
      document.querySelectorAll("[data-task-distribution]").forEach((node) => { node.textContent = demo.distribution(move.distributionIndex).id; });
      document.querySelectorAll("[data-task-source]").forEach((node) => { node.textContent = demo.shardId(move.source); });
      document.querySelectorAll("[data-task-destination]").forEach((node) => { node.textContent = demo.shardId(move.destination); });
      const stateChip = document.querySelector("[data-task-state]");
      stateChip.textContent = move.state;
      stateChip.className = `state-chip ${move.state === "ERROR" ? "error" : "running"}`;
      if (move.state !== "ERROR") {
        document.querySelector("[data-task-summary-title]").textContent = move.state === "RUNNING" ? "Move is in progress" : "Move is waiting to start";
        document.querySelector("[data-task-summary-body]").textContent = `Current journal phase: ${move.phase}. Routing and physical placement are shown separately below.`;
      }
      document.querySelector("[data-task-raw]").textContent = `KeyRange: ${demo.rangeState(move.rangeIndex, move.distributionIndex).locked ? "LOCKED" : "UNLOCKED"}\nMoveKeyRange: ${move.phase}\nTransferTransaction: ${demo.rangeState(move.rangeIndex, move.distributionIndex).transfer}\nMoveTask: ${move.phase === "PLANNED" ? "PLANNED" : "MOVED"}\nTaskGroup: ${move.state}`;
    }
  }

  demo.decorateLinks();
})();
