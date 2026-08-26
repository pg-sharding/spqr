(() => {
  const page = document.body.dataset.page;
  const params = new URLSearchParams(location.search);
  let total = Number(params.get("count")) || 240;
  const idParam = params.get("id") || "";
  const requestedIndex = /^kr-\d+$/.test(idParam) ? Number(idParam.replace(/\D/g, "")) - 1 : Number.NaN;
  const defaultIndex = Math.max(0, Math.min(total - 1, Math.round(total * 5 / 12) - 1));
  let selected = Number.isInteger(requestedIndex) && requestedIndex >= 0 ? Math.min(total - 1, requestedIndex) : defaultIndex;
  let windowSize = total <= 32 ? total : 24;
  let windowStart = total <= 32 ? 0 : Math.max(0, Math.min(total - windowSize, selected - 11));

  const shardFor = (index) => {
    const ratio = index / Math.max(1, total);
    if (ratio < .23 || ratio >= .8) return 1;
    if (ratio < .46 || ratio >= .63) return 2;
    return 3;
  };
  const idFor = (index) => `kr-${String(index + 1).padStart(Math.max(3, String(total).length), "0")}`;
  const incidentIndex = defaultIndex;
  const copyingIndex = Math.floor(total * .72);
  const awaitingIndex = Math.floor(total * .18);
  const manualLockIndex = Math.max(0, total - 3);
  const locked = () => [...new Set([incidentIndex, copyingIndex, awaitingIndex, manualLockIndex])];
  const rangeState = (index) => {
    if (index === incidentIndex) return { locked: true, task: "ERROR", move: "DATA_MOVED", transfer: "data_copied", evidence: "routing known · physical counts unknown" };
    if (index === copyingIndex) return { locked: true, task: "RUNNING", move: "DATA_MOVED", transfer: "data_copied", evidence: "copy reported · metadata pending" };
    if (index === awaitingIndex) return { locked: true, task: "RUNNING", move: "LOCKED", transfer: "locked", evidence: "routing known · source locked" };
    if (index === manualLockIndex) return { locked: true, task: "—", move: "—", transfer: "—", evidence: "routing known · no correlated move" };
    return { locked: false, task: "—", move: "—", transfer: "—", evidence: "routing known · no active move" };
  };
  const css = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();

  document.querySelectorAll("[data-range-total]").forEach((node) => { node.textContent = total.toLocaleString("en-US"); });
  document.querySelectorAll("[data-scale]").forEach((select) => {
    select.value = String(total);
    select.addEventListener("change", () => {
      const next = new URL(location.href);
      next.searchParams.set("count", select.value);
      next.searchParams.delete("id");
      location.href = next;
    });
  });
  document.querySelectorAll(".concept-switch").forEach((select) => select.addEventListener("change", () => { location.href = select.value; }));
  document.querySelectorAll("[data-presentation]").forEach((button) => button.addEventListener("click", () => {
    document.body.classList.toggle("presentation");
    button.textContent = document.body.classList.contains("presentation") ? "Exit presentation" : "Present";
  }));

  const selectRange = (index) => {
    selected = index;
    if (selected < windowStart || selected >= windowStart + windowSize) windowStart = Math.max(0, Math.min(total - windowSize, selected - 11));
    renderOverview();
  };

  const renderSelection = () => {
    const panel = document.querySelector("[data-selection-panel]");
    if (!panel) return;
    const source = `shard-0${shardFor(selected)}`;
    const state = rangeState(selected);
    const stateLabel = state.locked ? "LOCKED" : "UNLOCKED";
    panel.innerHTML = `<div class="selection-kicker">SELECTED KEY RANGE</div>
      <h2>${idFor(selected)}</h2>
      <span class="state-chip ${state.locked ? "locked" : "unlocked"}">${stateLabel}</span>
      <dl class="detail-list">
        <div class="detail-row"><dt>Interval</dt><dd>[${selected * 1000}, ${(selected + 1) * 1000})</dd></div>
        <div class="detail-row"><dt>Routes to</dt><dd>${source}</dd></div>
        <div class="detail-row"><dt>Task group</dt><dd>${state.task}</dd></div>
        <div class="detail-row"><dt>Move journal</dt><dd>${state.move}</dd></div>
        <div class="detail-row"><dt>Transfer</dt><dd>${state.transfer}</dd></div>
        <div class="detail-row"><dt>Evidence</dt><dd>${state.evidence}</dd></div>
      </dl>
      <a class="panel-link" href="range.html?id=${idFor(selected)}&count=${total}">Open exact range detail →</a>`;
  };

  const drawCanvas = () => {
    const canvas = document.querySelector("[data-overview-canvas]");
    if (!canvas) return;
    const rect = canvas.parentElement.getBoundingClientRect();
    const ratio = Math.min(2, devicePixelRatio || 1);
    canvas.width = Math.max(1, Math.floor(rect.width * ratio));
    canvas.height = Math.max(1, Math.floor(rect.height * ratio));
    const ctx = canvas.getContext("2d");
    ctx.scale(ratio, ratio);
    const width = rect.width;
    const bins = Math.min(total, Math.floor(width));
    const colors = [css("--shard-1"), css("--shard-2"), css("--shard-3")];
    for (let bin = 0; bin < bins; bin += 1) {
      const index = Math.floor(bin * total / bins);
      const x = bin * width / bins;
      const nextX = (bin + 1) * width / bins;
      ctx.fillStyle = colors[shardFor(index) - 1];
      ctx.fillRect(x, 15, Math.ceil(nextX - x), 28);
    }
    const viewX = windowStart / total * width;
    const viewWidth = Math.max(8, windowSize / total * width);
    ctx.strokeStyle = css("--primary"); ctx.lineWidth = 2;
    ctx.strokeRect(Math.max(1, viewX), 7, Math.min(width - 2, viewWidth), 44);
    locked().forEach((index) => {
      const x = (index + .5) / total * width;
      ctx.strokeStyle = css("--warning"); ctx.lineWidth = 2;
      ctx.beginPath(); ctx.moveTo(x, 11); ctx.lineTo(x, 47); ctx.stroke();
    });
    const errorX = (incidentIndex + .5) / total * width;
    ctx.fillStyle = css("--danger");
    ctx.beginPath(); ctx.moveTo(errorX, 2); ctx.lineTo(errorX - 5, 9); ctx.lineTo(errorX + 5, 9); ctx.fill();
  };

  const renderViewport = () => {
    const viewport = document.querySelector("[data-range-viewport]");
    if (!viewport) return;
    viewport.innerHTML = "";
    for (let index = windowStart; index < Math.min(total, windowStart + windowSize); index += 1) {
      const button = document.createElement("button");
      button.type = "button";
      button.className = `range-cell${locked().includes(index) ? " locked" : ""}${index === selected ? " selected" : ""}`;
      button.dataset.shard = String(shardFor(index));
      button.dataset.index = String(index);
      button.setAttribute("aria-label", `${idFor(index)}, shard-0${shardFor(index)}, ${locked().includes(index) ? "LOCKED" : "UNLOCKED"}`);
      button.innerHTML = `<strong>${idFor(index)}</strong><span>shard-0${shardFor(index)}</span>${locked().includes(index) ? "<em>LOCKED</em>" : ""}`;
      button.addEventListener("click", () => selectRange(index));
      button.addEventListener("keydown", (event) => {
        if (event.key !== "ArrowLeft" && event.key !== "ArrowRight") return;
        event.preventDefault();
        selectRange(Math.max(0, Math.min(total - 1, index + (event.key === "ArrowRight" ? 1 : -1))));
        requestAnimationFrame(() => document.querySelector(`[data-index="${selected}"]`)?.focus());
      });
      viewport.append(button);
    }
    document.querySelector("[data-window-title]").textContent = `Ranges ${windowStart + 1}–${Math.min(total, windowStart + windowSize)} of ${total.toLocaleString("en-US")}`;
    document.querySelector("[data-density-label]").textContent = total > 900 ? "Binned ownership layer · exceptions stay exact" : "One ordered mark per key range";
  };

  const renderAllocation = () => {
    const target = document.querySelector("[data-allocation-bars]");
    if (!target) return;
    const counts = [0, 0, 0];
    for (let index = 0; index < total; index += 1) counts[shardFor(index) - 1] += 1;
    const max = Math.max(...counts);
    target.innerHTML = counts.map((count, index) => `<div class="allocation-row"><strong>shard-0${index + 1}</strong><div class="allocation-track"><div class="allocation-fill" style="width:${count / max * 100}%"></div></div><span class="allocation-count">${count} ranges</span></div>`).join("");
  };

  const renderOverview = () => {
    renderViewport(); renderSelection(); renderAllocation(); requestAnimationFrame(drawCanvas);
  };

  if (page === "overview") {
    document.querySelector("[data-jump-button]")?.addEventListener("click", () => {
      const raw = document.querySelector("[data-jump]").value;
      const index = Number(raw.replace(/\D/g, "")) - 1;
      if (Number.isFinite(index) && index >= 0 && index < total) selectRange(index);
    });
    renderOverview();
    new ResizeObserver(drawCanvas).observe(document.querySelector("[data-overview-canvas]").parentElement);
  }

  if (page === "range") {
    const state = rangeState(selected);
    document.querySelector("[data-page-range-id]").textContent = idFor(selected);
    document.querySelector("[data-back-to-map]").href = `index.html?count=${total}&id=${idFor(selected)}`;
    const detail = document.querySelector("[data-range-detail]");
    const taskLink = selected === incidentIndex ? `<a class="panel-link" href="task.html?id=tg-7f31&count=${total}">Explain failed move →</a>` : "";
    detail.innerHTML = `<h2>Routing and lock</h2><p>Facts from KeyRangeService and matching move journals.</p>
      <span class="state-chip ${state.locked ? "locked" : "unlocked"}">${state.locked ? "LOCKED" : "UNLOCKED"}</span><dl class="detail-list">
      <div class="detail-row"><dt>ID</dt><dd>${idFor(selected)}</dd></div><div class="detail-row"><dt>Distribution</dt><dd>customers_by_id</dd></div>
      <div class="detail-row"><dt>Lower bound</dt><dd>${selected * 1000}</dd></div><div class="detail-row"><dt>Upper bound</dt><dd>${(selected + 1) * 1000}</dd></div>
      <div class="detail-row"><dt>Shard</dt><dd>shard-0${shardFor(selected)}</dd></div><div class="detail-row"><dt>Move journal</dt><dd>${state.move}</dd></div>
      <div class="detail-row"><dt>Transfer</dt><dd>${state.transfer}</dd></div></dl>${taskLink}`;
    const neighbors = document.querySelector("[data-neighbors]");
    for (let index = Math.max(0, selected - 2); index <= Math.min(total - 1, selected + 2); index += 1) {
      const link = document.createElement("a"); link.className = `context-range${index === selected ? " current" : ""}`; link.href = `range.html?count=${total}&id=${idFor(index)}`;
      link.innerHTML = `<strong>${idFor(index)}</strong><span>shard-0${shardFor(index)} · [${index * 1000}, ${(index + 1) * 1000})</span>`; neighbors.append(link);
    }
    document.querySelector("[data-raw-json]").textContent = JSON.stringify({ krid: idFor(selected), shardId: `shard-0${shardFor(selected)}`, distributionId: "customers_by_id", locked: state.locked, bound: { values: [String(selected * 1000)] } }, null, 2);
  }
})();
