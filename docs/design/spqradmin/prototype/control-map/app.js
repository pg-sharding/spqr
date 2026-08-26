(() => {
  const page = document.body.dataset.page;
  const params = new URLSearchParams(location.search);
  let total = Number(params.get("count")) || 240;
  const idParam = params.get("id") || "";
  const parsed = /^kr-\d+$/.test(idParam) ? Number(idParam.replace(/\D/g, "")) - 1 : Number.NaN;
  const defaultIndex = Math.max(0, Math.min(total - 1, Math.round(total * 5 / 12) - 1));
  let selected = Number.isInteger(parsed) && parsed >= 0 ? Math.min(total - 1, parsed) : defaultIndex;
  const viewportSize = total <= 24 ? total : 18;
  let viewportStart = total <= 24 ? 0 : Math.max(0, Math.min(total - viewportSize, selected - 8));
  const digits = Math.max(3, String(total).length);
  const idFor = (index) => `kr-${String(index + 1).padStart(digits, "0")}`;
  const shardFor = (index) => {
    const ratio = index / Math.max(1, total);
    if (ratio < .22 || ratio >= .81) return 1;
    if (ratio < .48 || ratio >= .64) return 2;
    return 3;
  };
  const locked = () => [selected, Math.floor(total * .72), Math.max(0, total - 3)];
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

  const drawMap = () => {
    const canvas = document.querySelector("[data-map-canvas]");
    if (!canvas) return;
    const rect = canvas.parentElement.getBoundingClientRect();
    const ratio = Math.min(2, devicePixelRatio || 1);
    canvas.width = Math.max(1, Math.floor(rect.width * ratio));
    canvas.height = Math.max(1, Math.floor(rect.height * ratio));
    const context = canvas.getContext("2d");
    context.scale(ratio, ratio);
    const width = rect.width;
    const bins = Math.min(total, Math.max(1, Math.floor(width)));
    const colors = [css("--shard-1"), css("--shard-2"), css("--shard-3")];
    for (let bin = 0; bin < bins; bin += 1) {
      const index = Math.floor(bin * total / bins);
      const left = bin * width / bins;
      context.fillStyle = colors[shardFor(index) - 1];
      context.fillRect(left, 16, Math.ceil((bin + 1) * width / bins - left), 34);
    }
    const viewLeft = viewportStart / total * width;
    const viewWidth = Math.max(9, viewportSize / total * width);
    context.strokeStyle = css("--primary"); context.lineWidth = 2;
    context.strokeRect(Math.max(1, viewLeft), 8, Math.min(width - 2, viewWidth), 50);
    locked().forEach((index) => {
      const x = (index + .5) / total * width;
      context.strokeStyle = css("--warning"); context.lineWidth = 2;
      context.beginPath(); context.moveTo(x, 12); context.lineTo(x, 54); context.stroke();
    });
    const errorX = (selected + .5) / total * width;
    context.fillStyle = css("--danger"); context.beginPath(); context.arc(errorX, 7, 4, 0, Math.PI * 2); context.fill();
  };

  const selectRange = (index) => {
    selected = Math.max(0, Math.min(total - 1, index));
    if (selected < viewportStart || selected >= viewportStart + viewportSize) viewportStart = Math.max(0, Math.min(total - viewportSize, selected - 8));
    renderControlMap();
  };

  const renderRanges = () => {
    const target = document.querySelector("[data-range-strip]");
    if (!target) return;
    target.innerHTML = "";
    for (let index = viewportStart; index < Math.min(total, viewportStart + viewportSize); index += 1) {
      const button = document.createElement("button");
      button.type = "button";
      button.className = `range-tile${locked().includes(index) ? " locked" : ""}${selected === index ? " selected" : ""}`;
      button.dataset.shard = String(shardFor(index));
      button.innerHTML = `<strong>${idFor(index)}</strong><span>shard-0${shardFor(index)}</span>${locked().includes(index) ? "<em>LOCKED</em>" : ""}`;
      button.addEventListener("click", () => selectRange(index));
      target.append(button);
    }
    document.querySelector("[data-window-label]").textContent = `Ranges ${viewportStart + 1}–${Math.min(total, viewportStart + viewportSize)}`;
    document.querySelector("[data-density-label]").textContent = total > 900 ? "Binned overview with an exact 18-range viewport." : "Every overview mark represents one key range.";
  };

  const renderSelected = () => {
    const target = document.querySelector("[data-selected-card]");
    if (!target) return;
    target.innerHTML = `<div><p class="eyebrow">SELECTED</p><h2>${idFor(selected)}</h2><span class="state-chip locked">LOCKED</span></div>
      <dl><div><dt>Bounds</dt><dd>[${selected * 1000}, ${(selected + 1) * 1000})</dd></div><div><dt>Routes to</dt><dd>shard-0${shardFor(selected)}</dd></div><div><dt>Linked task</dt><dd class="danger-text">tg-7f31 · ERROR</dd></div></dl>
      <a href="range.html?id=${idFor(selected)}&count=${total}">Inspect range →</a>`;
  };

  const renderActivity = () => {
    const target = document.querySelector("[data-activity-list]");
    if (!target) return;
    const items = [
      { id: "tg-7f31", index: selected, state: "ERROR", phase: "COORD_META_UPDATED", route: "02 → 03" },
      { id: "tg-904a", index: Math.floor(total * .72), state: "RUNNING", phase: "DATA_MOVED", route: "01 → 02" },
      { id: "tg-b180", index: Math.floor(total * .18), state: "RUNNING", phase: "LOCKED", route: "03 → 01" },
      { id: "tg-120c", index: Math.floor(total * .88), state: "PLANNED", phase: "PLANNED", route: "01 → 03" }
    ];
    target.innerHTML = "";
    items.forEach((item) => {
      const button = document.createElement("button"); button.type = "button";
      button.className = `activity-item ${item.state.toLowerCase()}${item.index === selected ? " selected" : ""}`;
      button.innerHTML = `<span class="activity-state">${item.state}</span><strong>${item.id}</strong><p>${idFor(item.index)} · ${item.route}</p><small>${item.phase}</small>`;
      button.addEventListener("click", () => selectRange(item.index));
      target.append(button);
    });
  };

  const renderControlMap = () => { renderRanges(); renderSelected(); renderActivity(); requestAnimationFrame(drawMap); };

  if (page === "overview") {
    document.querySelector("[data-jump-button]")?.addEventListener("click", () => {
      const index = Number(document.querySelector("[data-jump]").value.replace(/\D/g, "")) - 1;
      if (Number.isInteger(index) && index >= 0 && index < total) selectRange(index);
    });
    renderControlMap();
    new ResizeObserver(drawMap).observe(document.querySelector("[data-map-canvas]").parentElement);
  }

  if (page === "range") {
    const rangeId = idFor(selected);
    const shardId = `shard-0${shardFor(selected)}`;
    document.querySelector("[data-range-id]").textContent = rangeId;
    document.querySelector("[data-position-label]").textContent = `${selected + 1} of ${total.toLocaleString("en-US")}`;
    document.querySelector("[data-source-shard]").textContent = shardId;
    document.querySelector("[data-back-map]").href = `index.html?id=${rangeId}&count=${total}`;
    document.querySelector("[data-task-link]").href = `task.html?id=${rangeId}&count=${total}`;
    const neighbors = document.querySelector("[data-neighbor-strip]");
    for (let index = Math.max(0, selected - 2); index <= Math.min(total - 1, selected + 2); index += 1) {
      const link = document.createElement("a"); link.href = `range.html?id=${idFor(index)}&count=${total}`; link.className = index === selected ? "current" : "";
      link.dataset.shard = String(shardFor(index)); link.innerHTML = `<strong>${idFor(index)}</strong><span>shard-0${shardFor(index)}</span><small>[${index * 1000}, ${(index + 1) * 1000})</small>`; neighbors.append(link);
    }
    document.querySelector("[data-range-facts]").innerHTML = `<div><dt>ID</dt><dd>${rangeId}</dd></div><div><dt>Bounds</dt><dd>[${selected * 1000}, ${(selected + 1) * 1000})</dd></div><div><dt>Distribution</dt><dd>customers_by_id</dd></div><div><dt>Shard</dt><dd>${shardId}</dd></div><div><dt>Key range</dt><dd>LOCKED</dd></div><div><dt>Move journal</dt><dd>DATA_MOVED</dd></div>`;
    document.querySelector("[data-raw-json]").textContent = JSON.stringify({ krid: rangeId, shardId, distributionId: "customers_by_id", locked: true, bound: { values: [String(selected * 1000)] } }, null, 2);
  }

  if (page === "task") {
    const rangeId = idFor(selected);
    document.querySelector("[data-back-map]").href = `index.html?id=${rangeId}&count=${total}`;
    document.querySelector("[data-task-range]").href = `range.html?id=${rangeId}&count=${total}`;
    document.querySelector("[data-task-range]").textContent = rangeId;
  }
})();
