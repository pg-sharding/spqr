(() => {
  const page = document.body.dataset.page;
  const params = new URLSearchParams(location.search);
  let total = Number(params.get("count")) || 240;
  const idParam = params.get("id") || "";
  const rawIndex = /^kr-\d+$/.test(idParam) ? Number(idParam.replace(/\D/g, "")) - 1 : Number.NaN;
  const defaultIndex = Math.max(0, Math.min(total - 1, Math.round(total * 5 / 12) - 1));
  const selected = Number.isInteger(rawIndex) && rawIndex >= 0 ? Math.min(total - 1, rawIndex) : defaultIndex;
  const digits = Math.max(3, String(total).length);
  const idFor = (index) => `kr-${String(index + 1).padStart(digits, "0")}`;
  const shardFor = (index) => index / Math.max(1, total) < .46 ? 2 : 1;

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

  const moves = () => {
    const at = (ratio) => idFor(Math.min(total - 1, Math.floor(total * ratio)));
    return [
      { task: "tg-7f31", range: idFor(defaultIndex), from: 2, to: 3, state: "error", phase: "COORD_META_UPDATED", note: "Revision conflict after data copy" },
      { task: "tg-904a", range: at(.72), from: 1, to: 2, state: "running", phase: "DATA_MOVED", note: "Copying 6 relations · 68%" },
      { task: "tg-b180", range: at(.18), from: 3, to: 1, state: "running", phase: "LOCKED", note: "Waiting for transfer worker" },
      { task: "tg-120c", range: at(.88), from: 1, to: 3, state: "planned", phase: "PLANNED", note: "Queued behind tg-904a" },
      { task: "tg-438d", range: at(.56), from: 2, to: 1, state: "planned", phase: "PLANNED", note: "Scheduled" }
    ];
  };

  const renderDesk = (filter = "all") => {
    const headers = document.querySelector("[data-shard-headers]");
    const list = document.querySelector("[data-move-list]");
    if (!headers || !list) return;
    const counts = [Math.round(total * .34), Math.round(total * .38), 0];
    counts[2] = total - counts[0] - counts[1];
    headers.innerHTML = counts.map((count, index) => `<div><span class="shard-dot s${index + 1}"></span><strong>shard-0${index + 1}</strong><small>${count.toLocaleString("en-US")} ranges</small></div>`).join("");
    const visible = moves().filter((move) => filter === "all" || (filter === "attention" ? move.state === "error" : move.state === filter));
    list.innerHTML = visible.length ? visible.map((move) => `<a class="move-row ${move.state}" href="${move.state === "error" ? "task.html?id=tg-7f31" : `range.html?id=${move.range}`}&count=${total}">
      <div class="move-origin lane-${move.from}"><span>shard-0${move.from}</span></div>
      <div class="move-flow"><span class="move-state">${move.state.toUpperCase()}</span><strong>${move.range}</strong><small>${move.task} · ${move.phase}</small><div class="flow-line"><i></i><b>→</b></div><p>${move.note}</p></div>
      <div class="move-destination lane-${move.to}"><span>shard-0${move.to}</span></div>
    </a>`).join("") : `<div class="empty-state">No moves match this filter.</div>`;
  };

  if (page === "overview") {
    const filter = document.querySelector("[data-filter]");
    filter?.addEventListener("change", () => renderDesk(filter.value));
    renderDesk();
  }

  if (page === "range") {
    const rangeId = idFor(selected);
    const currentShard = `shard-0${shardFor(selected)}`;
    document.querySelector("[data-page-range-id]").textContent = rangeId;
    document.querySelector("[data-current-shard]").textContent = currentShard;
    document.querySelector("[data-back-to-desk]").href = `index.html?count=${total}`;
    document.querySelector("[data-task-link]").href = `task.html?id=tg-7f31&count=${total}`;
    document.querySelector("[data-range-facts]").innerHTML = `<div class="panel-heading"><div><h2>Routing facts</h2><p>KeyRangeService response</p></div></div><dl class="fact-list">
      <div><dt>ID</dt><dd>${rangeId}</dd></div><div><dt>Bounds</dt><dd>[${selected * 1000}, ${(selected + 1) * 1000})</dd></div>
      <div><dt>Distribution</dt><dd>customers_by_id</dd></div><div><dt>Current shard</dt><dd>${currentShard}</dd></div>
      <div><dt>Key range</dt><dd>LOCKED</dd></div><div><dt>Task group</dt><dd class="danger-text">ERROR</dd></div></dl>`;
    document.querySelector("[data-raw-json]").textContent = JSON.stringify({ krid: rangeId, shardId: currentShard, distributionId: "customers_by_id", locked: true, bound: { values: [String(selected * 1000)] } }, null, 2);
  }

  if (page === "task") {
    document.querySelector("[data-back-to-desk]").href = `index.html?count=${total}`;
    document.querySelector("[data-range-link]").href = `range.html?id=${idFor(selected)}&count=${total}`;
    document.querySelector("[data-range-link]").textContent = idFor(selected);
  }
})();
