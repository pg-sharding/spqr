(() => {
  const DISTRIBUTION_NAMES = [
    "customers_by_id",
    "orders_by_customer",
    "payments_by_account",
    "sessions_by_user",
    "events_by_tenant",
    "catalog_by_seller",
    "messages_by_chat",
    "invoices_by_org",
    "profiles_by_user",
    "ledger_by_account",
    "documents_by_workspace",
    "notifications_by_user"
  ];

  const oneOf = (value, allowed, fallback) => allowed.includes(Number(value)) ? Number(value) : fallback;
  const clamp = (value, min, max) => Math.max(min, Math.min(max, value));

  const create = (search = location.search) => {
    const params = new URLSearchParams(search);
    const distributionCount = oneOf(params.get("distributions"), [1, 8, 32], 8);
    const shardCount = oneOf(params.get("shards"), [2, 8, 32, 128], 8);
    const krCount = oneOf(params.get("count"), [12, 240, 1842], 240);
    const distributionIndex = clamp(Number(params.get("distribution")) || 0, 0, distributionCount - 1);
    const rangeParam = params.get("id") || "";
    const parsedRange = /^kr-\d+$/.test(rangeParam) ? Number(rangeParam.replace(/\D/g, "")) - 1 : Number.NaN;
    const incidentIndex = clamp(Math.round(krCount * 5 / 12) - 1, 0, krCount - 1);
    const selectedRangeIndex = Number.isInteger(parsedRange) && parsedRange >= 0 ? clamp(parsedRange, 0, krCount - 1) : incidentIndex;
    const shardParam = params.get("shard") || "";
    const parsedShard = /^shard-\d+$/.test(shardParam) ? Number(shardParam.replace(/\D/g, "")) - 1 : Number.NaN;
    const selectedShardIndex = Number.isInteger(parsedShard) && parsedShard >= 0 ? clamp(parsedShard, 0, shardCount - 1) : 0;
    const rangeDigits = Math.max(3, String(krCount).length);
    const shardDigits = Math.max(2, String(shardCount).length);

    const rangeId = (index) => `kr-${String(index + 1).padStart(rangeDigits, "0")}`;
    const shardId = (index) => `shard-${String(index + 1).padStart(shardDigits, "0")}`;
    const distribution = (index = distributionIndex) => ({
      index,
      id: DISTRIBUTION_NAMES[index] || `distribution_${String(index + 1).padStart(2, "0")}`,
      columnType: index % 3 === 0 ? "integer" : index % 3 === 1 ? "bigint" : "varchar",
      relationCount: 1 + index % 5
    });

    const activeShardCount = Math.min(shardCount, krCount);
    const shardIndexFor = (rangeIndex, distIndex = distributionIndex) => {
      const orderedBucket = Math.min(activeShardCount - 1, Math.floor(rangeIndex * activeShardCount / krCount));
      return (orderedBucket + distIndex * 7) % shardCount;
    };

    const copyingIndex = clamp(Math.floor(krCount * .72), 0, krCount - 1);
    const awaitingIndex = clamp(Math.floor(krCount * .18), 0, krCount - 1);
    const manualLockIndex = clamp(krCount - 3, 0, krCount - 1);
    const rangeState = (index, distIndex = distributionIndex) => {
      if (index === incidentIndex && distIndex % 4 === 0) return { locked: true, task: `tg-${String(distIndex + 1).padStart(2, "0")}e1`, taskState: "ERROR", move: "DATA_MOVED", transfer: "data_copied" };
      if (index === copyingIndex && distIndex % 2 === 0) return { locked: true, task: `tg-${String(distIndex + 1).padStart(2, "0")}a4`, taskState: "RUNNING", move: "DATA_MOVED", transfer: "data_copied" };
      if (index === awaitingIndex && distIndex % 3 === 0) return { locked: true, task: `tg-${String(distIndex + 1).padStart(2, "0")}b8`, taskState: "RUNNING", move: "LOCKED", transfer: "locked" };
      if (index === manualLockIndex && distIndex % 5 === 0) return { locked: true, task: "—", taskState: null, move: "—", transfer: "—" };
      return { locked: false, task: "—", taskState: null, move: "—", transfer: "—" };
    };

    const lockedIndices = (distIndex = distributionIndex) => [incidentIndex, copyingIndex, awaitingIndex, manualLockIndex]
      .filter((index, position, values) => values.indexOf(index) === position && rangeState(index, distIndex).locked);

    const moves = (distIndex = distributionIndex) => {
      const result = [];
      const add = (index, state, phase, suffix, destinationOffset) => {
        const source = shardIndexFor(index, distIndex);
        result.push({
          id: `tg-${String(distIndex + 1).padStart(2, "0")}${suffix}`,
          distributionIndex: distIndex,
          rangeIndex: index,
          rangeId: rangeId(index),
          state,
          phase,
          source,
          destination: (source + Math.max(1, destinationOffset % shardCount)) % shardCount
        });
      };
      if (distIndex % 4 === 0) add(incidentIndex, "ERROR", "COORD_META_UPDATED", "e1", 1);
      if (distIndex % 2 === 0) add(copyingIndex, "RUNNING", "DATA_MOVED", "a4", 1);
      if (distIndex % 3 === 0) add(awaitingIndex, "RUNNING", "LOCKED", "b8", 2);
      if (distIndex % 2 === 1) add(clamp(Math.floor(krCount * .56), 0, krCount - 1), "PLANNED", "PLANNED", "c2", 1);
      return result;
    };

    const distributionSummary = (distIndex) => {
      const distMoves = moves(distIndex);
      return {
        ...distribution(distIndex),
        rangeCount: krCount,
        shardsUsed: activeShardCount,
        lockedCount: lockedIndices(distIndex).length,
        runningMoves: distMoves.filter((move) => move.state === "RUNNING").length,
        errorMoves: distMoves.filter((move) => move.state === "ERROR").length
      };
    };

    const allMoves = () => Array.from({ length: distributionCount }, (_, index) => moves(index)).flat();
    const shardRangeCount = (shardIndex, distIndex = distributionIndex) => {
      let count = 0;
      for (let index = 0; index < krCount; index += 1) if (shardIndexFor(index, distIndex) === shardIndex) count += 1;
      return count;
    };
    const shardDistributions = (shardIndex) => Array.from({ length: distributionCount }, (_, index) => index)
      .filter((distIndex) => shardRangeCount(shardIndex, distIndex) > 0);

    const query = (extra = {}) => {
      const next = new URLSearchParams({
        distributions: String(distributionCount),
        shards: String(shardCount),
        count: String(krCount),
        distribution: String(distributionIndex)
      });
      Object.entries(extra).forEach(([key, value]) => {
        if (value === undefined || value === null || value === "") next.delete(key);
        else next.set(key, String(value));
      });
      return next;
    };

    const url = (path, extra = {}) => `${path}?${query(extra).toString()}`;
    const changeUrl = (changes = {}) => {
      const next = new URL(location.href);
      const nextParams = query(changes);
      next.search = nextParams.toString();
      return next;
    };

    const bindControls = (root = document) => {
      const controls = [
        ["[data-scale]", "count", krCount],
        ["[data-shard-scale]", "shards", shardCount],
        ["[data-distribution-scale]", "distributions", distributionCount],
        ["[data-distribution-picker]", "distribution", distributionIndex]
      ];
      controls.forEach(([selector, key, value]) => root.querySelectorAll(selector).forEach((select) => {
        select.value = String(value);
        select.addEventListener("change", () => {
          const changes = { [key]: select.value, id: null, shard: null };
          if (key === "distributions") changes.distribution = 0;
          location.href = changeUrl(changes);
        });
      }));
      root.querySelectorAll(".concept-switch").forEach((select) => select.addEventListener("change", () => {
        const next = new URL(select.value, location.href);
        next.search = query().toString();
        location.href = next;
      }));
    };

    const decorateLinks = (root = document) => root.querySelectorAll("a[href]").forEach((anchor) => {
      const href = anchor.getAttribute("href");
      if (!href || href.startsWith("http") || href.startsWith("#") || href === "../" || href === "./") return;
      const next = new URL(href, location.href);
      if (next.origin !== location.origin) return;
      const existing = new URLSearchParams(next.search);
      if (next.pathname.endsWith("/task.html") && !existing.has("id")) existing.set("distribution", "0");
      const stateParams = query();
      stateParams.forEach((value, key) => { if (!existing.has(key)) existing.set(key, value); });
      next.search = existing.toString();
      anchor.href = next;
    });

    return {
      distributionCount,
      shardCount,
      krCount,
      distributionIndex,
      selectedRangeIndex,
      selectedShardIndex,
      incidentIndex,
      copyingIndex,
      awaitingIndex,
      manualLockIndex,
      rangeId,
      shardId,
      distribution,
      distributionSummary,
      shardIndexFor,
      shardRangeCount,
      shardDistributions,
      rangeState,
      lockedIndices,
      moves,
      allMoves,
      query,
      url,
      changeUrl,
      bindControls,
      decorateLinks
    };
  };

  window.SPQRDemo = { create };
})();
