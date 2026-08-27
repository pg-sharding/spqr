(() => {
  const params = new URLSearchParams(location.search);
  const values = {
    distributions: [1, 8, 32].includes(Number(params.get('distributions'))) ? params.get('distributions') : '8',
    shards: [2, 8, 32, 128].includes(Number(params.get('shards'))) ? params.get('shards') : '8',
    count: [12, 240, 1842].includes(Number(params.get('count'))) ? params.get('count') : '240',
    distribution: '0'
  };

  const query = () => new URLSearchParams(values).toString();
  const updateTargets = () => {
    document.querySelectorAll('[data-variant-path]').forEach((node) => {
      const target = `${node.dataset.variantPath}?${query()}`;
      if (node.tagName === 'IFRAME') node.src = target;
      else node.href = target;
    });
    document.querySelectorAll('[data-gallery-link]').forEach((link) => { link.href = `${link.getAttribute('href').split('?')[0]}?${query()}`; });
    history.replaceState(null, '', `${location.pathname}?${query()}`);
  };

  document.querySelectorAll('[data-scenario]').forEach((select) => {
    select.value = values[select.dataset.scenario];
    select.addEventListener('change', () => { values[select.dataset.scenario] = select.value; updateTargets(); });
  });

  const resizePreviews = () => document.querySelectorAll('.preview-window').forEach((windowNode) => {
    const frame = windowNode.querySelector('iframe');
    const scale = windowNode.clientWidth / 1360;
    frame.style.transform = `scale(${scale})`;
    windowNode.style.height = `${Math.round(820 * scale)}px`;
  });
  new ResizeObserver(resizePreviews).observe(document.body);
  addEventListener('resize', resizePreviews);
  updateTargets();
  resizePreviews();
})();
