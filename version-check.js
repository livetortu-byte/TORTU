(() => {
  const pageVersion = document.querySelector('meta[name="app-version"]')?.content;
  if(!pageVersion) return;

  let checking = false;
  async function checkForUpdate(){
    if(checking || document.visibilityState === 'hidden') return;
    checking = true;
    try {
      const response = await fetch('/api/version', {cache:'no-store'});
      if(!response.ok) throw new Error('No se pudo consultar la versión.');
      const result = await response.json();
      if(result.version && result.version !== pageVersion) location.reload();
    } catch(error) {
      console.warn('No se pudo comprobar si hay una actualización:', error);
    } finally {
      checking = false;
    }
  }

  checkForUpdate();
  setInterval(checkForUpdate, 30000);
  document.addEventListener('visibilitychange', () => {
    if(document.visibilityState === 'visible') checkForUpdate();
  });
})();
