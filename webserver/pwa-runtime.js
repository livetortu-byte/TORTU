(() => {
  let screenWakeLock = null;

  async function keepScreenAwake() {
    if (document.visibilityState !== 'visible' || screenWakeLock) return;
    if (!('wakeLock' in navigator)) {
      console.warn('Este navegador no permite mantener la pantalla encendida.');
      return;
    }

    try {
      screenWakeLock = await navigator.wakeLock.request('screen');
      screenWakeLock.addEventListener('release', () => {
        screenWakeLock = null;
      });
    } catch (error) {
      console.error('No se pudo mantener la pantalla encendida:', error);
    }
  }

  document.addEventListener('visibilitychange', keepScreenAwake);
  keepScreenAwake();
})();
