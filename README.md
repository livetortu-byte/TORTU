# Carrera de Tortugas LIVE

## Ejecutar localmente

Requiere Node.js 20 o posterior.

```powershell
npm install
npm start
```

Abre `http://localhost:3000/dashboard.html`. El panel no necesita token; expón el puerto indicado por `PORT` (3000 por defecto) para el despliegue.

## Desplegar en EasyPanel

Despliega desde este repositorio usando el `Dockerfile` y configura el puerto interno `3000`. Activa HTTPS para poder instalar la PWA y solicitar el bloqueo de suspensión en móviles. Monta un volumen persistente en `/data` para conservar la configuración del panel después de un redeploy.

En el panel:

- Ingresa el usuario de TikTok LIVE sin contraseña ni cookies.
- Abre **Registro de eventos** para revisar avatar, apodo, tipo y contenido.
- Abre el overlay con **Abrir overlay**. Instálalo como PWA desde el navegador del teléfono y toca la pantalla una vez para activar pantalla completa y el bloqueo de suspensión cuando el navegador lo permita.
- Los comentarios que mencionen Cerro/Cerrista u Olimpia/Olimpista, incluso dentro de una frase, eligen el equipo del comentarista. Si cambia de equipo en otro comentario, también cambia su asignación.
- Usa **Probar evento completo** para verificar comentarios, cambio de equipo, metros y regalos configurados sin una transmisión activa.

El despliegue PWA y el bloqueo de pantalla requieren HTTPS en dispositivos móviles. El sistema operativo o el navegador todavía pueden suspender la pantalla; la aplicación vuelve a solicitar Wake Lock al regresar a primer plano.

El conector de TikTok LIVE es comunitario, no oficial, y requiere que el usuario ingresado esté transmitiendo. No necesita token de conexión de TikTok. Los controles del servidor no tienen autenticación, así que cualquiera con acceso a su URL puede cambiar la configuración y enviar eventos de prueba. Los eventos se conservan en memoria hasta reiniciar el servidor; la configuración del panel sí se guarda en `.tortugas-config.json`.

## Pruebas

```powershell
npm test
```
