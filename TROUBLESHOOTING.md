# Solución de Problemas - Aplicación Web

## Errores de WebSocket (WebSocketClosedError)

### Síntoma
Ves errores repetidos en la terminal como:
```
tornado.websocket.WebSocketClosedError
tornado.iostream.StreamClosedError: Stream is closed
```

### Explicación
Estos errores son **comunes e inofensivos** en Streamlit. Ocurren cuando:
- El navegador cierra la conexión WebSocket inesperadamente
- Hay refreshes rápidos de la página
- Cambios rápidos entre pestañas
- Problemas menores de red

**No afectan la funcionalidad de la aplicación.**

### Soluciones

1. **Ignorar los errores** (recomendado)
   - Los errores no afectan el funcionamiento
   - La aplicación sigue funcionando normalmente
   - Solo son mensajes en la terminal

2. **Cerrar y reiniciar Streamlit**
   ```bash
   # Presiona Ctrl+C en la terminal
   # Luego reinicia:
   streamlit run web_app.py
   ```

3. **Limpiar la caché de Streamlit**
   ```bash
   # Detén Streamlit y ejecuta:
   rm -rf ~/.streamlit/cache
   streamlit run web_app.py
   ```

4. **Reiniciar el navegador**
   - Cierra completamente el navegador
   - Vuelve a abrir y accede a la aplicación

## La aplicación no carga los datos

### Verificar archivos
```bash
ls -lh results/final/*_entity_mapping_complete.csv
```

### Verificar permisos
```bash
chmod 644 results/final/*.csv
```

### Verificar formato
Los archivos CSV deben tener estas columnas:
- `entity_id`
- `original_name`
- `normalized_name`
- `standard_name`
- `frequency`
- `component_size`

## La aplicación es lenta

### Para datasets grandes:
1. **Usa los filtros** para reducir la cantidad de grupos mostrados
2. **Trabaja con una muestra** más pequeña para pruebas
3. **Cierra pestañas del navegador** que no uses
4. **Aumenta la memoria disponible** para Python

### Optimizaciones
- Los datos se cachean automáticamente
- Las búsquedas se limitan a los datos cargados
- Los grupos grandes se muestran con límites

## Los cambios no se guardan

### Verificar directorio
```bash
ls -lh results/manual_review/
```

### Verificar permisos
```bash
chmod 755 results/manual_review/
```

### Verificar espacio en disco
```bash
df -h
```

## Errores al mover nombres

### Problema: "Grupo no existe"
- El grupo puede haber sido eliminado o renombrado
- Refresca la página y vuelve a cargar los datos

### Problema: "No se pueden mover nombres"
- Verifica que los nombres existen en el grupo
- Asegúrate de que el grupo destino existe

## La aplicación se cuelga

1. **Presiona Ctrl+C** en la terminal para detener Streamlit
2. **Reinicia la aplicación**
3. **Recarga la página** en el navegador
4. **Cierra y vuelve a abrir** el navegador

## Problemas de memoria

Si trabajas con datasets muy grandes:

1. **Aumenta la memoria de Python**:
   ```bash
   export STREAMLIT_SERVER_MAX_UPLOAD_SIZE=200
   streamlit run web_app.py
   ```

2. **Trabaja con muestras**:
   - Filtra los datos antes de cargarlos
   - Usa solo grupos específicos

## Contacto

Si encuentras otros problemas:
1. Revisa los logs en la terminal
2. Verifica los archivos generados en `results/manual_review/`
3. Consulta la documentación en `README_WEB_APP.md`

