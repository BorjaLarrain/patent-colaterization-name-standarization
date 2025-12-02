# Guía de Revisión Manual - Fase 6.2

## Formas de Hacer la Revisión Manual

### Opción 1: HTML Interactivo (Recomendado para revisión rápida)

**Archivo:** `results/manual_review/financial_review.html`

**Cómo usar:**
1. Abre el archivo HTML en tu navegador
2. Revisa cada componente problemático
3. Usa los botones para marcar tu decisión:
   - **✓ Válido**: El grupo está correcto, mantenerlo
   - **✂ Dividir**: El grupo tiene algunos nombres que no deberían estar juntos
   - **✗ Inválido**: El grupo es incorrecto, separar todos los nombres
4. Agrega notas en el campo de texto
5. El color del borde cambia según tu decisión

**Ventajas:**
- Visual y fácil de usar
- Puedes ver todos los nombres del componente de un vistazo
- Interactivo, puedes marcar decisiones rápidamente

**Desventajas:**
- No guarda automáticamente (necesitas documentar decisiones por separado)
- Mejor para revisión inicial rápida

---

### Opción 2: CSV/Excel (Recomendado para revisión detallada)

**Archivo:** `results/manual_review/financial_review_excel.csv`

**Cómo usar:**
1. Abre el CSV en Excel o Google Sheets
2. Cada fila es un nombre dentro de un componente problemático
3. Llena las columnas:
   - **decision**: `VALID`, `SPLIT`, o `INVALID`
   - **notes**: Explica tu decisión
4. Guarda el archivo

**Estructura del archivo:**
```
component_id | entity_id | standard_name | normalized_name | ... | decision | notes
0            | financial_0 | BANK OF... | BANK OF... | ... | VALID | Todos son variaciones del mismo banco
```

**Ventajas:**
- Puedes guardar tus decisiones directamente
- Fácil de filtrar y ordenar
- Puedes agregar múltiples columnas de análisis
- Se puede procesar automáticamente después

**Desventajas:**
- Menos visual que HTML
- Requiere más tiempo para llenar

---

### Opción 3: Muestra Aleatoria (Para validación general)

**Archivo:** `results/manual_review/financial_sample_review.csv`

**Cómo usar:**
1. Revisa una muestra aleatoria de 50 registros
2. Marca en la columna `is_correct`:
   - `YES`: El agrupamiento es correcto
   - `NO`: El agrupamiento es incorrecto
3. Agrega notas si es necesario

**Propósito:**
- Validar la calidad general del matching
- Identificar problemas sistemáticos
- Calcular tasa de error aproximada

---

## Criterios para Decisiones

### ✓ VÁLIDO (mantener grupo)
- Todos los nombres son variaciones legítimas de la misma entidad
- Ejemplo: "BANK OF AMERICA NA", "BANK OF AMERICA NATIONAL", "BANK OF AMERICA INC"

### ✂ DIVIDIR (separar algunos nombres)
- La mayoría de nombres son correctos, pero algunos no deberían estar
- Ejemplo: Grupo tiene "BANK OF AMERICA" y "BANK OF AMERICA VENTURES" (empresa diferente)
- **Acción:** Documentar qué nombres separar

### ✗ INVÁLIDO (separar todo)
- El grupo está completamente mal
- Ejemplo: "BAKER BEN" y "BAKER HUGHES" (persona vs empresa)
- **Acción:** Separar todos los nombres en grupos individuales

---

## Proceso Recomendado

### Paso 1: Revisión Inicial (HTML)
1. Abre `financial_review.html` en el navegador
2. Revisa rápidamente cada componente
3. Marca decisiones iniciales con los botones
4. Identifica componentes que necesitan revisión más detallada

### Paso 2: Revisión Detallada (Excel)
1. Abre `financial_review_excel.csv` en Excel
2. Filtra por componentes que marcaste como "DIVIDIR" o "INVALID"
3. Revisa nombre por nombre
4. Documenta decisiones específicas en las columnas

### Paso 3: Validación General (Muestra)
1. Revisa `financial_sample_review.csv`
2. Marca si cada caso es correcto o incorrecto
3. Calcula tasa de error: `incorrectos / total * 100`
4. Si tasa > 5%, considera ajustar parámetros

### Paso 4: Aplicar Decisiones
1. Usa el script `15_apply_manual_decisions.py` para aplicar cambios
2. O crea un script personalizado basado en tus decisiones

---

## Documentación de Decisiones

### Formato para Notas

**Para grupos VÁLIDOS:**
```
"Todos son variaciones de Bank of America. Incluye nombres con/sin sufijos legales."
```

**Para grupos a DIVIDIR:**
```
"Separar: 'BANK OF AMERICA VENTURES' (empresa diferente). Resto es válido."
```

**Para grupos INVÁLIDOS:**
```
"Falso positivo: 'BAKER BEN' es persona, 'BAKER HUGHES' es empresa. Separar todo."
```

---

## Herramientas Adicionales

### Script de Análisis
```bash
python scripts/analyze_problematic_matches.py
```
Analiza casos problemáticos específicos y muestra métricas de similitud.

### Visualización de Grafos
```bash
python scripts/visualize_graph.py
```
Genera visualización de cómo están conectados los nombres en cada componente.

---

## Tips para Revisión Eficiente

1. **Prioriza por tamaño**: Revisa primero grupos grandes (>20 nombres)
2. **Prioriza por similitud**: Revisa primero grupos con similitud < 90%
3. **Usa búsqueda**: En Excel, busca palabras clave sospechosas
4. **Revisa nombres de referencia**: Verifica que nombres conocidos (Figura 10) estén correctos
5. **Documenta patrones**: Si encuentras un patrón de error, documéntalo para ajustar el algoritmo

---

## Ejemplo de Revisión

### Componente 0: BANK OF NEW YORK MELLON

**Nombres en el grupo:**
- BANK OF NEW YORK MELLON TRUST CO NA
- BANK OF NEW YORK
- BANK OF NEW YORK COMMERCIAL
- BANK OF NEW YORK TRUST CO NA
- ... (60 nombres total)

**Análisis:**
- Todos son variaciones legítimas de Bank of New York
- Incluye nombres históricos y actuales
- Incluye diferentes divisiones (Trust, Commercial)

**Decisión:** ✓ VÁLIDO

**Notas:** "Grupo correcto. Incluye todas las variaciones de Bank of New York Mellon y sus predecesores."

---

## Próximos Pasos

Después de completar la revisión manual:

1. **Aplicar decisiones** usando script de aplicación
2. **Ajustar parámetros** si encuentras patrones de error
3. **Re-ejecutar pipeline** si es necesario
4. **Generar reporte final** con estadísticas de calidad

