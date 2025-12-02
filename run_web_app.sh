#!/bin/bash
# Script para iniciar la aplicación web de revisión de entidades

echo "🔍 Iniciando aplicación web para revisión de entidades..."
echo ""

# Verificar que Streamlit esté instalado
if ! command -v streamlit &> /dev/null; then
    echo "❌ Streamlit no está instalado."
    echo "Por favor instala las dependencias:"
    echo "   pip install -r requirements_web.txt"
    exit 1
fi

# Verificar que los archivos de datos existan
if [ ! -f "results/final/financial_entity_mapping_complete.csv" ]; then
    echo "⚠️  Advertencia: No se encontró results/final/financial_entity_mapping_complete.csv"
    echo "   Asegúrate de haber ejecutado el pipeline completo primero."
    echo ""
fi

# Iniciar la aplicación
echo "🚀 Iniciando aplicación en http://localhost:8501"
echo ""
streamlit run web_app.py

