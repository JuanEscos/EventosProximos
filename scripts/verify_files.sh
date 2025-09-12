#!/bin/bash

echo '=== VERIFICANDO ARCHIVOS REQUERIDOS ==='

required_files=("01events_*.json" "02competiciones_detalladas_*.json" "participantes_procesado_*.csv" "participants_completos_final.json")
missing_files=0

for pattern in "${required_files[@]}"; do
    # Buscar el archivo más reciente que coincida con el patrón
    latest_file=$(find ./output -name "$pattern" -printf '%T@ %p\n' 2>/dev/null | sort -n | tail -1 | cut -f2- -d" ")
    
    if [ -n "$latest_file" ] && [ -f "$latest_file" ]; then
        file_info=$(ls -la "$latest_file")
        echo "✅ $pattern: ENCONTRADO ($file_info)"
    else
        echo "❌ $pattern: NO ENCONTRADO"
        missing_files=$((missing_files + 1))
        
        # Mostrar archivos existentes para debug
        if [ "$pattern" == "participantes_procesado_*.csv" ]; then
            echo "   Archivos CSV existentes:"
            find ./output -name "*.csv" -exec ls -la {} \; 2>/dev/null || echo "   No hay archivos CSV"
        fi
    fi
done

echo "=== RESUMEN ==="
if [ $missing_files -eq 0 ]; then
    echo "✅ TODOS los archivos requeridos están presentes"
    exit 0
else
    echo "❌ Faltan $missing_files archivos requeridos"
    
    # Mostrar archivos existentes para debug
    echo "=== ARCHIVOS EXISTENTES EN ./output/ ==="
    ls -la ./output/ 2>/dev/null || echo "No se puede acceder al directorio ./output/"
    
    exit 1
fi
