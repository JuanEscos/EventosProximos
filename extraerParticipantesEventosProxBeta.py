# ============================== MÓDULO 4: GENERACIÓN DE ARCHIVOS FINALES ==============================

def generate_csv_output():
    """Generar archivo CSV procesado con la estructura requerida"""
    log("=== GENERANDO ARCHIVO CSV PROCESADO ===")
    
    # Buscar archivo de participantes más reciente
    participant_files = glob(os.path.join(OUT_DIR, "03todos_participantes_*.json"))
    if not participant_files:
        log("❌ No se encontraron archivos de participantes")
        
        # DEBUG: Mostrar qué archivos sí existen
        log("📁 Archivos en directorio output:")
        all_files = glob(os.path.join(OUT_DIR, "*"))
        for file in all_files:
            log(f"   {os.path.basename(file)}")
        
        return False
    
    latest_participant_file = max(participant_files, key=os.path.getctime)
    
    # Cargar participantes
    with open(latest_participant_file, 'r', encoding='utf-8') as f:
        participants = json.load(f)
    
    if not participants:
        log("⚠️  No hay participantes para procesar")
        return False
    
    # Definir campos para el CSV (exactamente como los necesitas)
    fieldnames = [
        'participants_url', 'BinomID', 'Dorsal', 'Guía', 'Perro', 'Raza', 'Edad', 
        'Género', 'Altura (cm)', 'Nombre de Pedigree', 'País', 'Licencia', 'Club', 
        'Federación', 'Equipo', 'event_uuid', 'event_title'
    ]
    
    # Añadir campos de días/mangas (Día 1-6, Fecha 1-6, Mangas 1-6)
    for i in range(1, 7):
        fieldnames.extend([f'Día {i}', f'Fecha {i}', f'Mangas {i}'])
    
    # Guardar como CSV
    today_str = datetime.now().strftime("%Y-%m-%d")
    csv_file = os.path.join(OUT_DIR, f'participantes_procesado_{today_str}.csv')
    
    with open(csv_file, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for participant in participants:
            # Asegurar que todos los campos existan y reemplazar None por ''
            row = {}
            for field in fieldnames:
                value = participant.get(field, '')
                if value is None:
                    value = ''
                row[field] = value
            writer.writerow(row)
    
    log(f"✅ Archivo CSV generado: {csv_file}")
    
    # Mostrar ejemplo del primer participante
    if participants:
        first_participant = participants[0]
        print(f"\n📋 EJEMPLO DE PARTICIPANTE EXTRAÍDO:")
        for field in ['Guía', 'Perro', 'Dorsal', 'Club', 'Raza']:
            value = first_participant.get(field, 'N/A')
            if value:
                print(f"   {field}: {value}")
    
    return True

def generate_final_json():
    """Generar el archivo JSON final unificado"""
    log("=== GENERANDO ARCHIVO JSON FINAL ===")
    
    # Buscar archivos más recientes
    event_files = glob(os.path.join(OUT_DIR, "01events_*.json"))
    detailed_files = glob(os.path.join(OUT_DIR, "02competiciones_detalladas_*.json"))
    participant_files = glob(os.path.join(OUT_DIR, "03todos_participantes_*.json"))
    
    if not event_files:
        log("❌ No se encontraron archivos de eventos")
        return False
    
    # Cargar eventos
    latest_event_file = max(event_files, key=os.path.getctime)
    with open(latest_event_file, 'r', encoding='utf-8') as f:
        events = json.load(f)
    
    # Cargar información detallada si existe
    detailed_events = []
    if detailed_files:
        latest_detailed_file = max(detailed_files, key=os.path.getctime)
        with open(latest_detailed_file, 'r', encoding='utf-8') as f:
            detailed_events = json.load(f)
    
    # Cargar participantes si existen
    all_participants = []
    if participant_files:
        latest_participant_file = max(participant_files, key=os.path.getctime)
        with open(latest_participant_file, 'r', encoding='utf-8') as f:
            all_participants = json.load(f)
    
    # Crear estructura final
    final_data = {
        'metadata': {
            'fecha_generacion': datetime.now().isoformat(),
            'total_eventos': len(events),
            'total_eventos_detallados': len(detailed_events),
            'total_participantes': len(all_participants),
            'version': '1.0'
        },
        'eventos': events,
        'eventos_detallados': detailed_events,
        'participantes': all_participants
    }
    
    # Guardar archivo final
    final_file = os.path.join(OUT_DIR, "participants_completos_final.json")
    with open(final_file, 'w', encoding='utf-8') as f:
        json.dump(final_data, f, ensure_ascii=False, indent=2)
    
    log(f"✅ Archivo final JSON generado: {final_file}")
    
    # Resumen final
    print(f"\n{'='*80}")
    print("RESUMEN FINAL DEL PROCESO:")
    print(f"{'='*80}")
    print(f"📊 Eventos básicos: {len(events)}")
    print(f"📊 Eventos con info detallada: {len(detailed_events)}")
    print(f"📊 Total participantes: {len(all_participants)}")
    
    # Calcular total de participantes desde eventos detallados
    if detailed_events:
        total_participants_from_events = sum(event.get('numero_participantes', 0) for event in detailed_events)
        print(f"📊 Total participantes (desde eventos): {total_participants_from_events}")
    
    # Verificar archivos generados
    print(f"\n📁 ARCHIVOS GENERADOS:")
    output_files = glob(os.path.join(OUT_DIR, "*"))
    for file in sorted(output_files):
        size = os.path.getsize(file)
        print(f"   {os.path.basename(file)} - {size} bytes")
    
    print(f"\n{'='*80}")
    
    return True
#!/bin/bash

    #!/bin/bash

    echo '=== VERIFICANDO ARCHIVOS REQUERIDOS ==='
    
    required_files=("01events_*.json" "02competiciones_detalladas_*.json" "participantes_procesado_*.csv" "participants_completos_final.json")
    missing_files=0
    
    for pattern in "${required_files[@]}"; do
        # Buscar archivos que coincidan con el patrón
        files=$(ls ./output/$pattern 2>/dev/null || true)
        
        if [ -n "$files" ]; then
            # Tomar el archivo más reciente
            latest_file=$(ls -t ./output/$pattern 2>/dev/null | head -1)
            if [ -n "$latest_file" ] && [ -f "$latest_file" ]; then
                file_info=$(ls -la "$latest_file")
                echo "✅ $pattern: ENCONTRADO ($file_info)"
            else
                echo "❌ $pattern: NO ENCONTRADO"
                missing_files=$((missing_files + 1))
            fi
        else
            echo "❌ $pattern: NO ENCONTRADO"
            missing_files=$((missing_files + 1))
        fi
    done
    
    echo "=== RESUMEN ==="
    if [ $missing_files -eq 0 ]; then
        echo "✅ TODOS los archivos requeridos están presentes"
        exit 0
    else
        echo "❌ Faltan $missing_files archivos requeridos"
        exit 1
    fi
# ============================== FUNCIÓN PRINCIPAL ==============================

def main():
    """Función principal"""
    print("🚀 INICIANDO FLOWAGILITY SCRAPER COMPLETO")
    print("📋 Este proceso realizará la extracción completa de datos de competiciones")
    print(f"📂 Directorio de salida: {OUT_DIR}")
    print("=" * 80)
    
    # Crear directorio de salida
    os.makedirs(OUT_DIR, exist_ok=True)
    
    parser = argparse.ArgumentParser(description="FlowAgility Scraper Mejorado")
    parser.add_argument("--module", choices=["events", "info", "participants", "csv", "all"], default="all", help="Módulo a ejecutar")
    args = parser.parse_args()
    
    try:
        success = True
        
        # Módulo 1: Eventos básicos
        if args.module in ["events", "all"]:
            log("🏁 INICIANDO EXTRACCIÓN DE EVENTOS BÁSICOS")
            events = extract_events()
            if not events:
                log("❌ Falló la extracción de eventos")
                success = False
            else:
                log("✅ Eventos básicos extraídos correctamente")
        
        # Módulo 2: Información detallada
        if args.module in ["info", "all"] and success:
            log("🏁 INICIANDO EXTRACCIÓN DE INFORMACIÓN DETALLADA")
            detailed_events = extract_detailed_info()
            if not detailed_events:
                log("⚠️  No se pudo extraer información detallada, continuando con datos básicos")
            else:
                log("✅ Información detallada extraída correctamente")
        
        # Módulo 3: Participantes
        if args.module in ["participants", "all"] and success:
            log("🏁 INICIANDO EXTRACCIÓN DE PARTICIPANTES")
            participants = extract_participants()
            if not participants:
                log("⚠️  No se pudo extraer participantes, continuando sin ellos")
            else:
                log("✅ Participantes extraídos correctamente")
        
        # Módulo 4: CSV Procesado
        if args.module in ["csv", "all"] and success:
            log("🏁 GENERANDO ARCHIVO CSV PROCESADO")
            if not generate_csv_output():
                log("⚠️  No se pudo generar el archivo CSV")
            else:
                log("✅ Archivo CSV generado correctamente")
        
        # Archivo final JSON
        if args.module in ["all"] and success:
            log("🏁 GENERANDO ARCHIVO FINAL JSON")
            if not generate_final_json():
                log("❌ Falló la generación del archivo final JSON")
                success = False
            else:
                log("✅ Archivo final JSON generado correctamente")
        
        if success:
            log("🎉 PROCESO COMPLETADO EXITOSAMENTE")
            print("\n✅ Todos los módulos se ejecutaron correctamente")
            print("📊 Los archivos están listos para GitHub Actions y FTP")
        else:
            log("❌ PROCESO COMPLETADO CON ERRORES")
            print("\n⚠️  Algunos módulos tuvieron errores")
            print("📋 Revisa los logs para más detalles")
        
        return success
        
    except Exception as e:
        log(f"❌ ERROR CRÍTICO DURANTE LA EJECUCIÓN: {e}")
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
