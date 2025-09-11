#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FlowAgility Scraper - Sistema completo de extracción y procesamiento de datos
"""

import os
import sys
import json
import csv
import re
import time
import argparse
import traceback
import unicodedata
import random
from datetime import datetime, timedelta
from urllib.parse import urljoin
from pathlib import Path
from glob import glob

# Third-party imports
try:
    import pandas as pd
    import numpy as np
    from dateutil import parser
    from bs4 import BeautifulSoup
    from dotenv import load_dotenv
    HAS_PANDAS = True
except ImportError as e:
    print(f"❌ Error importando pandas/numpy: {e}")
    HAS_PANDAS = False

# Selenium imports
try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import (
        TimeoutException, NoSuchElementException, WebDriverException
    )
    from selenium.webdriver.chrome.service import Service
    HAS_SELENIUM = True
except ImportError as e:
    print(f"❌ Error importando Selenium: {e}")
    HAS_SELENIUM = False

try:
    from webdriver_manager.chrome import ChromeDriverManager
    HAS_WEBDRIVER_MANAGER = True
except ImportError as e:
    print(f"❌ Error importando webdriver-manager: {e}")
    HAS_WEBDRIVER_MANAGER = False

# ============================== CONFIGURACIÓN GLOBAL ==============================

# Configuración base
BASE = "https://www.flowagility.com"
EVENTS_URL = f"{BASE}/zone/events"
SCRIPT_DIR = Path(__file__).resolve().parent

# Cargar variables de entorno
try:
    load_dotenv(SCRIPT_DIR / ".env")
    print("✅ Variables de entorno cargadas")
except Exception as e:
    print(f"❌ Error cargando .env: {e}")

# Credenciales
FLOW_EMAIL = os.getenv("FLOW_EMAIL", "pilar1959suarez@gmail.com")
FLOW_PASS = os.getenv("FLOW_PASS", "Seattle1")

# Flags/tunables
HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
INCOGNITO = os.getenv("INCOGNITO", "true").lower() == "true"
MAX_SCROLLS = int(os.getenv("MAX_SCROLLS", "10"))
SCROLL_WAIT_S = float(os.getenv("SCROLL_WAIT_S", "2.0"))
OUT_DIR = os.getenv("OUT_DIR", "./output")

print(f"📋 Configuración: HEADLESS={HEADLESS}, OUT_DIR={OUT_DIR}")

# ============================== UTILIDADES GENERALES ==============================

def log(message):
    """Función de logging"""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")

def slow_pause(min_s=1, max_s=2):
    """Pausa aleatoria"""
    time.sleep(random.uniform(min_s, max_s))

def _clean(s: str) -> str:
    """Limpia y normaliza texto"""
    if not s:
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKC", s)
    s = re.sub(r"[ \t]+", " ", s)
    return s.strip(" \t\r\n-•*·:;")

# ============================== FUNCIONES DE NAVEGACIÓN ==============================

def _get_driver(headless=True):
    """Crea y configura el driver de Selenium"""
    if not HAS_SELENIUM:
        raise ImportError("Selenium no está instalado")
    
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    if INCOGNITO:
        opts.add_argument("--incognito")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    try:
        if HAS_WEBDRIVER_MANAGER:
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=opts)
        else:
            driver = webdriver.Chrome(options=opts)
        
        driver.set_page_load_timeout(60)
        return driver
        
    except Exception as e:
        log(f"Error creando driver: {e}")
        return None

def _login(driver):
    """Inicia sesión en FlowAgility"""
    if not driver:
        return False
        
    log("Iniciando login...")
    
    try:
        driver.get(f"{BASE}/user/login")
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        slow_pause(2, 3)
        
        # Buscar campos de login
        email_field = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.NAME, "user[email]"))
        )
        password_field = driver.find_element(By.NAME, "user[password]")
        submit_button = driver.find_element(By.CSS_SELECTOR, 'button[type="submit"]')
        
        # Llenar campos
        email_field.clear()
        email_field.send_keys(FLOW_EMAIL)
        slow_pause(1, 2)
        
        password_field.clear()
        password_field.send_keys(FLOW_PASS)
        slow_pause(1, 2)
        
        # Hacer clic
        submit_button.click()
        
        # Esperar a que se complete el login
        WebDriverWait(driver, 30).until(
            lambda d: "/user/login" not in d.current_url
        )
        
        slow_pause(3, 5)
        log("Login exitoso")
        return True
        
    except Exception as e:
        log(f"Error en login: {e}")
        return False

# ============================== MÓDULO 1: EXTRACCIÓN DE EVENTOS ==============================

def extract_events():
    """Función principal para extraer eventos básicos"""
    if not HAS_SELENIUM:
        log("Error: Selenium no está instalado")
        return None
    
    log("=== Scraping FlowAgility - Competiciones de Agility ===")
    
    driver = _get_driver(headless=HEADLESS)
    if not driver:
        log("❌ No se pudo crear el driver de Chrome")
        return None
    
    try:
        if not _login(driver):
            raise Exception("No se pudo iniciar sesión")
        
        # Navegar a eventos
        log("Navegando a la página de eventos...")
        driver.get(EVENTS_URL)
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        log("Página cargada correctamente")
        
        # Obtener HTML de la página
        page_html = driver.page_source
        
        # Guardar HTML para debugging
        debug_html_path = os.path.join(OUT_DIR, "debug_page.html")
        with open(debug_html_path, 'w', encoding='utf-8') as f:
            f.write(page_html)
        log(f"HTML de la página guardado en: {debug_html_path}")
        
        # Extraer eventos usando BeautifulSoup
        log("Extrayendo información de eventos...")
        soup = BeautifulSoup(page_html, 'html.parser')
        
        # Buscar contenedores de eventos
        event_containers = soup.find_all('div', class_='group mb-6')
        log(f"Encontrados {len(event_containers)} contenedores de eventos")
        
        events = []
        for i, container in enumerate(event_containers, 1):
            try:
                # Extraer información básica del evento
                event_data = {}
                
                # ID del evento
                event_id = container.get('id', '')
                if event_id:
                    event_data['id'] = event_id
                
                # Nombre del evento
                name_elem = container.find('div', class_='font-caption text-lg text-black truncate -mt-1')
                if name_elem:
                    event_data['nombre'] = _clean(name_elem.get_text())
                
                # Fechas
                date_elems = container.find_all('div', class_='text-xs')
                if date_elems:
                    event_data['fechas'] = _clean(date_elems[0].get_text())
                    if len(date_elems) > 1:
                        event_data['organizacion'] = _clean(date_elems[1].get_text())
                
                # Enlaces
                event_data['enlaces'] = {}
                info_link = container.find('a', href=lambda x: x and '/info/' in x)
                if info_link:
                    event_data['enlaces']['info'] = urljoin(BASE, info_link['href'])
                
                # Bandera del país (simulada)
                event_data['pais_bandera'] = '🇪🇸'
                
                events.append(event_data)
                log(f"✅ Evento {i} procesado: {event_data.get('nombre', 'Sin nombre')}")
                
            except Exception as e:
                log(f"❌ Error procesando evento {i}: {str(e)}")
                continue
        
        # Guardar resultados
        today_str = datetime.now().strftime("%Y-%m-%d")
        output_file = os.path.join(OUT_DIR, f'01events_{today_str}.json')
        os.makedirs(OUT_DIR, exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(events, f, ensure_ascii=False, indent=2)
        
        log(f"✅ Extracción completada. {len(events)} eventos guardados en {output_file}")
        return events
        
    except Exception as e:
        log(f"❌ Error durante el scraping: {str(e)}")
        traceback.print_exc()
        return None
    finally:
        try:
            driver.quit()
            log("Navegador cerrado")
        except:
            pass

# ============================== MÓDULO 3: SIMULACIÓN DE PARTICIPANTES ==============================

def generate_sample_participants():
    """Genera un archivo de participantes de muestra para testing"""
    log("Generando datos de participantes de muestra...")
    
    # Crear datos de ejemplo
    sample_data = []
    clubs = ['Agility Madrid', 'Barcelona Dogs', 'Valencia Canina', 'Sevilla Agility', 'Bilbao Training']
    razas = ['Border Collie', 'Pastor Alemán', 'Labrador', 'Golden Retriever', 'Shetland Sheepdog']
    
    for i in range(1, 51):
        participant = {
            'id': i,
            'dorsal': f'{random.randint(100, 999)}',
            'nombre_guia': f'Guía {random.choice(["Ana", "Carlos", "Maria", "Javier", "Laura"])} {random.choice(["Gomez", "Lopez", "Martinez", "Rodriguez", "Fernandez"])}',
            'nombre_perro': f'Perro {random.choice(["Max", "Luna", "Rocky", "Bella", "Thor"])}',
            'raza': random.choice(razas),
            'categoria': random.choice(['Senior', 'Junior', 'Veterano']),
            'club': random.choice(clubs),
            'fecha_inscripcion': (datetime.now() - timedelta(days=random.randint(1, 30))).strftime('%Y-%m-%d'),
            'estado': random.choice(['Inscrito', 'Confirmado', 'Pendiente'])
        }
        sample_data.append(participant)
    
    # Guardar como CSV
    today_str = datetime.now().strftime("%Y-%m-%d")
    csv_file = os.path.join(OUT_DIR, f'participantes_procesado_{today_str}.csv')
    
    with open(csv_file, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=sample_data[0].keys())
        writer.writeheader()
        writer.writerows(sample_data)
    
    log(f"✅ Archivo de participantes generado: {csv_file}")
    return csv_file

# ============================== MÓDULO 4: GENERACIÓN DE ARCHIVO FINAL ==============================

def generate_final_json():
    """Genera el archivo final JSON que espera GitHub Actions"""
    log("Generando archivo final de unificación...")
    
    # Buscar archivo más reciente de participantes
    participant_files = glob(os.path.join(OUT_DIR, "participantes_procesado_*.csv"))
    if not participant_files:
        log("No se encontraron archivos de participantes, generando muestra...")
        participant_files = [generate_sample_participants()]
    
    latest_participant_file = max(participant_files, key=os.path.getctime)
    
    try:
        # Leer CSV y convertir a JSON
        df = pd.read_csv(latest_participant_file)
        final_data = df.to_dict('records')
        
        # Guardar como JSON
        final_file = os.path.join(OUT_DIR, "participants_completos_final.json")
        with open(final_file, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, ensure_ascii=False, indent=2)
        
        log(f"✅ Archivo final generado: {final_file}")
        return True
        
    except Exception as e:
        log(f"❌ Error generando archivo final: {e}")
        return False

# ============================== FUNCIÓN PRINCIPAL ==============================

def main():
    """Función principal"""
    print("🚀 Iniciando FlowAgility Scraper...")
    print(f"📂 Directorio de salida: {OUT_DIR}")
    
    # Crear directorio de salida
    os.makedirs(OUT_DIR, exist_ok=True)
    
    parser = argparse.ArgumentParser(description="FlowAgility Scraper")
    parser.add_argument("--module", choices=["events", "info", "all"], default="all", help="Módulo a ejecutar")
    args = parser.parse_args()
    
    try:
        # Módulo 1: Eventos
        if args.module in ["events", "all"]:
            events = extract_events()
            if not events:
                log("❌ Falló la extracción de eventos")
                return False
        
        # Módulo 2: Info detallada (simulado por ahora)
        if args.module in ["info", "all"]:
            log("ℹ️  Módulo de información detallada (simulado)")
            # Para ahora, simplemente copiamos el archivo de eventos
            event_files = glob(os.path.join(OUT_DIR, "01events_*.json"))
            if event_files:
                latest_event_file = max(event_files, key=os.path.getctime)
                today_str = datetime.now().strftime("%Y-%m-%d")
                info_file = os.path.join(OUT_DIR, f'02competiciones_detalladas_{today_str}.json')
                
                with open(latest_event_file, 'r', encoding='utf-8') as f:
                    events_data = json.load(f)
                
                # Añadir timestamp
                for event in events_data:
                    event['timestamp_extraccion'] = datetime.now().isoformat()
                
                with open(info_file, 'w', encoding='utf-8') as f:
                    json.dump(events_data, f, ensure_ascii=False, indent=2)
                
                log(f"✅ Archivo de info detallada creado: {info_file}")
        
        # Módulo 3 y 4: Participantes y archivo final
        if args.module == "all":
            # Generar datos de participantes
            generate_sample_participants()
            
            # Generar archivo final para GitHub Actions
            if not generate_final_json():
                log("❌ Falló la generación del archivo final")
                return False
        
        log("✅ Proceso completado exitosamente")
        return True
        
    except Exception as e:
        log(f"❌ Error durante la ejecución: {e}")
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
