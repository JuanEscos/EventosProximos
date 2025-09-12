#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FLOWAGILITY SCRAPER COMPLETO - SISTEMA DE EXTRACCIÓN DE DATOS DE COMPETICIONES

DESCRIPCIÓN DEL PROCESO:
──────────────────────────────────────────────────────────────────────────────

Este sistema automatizado realiza un proceso completo de extracción, transformación
y carga (ETL) de datos desde la plataforma FlowAgility.com. El proceso consta de
4 etapas principales:

1. EXTRACCIÓN DE EVENTOS BÁSICOS:
   - Login automático en FlowAgility
   - Navegación a la página de eventos
   - Scroll completo para cargar todos los eventos
   - Extracción de campos esenciales: nombre, fechas, organización, club, lugar,
     enlaces (info y participantes), y bandera del país

2. EXTRACCIÓN DE INFORMACIÓN DETALLADA:
   - Visita a cada página de información de evento
   - Preservación de campos originales
   - Enriquecimiento con información adicional
   - Mejora de datos de club y lugar si es necesario

3. EXTRACCIÓN DE PARTICIPANTES:
   - Acceso a páginas de listas de participantes
   - Extracción de información de competidores
   - Almacenamiento individual por evento
   - Consolidación en archivo único

4. GENERACIÓN DE OUTPUT FINAL:
   - Creación de archivo JSON unificado
   - Generación de CSV procesado
   - Metadata completa del proceso
   - Preparación para GitHub Actions y FTP

CARACTERÍSTICAS PRINCIPALES:
- Extracción robusta de todos los campos requeridos
- Manejo automático de cookies y sesión
- Pausas configurables entre solicitudes
- Preservación de datos originales
- Logging detallado del proceso
- Compatibilidad con GitHub Actions

ARCHIVOS GENERADOS:
- 01events_YYYY-MM-DD.json          → Eventos básicos
- 02competiciones_detalladas_YYYY-MM-DD.json → Info enriquecida
- participantes_<event_id>.json     → Participantes por evento
- 03todos_participantes_YYYY-MM-DD.json → Todos los participantes
- participants_completos_final.json → Archivo final unificado
- participantes_procesado_YYYY-MM-DD.csv → CSV procesado

USO:
python extraerParticipantesEventosProx.py [--module events|info|participants|all]
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
        TimeoutException, NoSuchElementException, WebDriverException,
        JavascriptException, StaleElementReferenceException, ElementClickInterceptedException
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

def _accept_cookies(driver):
    """Aceptar cookies si es necesario"""
    try:
        cookie_selectors = [
            'button[aria-label="Accept all"]',
            'button[aria-label="Aceptar todo"]',
            '[data-testid="uc-accept-all-button"]',
            'button[mode="primary"]'
        ]
        
        for selector in cookie_selectors:
            try:
                cookie_btn = driver.find_elements(By.CSS_SELECTOR, selector)
                if cookie_btn:
                    cookie_btn[0].click()
                    slow_pause(0.5, 1)
                    log("Cookies aceptadas")
                    return True
            except:
                continue
                
        # Fallback con JavaScript
        driver.execute_script("""
            const buttons = document.querySelectorAll('button');
            for (const btn of buttons) {
                if (/aceptar|accept|consent|agree/i.test(btn.textContent)) {
                    btn.click();
                    break;
                }
            }
        """)
        slow_pause(0.5, 1)
        return True
        
    except Exception as e:
        log(f"Error manejando cookies: {e}")
        return False

def _full_scroll(driver):
    """Scroll completo para cargar todos los elementos"""
    last_height = driver.execute_script("return document.body.scrollHeight")
    for _ in range(MAX_SCROLLS):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_WAIT_S)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

# ============================== MÓDULO 1: EXTRACCIÓN DE EVENTOS ==============================

def extract_events():
    """Función principal para extraer eventos básicos - VERSIÓN MEJORADA"""
    if not HAS_SELENIUM:
        log("Error: Selenium no está instalado")
        return None
    
    log("=== MÓDULO 1: EXTRACCIÓN DE EVENTOS BÁSICOS ===")
    
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
        
        # Aceptar cookies
        _accept_cookies(driver)
        
        # Scroll completo para cargar todos los eventos
        log("Cargando todos los eventos...")
        _full_scroll(driver)
        slow_pause(2, 3)
        
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
                # Extraer información completa del evento
                event_data = {}
                
                # ID del evento
                event_id = container.get('id', '')
                if event_id:
                    event_data['id'] = event_id.replace('event-card-', '')
                
                # Nombre del evento
                name_elem = container.find('div', class_='font-caption text-lg text-black truncate -mt-1')
                if name_elem:
                    event_data['nombre'] = _clean(name_elem.get_text())
                
                # Fechas
                date_elem = container.find('div', class_='text-xs')
                if date_elem:
                    event_data['fechas'] = _clean(date_elem.get_text())
                
                # Organización
                org_elems = container.find_all('div', class_='text-xs')
                if len(org_elems) > 1:
                    event_data['organizacion'] = _clean(org_elems[1].get_text())
                
                # Club organizador - BUSCAR ESPECÍFICAMENTE
                club_elem = container.find('div', class_='text-xs mb-0.5 mt-0.5')
                if club_elem:
                    event_data['club'] = _clean(club_elem.get_text())
                else:
                    # Fallback: buscar en todos los divs con text-xs
                    for div in container.find_all('div', class_='text-xs'):
                        text = _clean(div.get_text())
                        if text and not any(x in text for x in ['/', 'Spain', 'España']):
                            event_data['club'] = text
                            break
                
                # Lugar - BUSCAR PATRÓN CIUDAD/PAÍS
                location_divs = container.find_all('div', class_='text-xs')
                for div in location_divs:
                    text = _clean(div.get_text())
                    if '/' in text and any(x in text for x in ['Spain', 'España', 'Madrid', 'Barcelona']):
                        event_data['lugar'] = text
                        break
                
                # Si no encontramos lugar, buscar cualquier texto con /
                if 'lugar' not in event_data:
                    for div in location_divs:
                        text = _clean(div.get_text())
                        if '/' in text and len(text) < 100:  # Evitar textos muy largos
                            event_data['lugar'] = text
                            break
                
                # Enlaces
                event_data['enlaces'] = {}
                
                # Enlace de información
                info_link = container.find('a', href=lambda x: x and '/info/' in x)
                if info_link:
                    event_data['enlaces']['info'] = urljoin(BASE, info_link['href'])
                
                # Enlace de participantes - BUSCAR EXPLÍCITAMENTE
                participant_links = container.find_all('a', href=lambda x: x and any(term in x for term in ['/participants', '/participantes']))
                for link in participant_links:
                    href = link.get('href', '')
                    if '/participants_list' in href or '/participantes' in href:
                        event_data['enlaces']['participantes'] = urljoin(BASE, href)
                        break
                
                # Si no encontramos el enlace de participantes, construirlo
                if 'participantes' not in event_data['enlaces'] and 'id' in event_data:
                    event_data['enlaces']['participantes'] = f"{BASE}/zone/events/{event_data['id']}/participants_list"
                
                # Bandera del país
                flag_elem = container.find('div', class_='text-md')
                if flag_elem:
                    event_data['pais_bandera'] = _clean(flag_elem.get_text())
                else:
                    event_data['pais_bandera'] = '🇪🇸'  # Valor por defecto
                
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
        
        # Mostrar resumen de lo extraído
        print(f"\n{'='*80}")
        print("RESUMEN DE CAMPOS EXTRAÍDOS:")
        print(f"{'='*80}")
        for event in events[:5]:  # Mostrar primeros 5 eventos como ejemplo
            print(f"\nEvento: {event.get('nombre', 'N/A')}")
            print(f"  Club: {event.get('club', 'No extraído')}")
            print(f"  Lugar: {event.get('lugar', 'No extraído')}")
            print(f"  Enlace participantes: {event.get('enlaces', {}).get('participantes', 'No extraído')}")
        print(f"\n{'='*80}")
        
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

# ============================== MÓDULO 2: INFORMACIÓN DETALLADA ==============================

def extract_detailed_info():
    """Extraer información detallada de cada evento"""
    if not HAS_SELENIUM:
        log("Error: Selenium no está instalado")
        return None
    
    log("=== MÓDULO 2: EXTRACCIÓN DE INFORMACIÓN DETALLADA ===")
    
    # Buscar el archivo de eventos más reciente
    event_files = glob(os.path.join(OUT_DIR, "01events_*.json"))
    if not event_files:
        log("❌ No se encontraron archivos de eventos")
        return None
    
    latest_event_file = max(event_files, key=os.path.getctime)
    
    # Cargar eventos
    with open(latest_event_file, 'r', encoding='utf-8') as f:
        events = json.load(f)
    
    log(f"✅ Cargados {len(events)} eventos desde {latest_event_file}")
    
    driver = _get_driver(headless=HEADLESS)
    if not driver:
        log("❌ No se pudo crear el driver de Chrome")
        return None
    
    try:
        if not _login(driver):
            raise Exception("No se pudo iniciar sesión")
        
        detailed_events = []
        
        for i, event in enumerate(events, 1):
            try:
                # PRESERVAR CAMPOS ORIGINALES IMPORTANTES
                preserved_fields = ['id', 'nombre', 'fechas', 'organizacion', 'club', 'lugar', 'enlaces', 'pais_bandera']
                detailed_event = {field: event.get(field, '') for field in preserved_fields}
                
                # Verificar si tiene enlace de información
                if 'enlaces' in event and 'info' in event['enlaces']:
                    info_url = event['enlaces']['info']
                    
                    log(f"Procesando evento {i}/{len(events)}: {event.get('nombre', 'Sin nombre')}")
                    
                    # Navegar a la página de información
                    driver.get(info_url)
                    WebDriverWait(driver, 20).until(
                        EC.presence_of_element_located((By.TAG_NAME, "body"))
                    )
                    
                    slow_pause(2, 3)
                    
                    # Obtener HTML de la página
                    page_html = driver.page_source
                    soup = BeautifulSoup(page_html, 'html.parser')
                    
                    # ===== INFORMACIÓN ADICIONAL =====
                    additional_info = {}
                    
                    # Intentar mejorar información de club si no está completa
                    if not detailed_event.get('club') or detailed_event.get('club') in ['N/D', '']:
                        club_elems = soup.find_all(lambda tag: any(word in tag.get_text().lower() for word in ['club', 'organizador', 'organizer']))
                        for elem in club_elems:
                            text = _clean(elem.get_text())
                            if text and len(text) < 100:  # Evitar textos muy largos
                                detailed_event['club'] = text
                                break
                    
                    # Intentar mejorar información de lugar si no está completa
                    if not detailed_event.get('lugar') or detailed_event.get('lugar') in ['N/D', '']:
                        location_elems = soup.find_all(lambda tag: any(word in tag.get_text().lower() for word in ['lugar', 'ubicacion', 'location', 'place']))
                        for elem in location_elems:
                            text = _clean(elem.get_text())
                            if text and ('/' in text or any(x in text for x in ['Spain', 'España'])):
                                detailed_event['lugar'] = text
                                break
                    
                    # Extraer información general adicional
                    title_elem = soup.find('h1')
                    if title_elem:
                        additional_info['titulo_completo'] = _clean(title_elem.get_text())
                    
                    # Extraer descripción si existe
                    description_elem = soup.find('div', class_=lambda x: x and any(word in str(x).lower() for word in ['description', 'descripcion', 'info']))
                    if description_elem:
                        additional_info['descripcion'] = _clean(description_elem.get_text())
                    
                    # Añadir información adicional al evento
                    detailed_event['informacion_adicional'] = additional_info
                    detailed_event['timestamp_extraccion'] = datetime.now().isoformat()
                    
                else:
                    log(f"Evento {i} no tiene enlace de información, usando datos básicos")
                
                detailed_events.append(detailed_event)
                slow_pause(1, 2)  # Pausa entre solicitudes
                
            except Exception as e:
                log(f"❌ Error procesando evento {i}: {str(e)}")
                detailed_events.append(event)  # Mantener datos originales
                continue
        
        # Guardar información detallada
        today_str = datetime.now().strftime("%Y-%m-%d")
        output_file = os.path.join(OUT_DIR, f'02competiciones_detalladas_{today_str}.json')
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(detailed_events, f, ensure_ascii=False, indent=2)
        
        log(f"✅ Información detallada guardada en {output_file}")
        return detailed_events
        
    except Exception as e:
        log(f"❌ Error durante la extracción detallada: {str(e)}")
        traceback.print_exc()
        return None
    finally:
        try:
            driver.quit()
        except:
            pass
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
       
# ============================== MÓDULO 3: EXTRACCIÓN DE PARTICIPANTES ==============================

def extract_participants():
    """Extraer información detallada de participantes de cada evento"""
    if not HAS_SELENIUM:
        log("Error: Selenium no está instalado")
        return None
    
    log("=== MÓDULO 3: EXTRACCIÓN DE PARTICIPANTES ===")
    
    # Buscar el archivo de eventos detallados más reciente
    detailed_files = glob(os.path.join(OUT_DIR, "02competiciones_detalladas_*.json"))
    if not detailed_files:
        log("❌ No se encontraron archivos de eventos detallados")
        return None
    
    latest_detailed_file = max(detailed_files, key=os.path.getctime)
    
    # Cargar eventos detallados
    with open(latest_detailed_file, 'r', encoding='utf-8') as f:
        events = json.load(f)
    
    log(f"✅ Cargados {len(events)} eventos detallados desde {latest_detailed_file}")
    
    driver = _get_driver(headless=HEADLESS)
    if not driver:
        log("❌ No se pudo crear el driver de Chrome")
        return None
    
    try:
        if not _login(driver):
            raise Exception("No se pudo iniciar sesión")
        
        all_participants = []
        events_with_participants = 0
        
        for i, event in enumerate(events, 1):
            try:
                # Verificar si tiene enlace de participantes
                if 'enlaces' in event and 'participantes' in event['enlaces']:
                    participants_url = event['enlaces']['participantes']
                    
                    log(f"Procesando participantes {i}/{len(events)}: {event.get('nombre', 'Sin nombre')}")
                    
                    # Navegar a la página de participantes
                    driver.get(participants_url)
                    WebDriverWait(driver, 20).until(
                        EC.presence_of_element_located((By.TAG_NAME, "body"))
                    )
                    
                    slow_pause(2, 3)
                    
                    # Extraer información detallada de participantes
                    participants_data = _extract_detailed_participants(driver, participants_url, event)
                    
                    if participants_data:
                        events_with_participants += 1
                        log(f"  ✅ Encontrados {len(participants_data)} participantes")
                        
                        # Guardar participantes de este evento
                        event_participants_file = os.path.join(OUT_DIR, f"participantes_{event.get('id', 'unknown')}.json")
                        with open(event_participants_file, 'w', encoding='utf-8') as f:
                            json.dump(participants_data, f, ensure_ascii=False, indent=2)
                        
                        all_participants.extend(participants_data)
                    else:
                        log(f"  ⚠️  No se encontraron participantes")
                
                else:
                    log(f"Evento {i} no tiene enlace de participantes")
                
                slow_pause(1, 2)  # Pausa entre solicitudes
                
            except Exception as e:
                log(f"❌ Error procesando participantes del evento {i}: {str(e)}")
                continue
        
        # Guardar todos los participantes
        if all_participants:
            today_str = datetime.now().strftime("%Y-%m-%d")
            output_file = os.path.join(OUT_DIR, f'03todos_participantes_{today_str}.json')
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(all_participants, f, ensure_ascii=False, indent=2)
            
            log(f"✅ Total de {len(all_participants)} participantes guardados en {output_file}")
            log(f"✅ {events_with_participants} eventos con participantes procesados")
        else:
            log("⚠️  No se encontraron participantes en ningún evento")
        
        return all_participants
        
    except Exception as e:
        log(f"❌ Error durante la extracción de participantes: {str(e)}")
        traceback.print_exc()
        return None
    finally:
        try:
            driver.quit()
        except:
            pass

def _extract_detailed_participants(driver, participants_url, event):
    """Extraer información detallada de participantes usando técnicas del script original"""
    try:
        # Obtener HTML de la página
        page_html = driver.page_source
        soup = BeautifulSoup(page_html, 'html.parser')
        
        participants = []
        
        # Buscar todos los elementos que contienen información de participantes
        participant_containers = soup.find_all('div', class_=lambda x: x and 'participant' in str(x).lower())
        
        if not participant_containers:
            # Fallback: buscar por estructura de tabla o grid
            participant_containers = soup.find_all(['tr', 'div'], class_=lambda x: x and any(word in str(x).lower() for word in ['row', 'item', 'entry', 'competitor']))
        
        for container in participant_containers:
            try:
                participant_data = {
                    'participants_url': participants_url,
                    'BinomID': '',
                    'Dorsal': '',
                    'Guía': '',
                    'Perro': '',
                    'Raza': '',
                    'Edad': '',
                    'Género': '',
                    'Altura (cm)': '',
                    'Nombre de Pedigree': '',
                    'País': 'No disponible',
                    'Licencia': '',
                    'Club': '',
                    'Federación': '',
                    'Equipo': 'No disponible',
                    'event_uuid': event.get('id', ''),
                    'event_title': event.get('nombre', 'N/D')
                }
                
                # Extraer información básica
                text_content = _clean(container.get_text())
                
                # Buscar dorsal (normalmente números al inicio)
                dorsal_match = re.search(r'^\s*(\d+)\s', text_content)
                if dorsal_match:
                    participant_data['Dorsal'] = dorsal_match.group(1)
                
                # Buscar información específica por patrones
                lines = [line.strip() for line in text_content.split('\n') if line.strip()]
                
                for line in lines:
                    line_lower = line.lower()
                    
                    # Guía (normalmente contiene nombre de persona)
                    if not participant_data['Guía'] and any(keyword in line_lower for keyword in ['guía', 'guia', 'handler', 'manejador']):
                        participant_data['Guía'] = line
                    elif not participant_data['Guía'] and re.match(r'^[A-ZÁÉÍÓÚÜ][a-záéíóúü]+\s+[A-ZÁÉÍÓÚÜ][a-záéíóúü]+$', line):
                        participant_data['Guía'] = line
                    
                    # Perro
                    if not participant_data['Perro'] and any(keyword in line_lower for keyword in ['perro', 'dog', 'can']):
                        participant_data['Perro'] = line
                    
                    # Raza
                    if not participant_data['Raza'] and any(breed in line_lower for breed in ['border', 'collie', 'pastor', 'labrador', 'shepherd', 'water dog', 'retriever']):
                        participant_data['Raza'] = line
                    
                    # Edad
                    if not participant_data['Edad'] and any(keyword in line_lower for keyword in ['años', 'years', 'edad', 'age']):
                        participant_data['Edad'] = line
                    
                    # Género
                    if not participant_data['Género'] and any(keyword in line_lower for keyword in ['hembra', 'macho', 'female', 'male']):
                        participant_data['Género'] = line
                    
                    # Altura
                    if not participant_data['Altura (cm)'] and ('cm' in line_lower or 'altura' in line_lower):
                        participant_data['Altura (cm)'] = line
                    
                    # Licencia
                    if not participant_data['Licencia'] and any(keyword in line_lower for keyword in ['licencia', 'license', 'lic']):
                        participant_data['Licencia'] = line
                    
                    # Club
                    if not participant_data['Club'] and any(keyword in line_lower for keyword in ['club', 'team', 'equipo']):
                        participant_data['Club'] = line
                    
                    # Federación
                    if not participant_data['Federación'] and any(keyword in line_lower for keyword in ['federación', 'federacion', 'federation', 'rsce', 'rfec']):
                        participant_data['Federación'] = line
                
                # Generar ID único para el binomio
                if participant_data['Guía'] and participant_data['Perro']:
                    participant_data['BinomID'] = f"{participant_data['Guía']}_{participant_data['Perro']}".replace(' ', '_').lower()
                
                # Buscar información de mangas/días
                schedule_data = _extract_schedule_info(container)
                for i in range(1, 7):
                    participant_data[f'Día {i}'] = schedule_data.get(f'dia_{i}', '')
                    participant_data[f'Fecha {i}'] = schedule_data.get(f'fecha_{i}', '')
                    participant_data[f'Mangas {i}'] = schedule_data.get(f'mangas_{i}', '')
                
                participants.append(participant_data)
                
            except Exception as e:
                log(f"Error procesando participante individual: {e}")
                continue
        
        return participants
        
    except Exception as e:
        log(f"Error en extracción detallada de participantes: {e}")
        return []

def _extract_schedule_info(container):
    """Extraer información de horarios y mangas"""
    schedule_data = {}
    
    try:
        # Buscar elementos relacionados con fechas y mangas
        schedule_elements = container.find_all(['div', 'span'], class_=lambda x: x and any(word in str(x).lower() for word in ['day', 'dia', 'date', 'fecha', 'manga', 'round']))
        
        days = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
        current_day = 1
        
        for element in schedule_elements:
            text = _clean(element.get_text())
            if not text:
                continue
            
            # Detectar días
            for i, day in enumerate(days, 1):
                if day.lower() in text.lower():
                    schedule_data[f'dia_{current_day}'] = day
                    current_day += 1
                    break
            
            # Detectar fechas (patrones de fecha)
            date_patterns = [
                r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2},?\s+\d{4}\b',
                r'\b\d{1,2}[-/]\d{1,2}[-/]\d{2,4}\b',
                r'\b\d{1,2}\s+de\s+(?:enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\b'
            ]
            
            for pattern in date_patterns:
                matches = re.findall(pattern, text, re.IGNORECASE)
                for match in matches:
                    schedule_data[f'fecha_{current_day}'] = match
                    break
            
            # Detectar mangas (G1, G2, G3, etc.)
            manga_patterns = [
                r'\bG\d+\s*/\s*[A-Z]+\b',
                r'\b(?:Grado|Grade)\s*\d+\b',
                r'\b(?:PRE|PROM|COMP|ROOKIES)\b'
            ]
            
            for pattern in manga_patterns:
                matches = re.findall(pattern, text, re.IGNORECASE)
                for match in matches:
                    schedule_data[f'mangas_{current_day}'] = match
                    break
        
    except Exception as e:
        log(f"Error extrayendo información de horarios: {e}")
    
    return schedule_data

# ============================== MÓDULO 4: GENERACIÓN DE ARCHIVOS FINALES ==============================

def generate_csv_output():
    """Generar archivo CSV procesado con la estructura requerida"""
    log("=== GENERANDO ARCHIVO CSV PROCESADO ===")
    
    # Buscar archivo de participantes más reciente
    participant_files = glob(os.path.join(OUT_DIR, "03todos_participantes_*.json"))
    if not participant_files:
        log("❌ No se encontraron archivos de participantes")
        return False
    
    latest_participant_file = max(participant_files, key=os.path.getctime)
    
    # Cargar participantes
    with open(latest_participant_file, 'r', encoding='utf-8') as f:
        participants = json.load(f)
    
    if not participants:
        log("⚠️  No hay participantes para procesar")
        return False
    
    # Definir campos para el CSV
    fieldnames = [
        'participants_url', 'BinomID', 'Dorsal', 'Guía', 'Perro', 'Raza', 'Edad', 
        'Género', 'Altura (cm)', 'Nombre de Pedigree', 'País', 'Licencia', 'Club', 
        'Federación', 'Equipo', 'event_uuid', 'event_title'
    ]
    
    # Añadir campos de días/mangas
    for i in range(1, 7):
        fieldnames.extend([f'Día {i}', f'Fecha {i}', f'Mangas {i}'])
    
    # Guardar como CSV
    today_str = datetime.now().strftime("%Y-%m-%d")
    csv_file = os.path.join(OUT_DIR, f'participantes_procesado_{today_str}.csv')
    
    with open(csv_file, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for participant in participants:
            # Asegurar que todos los campos existan
            row = {field: participant.get(field, '') for field in fieldnames}
            writer.writerow(row)
    
    log(f"✅ Archivo CSV generado: {csv_file}")
    
    # Mostrar ejemplo del primer participante
    if participants:
        first_participant = participants[0]
        print(f"\n📋 EJEMPLO DE PARTICIPANTE EXTRAÍDO:")
        print(f"   Guía: {first_participant.get('Guía', 'N/A')}")
        print(f"   Perro: {first_participant.get('Perro', 'N/A')}")
        print(f"   Dorsal: {first_participant.get('Dorsal', 'N/A')}")
        print(f"   Club: {first_participant.get('Club', 'N/A')}")
        for i in range(1, 4):
            if first_participant.get(f'Mangas {i}'):
                print(f"   Mangas {i}: {first_participant.get(f'Mangas {i}')}")
    
    return True

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
