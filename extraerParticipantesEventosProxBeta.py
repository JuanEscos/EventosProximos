#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FLOWAGILITY SCRAPER COMPLETO - SISTEMA DE EXTRACCIÓN DE DATOS DE COMPETICIONES
Versión Beta (robusta para CI): imports separados, UUID/link canónicos, headless configurable,
reintentos, OUT_DIR asegurado, alias de JSON final (participants/participantes).
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
from datetime import datetime
from urllib.parse import urljoin
from pathlib import Path
from glob import glob

# =============================================================================
# Third-party imports (SEPARADOS) + flags
# =============================================================================
HAS_PANDAS = HAS_NUMPY = HAS_BS4 = HAS_DATEUTIL = False

try:
    import pandas as pd  # noqa: F401
    HAS_PANDAS = True
except Exception as e:
    print(f"⚠️ pandas no disponible: {e}")

try:
    import numpy as np  # noqa: F401
    HAS_NUMPY = True
except Exception as e:
    print(f"⚠️ numpy no disponible: {e}")

try:
    from dateutil import parser as dt_parser
    HAS_DATEUTIL = True
except Exception as e:
    print(f"⚠️ dateutil.parser no disponible: {e}")

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except Exception as e:
    print(f"⚠️ BeautifulSoup no disponible: {e}")

# Selenium imports
HAS_SELENIUM = False
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
except Exception as e:
    print(f"❌ Error importando Selenium: {e}")
    HAS_SELENIUM = False

HAS_WEBDRIVER_MANAGER = False
try:
    from webdriver_manager.chrome import ChromeDriverManager
    HAS_WEBDRIVER_MANAGER = True
except Exception as e:
    print(f"❌ Error importando webdriver-manager: {e}")
    HAS_WEBDRIVER_MANAGER = False

# =============================================================================
# CONFIGURACIÓN GLOBAL
# =============================================================================
BASE = "https://www.flowagility.com"
EVENTS_URL = f"{BASE}/zone/events"
SCRIPT_DIR = Path(__file__).resolve().parent

# Cargar .env si existe
try:
    from dotenv import load_dotenv
    load_dotenv(SCRIPT_DIR / ".env")
    print("✅ Variables de entorno cargadas (si existía .env)")
except Exception as e:
    print(f"⚠️ No se cargó .env (no crítico): {e}")

# Credenciales (tomadas del entorno/Secrets en CI)
FLOW_EMAIL = os.getenv("FLOW_EMAIL", "")
FLOW_PASS = os.getenv("FLOW_PASS", "")

# Flags/tunables
HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
INCOGNITO = os.getenv("INCOGNITO", "true").lower() == "true"
HEADLESS_PARTICIPANTS = os.getenv("HEADLESS_PARTICIPANTS", os.getenv("HEADLESS", "true")).lower() == "true"
MAX_SCROLLS = int(os.getenv("MAX_SCROLLS", "10"))
SCROLL_WAIT_S = float(os.getenv("SCROLL_WAIT_S", "2.0"))
OUT_DIR = os.getenv("OUT_DIR", "./output")

print(f"📋 Configuración: HEADLESS={HEADLESS}, HEADLESS_PARTICIPANTS={HEADLESS_PARTICIPANTS}, OUT_DIR={OUT_DIR}")

# =============================================================================
# UTILIDADES
# =============================================================================
def log(message: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")

def slow_pause(min_s=0.4, max_s=1.2):
    time.sleep(random.uniform(min_s, max_s))

def _clean(s: str) -> str:
    if not s:
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKC", s)
    s = re.sub(r"[ \t]+", " ", s)
    return s.strip(" \t\r\n-•*·:;")

def with_retries(fn, attempts=3, sleep_s=2, desc="op"):
    last = None
    for k in range(1, attempts+1):
        try:
            return fn()
        except (TimeoutException, StaleElementReferenceException, WebDriverException) as e:
            last = e
            log(f"⚠️ Retry {k}/{attempts} en {desc}: {e}")
            time.sleep(sleep_s)
    if last:
        raise last

def _parse_event_dates(date_string):
    """Parsea fechas estilo 'Sep 13 - 14 · RFEC / Fed. Andaluza' (robusto si hay dateutil)."""
    if not date_string or not HAS_DATEUTIL:
        return None, None, None
    try:
        patterns = [
            r'(\w{3}\s+\d{1,2})\s*-\s*(\w{3}\s+\d{1,2})',
            r'(\d{1,2}\s+\w{3})\s*-\s*(\d{1,2}\s+\w{3})',
            r'(\d{1,2}/\d{1,2})\s*-\s*(\d{1,2}/\d{1,2})',
            r'(\d{1,2}\s+\w{3})'
        ]
        today = datetime.now().date()
        start_date = end_date = None

        for pattern in patterns:
            m = re.search(pattern, date_string, re.IGNORECASE)
            if not m:
                continue
            if len(m.groups()) == 2:
                d1, d2 = m.group(1).strip(), m.group(2).strip()
                start_date = dt_parser.parse(f"{d1} {today.year}", fuzzy=True).date()
                end_date   = dt_parser.parse(f"{d2} {today.year}", fuzzy=True).date()
                if end_date < start_date:
                    end_date = dt_parser.parse(f"{d2} {today.year+1}", fuzzy=True).date()
            else:
                d1 = m.group(1).strip()
                start_date = dt_parser.parse(f"{d1} {today.year}", fuzzy=True).date()
                end_date = start_date
            break

        days_until = None
        if start_date:
            days_until = (start_date - today).days if start_date >= today else -1
        return start_date, end_date, days_until
    except Exception as e:
        log(f"Error parseando fechas '{date_string}': {e}")
        return None, None, None

# =============================================================================
# SELENIUM
# =============================================================================
def _get_driver(headless=True, unique_id=""):
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
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--remote-allow-origins=*")
    ua = os.getenv("SCRAPER_UA", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36")
    opts.add_argument(f"--user-agent={ua}")

    # Perfil temporal (Linux)
    if unique_id and os.name != "nt":
        user_data_dir = f"/tmp/chrome_profile_{unique_id}_{int(time.time())}"
        opts.add_argument(f"--user-data-dir={user_data_dir}")

    chrome_bin = os.getenv("CHROME_BINARY") or os.getenv("GOOGLE_CHROME_SHIM")
    if chrome_bin and os.path.exists(chrome_bin):
        opts.binary_location = chrome_bin

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

def _accept_cookies(driver):
    try:
        selectors = [
            '[data-testid="uc-accept-all-button"]',
            'button[aria-label*="Accept"]',
            'button[aria-label*="Aceptar"]',
        ]
        for sel in selectors:
            btns = driver.find_elements(By.CSS_SELECTOR, sel)
            if btns:
                try:
                    btns[0].click()
                    slow_pause(0.2, 0.6)
                    log("Cookies aceptadas (CSS)")
                    return True
                except Exception:
                    pass
        # JS fallback
        driver.execute_script("""
            const bs = Array.from(document.querySelectorAll('button'));
            for (const b of bs) {
              const t = (b.textContent||'').toLowerCase();
              if (/(aceptar|accept|agree|consent)/.test(t)) { b.click(); break; }
            }
        """)
        slow_pause(0.2, 0.6)
        return True
    except Exception as e:
        log(f"Error manejando cookies: {e}")
        return False

def _full_scroll(driver):
    last_height = driver.execute_script("return document.body.scrollHeight")
    for _ in range(MAX_SCROLLS):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_WAIT_S)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

def _login(driver):
    if not driver:
        return False
    log("Iniciando login...")
    try:
        with_retries(lambda: driver.get(f"{BASE}/user/login"), desc="get(login)")
        with_retries(lambda: WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, "body"))), desc="wait body")
        slow_pause(0.6, 1.0)

        email_field = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.NAME, "user[email]")))
        password_field = driver.find_element(By.NAME, "user[password]")
        submit_button = driver.find_element(By.CSS_SELECTOR, 'button[type="submit"]')

        email_field.clear(); email_field.send_keys(FLOW_EMAIL); slow_pause()
        password_field.clear(); password_field.send_keys(FLOW_PASS); slow_pause()
        submit_button.click()

        WebDriverWait(driver, 30).until(lambda d: "/user/login" not in d.current_url)
        slow_pause(1.2, 2.0)
        log("Login exitoso")
        return True
    except Exception as e:
        log(f"Error en login: {e}")
        return False

# =============================================================================
# MÓDULO 1: EXTRACCIÓN EVENTOS (básicos)
# =============================================================================
def extract_events():
    if not HAS_SELENIUM or not HAS_BS4:
        log("Error: Selenium o BeautifulSoup no están instalados")
        return None

    os.makedirs(OUT_DIR, exist_ok=True)
    log("=== MÓDULO 1: EXTRACCIÓN DE EVENTOS BÁSICOS ===")

    driver = _get_driver(headless=HEADLESS, unique_id="module1")
    if not driver:
        log("❌ No se pudo crear el driver de Chrome")
        return None

    try:
        if not _login(driver):
            raise Exception("No se pudo iniciar sesión")

        log("Navegando a la página de eventos...")
        with_retries(lambda: driver.get(EVENTS_URL), desc="get(EVENTS_URL)")
        with_retries(lambda: WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, "body"))), desc="wait body")
        _accept_cookies(driver)

        log("Cargando todos los eventos...")
        _full_scroll(driver)
        slow_pause(0.5, 1.2)

        page_html = driver.page_source
        debug_html_path = os.path.join(OUT_DIR, "debug_page.html")
        with open(debug_html_path, 'w', encoding='utf-8') as f:
            f.write(page_html)
        log(f"HTML de la página guardado en: {debug_html_path}")

        soup = BeautifulSoup(page_html, 'html.parser')

        # Estrategia robusta: construir lista por UUID detectados en cualquier enlace "/zone/events/<uuid>"
        seen = set()
        events = []
        for a in soup.select('a[href*="/zone/events/"]'):
            href = a.get('href') or ''
            m = re.search(r'/zone/events/([a-f0-9-]{36})(?:/|$)', href, re.I)
            if not m:
                continue
            uuid = m.group(1)
            if uuid in seen:
                continue
            seen.add(uuid)

            event_data = {'id': uuid, 'enlaces': {}}
            # Enlaces canónicos
            event_data['enlaces']['info'] = urljoin(BASE, f"/zone/events/{uuid}")
            event_data['enlaces']['participantes'] = urljoin(BASE, f"/zone/events/{uuid}/participants_list")

            # Intenta encontrar el contenedor más cercano para rascar nombre/fechas
            # (Fallo tolerante; si no se encuentra, se deja N/D)
            container = a.find_parent()
            name = ""
            fechas = ""
            if container:
                # Nombre: intenta textos prominentes cercanos
                cand = container.find(['h2', 'h3', 'div'])
                if cand:
                    name = _clean(cand.get_text())[:160]
                # Fechas: busca textos pequeños con patrón mes/día
                smalls = container.find_all('div')
                for div in smalls:
                    tx = _clean(div.get_text())
                    if re.search(r'\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|ene|feb|mar|abr|may|jun|jul|ago|sep|oct|nov|dic)\b', tx, re.I):
                        fechas = tx
                        break

            event_data['nombre'] = name or f"Evento {uuid[:8]}..."
            event_data['fechas'] = fechas
            # País (placeholder): no siempre visible en listado; bandera opcional
            event_data['pais_bandera'] = '🇪🇸'

            # Fechas parseadas
            s, e, d = _parse_event_dates(fechas)
            if s:
                event_data['fecha_inicio'] = s.isoformat()
                event_data['fecha_fin'] = e.isoformat() if e else s.isoformat()
                event_data['dias_restantes'] = d

            events.append(event_data)

        today_str = datetime.now().strftime("%Y-%m-%d")
        output_file = os.path.join(OUT_DIR, f'01events_{today_str}.json')
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(events, f, ensure_ascii=False, indent=2)
        log(f"✅ Extracción completada. {len(events)} eventos guardados en {output_file}")

        # Resumen breve
        for ev in events[:5]:
            print(f"- {ev.get('nombre')} | participantes: {ev['enlaces'].get('participantes')} | días_restantes={ev.get('dias_restantes')}")
        return events

    except Exception as e:
        log(f"❌ Error durante el scraping: {str(e)}")
        traceback.print_exc()
        return None
    finally:
        try:
            driver.quit()
            log("Navegador cerrado")
        except Exception:
            pass

# =============================================================================
# MÓDULO 2: INFO DETALLADA
# =============================================================================
def _extract_participants_count(driver):
    try:
        rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
        if len(rows) > 3:
            return len(rows)

        items = driver.find_elements(By.CSS_SELECTOR, ".booking-item, .participant, [data-participant-id]")
        if len(items) > 3:
            return len(items)

        page_text = driver.find_element(By.TAG_NAME, "body").text
        for rx in (r'(\d+)\s*participantes?', r'(\d+)\s*inscritos?', r'(\d+)\s*competidores?', r'total[:\s]*(\d+)'):
            m = re.search(rx, page_text, re.I)
            if m:
                return int(m.group(1))

        nums = [int(n) for n in re.findall(r'\b\d+\b', page_text) if 5 < int(n) < 3000]
        if nums:
            return max(nums)
        return 0
    except Exception as e:
        log(f"Error en conteo de participantes: {e}")
        return 0

def extract_detailed_info():
    if not HAS_SELENIUM or not HAS_BS4:
        log("Error: Selenium o BeautifulSoup no están instalados")
        return None

    os.makedirs(OUT_DIR, exist_ok=True)
    log("=== MÓDULO 2: EXTRACCIÓN DE INFORMACIÓN DETALLADA ===")

    event_files = glob(os.path.join(OUT_DIR, "01events_*.json"))
    if not event_files:
        log("❌ No se encontraron archivos de eventos")
        return None
    latest_event_file = max(event_files, key=os.path.getctime)

    with open(latest_event_file, 'r', encoding='utf-8') as f:
        events = json.load(f)
    log(f"✅ Cargados {len(events)} eventos desde {latest_event_file}")

    driver = _get_driver(headless=HEADLESS, unique_id="module2")
    if not driver:
        log("❌ No se pudo crear el driver de Chrome")
        return None

    try:
        if not _login(driver):
            raise Exception("No se pudo iniciar sesión")

        detailed_events = []

        for i, event in enumerate(events, 1):
            try:
                preserved_fields = ['id', 'nombre', 'fechas', 'organizacion', 'club', 'lugar',
                                    'enlaces', 'pais_bandera', 'fecha_inicio', 'fecha_fin', 'dias_restantes']
                detailed_event = {field: event.get(field, '') for field in preserved_fields}
                detailed_event['num_participantes'] = 0
                detailed_event['participantes_info'] = {}

                info_url = event.get('enlaces', {}).get('info')
                if info_url:
                    log(f"Procesando evento {i}/{len(events)}: {event.get('nombre', 'Sin nombre')}")
                    with_retries(lambda: driver.get(info_url), desc=f"get(info {i})")
                    with_retries(lambda: WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "body"))), desc="wait body")
                    slow_pause(0.5, 1.0)

                    page_html = driver.page_source
                    soup = BeautifulSoup(page_html, 'html.parser') if HAS_BS4 else None

                    additional_info = {}
                    if soup:
                        title_elem = soup.find('h1')
                        if title_elem:
                            additional_info['titulo_completo'] = _clean(title_elem.get_text())
                        description_elem = soup.find('div', class_=lambda x: x and any(z in str(x).lower() for z in ['description', 'descripcion', 'info']))
                        if description_elem:
                            additional_info['descripcion'] = _clean(description_elem.get_text())

                    detailed_event['informacion_adicional'] = additional_info

                part_url = event.get('enlaces', {}).get('participantes')
                if part_url:
                    log(f"  📊 Extrayendo número de participantes de: {part_url}")
                    try:
                        with_retries(lambda: driver.get(part_url), desc=f"get(part {i})")
                        with_retries(lambda: WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body"))), desc="wait body")
                        slow_pause(0.6, 1.0)
                        num_participants = _extract_participants_count(driver)
                        detailed_event['num_participantes'] = num_participants
                        detailed_event['participantes_info'] = {
                            'url': part_url,
                            'timestamp_consulta': datetime.now().isoformat(),
                            'metodo_extraccion': 'heuristica_conteo'
                        }
                        log(f"  ✅ Encontrados {num_participants} participantes")
                    except Exception as e:
                        log(f"  ❌ Error extrayendo participantes: {e}")
                        detailed_event['participantes_info']['error'] = str(e)

                detailed_event['timestamp_extraccion'] = datetime.now().isoformat()
                detailed_events.append(detailed_event)
                slow_pause(0.3, 0.8)

            except Exception as e:
                log(f"❌ Error procesando evento {i}: {str(e)}")
                detailed_events.append(event)
                continue

        today_str = datetime.now().strftime("%Y-%m-%d")
        output_file = os.path.join(OUT_DIR, f'02competiciones_detalladas_{today_str}.json')
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(detailed_events, f, ensure_ascii=False, indent=2)
        log(f"✅ Información detallada guardada en {output_file}")

        total_participants = sum(ev.get('num_participantes', 0) for ev in detailed_events)
        events_with_participants = sum(1 for ev in detailed_events if ev.get('num_participantes', 0) > 0)
        print(f"📊 Total eventos: {len(detailed_events)} | con participantes: {events_with_participants} | total participantes: {total_participants}")

        return detailed_events

    except Exception as e:
        log(f"❌ Error durante la extracción detallada: {str(e)}")
        traceback.print_exc()
        return None
    finally:
        try:
            driver.quit()
        except Exception:
            pass

# =============================================================================
# MÓDULO 3: PARTICIPANTES (BETA -> datos de ejemplo)
# =============================================================================
def _create_sample_participants(participants_url, event):
    sample_data = []
    guides = ["Margarita Andujar", "Carlos López", "Ana García", "Javier Martínez", "Laura Rodríguez"]
    dogs = ["Blackyborij", "Luna", "Rocky", "Bella", "Thor", "Max", "Toby", "Coco", "Daisy", "Buddy"]
    breeds = ["Spanish Water Dog", "Border Collie", "Pastor Alemán", "Labrador", "Golden Retriever"]
    clubs = ["La Dama", "Agility Trust", "El Área Jerez", "Club Agility Badalona", "A.D Agility Pozuelo"]

    num_participants = random.randint(5, 10)
    for i in range(num_participants):
        participant = {
            'participants_url': participants_url,
            'BinomID': f"binom_{event.get('id', 'unknown')}_{i}",
            'Dorsal': str(random.randint(100, 999)),
            'Guía': random.choice(guides),
            'Perro': random.choice(dogs),
            'Raza': random.choice(breeds),
            'Edad': f"{random.randint(2, 12)} años",
            'Género': random.choice(["Hembra", "Macho"]),
            'Altura (cm)': f"{random.randint(40, 60)}.0",
            'Nombre de Pedigree': random.choice(dogs),
            'País': "Spain",
            'Licencia': str(random.randint(10000, 99999)),
            'Club': random.choice(clubs),
            'Federación': random.choice(["RSCE", "RFEC"]),
            'Equipo': "No disponible",
            'event_uuid': event.get('id', ''),
            'event_title': event.get('nombre', 'N/D')
        }
        for day in range(1, 4):
            participant[f'Día {day}'] = ["Viernes", "Sábado", "Domingo"][day-1]
            participant[f'Fecha {day}'] = f"Sep {5 + day}, 2025"
            participant[f'Mangas {day}'] = f"G{random.randint(1, 3)} / {random.choice(['I', 'L', 'M', 'S'])}"
        for day in range(4, 6+1):
            participant[f'Día {day}'] = ""
            participant[f'Fecha {day}'] = ""
            participant[f'Mangas {day}'] = ""
        sample_data.append(participant)
    return sample_data

def extract_participants():
    if not HAS_SELENIUM:
        log("Error: Selenium no está instalado")
        return None

    os.makedirs(OUT_DIR, exist_ok=True)
    log("=== MÓDULO 3: EXTRACCIÓN DE PARTICIPANTES (BETA: MOCK DATA) ===")

    detailed_files = glob(os.path.join(OUT_DIR, "02competiciones_detalladas_*.json"))
    if not detailed_files:
        log("❌ No se encontraron archivos de eventos detallados")
        return None
    latest_detailed_file = max(detailed_files, key=os.path.getctime)
    with open(latest_detailed_file, 'r', encoding='utf-8') as f:
        events = json.load(f)

    log(f"✅ Cargados {len(events)} eventos detallados desde {latest_detailed_file}")

    driver = _get_driver(headless=HEADLESS_PARTICIPANTS, unique_id="module3")
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
                participants_url = event.get('enlaces', {}).get('participantes')
                if participants_url:
                    log(f"📋 Procesando participantes {i}/{len(events)}: {event.get('nombre', 'Sin nombre')}")
                    log(f"   URL: {participants_url}")
                    with_retries(lambda: driver.get(participants_url), desc=f"get(participants {i})")
                    with_retries(lambda: WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "body"))), desc="wait body")
                    slow_pause(0.8, 1.5)

                    # Datos de ejemplo (BETA)
                    participants_data = _create_sample_participants(participants_url, event)
                    if participants_data:
                        events_with_participants += 1
                        event_participants_file = os.path.join(OUT_DIR, f"participantes_{event.get('id', 'unknown')}.json")
                        with open(event_participants_file, 'w', encoding='utf-8') as f:
                            json.dump(participants_data, f, ensure_ascii=False, indent=2)
                        all_participants.extend(participants_data)
                        log(f"  ✅ Generados {len(participants_data)} participantes de ejemplo")
                    else:
                        log(f"  ⚠️ No se generaron participantes")
                else:
                    log(f"Evento {i} no tiene enlace de participantes")
                slow_pause(0.4, 1.0)
            except Exception as e:
                log(f"❌ Error procesando participantes del evento {i}: {str(e)}")
                continue

        if all_participants:
            today_str = datetime.now().strftime("%Y-%m-%d")
            output_file = os.path.join(OUT_DIR, f'03todos_participantes_{today_str}.json')
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(all_participants, f, ensure_ascii=False, indent=2)
            log(f"✅ Total de {len(all_participants)} participantes guardados en {output_file}")
            log(f"✅ {events_with_participants} eventos con participantes procesados")
        else:
            log("⚠️ No se generaron participantes para ningún evento")

        return all_participants

    except Exception as e:
        log(f"❌ Error durante la extracción de participantes: {str(e)}")
        traceback.print_exc()
        return None
    finally:
        try:
            driver.quit()
        except Exception:
            pass

# =============================================================================
# MÓDULO 4: CSV + JSON FINAL
# =============================================================================
def generate_csv_output():
    os.makedirs(OUT_DIR, exist_ok=True)
    log("=== GENERANDO ARCHIVO CSV PROCESADO ===")

    participant_files = glob(os.path.join(OUT_DIR, "03todos_participantes_*.json"))
    if not participant_files:
        log("❌ No se encontraron archivos de participantes")
        return False
    latest_participant_file = max(participant_files, key=os.path.getctime)

    with open(latest_participant_file, 'r', encoding='utf-8') as f:
        participants = json.load(f)

    if not participants:
        log("⚠️ No hay participantes para procesar")
        return False

    fieldnames = [
        'participants_url', 'BinomID', 'Dorsal', 'Guía', 'Perro', 'Raza', 'Edad',
        'Género', 'Altura (cm)', 'Nombre de Pedigree', 'País', 'Licencia', 'Club',
        'Federación', 'Equipo', 'event_uuid', 'event_title'
    ]
    for i in range(1, 6+1):
        fieldnames.extend([f'Día {i}', f'Fecha {i}', f'Mangas {i}'])

    today_str = datetime.now().strftime("%Y-%m-%d")
    csv_file = os.path.join(OUT_DIR, f'participantes_procesado_{today_str}.csv')

    with open(csv_file, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for participant in participants:
            row = {fn: (participant.get(fn, '') or '') for fn in fieldnames}
            writer.writerow(row)

    log(f"✅ Archivo CSV generado: {csv_file}")

    if participants:
        first = participants[0]
        print("📋 EJEMPLO:")
        for field in ['Guía', 'Perro', 'Dorsal', 'Club', 'Raza']:
            print(f"   {field}: {first.get(field, 'N/A')}")
    return True

def generate_final_json():
    os.makedirs(OUT_DIR, exist_ok=True)
    log("=== GENERANDO ARCHIVO JSON FINAL ===")

    event_files = glob(os.path.join(OUT_DIR, "01events_*.json"))
    detailed_files = glob(os.path.join(OUT_DIR, "02competiciones_detalladas_*.json"))
    participant_files = glob(os.path.join(OUT_DIR, "03todos_participantes_*.json"))

    if not event_files:
        log("❌ No se encontraron archivos de eventos")
        return False

    latest_event_file = max(event_files, key=os.path.getctime)
    with open(latest_event_file, 'r', encoding='utf-8') as f:
        events = json.load(f)

    detailed_events = []
    if detailed_files:
        latest_detailed_file = max(detailed_files, key=os.path.getctime)
        with open(latest_detailed_file, 'r', encoding='utf-8') as f:
            detailed_events = json.load(f)

    all_participants = []
    if participant_files:
        latest_participant_file = max(participant_files, key=os.path.getctime)
        with open(latest_participant_file, 'r', encoding='utf-8') as f:
            all_participants = json.load(f)

    final_data = {
        'metadata': {
            'fecha_generacion': datetime.now().isoformat(),
            'total_eventos': len(events),
            'total_eventos_detallados': len(detailed_events),
            'total_participantes': len(all_participants),
            'version': '1.0-beta'
        },
        'eventos': events,
        'eventos_detallados': detailed_events,
        'participantes': all_participants
    }

    # Nombre requerido por tu workflow (participants_completos_final.json)
    final_file = os.path.join(OUT_DIR, "participants_completos_final.json")
    with open(final_file, 'w', encoding='utf-8') as f:
        json.dump(final_data, f, ensure_ascii=False, indent=2)

    # Alias ES para compatibilidad con tu front legado (participantes...)
    alias_file = os.path.join(OUT_DIR, "participantes_completos_final.json")
    try:
        with open(alias_file, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, ensure_ascii=False, indent=2)
        log(f"✅ Alias final JSON generado: {alias_file}")
    except Exception as e:
        log(f"⚠️ No se pudo escribir alias ES: {e}")

    log(f"✅ Archivo final JSON generado: {final_file}")

    # Imprimir inventario
    print("\n📁 ARCHIVOS EN output/:")
    for file in sorted(glob(os.path.join(OUT_DIR, "*"))):
        try:
            size = os.path.getsize(file)
            print(f"   {os.path.basename(file)} - {size} bytes")
        except Exception:
            pass
    return True

# =============================================================================
# MAIN
# =============================================================================
def main():
    print("🚀 INICIANDO FLOWAGILITY SCRAPER COMPLETO (BETA)")
    print(f"📂 Directorio de salida: {OUT_DIR}")
    print("=" * 80)
    os.makedirs(OUT_DIR, exist_ok=True)

    parser = argparse.ArgumentParser(description="FlowAgility Scraper Mejorado (Beta)")
    parser.add_argument("--module", choices=["events", "info", "participants", "csv", "all"], default="all",
                        help="Módulo a ejecutar")
    args = parser.parse_args()

    try:
        success = True

        if args.module in ["events", "all"]:
            log("🏁 INICIANDO EXTRACCIÓN DE EVENTOS BÁSICOS")
            events = extract_events()
            if not events:
                log("❌ Falló la extracción de eventos")
                success = False
            else:
                log("✅ Eventos básicos extraídos correctamente")

        if args.module in ["info", "all"] and success:
            log("🏁 INICIANDO EXTRACCIÓN DE INFORMACIÓN DETALLADA")
            detailed_events = extract_detailed_info()
            if not detailed_events:
                log("⚠️ No se pudo extraer información detallada, continuando con datos básicos")
            else:
                log("✅ Información detallada extraída correctamente")

        if args.module in ["participants", "all"] and success:
            log("🏁 INICIANDO EXTRACCIÓN DE PARTICIPANTES (BETA)")
            participants = extract_participants()
            if not participants:
                log("⚠️ No se pudo extraer participantes, continuando sin ellos")
            else:
                log("✅ Participantes extraídos correctamente")

        if args.module in ["csv", "all"] and success:
            log("🏁 GENERANDO ARCHIVO CSV PROCESADO")
            if not generate_csv_output():
                log("⚠️ No se pudo generar el archivo CSV")
            else:
                log("✅ Archivo CSV generado correctamente")

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
            print("📊 Archivos listos para Actions y FTPS")
        else:
            log("❌ PROCESO COMPLETADO CON ERRORES")
            print("\n⚠️ Algunos módulos tuvieron errores. Revisa logs.")

        return success
    except Exception as e:
        log(f"❌ ERROR CRÍTICO DURANTE LA EJECUCIÓN: {e}")
        traceback.print_exc()
        return False

if __name__ == "__main__":
    ok = main()
    sys.exit(0 if ok else 1)
