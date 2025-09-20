#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
FLOWAGILITY SCRAPER - EXTRACCIÓN DE EVENTOS E INFORMACIÓN DETALLADA
FLOWAGILITY SCRAPER - SISTEMA AUTOMATIZADO DE EXTRACCIÓN DE DATOS

🌐 DESCRIPCIÓN DEL PROCESO:
──────────────────────────────────────────────────────────────────────────────
Este sistema realiza la extracción automatizada de información de competiciones 
de agility desde la plataforma FlowAgility.com. El proceso consta de dos etapas
principales que se ejecutan secuencialmente:

1. 📋 MÓDULO 1: EXTRACCIÓN DE EVENTOS BÁSICOS
   • Autenticación automática en FlowAgility.com
   • Navegación a la página principal de eventos
   • Scroll completo para cargar todos los eventos visibles
   • Extracción estructurada de información básica:
     - ID único del evento
     - Nombre de la competición
     - Fechas de celebración
     - Organización (FCI/RSCE, RFEC, etc.)
     - Club organizador
     - Lugar/ubicación
     - Enlaces a información y participantes
     - Bandera del país

2. 📊 MÓDULO 2: INFORMACIÓN DETALLADA + PARTICIPANTES
   • Acceso individual a cada página de información de evento
   • Extracción de datos adicionales y mejora de información
   • Acceso a páginas de listas de participantes
   • Conteo preciso del número de participantes por evento
   • Preservación de datos originales con enriquecimiento

🎯 OBJETIVOS PRINCIPALES:
• Extraer información completa y estructurada de todas las competiciones
• Obtener el número real de participantes por evento
• Generar archivos JSON consistentes para procesos downstream
• Mantener compatibilidad con sistemas existentes

📁 ARCHIVOS GENERADOS:
• 01events_YYYY-MM-DD.json       → Eventos básicos (con fecha)
• 01events.json                  → Eventos básicos (siempre actual)
• 02info_YYYY-MM-DD.json         → Info detallada + participantes (con fecha)
• 02info.json                    → Info detallada (siempre actual)

⚙️  CONFIGURACIÓN:
• Credenciales mediante variables de entorno (.env)
• Modo headless/visible configurable
• Pausas aleatorias entre solicitudes
• Timeouts ajustables para diferentes conexiones

🛡️  CARACTERÍSTICAS TÉCNICAS:
• Manejo robusto de errores y reintentos
• Detección y aceptación automática de cookies
• Scroll completo para carga de contenido dinámico
• Preservación de datos originales en fallos
• Logging detallado de cada etapa del proceso

🚦 FLUJO DE EJECUCIÓN:
1. Inicio de sesión automático
2. Aceptación de cookies (si es necesario)
3. Carga completa de página de eventos
4. Extracción y parsing de HTML
5. Procesamiento individual por evento
6. Generación de archivos de salida
7. Resumen estadístico final

📊 ESTADÍSTICAS CALCULADAS:
• Total de eventos procesados
• Eventos con información detallada
• Eventos con participantes identificados
• Número total de participantes
• Ranking de eventos por participación

⚠️  NOTAS IMPORTANTES:
• Requiere ChromeDriver compatible
• Necesita credenciales válidas de FlowAgility
• Las pausas evitan bloqueos por rate limiting
• Los archivos se sobrescriben en cada ejecución

🔄 USO:
python flowagility_scraper.py [--module events|info|all]
"""

import os
import sys
import json
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

# Third-party imports
try:
    from bs4 import BeautifulSoup
    from dotenv import load_dotenv
except ImportError as e:
    print(f"❌ Error importando dependencias: {e}")
    sys.exit(1)

# Selenium imports
try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, NoSuchElementException
    from selenium.webdriver.chrome.service import Service
    HAS_SELENIUM = True
except ImportError as e:
    print(f"❌ Error importando Selenium: {e}")
    HAS_SELENIUM = False

try:
    from webdriver_manager.chrome import ChromeDriverManager
    HAS_WEBDRIVER_MANAGER = True
except ImportError:
    HAS_WEBDRIVER_MANAGER = False

# ============================== CONFIGURACIÓN GLOBAL ==============================

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
FLOW_EMAIL = os.getenv("FLOW_EMAIL", "rosaperez1134@yahoo.com")
FLOW_PASS = os.getenv("FLOW_PASS", "Seattle1")

# Flags/tunables
HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
INCOGNITO = os.getenv("INCOGNITO", "true").lower() == "true"
MAX_SCROLLS = int(os.getenv("MAX_SCROLLS", "15"))
SCROLL_WAIT_S = float(os.getenv("SCROLL_WAIT_S", "3.0"))
OUT_DIR = os.getenv("OUT_DIR", "./output")

print(f"📋 Configuración: HEADLESS={HEADLESS}, OUT_DIR={OUT_DIR}")

# ============================== UTILIDADES GENERALES ==============================

def log(message):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")

def slow_pause(min_s=1, max_s=2):
    time.sleep(random.uniform(min_s, max_s))

def _clean(s: str) -> str:
    if not s:
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKC", s)
    s = re.sub(r"[ \t]+", " ", s)
    return s.strip(" \t\r\n-•*·:;")

def _clean_output_directory():
    try:
        files_to_keep = ['config.json', 'settings.ini']
        for file in os.listdir(OUT_DIR):
            if file not in files_to_keep:
                file_path = os.path.join(OUT_DIR, file)
                if os.path.isfile(file_path):
                    os.remove(file_path)
                    log(f"🧹 Eliminado archivo antiguo: {file}")
        log("✅ Directorio de output limpiado")
    except Exception as e:
        log(f"⚠️  Error limpiando directorio: {e}")

# ============================== FUNCIONES DE NAVEGACIÓN ==============================

def _get_driver(headless=True):
    """Crea y configura el driver de Selenium (ajustado para CI)."""
    if not HAS_SELENIUM:
        raise ImportError("Selenium no está instalado")
    
    opts = Options()
    # Headless e incógnito
    if headless:
        opts.add_argument("--headless=new")
    if INCOGNITO:
        opts.add_argument("--incognito")

    # Estables para CI
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-infobars")
    opts.add_argument("--disable-browser-side-navigation")
    opts.add_argument("--disable-features=VizDisplayCompositor")
    opts.add_argument("--disable-setuid-sandbox")
    opts.add_argument("--ignore-certificate-errors")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)

    try:
        # Usa chromedriver del sistema o webdriver_manager como fallback
        chromedriver_path = None
        for path in ["/usr/local/bin/chromedriver", "/usr/bin/chromedriver", "/snap/bin/chromedriver"]:
            if os.path.exists(path):
                chromedriver_path = path
                break
        if not chromedriver_path and HAS_WEBDRIVER_MANAGER:
            chromedriver_path = ChromeDriverManager().install()

        if chromedriver_path:
            service = Service(executable_path=chromedriver_path)
            driver = webdriver.Chrome(service=service, options=opts)
        else:
            driver = webdriver.Chrome(options=opts)

        # Anti-detección básica
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        # Timeouts (ligeramente amplios para LiveView)
        driver.set_page_load_timeout(120)
        driver.implicitly_wait(45)
        return driver
        
    except Exception as e:
        log(f"Error creando driver: {e}")
        traceback.print_exc()
        return None

def _login(driver):
    """Inicia sesión en FlowAgility."""
    if not driver:
        return False
        
    log("Iniciando login...")
    try:
        driver.get(f"{BASE}/user/login")
        WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        slow_pause(3, 5)

        # Si ya estamos dentro
        if "/user/login" not in driver.current_url:
            log("Ya autenticado (redirección detectada)")
            return True
        
        # Selectores comunes
        email_selectors = [
            (By.NAME, "user[email]"),
            (By.ID, "user_email"),
            (By.CSS_SELECTOR, "input[type='email']"),
            (By.XPATH, "//input[contains(@name, 'email')]")
        ]
        password_selectors = [
            (By.NAME, "user[password]"),
            (By.ID, "user_password"),
            (By.CSS_SELECTOR, "input[type='password']")
        ]
        submit_selectors = [
            (By.CSS_SELECTOR, 'button[type="submit"]'),
            (By.XPATH, "//button[contains(text(), 'Sign') or contains(text(), 'Log') or contains(text(), 'Iniciar')]")
        ]
        
        email_field = None
        for selector in email_selectors:
            try:
                email_field = WebDriverWait(driver, 10).until(EC.element_to_be_clickable(selector))
                break
            except:
                continue
        if not email_field:
            log("❌ No se pudo encontrar campo email")
            return False
        
        password_field = None
        for selector in password_selectors:
            try:
                password_field = driver.find_element(*selector)
                break
            except:
                continue
        if not password_field:
            log("❌ No se pudo encontrar campo password")
            return False
        
        submit_button = None
        for selector in submit_selectors:
            try:
                submit_button = driver.find_element(*selector)
                break
            except:
                continue
        if not submit_button:
            log("❌ No se pudo encontrar botón submit")
            return False
        
        email_field.clear(); email_field.send_keys(FLOW_EMAIL); slow_pause(1, 2)
        password_field.clear(); password_field.send_keys(FLOW_PASS); slow_pause(1, 2)
        submit_button.click()
        
        try:
            WebDriverWait(driver, 45).until(
                lambda d: "/user/login" not in d.current_url or "dashboard" in d.current_url or "zone" in d.current_url
            )
            slow_pause(5, 8)
            if "/user/login" in driver.current_url:
                log("❌ Login falló - aún en página de login")
                try:
                    error_elements = driver.find_elements(By.CSS_SELECTOR, ".error, .alert, .text-red-600")
                    for error in error_elements:
                        log(f"Mensaje error: {error.text}")
                except:
                    pass
                return False
            else:
                log(f"✅ Login exitoso - Redirigido a: {driver.current_url}")
                return True
        except TimeoutException:
            log("❌ Timeout esperando redirección de login")
            try:
                driver.save_screenshot("/tmp/login_timeout.png")
                log("📸 Screenshot guardado en /tmp/login_timeout.png")
            except:
                pass
            return False
        
    except Exception as e:
        log(f"❌ Error en login: {e}")
        log(f"Traceback: {traceback.format_exc()}")
        return False

def _accept_cookies(driver):
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
        # Fallback por JS
        driver.execute_script("""
            const buttons = document.querySelectorAll('button');
            for (const btn of buttons) {
                if (/aceptar|accept|consent|agree/i.test(btn.textContent)) { btn.click(); break; }
            }
        """)
        slow_pause(0.5, 1)
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

# ============================== MÓDULO 1: EXTRACCIÓN DE EVENTOS ==============================

def extract_events():
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
        WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        _accept_cookies(driver)

        # Scroll completo
        log("Cargando todos los eventos...")
        _full_scroll(driver)
        slow_pause(2, 3)
        
        # Parse
        page_html = driver.page_source
        soup = BeautifulSoup(page_html, 'html.parser')
        event_containers = soup.find_all('div', class_='group mb-6')
        log(f"Encontrados {len(event_containers)} contenedores de eventos")
        
        events = []
        for i, container in enumerate(event_containers, 1):
            try:
                event_data = {}
                event_id = container.get('id', '')
                if event_id:
                    event_data['id'] = event_id.replace('event-card-', '')
                
                name_elem = container.find('div', class_='font-caption text-lg text-black truncate -mt-1')
                if name_elem:
                    event_data['nombre'] = _clean(name_elem.get_text())
                
                date_elem = container.find('div', class_='text-xs')
                if date_elem:
                    event_data['fechas'] = _clean(date_elem.get_text())
                
                org_elems = container.find_all('div', class_='text-xs')
                if len(org_elems) > 1:
                    event_data['organizacion'] = _clean(org_elems[1].get_text())
                
                club_elem = container.find('div', class_='text-xs mb-0.5 mt-0.5')
                if club_elem:
                    event_data['club'] = _clean(club_elem.get_text())
                else:
                    for div in container.find_all('div', class_='text-xs'):
                        text = _clean(div.get_text())
                        if text and not any(x in text for x in ['/', 'Spain', 'España']):
                            event_data['club'] = text
                            break
                
                location_divs = container.find_all('div', class_='text-xs')
                for div in location_divs:
                    text = _clean(div.get_text())
                    if '/' in text and any(x in text for x in ['Spain', 'España', 'Madrid', 'Barcelona']):
                        event_data['lugar'] = text
                        break
                if 'lugar' not in event_data:
                    for div in location_divs:
                        text = _clean(div.get_text())
                        if '/' in text and len(text) < 100:
                            event_data['lugar'] = text
                            break
                
                event_data['enlaces'] = {}
                info_link = container.find('a', href=lambda x: x and '/info/' in x)
                if info_link:
                    event_data['enlaces']['info'] = urljoin(BASE, info_link['href'])
                
                participant_links = container.find_all('a', href=lambda x: x and any(term in x for term in ['/participants', '/participantes']))
                for link in participant_links:
                    href = link.get('href', '')
                    if '/participants_list' in href or '/participantes' in href:
                        event_data['enlaces']['participantes'] = urljoin(BASE, href)
                        break
                if 'participantes' not in event_data['enlaces'] and 'id' in event_data:
                    event_data['enlaces']['participantes'] = f"{BASE}/zone/events/{event_data['id']}/participants_list"
                
                flag_elem = container.find('div', class_='text-md')
                if flag_elem:
                    event_data['pais_bandera'] = _clean(flag_elem.get_text())
                else:
                    event_data['pais_bandera'] = '🇪🇸'
                
                events.append(event_data)
                log(f"✅ Evento {i} procesado: {event_data.get('nombre', 'Sin nombre')}")
            except Exception as e:
                log(f"❌ Error procesando evento {i}: {str(e)}")
                continue
        
        # Guardar
        today_str = datetime.now().strftime("%Y-%m-%d")
        output_file = os.path.join(OUT_DIR, f'01events_{today_str}.json')
        os.makedirs(OUT_DIR, exist_ok=True)
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(events, f, ensure_ascii=False, indent=2)
        
        latest_file = os.path.join(OUT_DIR, '01events.json')
        with open(latest_file, 'w', encoding='utf-8') as f:
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

# ============================== MÓDULO 2: INFORMACIÓN DETALLADA ==============================

def _extract_description(soup, max_length=800):
    try:
        description_selectors = [
            'div[class*="description"]',
            'div[class*="descripcion"]',
            'div[class*="info"]',
            'div[class*="content"]',
            'div[class*="text"]',
            'div[class*="body"]'
        ]
        description_text = ""
        for selector in description_selectors:
            try:
                elem = soup.select_one(selector)
                if elem:
                    text = _clean(elem.get_text())
                    if text and len(text) > 50:
                        description_text = text
                        break
            except:
                continue
        if not description_text:
            all_text = soup.get_text()
            lines = all_text.split('\n')
            meaningful_lines = [line.strip() for line in lines if len(line.strip()) > 50]
            if meaningful_lines:
                description_text = ' '.join(meaningful_lines[:3])
        if description_text and len(description_text) > max_length:
            description_text = description_text[:max_length] + "... [texto truncado]"
        return description_text
    except Exception as e:
        log(f"Error extrayendo descripción: {e}")
        return ""

def _count_participants_correctly(soup):
    """(Legado) Conteo sobre HTML estático – mantenido como fallback de emergencia."""
    try:
        detail_buttons = soup.find_all(attrs={'phx-click': lambda x: x and 'booking_details' in x})
        if detail_buttons:
            return len(detail_buttons)
        booking_elements = soup.find_all(attrs={'phx-value-booking_id': True})
        if booking_elements:
            return len(booking_elements)
        participant_classes = [
            '[class*="participant"]',
            '[class*="competitor"]',
            '[class*="booking"]',
            '[class*="inscrito"]'
        ]
        for class_selector in participant_classes:
            elements = soup.select(class_selector)
            if elements and 1 <= len(elements) <= 200:
                return len(elements)
        tables = soup.find_all('table')
        for table in tables:
            rows = table.find_all('tr')
            if len(rows) > 1:
                first_row_text = rows[0].get_text().lower()
                if any(keyword in first_row_text for keyword in ['dorsal', 'guía', 'perro', 'nombre']):
                    return len(rows) - 1
                return len(rows)
        page_text = soup.get_text().lower()
        count_patterns = [
            r'(\d+)\s*participantes?',
            r'(\d+)\s*inscritos?',
            r'(\d+)\s*competidores?',
            r'total:\s*(\d+)',
            r'inscripciones:\s*(\d+)'
        ]
        for pattern in count_patterns:
            match = re.search(pattern, page_text)
            if match:
                count = int(match.group(1))
                if 1 <= count <= 200:
                    return count
        if any(phrase in page_text for phrase in ['no hay participantes', 'sin participantes', 'no participants', 'empty', '0 participantes']):
            return 0
        return 0
    except Exception as e:
        log(f"Error contando participantes: {e}")
        return 0

# ======== NUEVO: Helpers LiveView (DOM vivo) ========

def _wait_liveview_ready(driver, hard_timeout=25):
    """Espera a que LiveView haya hidratado el DOM (html.phx-connected o [data-phx-root])."""
    try:
        WebDriverWait(driver, hard_timeout).until(
            lambda d: (
                "phx-connected" in d.find_element(By.TAG_NAME, "html").get_attribute("class")
            ) or d.find_elements(By.CSS_SELECTOR, "[data-phx-root]")
        )
        time.sleep(1.0)  # pequeño extra para que pinte listas
        return True
    except Exception:
        return False

def _count_participants_liveview(driver, soft_scroll=True) -> int:
    """
    Cuenta participantes DIRECTAMENTE en el DOM ya renderizado por LiveView.
    - Busca booking cards / filas con booking_id.
    - Deduplica por booking_id.
    - Fallback a tablas y a chips/resúmenes si es necesario.
    """
    _wait_liveview_ready(driver, hard_timeout=25)

    # Pequeños scrolls para forzar lazy render
    if soft_scroll:
        try:
            for _ in range(2):
                driver.execute_script("window.scrollBy(0, document.body.scrollHeight/2);")
                time.sleep(0.6)
        except Exception:
            pass

    booking_selectors = [
        "[phx-value-booking_id]",
        "[data-phx-value-booking_id]",
        '[phx-click="booking_details"]',
        "[data-phx-click*=booking_details]",
        '[id^="booking-"]',
    ]

    booking_ids = set()
    for sel in booking_selectors:
        try:
            nodes = driver.find_elements(By.CSS_SELECTOR, sel)
            for el in nodes:
                bid = (
                    el.get_attribute("phx-value-booking_id")
                    or el.get_attribute("data-phx-value-booking_id")
                    or el.get_attribute("id")
                    or ""
                )
                m = re.search(r"(\d{3,})", bid or "")
                if m:
                    booking_ids.add(m.group(1))
        except Exception:
            continue

    if booking_ids:
        return len(booking_ids)

    # Fallback: tablas con pinta de listado
    try:
        tables = driver.find_elements(By.TAG_NAME, "table")
        for t in tables:
            rows = t.find_elements(By.TAG_NAME, "tr")
            if len(rows) > 1:
                header_text = rows[0].text.lower()
                if any(k in header_text for k in ["dorsal", "guía", "guia", "perro", "nombre"]):
                    return max(0, len(rows) - 1)
                if 5 <= len(rows) <= 500:
                    return len(rows) - 1
    except Exception:
        pass

    # Fallback final: chips/resúmenes en texto
    try:
        body_txt = driver.find_element(By.TAG_NAME, "body").text.lower()
        for pat in [r'(\d+)\s*participantes?', r'(\d+)\s*inscritos?', r'(\d+)\s*competidores?', r'total:\s*(\d+)']:
            m = re.search(pat, body_txt)
            if m:
                n = int(m.group(1))
                if 0 <= n <= 2000:
                    return n
    except Exception:
        pass

    return 0

def extract_detailed_info():
    """Extraer información detallada de cada evento incluyendo número de participantes."""
    if not HAS_SELENIUM:
        log("Error: Selenium no está instalado")
        return None
    
    log("=== MÓDULO 2: EXTRACCIÓN DE INFORMACIÓN DETALLADA ===")
    
    # Archivo de eventos más reciente
    event_files = glob(os.path.join(OUT_DIR, "01events_*.json"))
    if not event_files:
        log("❌ No se encontraron archivos de eventos")
        return None
    
    latest_event_file = max(event_files, key=os.path.getctime)
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
                preserved_fields = ['id', 'nombre', 'fechas', 'organizacion', 'club', 'lugar', 'enlaces', 'pais_bandera']
                detailed_event = {field: event.get(field, '') for field in preserved_fields}
                
                # Inicialización
                detailed_event['numero_participantes'] = 0
                detailed_event['participantes_info'] = 'No disponible'
                
                # ===== INFO DEL EVENTO (página /info) =====
                info_processed = False
                if 'enlaces' in event and 'info' in event['enlaces']:
                    info_url = event['enlaces']['info']
                    log(f"Procesando evento {i}/{len(events)}: {event.get('nombre', 'Sin nombre')}")
                    try:
                        driver.get(info_url)
                        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                        slow_pause(2, 3)
                        page_html = driver.page_source
                        soup = BeautifulSoup(page_html, 'html.parser')

                        additional_info = {}
                        if not detailed_event.get('club') or detailed_event.get('club') in ['N/D', '']:
                            club_elems = soup.find_all(lambda tag: any(word in tag.get_text().lower() for word in ['club', 'organizador', 'organizer']))
                            for elem in club_elems:
                                text = _clean(elem.get_text())
                                if text and len(text) < 100:
                                    detailed_event['club'] = text
                                    break
                        if not detailed_event.get('lugar') or detailed_event.get('lugar') in ['N/D', '']:
                            location_elems = soup.find_all(lambda tag: any(word in tag.get_text().lower() for word in ['lugar', 'ubicacion', 'location', 'place']))
                            for elem in location_elems:
                                text = _clean(elem.get_text())
                                if text and ('/' in text or any(x in text for x in ['Spain', 'España'])):
                                    detailed_event['lugar'] = text
                                    break
                        title_elem = soup.find('h1')
                        if title_elem:
                            additional_info['titulo_completo'] = _clean(title_elem.get_text())
                        description_text = _extract_description(soup, max_length=800)
                        if description_text:
                            additional_info['descripcion'] = description_text
                        detailed_event['informacion_adicional'] = additional_info
                        info_processed = True
                    except Exception as e:
                        log(f"  ❌ Error procesando información: {e}")
                
                # ===== PARTICIPANTES (LiveView DOM VIVO) =====
                if 'enlaces' in event and 'participantes' in event['enlaces']:
                    participants_url = event['enlaces']['participantes']
                    log(f"  Extrayendo número de participantes de: {participants_url}")
                    try:
                        # 1) Navegar y esperar hidratación LiveView
                        driver.get(participants_url)
                        WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                        _wait_liveview_ready(driver, hard_timeout=25)

                        # 2) Si la sesión caducó, relogin y reintento
                        if "/user/login" in (driver.current_url or ""):
                            log("  ℹ️ Sesión caducada; reintentando login…")
                            if _login(driver):
                                driver.get(participants_url)
                                WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                                _wait_liveview_ready(driver, hard_timeout=25)

                        # 3) Conteo robusto en DOM vivo
                        num_participants = _count_participants_liveview(driver)

                        if num_participants > 0:
                            detailed_event['numero_participantes'] = num_participants
                            detailed_event['participantes_info'] = f"{num_participants} participantes"
                            log(f"  ✅ Encontrados {num_participants} participantes")
                        else:
                            detailed_event['numero_participantes'] = 0
                            detailed_event['participantes_info'] = 'Sin participantes'
                            log("  ⚠️  No se encontraron participantes")
                            
                    except Exception as e:
                        log(f"  ❌ Error accediendo a participantes: {e}")
                        detailed_event['numero_participantes'] = 0
                        detailed_event['participantes_info'] = f"Error: {str(e)}"
                
                # Finalizar evento
                detailed_event['timestamp_extraccion'] = datetime.now().isoformat()
                detailed_event['procesado_info'] = info_processed
                detailed_events.append(detailed_event)
                slow_pause(1, 2)
                
            except Exception as e:
                log(f"❌ Error procesando evento {i}: {str(e)}")
                event['timestamp_extraccion'] = datetime.now().isoformat()
                event['procesado_info'] = False
                event['numero_participantes'] = 0
                event['participantes_info'] = f"Error: {str(e)}"
                detailed_events.append(event)
                continue
        
        # Guardar información detallada
        today_str = datetime.now().strftime("%Y-%m-%d")
        output_file = os.path.join(OUT_DIR, f'02info_{today_str}.json')
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(detailed_events, f, ensure_ascii=False, indent=2)
        
        latest_file = os.path.join(OUT_DIR, '02info.json')
        with open(latest_file, 'w', encoding='utf-8') as f:
            json.dump(detailed_events, f, ensure_ascii=False, indent=2)
        
        log(f"✅ Información detallada guardada en {output_file}")
        
        # Resumen
        total_participants = sum(event.get('numero_participantes', 0) for event in detailed_events)
        events_with_participants = sum(1 for event in detailed_events if event.get('numero_participantes', 0) > 0)
        events_with_info = sum(1 for event in detailed_events if event.get('procesado_info', False))
        
        print(f"\n{'='*80}")
        print("RESUMEN FINAL:")
        print(f"{'='*80}")
        print(f"Eventos procesados: {len(detailed_events)}")
        print(f"Eventos con información detallada: {events_with_info}")
        print(f"Eventos con participantes: {events_with_participants}")
        print(f"Total participantes: {total_participants}")
        if events_with_participants > 0:
            print(f"\n📊 Eventos con más participantes:")
            sorted_events = sorted(
                [e for e in detailed_events if e.get('numero_participantes', 0) > 0],
                key=lambda x: x.get('numero_participantes', 0), reverse=True
            )
            for event in sorted_events[:5]:
                print(f"  {event.get('nombre', 'N/A')}: {event.get('numero_participantes')} participantes")
        print(f"\n{'='*80}\n")
        
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

# ============================== FUNCIÓN PRINCIPAL ==============================

def main():
    print("🚀 INICIANDO FLOWAGILITY SCRAPER")
    print("📋 Este proceso realizará la extracción de eventos e información detallada")
    print(f"📂 Directorio de salida: {OUT_DIR}")
    print("=" * 80)
    
    # Crear directorio de output
    os.makedirs(OUT_DIR, exist_ok=True)
    
    # Limpiar archivos antiguos
    _clean_output_directory()
    
    parser = argparse.ArgumentParser(description="FlowAgility Scraper - Eventos e Info Detallada")
    parser.add_argument("--module", choices=["events", "info", "all"], default="all", help="Módulo a ejecutar")
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
                log("⚠️  No se pudo extraer información detallada")
            else:
                log("✅ Información detallada extraída correctamente")
        
        if success:
            log("🎉 PROCESO COMPLETADO EXITOSAMENTE")
            print(f"\n📁 ARCHIVOS GENERADOS EN {OUT_DIR}:")
            output_files = glob(os.path.join(OUT_DIR, "*"))
            for file in sorted(output_files):
                if os.path.isfile(file):
                    size = os.path.getsize(file)
                    print(f"   {os.path.basename(file)} - {size} bytes")
        else:
            log("❌ PROCESO COMPLETADO CON ERRORES")
        
        return success
        
    except Exception as e:
        log(f"❌ ERROR CRÍTICO DURANTE LA EJECUCIÓN: {e}")
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
