import os
import time
import re
import requests
import json
import urllib.parse
from datetime import datetime
from bs4 import BeautifulSoup
from woocommerce import API

# ============================================================
#  PHONEHOUSE SCRAPER (SCROLL + MASK + FULL PRODUCT FETCH)
# ============================================================
# Cambios clave (v2):
#  - Selenium scroll para cargar el listado completo.
#  - Descubrimiento de URLs más agresivo:
#       * <a href>, atributos data-*, y búsqueda regex en el HTML completo.
#  - Para cada URL de producto detectada, se hace fetch de la ficha (requests)
#    para extraer precio/imagen/título y (si falta) RAM/capacidad desde specs.
#  - Logs enmascarados: no imprime querystrings/afiliados.
# ============================================================

# --- CONFIG ---
DEFAULT_START_URL = "https://www.phonehouse.es/moviles-y-telefonia/moviles/todos-los-smartphones.html"
EXPECTED_PATH = '/moviles-y-telefonia/moviles/todos-los-smartphones.html'
LIST_ID = '31'
LIST_NAME = 'Todos los Móviles y Smartphones'
START_URL = os.getenv('SOURCE_URL_PHONEHOUSE') or 'https://www.phonehouse.es/moviles-y-telefonia/moviles/todos-los-smartphones.html'
EXPECTED_PATH = '/moviles-y-telefonia/moviles/todos-los-smartphones.html'

# Afiliado (secret/env). Acepta "utm=..." o "?utm=..."
AFF_RAW = os.environ.get("AFF_PHONEHOUSE", "").strip()
if AFF_RAW and not AFF_RAW.startswith("?") and not AFF_RAW.startswith("&"):
    AFF_RAW = "?" + AFF_RAW

FUENTE = "Phone House"
ID_IMPORTACION = "https://www.phonehouse.es"
ENVIADO_DESDE = "España"
ENVIADO_DESDE_TG = "🇪🇸 España"
CODIGO_DESCUENTO = "OFERTA PROMO"

OBJETIVO = 72

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Referer": "https://www.phonehouse.es/",
}

# WooCommerce API
wcapi = API(
    url=os.environ["WP_URL"],
    consumer_key=os.environ["WP_KEY"],
    consumer_secret=os.environ["WP_SECRET"],
    version="wc/v3",
    timeout=60
)

# Summaries
summary_creados, summary_eliminados, summary_actualizados = [], [], []
summary_ignorados, summary_sin_stock_nuevos, summary_fallidos = [], [], []
summary_duplicados = []  # variantes/color/duplicados detectados

# --------------------------
# UTILIDADES
# --------------------------
def normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def mask_url(url: str) -> str:
    """Enmascara la URL para logs (no muestra querystring completa)."""
    try:
        u = urllib.parse.urlsplit(url)
        base = f"{u.scheme}://{u.netloc}{u.path}"
        return base + ("?***" if u.query else "")
    except Exception:
        return "***"

def abs_url(base: str, href: str) -> str:
    try:
        if href.startswith("//"):
            href = "https:" + href
        return urllib.parse.urljoin(base, href)
    except Exception:
        return href

def parse_eur_int(txt: str) -> int:
    """Convierte un texto que contiene un precio en euros a entero.

    Importante: evita falsos positivos como 'G54' cuando el texto contiene '€'.
    Regla: prioriza números pegados al símbolo € (p.ej. '149€' o '149 €').
    """
    if not txt:
        return 0

def parse_eur_all(txt: str) -> list[int]:
    """Devuelve todos los precios en euros encontrados como enteros, priorizando patrones con €."""
    if not txt:
        return []
    t = txt.replace("\xa0", " ").strip()
    vals = []
    for m in re.findall(r"(\d{1,5}(?:[\.,]\d{1,2})?)\s*€", t):
        num = m.replace(".", "").replace(",", ".")
        try:
            vals.append(int(float(num)))
        except Exception:
            pass
    return vals

    t = txt.replace("\xa0", " ").strip()

    # Prioridad 1: números inmediatamente antes de '€'
    matches = re.findall(r"(\d{1,5}(?:[\.,]\d{1,2})?)\s*€", t)
    if matches:
        num = matches[0].replace(".", "").replace(",", ".")
        try:
            return int(float(num))
        except Exception:
            return 0

    # Prioridad 2: si hay símbolo euro pero con formato raro, intenta el último número
    if "€" in t:
        nums = re.findall(r"\d{1,5}(?:[\.,]\d{1,2})?", t)
        if nums:
            num = nums[-1].replace(".", "").replace(",", ".")
            try:
                return int(float(num))
            except Exception:
                return 0

    # Fallback conservador
    m = re.search(r"(\d{1,5}(?:[\.,]\d{1,2})?)", t)
    if not m:
        return 0
    num = m.group(1).replace(".", "").replace(",", ".")
    try:
        return int(float(num))
    except Exception:
        return 0
    t = txt.replace("\xa0", " ").strip()
    # Ej: "1.239,00 €" o "999€"
    m = re.search(r"(\d{1,5}(?:[.,]\d{1,2})?)", t)
    if not m:
        return 0
    num = m.group(1).replace(".", "").replace(",", ".")
    try:
        return int(float(num))
    except Exception:
        return 0

def acortar_url(url_larga: str) -> str:
    """Acorta con is.gd (si falla, devuelve la original)."""
    try:
        url_encoded = urllib.parse.quote(url_larga, safe="")
        r = requests.get(f"https://is.gd/create.php?format=simple&url={url_encoded}", timeout=10)
        return r.text.strip() if r.status_code == 200 else url_larga
    except Exception:
        return url_larga

# --------------------------
# RAM iPhone
# --------------------------
IPHONE_RAM_MAP = [
    ("iphone 17 pro max", "12GB"),
    ("iphone 17 pro", "12GB"),
    ("iphone 17 air", "12GB"),
    ("iphone air", "12GB"),
    ("iphone 17", "8GB"),
    ("iphone 16 pro max", "8GB"),
    ("iphone 16 pro", "8GB"),
    ("iphone 16 plus", "8GB"),
    ("iphone 16e", "8GB"),
    ("iphone 16", "8GB"),
    ("iphone 15 pro max", "8GB"),
    ("iphone 15 pro", "8GB"),
    ("iphone 15 plus", "6GB"),
    ("iphone 15", "6GB"),
    ("iphone 14 pro max", "6GB"),
    ("iphone 14 pro", "6GB"),
    ("iphone 14 plus", "6GB"),
    ("iphone 14", "6GB"),
    ("iphone 13 pro max", "6GB"),
    ("iphone 13 pro", "6GB"),
    ("iphone 13 mini", "4GB"),
    ("iphone 13", "4GB"),
    ("iphone 12 pro max", "6GB"),
    ("iphone 12 pro", "6GB"),
    ("iphone 12 mini", "4GB"),
    ("iphone 12", "4GB"),
]

def ram_por_modelo_iphone(nombre: str):
    if not nombre:
        return None
    n = nombre.lower()
    if "iphone" not in n:
        return None
    for needle, ram in IPHONE_RAM_MAP:
        if needle in n:
            return ram
    return None

# --------------------------
# EXTRACCIÓN (título -> RAM/cap)
# --------------------------
def extraer_nombre_memoria_capacidad(titulo: str):
    """
    Devuelve (nombre, capacidad, memoria).
    - Capacidad restringida a tamaños típicos para evitar falsos positivos.
    - RAM restringida a tamaños típicos.
    """
    t = normalize_spaces(titulo)

    # Formatos combo CAP+RAM o RAM+CAP (con + o /)
    m_combo = re.search(
        r"(?P<cap>\d{2,4})\s*(?P<unit>TB|GB)\s*[\+/]\s*(?P<ram>\d{1,2})\s*GB(?:\s*RAM)?\b"
        r"|(?P<ram2>\d{1,2})\s*GB(?:\s*RAM)?\s*[\+/]\s*(?P<cap2>\d{2,4})\s*(?P<unit2>TB|GB)\b",
        t,
        flags=re.I
    )
    if m_combo:
        if m_combo.group("cap") and m_combo.group("ram"):
            capacidad = f"{m_combo.group('cap')}{m_combo.group('unit').upper()}"
            memoria = f"{m_combo.group('ram')}GB"
        else:
            capacidad = f"{m_combo.group('cap2')}{m_combo.group('unit2').upper()}"
            memoria = f"{m_combo.group('ram2')}GB"
        nombre = t[:m_combo.start()].strip()
        return normalize_spaces(nombre), capacidad, memoria

    # Capacidad (almacenamiento)
    m_cap = re.search(r"\b(64|128|256|512|1024)\s*GB\b|\b(1|2)\s*TB\b", t, flags=re.I)
    capacidad = ""
    if m_cap:
        if m_cap.group(1):
            capacidad = f"{m_cap.group(1)}GB"
        else:
            capacidad = f"{m_cap.group(2)}TB"

    # RAM
    m_ram = re.search(r"\b(3|4|6|8|12|16)\s*GB(?:\s*RAM)?\b", t, flags=re.I)
    memoria = f"{m_ram.group(1)}GB" if m_ram else ""

    # Nombre
    cut_positions = []
    if m_cap:
        cut_positions.append(m_cap.start())
    if m_ram:
        cut_positions.append(m_ram.start())
    cut = min(cut_positions) if cut_positions else len(t)
    nombre = t[:cut].strip()
    return normalize_spaces(nombre), capacidad, memoria

def extraer_specs_ram_cap(soup: BeautifulSoup):
    """
    Intenta extraer RAM y capacidad desde la ficha, incluso si no están en el título.
    Heurísticas:
      - Busca textos tipo "Memoria RAM", "RAM", "Almacenamiento", "Capacidad", "Memoria interna"
      - Devuelve (capacidad, ram) (pueden ser "").
    """
    text = normalize_spaces(soup.get_text(" "))

    # Capacidad
    cap = ""
    # Preferimos valores típicos de almacenamiento
    m_cap = re.search(r"\b(64|128|256|512|1024)\s*GB\b|\b(1|2)\s*TB\b", text, flags=re.I)
    if m_cap:
        cap = f"{m_cap.group(1)}GB" if m_cap.group(1) else f"{m_cap.group(2)}TB"

    # RAM
    ram = ""
    # Primero intenta cerca de "RAM"
    m_ram = re.search(r"(?:memoria\s*ram|ram)\D{0,30}\b(3|4|6|8|12|16)\s*gb\b", text, flags=re.I)
    if m_ram:
        ram = f"{m_ram.group(1)}GB"
    else:
        # fallback: cualquier RAM típica; ojo: puede confundir con almacenamiento, pero priorizamos si ya hay cap
        m_ram2 = re.search(r"\b(3|4|6|8|12|16)\s*GB\b", text, flags=re.I)
        if m_ram2:
            ram = f"{m_ram2.group(1)}GB"

    return cap, ram

# --------------------------
# SCROLL: obtener HTML renderizado
# --------------------------
def obtener_html_con_scroll(url: str) -> str | None:
    """Carga con Selenium y scrollea hasta estabilizar altura."""
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.common.exceptions import TimeoutException
    except Exception:
        return None

    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1400,900")

    driver = webdriver.Chrome(options=chrome_options)
    try:
        driver.set_page_load_timeout(40)
        driver.get(url)
        time.sleep(2)
        current = getattr(driver, 'current_url', '') or ''
        print(f"URL final (Selenium): {mask_url(current)}", flush=True)
        if EXPECTED_PATH not in current:
            print(f"⚠️  Redirección detectada (no estamos en {EXPECTED_PATH}). Reintentando...", flush=True)
            driver.get('https://www.phonehouse.es' + EXPECTED_PATH)
            time.sleep(2)
            current = getattr(driver, 'current_url', '') or ''
            print(f"URL final (Selenium) tras reintento: {mask_url(current)}", flush=True)
        if EXPECTED_PATH not in current:
            print("❌ ERROR: No se pudo acceder a 'todos-los-smartphones'. Abortando.", flush=True)
            try:
                driver.quit()
            except Exception:
                pass
            return None
        try:
            print(f"URL final (Selenium): {mask_url(driver.current_url)}", flush=True)
        except Exception:
            pass

        time.sleep(2)

        last_height = driver.execute_script("return document.body.scrollHeight")
        stable_rounds = 0
        max_rounds = 45

        print("🧭 Haciendo scroll hasta el final...", flush=True)

        for _ in range(max_rounds):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1.6)
            new_height = driver.execute_script("return document.body.scrollHeight")

            if new_height == last_height:
                stable_rounds += 1
                if stable_rounds >= 3:
                    break
            else:
                stable_rounds = 0
                last_height = new_height

        return driver.page_source
    except TimeoutException:
        return None
    except Exception:
        return None
    finally:
        try:
            driver.quit()
        except Exception:
            pass

# --------------------------
# DESCUBRIR URLs de producto
# --------------------------
PRODUCT_PATH_RE = re.compile(r"/movil/[^/]+/[^/?#]+\.html", re.I)



def obtener_productos_desde_dom(url: str, objetivo: int = 72):
    """Extrae productos del LISTADO (cards) usando Selenium DOM.

    Reglas clave:
      - Solo acepta items del listado principal (input data-item_list_id=31, name=Todos los Móviles y Smartphones)
      - Precio SOLO desde span.precio-2 / precio tachado del card (ignora 'Otras ofertas desde')
      - Nunca usa la ficha para precios (evita cuotas 4€/mes)
    """
    try:
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
    except Exception as e:
        print(f"❌ Selenium no disponible: {e}", flush=True)
        return []

    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1400,2200")
    opts.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    driver = webdriver.Chrome(options=opts)

    hoy = datetime.now().strftime("%d/%m/%Y")

    try:
        driver.get(url)
        time.sleep(2)

        current = getattr(driver, 'current_url', '') or ''
        print(f"URL final (Selenium): {mask_url(current)}", flush=True)

        # Forzar URL esperada si hay redirección
        if EXPECTED_PATH not in current:
            print(f"⚠️  Redirección detectada. Reintentando a {EXPECTED_PATH}...", flush=True)
            driver.get('https://www.phonehouse.es' + EXPECTED_PATH)
            time.sleep(2)
            current = getattr(driver, 'current_url', '') or ''
            print(f"URL final (Selenium) tras reintento: {mask_url(current)}", flush=True)

        if EXPECTED_PATH not in current:
            print("❌ ERROR: no estamos en 'todos-los-smartphones'. Abortando.", flush=True)
            return []

        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.item-listado-final"))
        )

        print("🧭 Haciendo scroll hasta el final...", flush=True)
        last_h = 0
        stable = 0
        for _ in range(70):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1.2)
            h = driver.execute_script("return document.body.scrollHeight")
            if h == last_h:
                stable += 1
            else:
                stable = 0
            last_h = h
            if stable >= 3:
                break

        # Items del listado principal (el <input> tiene dataset GTM)
        items = driver.find_elements(
            By.CSS_SELECTOR,
            f"div.item-listado-final > input[data-item_list_id='{LIST_ID}'][data-item_list_name='{LIST_NAME}']",
        )
        print(f"✅ Items de listado (id={LIST_ID}) detectados: {len(items)}", flush=True)
        if len(items) == 0:
            print("❌ No se detecta el listado esperado. Para evitar importar productos de otras secciones, se aborta.", flush=True)
            return []

        productos = []
        seen_urls = set()

        def _safe_text(el):
            try:
                return normalize_spaces(el.text or "")
            except Exception:
                return ""

        for inp in items:
            if len(productos) >= objetivo:
                break

            try:
                card = inp.find_element(By.XPATH, "..")
            except Exception:
                continue

            # URL ficha
            try:
                a = card.find_element(By.CSS_SELECTOR, "a[href^='/movil/'], a[href*='/movil/']")
                href = (a.get_attribute("href") or "").strip()
            except Exception:
                continue

            if not href:
                continue
            href = href.split("?")[0]

            # evitar reacondicionados / renuevo si aparecieran en el listado
            low = href.lower()
            if any(x in low for x in ["reacondicionado", "reacondicionados", "renuevo", "reacond"]):
                continue

            if href in seen_urls:
                continue
            seen_urls.add(href)

            # título del card
            try:
                h3 = card.find_element(By.CSS_SELECTOR, "h3.marca-item")
                titulo = _safe_text(h3)
            except Exception:
                titulo = ""
            if len(titulo) < 6:
                continue

            # precio actual (card)
            precio_actual = 0
            precio_original = 0
            try:
                box = card.find_element(By.CSS_SELECTOR, ".listado-precios-libre, .precios-items-mosaico, [class*='listado-precios'], [class*='precios']")
            except Exception:
                box = None

            if box:
                # actual: span.precio-2
                try:
                    el_act = box.find_element(By.CSS_SELECTOR, "span.precio-2")
                    at = _safe_text(el_act)
                    vals = [v for v in parse_eur_all(at) if 20 <= v <= 5000]
                    if vals:
                        precio_actual = vals[0]
                except Exception:
                    pass

                if precio_actual == 0:
                    # fallback: primer span.precio no tachado
                    try:
                        el_act = box.find_element(By.CSS_SELECTOR, "span.precio:not(.precio-tachado):not(.precio-tachado-finales):not(.precio-tachado-final)")
                        at = _safe_text(el_act)
                        vals = [v for v in parse_eur_all(at) if 20 <= v <= 5000]
                        if vals:
                            precio_actual = vals[0]
                    except Exception:
                        pass

                # original tachado (si existe)
                try:
                    el_org = box.find_element(By.CSS_SELECTOR, "span.precio-tachado, span.precio-tachado-finales, span.precio-tachado-final, s, del")
                    ot = _safe_text(el_org)
                    ovals = [v for v in parse_eur_all(ot) if 20 <= v <= 5000]
                    if ovals:
                        precio_original = ovals[0]
                except Exception:
                    pass

                if precio_original == 0:
                    precio_original = precio_actual

            if precio_actual < 20:
                continue

            # imagen
            img = ""
            try:
                im = card.find_element(By.CSS_SELECTOR, "img")
                for attr in ["src", "data-src", "data-original", "data-lazy"]:
                    v = (im.get_attribute(attr) or "").strip()
                    if v and "logo" not in v.lower():
                        if v.startswith("//"):
                            v = "https:" + v
                        img = abs_url("https://www.phonehouse.es", v)
                        break
            except Exception:
                pass

            # specs desde título
            nombre, cap, ram = extraer_nombre_memoria_capacidad(titulo)
            es_iphone = "iphone" in (nombre or "").lower()
            if es_iphone and not ram:
                ram = ram_por_modelo_iphone(nombre) or ""

            # solo móviles con RAM y capacidad
            if not cap:
                continue
            if (not ram) and (not es_iphone):
                continue
            if es_iphone and not ram:
                continue

            version = "IOS" if es_iphone else "Global"
            key = f"{nombre}_{cap}_{ram}"

            if any(p.get('clave_unica') == key for p in productos):
                summary_duplicados.append(f"{nombre} {cap} {ram}".strip())
                continue

            productos.append({
                "nombre": nombre,
                "memoria": ram,
                "capacidad": cap,
                "precio_actual": int(precio_actual),
                "precio_original": int(precio_original or precio_actual),
                "img": img,
                "url_imp": href,
                "enviado_desde": ENVIADO_DESDE,
                "enviado_desde_tg": ENVIADO_DESDE_TG,
                "fecha": hoy,
                "en_stock": True,
                "clave_unica": key,
                "version": version,
                "fuente": FUENTE,
                "codigo_descuento": CODIGO_DESCUENTO,
            })

        print(f"✅ Productos DOM válidos: {len(productos)}", flush=True)
        return productos

    finally:
        try:
            driver.quit()
        except Exception:
            pass

def descubrir_urls_producto(html: str, base_url: str):
    """Devuelve set de URLs de ficha asociadas a tarjetas del listado (heurística robusta).

    Evita 'productos fantasma' exigiendo que el enlace /movil/... esté dentro de un bloque
    que también contenga:
      - algún precio en euros, y
      - un título visible (h2/h3/h4 o elemento con clase que contenga 'marca'/'item'/'title').

    Además, imprime diagnósticos básicos para entender cambios de HTML.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Diagnósticos
    try:
        a_mov = soup.find_all("a", href=PRODUCT_PATH_RE)
        print(f"   🧪 Diagnóstico: <a href='/movil/...'> encontrados: {len(a_mov)}", flush=True)
        n_precios = (len(soup.select('.precios-items-mosaico'))
                    + len(soup.select('.listado-precios-libre'))
                    + len(soup.select('[class*="listado-precios"]'))
                    + len(soup.select('[class*="precios-items"]')))
        print(f"   🧪 Diagnóstico: contenedores precios (mosaico/otros): {n_precios}", flush=True)
        n_title_like = len(soup.select('h3[class*="marca"], h3[class*="item"], [class*="marca-item"], [class*="product-name"], [class*="title"]'))
        print(f"   🧪 Diagnóstico: nodos título (marca/item/title): {n_title_like}", flush=True)
    except Exception:
        pass

    urls = set()

    def _has_price(block) -> bool:
        try:
            txt = block.get_text(" ", strip=True)
            if "€" not in txt:
                return False
            if "otras ofertas" in txt.lower():
                # no excluye, pero evita usarlo como única señal
                pass
            # debe contener al menos un patrón numero+€
            return bool(re.search(r"\d{1,5}(?:[\.,]\d{1,2})?\s*€", txt))
        except Exception:
            return False

    def _title_text(block) -> str:
        # prioridad: h3/h2/h4
        for tag in block.find_all(["h2","h3","h4"], limit=3):
            t = normalize_spaces(tag.get_text(" ", strip=True))
            if len(t) >= 8:
                return t
        # clases típicas
        cand = block.find(attrs={"class": re.compile(r"marca|item|title|name|product", re.I)})
        if cand:
            t = normalize_spaces(cand.get_text(" ", strip=True))
            if len(t) >= 8:
                return t
        return ""

    # Estrategia: partir de enlaces /movil/... y subir hasta un bloque que tenga precio+título
    for a in soup.find_all("a", href=PRODUCT_PATH_RE):
        href = (a.get("href") or "").strip()
        if not href:
            continue

        u = abs_url(base_url, href).split("?")[0]
        low = u.lower()
        if any(x in low for x in ["accesorio", "funda", "cargador", "protector", "seguro", "financiacion"]):
            continue
        if any(x in low for x in ['reacondicionado','reacondicionados','renuevo','renov','reacond']):
            continue

        block = a
        found = False
        for _ in range(10):
            block = getattr(block, "parent", None)
            if not block:
                break
            if not getattr(block, "get_text", None):
                continue
            if _has_price(block) and _title_text(block):
                found = True
                break

        if found:
            urls.add(u)

    return urls

# --------------------------
# FETCH DE FICHA PARA COMPLETAR DATOS
# --------------------------
def fetch_ficha_producto(url: str, session: requests.Session, max_retries: int = 3):
    """
    Devuelve dict con {titulo, precio_actual, precio_original, img, capacidad, memoria}
    leyendo la ficha del producto.

    Ajustes para PhoneHouse:
      - Precio actual: el precio "principal" (no 'desde', no cuotas/mes, no financiación).
      - Precio original: el precio tachado junto al actual cuando exista.
      - Imagen: múltiples fallbacks (og:image, twitter:image, JSON-LD, <img> con products-image).
    """

    def _is_bad_price_context(txt: str) -> bool:
        t = (txt or "").lower()
        return (
            "desde" in t
            or "otras ofertas" in t
            or "€/mes" in t
            or "/mes" in t
            or " mes" in t
            or "financi" in t
            or "cuota" in t
        )

    def _extract_jsonld_product(soup: BeautifulSoup):
        """
        Intenta extraer nombre/imagen/precio desde JSON-LD Product.
        Devuelve dict parcial.
        """
        out = {"titulo": "", "img": "", "price": 0.0, "price_original": 0.0}
        scripts = soup.find_all("script", type=re.compile(r"ld\+json", re.I))
        for sc in scripts:
            raw = (sc.string or sc.get_text() or "").strip()
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except Exception:
                continue

            nodes = data if isinstance(data, list) else [data]
            for node in list(nodes):
                if isinstance(node, dict) and isinstance(node.get("@graph"), list):
                    nodes.extend([x for x in node["@graph"] if isinstance(x, dict)])

            for node in nodes:
                if not isinstance(node, dict):
                    continue
                t = node.get("@type")
                if isinstance(t, list):
                    t = " ".join([str(x) for x in t])
                t = str(t or "")
                if "Product" not in t:
                    continue

                if not out["titulo"]:
                    out["titulo"] = normalize_spaces(node.get("name") or "")

                img = node.get("image")
                if isinstance(img, list) and img:
                    out["img"] = str(img[0]).strip()
                elif isinstance(img, str):
                    out["img"] = img.strip()

                offers = node.get("offers")
                if isinstance(offers, list) and offers:
                    offers = offers[0]
                if isinstance(offers, dict):
                    price = offers.get("price")
                    try:
                        if price is not None:
                            out["price"] = float(str(price).replace(",", "."))
                    except Exception:
                        pass

                    ps = offers.get("priceSpecification")
                    vals = []
                    if isinstance(ps, list):
                        for spec in ps:
                            if not isinstance(spec, dict):
                                continue
                            try:
                                vals.append(float(str(spec.get("price")).replace(",", ".")))
                            except Exception:
                                continue
                    elif isinstance(ps, dict):
                        try:
                            vals.append(float(str(ps.get("price")).replace(",", ".")))
                        except Exception:
                            pass
                    if vals:
                        out["price_original"] = max(vals)

        return out

    def _extract_img(soup: BeautifulSoup):
        for tag in soup.select('meta[property="og:image"], meta[name="twitter:image"]'):
            val = tag.get("content")
            if val and "http" in val:
                return val.strip()

        link_img = soup.find("link", rel="image_src")
        if link_img and link_img.get("href"):
            return link_img["href"].strip()

        j = _extract_jsonld_product(soup)
        if j.get("img"):
            return j["img"]

        for im in soup.find_all("img", src=True):
            s = (im.get("src") or "").strip()
            if s and ("products-image" in s) and ("logo" not in s.lower()):
                return s

        for im in soup.find_all("img"):
            for attr in ("data-src", "data-original", "data-lazy"):
                s = (im.get(attr) or "").strip()
                if s and ("products-image" in s) and ("logo" not in s.lower()):
                    return s

        return ""

    def _extract_prices_html(soup: BeautifulSoup, jsonld_price: int = 0):
        """Extrae (actual, original) con prioridad a PhoneHouse y con fallback seguro.

        1) Si existe .precios-items-mosaico:
            - actual: span.precio-2 o span.precio (no tachado)
            - original: span.precio-tachado o <s>/<del>
        2) Si no existe, usa meta product:price:amount.
        3) Si sigue sin, usa jsonld_price.
        4) Fallback global muy conservador (evita 'desde' y cuotas).
        """

        # 1) PhoneHouse listado / mosaico
        box = soup.select_one(".precios-items-mosaico")
        if box:
            act = 0
            orig = 0

            # Actual: span.precio-2 o span.precio (no tachado)
            cand = box.select_one("span.precio-2") or box.select_one("span.precio:not(.precio-tachado)")
            if cand:
                at = normalize_spaces(cand.get_text(" ", strip=True))
                # Puede venir como "149 €" o "149€"
                act_vals = parse_eur_all(at)
                act = act_vals[0] if act_vals else parse_eur_int(at)

                # Heurística anti-fragmentación: si act es muy pequeña pero jsonld_price es plausible, usar jsonld
                if act and act < 20 and jsonld_price > 50:
                    act = jsonld_price

            # Original tachado
            oc = box.select_one("span.precio-tachado") or box.select_one("s") or box.select_one("del")
            if oc:
                ot = normalize_spaces(oc.get_text(" ", strip=True))
                ovals = parse_eur_all(ot)
                orig = ovals[0] if ovals else parse_eur_int(ot)

            # Si no hay original, intenta extraer todos los precios del box
            if act and orig == 0:
                allp = parse_eur_all(normalize_spaces(box.get_text(" ", strip=True)))
                bigger = sorted({p for p in allp if p > act})
                orig = bigger[0] if bigger else act

            if act:
                if orig == 0:
                    orig = act
                return act, orig

        # 2) Meta
        actual = 0
        original = 0
        mp = soup.find("meta", property="product:price:amount")
        if mp and mp.get("content"):
            actual = parse_eur_int(mp["content"])

        # 3) JSON-LD
        if (actual == 0) and jsonld_price:
            actual = jsonld_price

        # 4) Fallback global (conservador)
        if actual == 0:
            prices = []
            for el in soup.find_all(["span", "div", "p", "s", "del"]):
                t = normalize_spaces(el.get_text(" ", strip=True))
                if "€" not in t:
                    continue
                if _is_bad_price_context(t):
                    continue
                prices.extend(parse_eur_all(t))
            prices = [p for p in prices if p > 0]
            if prices:
                actual = min(prices)
                bigger = sorted({p for p in prices if p > actual})
                original = bigger[0] if bigger else max(prices)

        if original == 0:
            original = actual

        return actual, original

    for attempt in range(1, max_retries + 1):
        try:
            r = session.get(url, headers=HEADERS, timeout=30)
            if r.status_code in (429, 503):
                time.sleep(2.5 * attempt)
                continue
            if r.status_code != 200:
                time.sleep(1.0 * attempt)
                continue

            soup = BeautifulSoup(r.text, "html.parser")

            # Título
            titulo = ""
            h1 = soup.find("h1")
            if h1:
                titulo = normalize_spaces(h1.get_text(" ", strip=True))
            if not titulo:
                og = soup.find("meta", property="og:title")
                if og and og.get("content"):
                    titulo = normalize_spaces(og["content"])

            j2 = _extract_jsonld_product(soup)
            if not titulo and j.get("titulo"):
                titulo = j["titulo"]

            # Imagen
            img = _extract_img(soup)
            img = abs_url(url, img) if img else ""

            # Precios HTML
                        # JSON-LD (si existe) para apoyar el parseo
            j = j2
            j_price_int = 0
            try:
                if (j.get('price') or 0):
                    j_price_int = int(round(float(j.get('price') or 0)))
            except Exception:
                j_price_int = 0

            precio_actual, precio_original = _extract_prices_html(soup, jsonld_price=j_price_int)

            # JSON-LD precio actual solo si el HTML parece vacío
            try:
                jprice = float(j.get("price") or 0)
                if jprice > 0 and precio_actual == 0:
                    precio_actual = int(round(jprice))
            except Exception:
                pass

            # RAM/capacidad
            nombre, cap, ram = extraer_nombre_memoria_capacidad(titulo or "")
            es_iphone = "iphone" in (nombre or "").lower()
            if es_iphone and not ram:
                ram = ram_por_modelo_iphone(nombre) or ""

            if not cap or ((not ram) and (not es_iphone)):
                cap2, ram2 = extraer_specs_ram_cap(soup)
                cap = cap or cap2
                if not ram:
                    ram = ram2

            # DEBUG
            try:
                _t = (titulo or "").replace("\n", " ")
                print(f"   🧾 [FICHA] {mask_url(url)} | actual={precio_actual}€ | original={precio_original}€ | jsonld={j_price_int}€ | img={'OK' if img else 'VACIA'} | titulo='{_t[:60]}'", flush=True)
            except Exception:
                pass

            return {
                "titulo": titulo,
                "nombre": nombre,
                "capacidad": cap,
                "memoria": ram,
                "es_iphone": es_iphone,
                "precio_actual": int(precio_actual or 0),
                "precio_original": int(precio_original or 0),
                "img": img,
            }
        except Exception:
            time.sleep(1.0 * attempt)
            continue

    return None

# --------------------------
# EXTRACCIÓN REMOTA COMPLETA
# --------------------------
def obtener_datos_remotos():
    """Extrae productos de PhoneHouse desde el listado (solo cards DOM)."""
    print("", flush=True)
    print("--- FASE 1: ESCANEANDO PHONE HOUSE ---", flush=True)
    print(f"URL: {mask_url(START_URL)}", flush=True)

    productos = obtener_productos_desde_dom(START_URL, objetivo=OBJETIVO)

    print("", flush=True)
    print("📊 RESUMEN EXTRACCIÓN:", flush=True)
    print(f"   Productos únicos encontrados: {len(productos)}", flush=True)
    return productos

def obtener_todas_las_categorias():
    categorias = []
    page = 1
    while True:
        try:
            res = wcapi.get("products/categories", params={"per_page": 100, "page": page}).json()
            if not res or "message" in res or len(res) == 0:
                break
            categorias.extend(res)
            page += 1
        except Exception:
            break
    return categorias

def resolver_jerarquia(nombre_completo, cache_categorias):
    palabras = (nombre_completo or "").split()
    nombre_padre = palabras[0] if palabras else "Otros"
    nombre_hijo = nombre_completo

    id_cat_padre = None
    id_cat_hijo = None

    for cat in cache_categorias:
        if cat.get("name", "").lower() == nombre_padre.lower() and cat.get("parent") == 0:
            id_cat_padre = cat.get("id")
            break
    if not id_cat_padre:
        res = wcapi.post("products/categories", {"name": nombre_padre}).json()
        id_cat_padre = res.get("id")
        cache_categorias.append(res)

    for cat in cache_categorias:
        if cat.get("name", "").lower() == nombre_hijo.lower() and cat.get("parent") == id_cat_padre:
            id_cat_hijo = cat.get("id")
            break
    if not id_cat_hijo:
        res = wcapi.post("products/categories", {"name": nombre_hijo, "parent": id_cat_padre}).json()
        id_cat_hijo = res.get("id")
        cache_categorias.append(res)

    return id_cat_padre, id_cat_hijo

def obtener_imagen_categoria(cache_categorias, cat_id):
    if not cat_id:
        return ""
    for c in cache_categorias:
        if c.get("id") == cat_id:
            img = c.get("image") or {}
            return img.get("src") or ""
    return ""

def actualizar_imagen_categoria(cache_categorias, cat_id, img_src):
    if not cat_id or not img_src:
        return False
    if obtener_imagen_categoria(cache_categorias, cat_id):
        return False
    try:
        res = wcapi.put(f"products/categories/{cat_id}", {"image": {"src": img_src}}).json()
        for i, c in enumerate(cache_categorias):
            if c.get("id") == cat_id:
                cache_categorias[i] = res
                break
        return True
    except Exception:
        return False

# --------------------------
# SINCRONIZACIÓN WC
# --------------------------
def sincronizar(remotos):
    print("\n--- FASE 2: SINCRONIZANDO ---", flush=True)
    cache_categorias = obtener_todas_las_categorias()

    # Cargar productos importados (por meta importado_de)
    locales = []
    page = 1
    while True:
        res = wcapi.get("products", params={"per_page": 100, "page": page}).json()
        if not res or "message" in res:
            break
        for p in res:
            meta = {m["key"]: str(m.get("value", "")) for m in p.get("meta_data", [])}
            if "phonehouse.es" in meta.get("importado_de", "").lower():
                locales.append({"id": p["id"], "nombre": p.get("name", ""), "meta": meta})
        if len(res) < 100:
            break
        page += 1

    print(f"📦 Productos Phone House existentes en la web: {len(locales)}", flush=True)
    print(f"📦 Productos remotos a procesar: {len(remotos)}", flush=True)

    for r in remotos:
        try:
            # --- LOG DETALLADO (DEBUG) ---
            print("-" * 60, flush=True)
            print(f"Detectado {r.get('nombre','(sin nombre)')}", flush=True)
            print(f"1) Nombre:          {r.get('nombre','')}", flush=True)
            print(f"2) Memoria (RAM):   {r.get('memoria','')}", flush=True)
            print(f"3) Capacidad:       {r.get('capacidad','')}", flush=True)
            print(f"4) Versión ROM:     {r.get('version','Global')}", flush=True)
            print(f"5) Precio Actual:   {r.get('precio_actual',0)}€", flush=True)
            print(f"6) Precio Original: {r.get('precio_original',0)}€", flush=True)
            print(f"7) Enviado desde:   {r.get('enviado_desde','')}", flush=True)
            print(f"8) Stock Real:      {r.get('cantidad','N/D')}", flush=True)
            img = (r.get('img','') or '')
            print(f"9) URL Imagen:      {(img[:75] + '...') if img else '(vacía)'}", flush=True)
            print(f"10) Enlace Compra:  {mask_url(r.get('url_imp',''))}", flush=True)
            print("-" * 60, flush=True)
            url_base = (r["url_imp"] or "").strip().split("?")[0]
            url_con_afiliado = f"{url_base}{AFF_RAW}" if AFF_RAW else url_base
            url_oferta = acortar_url(url_con_afiliado)

            # match por enlace_de_compra_importado
            match = next(
                (
                    l for l in locales
                    if l["meta"].get("enlace_de_compra_importado", "").strip().split("?")[0].rstrip("/") == url_base.rstrip("/")
                ),
                None
            )

            id_padre, id_hijo = resolver_jerarquia(r["nombre"], cache_categorias)

            img_subcat = obtener_imagen_categoria(cache_categorias, id_hijo)
            if (not img_subcat) and r.get("img"):
                actualizar_imagen_categoria(cache_categorias, id_hijo, r["img"])
                img_subcat = obtener_imagen_categoria(cache_categorias, id_hijo)
            img_final_producto = img_subcat or r.get("img") or ""

            if match:
                # comparar precio_actual
                try:
                    p_acf = int(float(match["meta"].get("precio_actual", 0) or 0))
                except Exception:
                    p_acf = 0

                if r["precio_actual"] != p_acf:
                    cambio_str = f"{p_acf}€ -> {r['precio_actual']}€"
                    print(f"🔄 ACTUALIZANDO: {r['nombre']} ({cambio_str})", flush=True)
                    wcapi.put(
                        f"products/{match['id']}",
                        {
                            "sale_price": str(r["precio_actual"]),
                            "regular_price": str(r["precio_original"]),
                            "meta_data": [
                                {"key": "precio_actual", "value": str(r["precio_actual"])},
                                {"key": "precio_original", "value": str(r["precio_original"])},
                                {"key": "enviado_desde_tg", "value": ENVIADO_DESDE_TG},
                                {"key": "url_oferta", "value": url_oferta},
                                {"key": "url_sin_acortar_con_mi_afiliado", "value": url_con_afiliado},
                                {"key": "imagen_producto", "value": r.get("img","")},
                                {"key": "version", "value": r.get("version","Global")},
                            ],
                        },
                    )
                    summary_actualizados.append({"nombre": r["nombre"], "id": match["id"], "cambio": cambio_str})
                else:
                    summary_ignorados.append({"nombre": r["nombre"], "id": match["id"]})

            else:
                print(f"🆕 CREANDO: {r['nombre']}", flush=True)
                data = {
                    "name": r["nombre"],
                    "type": "simple",
                    "status": "publish",
                    "regular_price": str(r["precio_original"]),
                    "sale_price": str(r["precio_actual"]),
                    "categories": [{"id": id_padre}, {"id": id_hijo}] if id_hijo else ([{"id": id_padre}] if id_padre else []),
                    "images": [{"src": img_final_producto}] if img_final_producto else [],
                    "meta_data": [
                        {"key": "nombre_movil_final", "value": r["nombre"]},
                        {"key": "importado_de", "value": ID_IMPORTACION},
                        {"key": "fecha", "value": r["fecha"]},
                        {"key": "memoria", "value": r["memoria"]},
                        {"key": "capacidad", "value": r["capacidad"]},
                        {"key": "fuente", "value": FUENTE},
                        {"key": "precio_actual", "value": str(r["precio_actual"])},
                        {"key": "precio_original", "value": str(r["precio_original"])},
                        {"key": "codigo_de_descuento", "value": CODIGO_DESCUENTO},
                        {"key": "enviado_desde", "value": ENVIADO_DESDE},
                        {"key": "enviado_desde_tg", "value": ENVIADO_DESDE_TG},
                        {"key": "enlace_de_compra_importado", "value": url_base},
                        {"key": "url_importada_sin_afiliado", "value": url_base},
                        {"key": "url_sin_acortar_con_mi_afiliado", "value": url_con_afiliado},
                        {"key": "url_oferta", "value": url_oferta},
                        {"key": "imagen_producto", "value": r.get("img","")},
                        {"key": "version", "value": r.get("version","Global")},
                    ],
                }

                intentos = 0
                max_intentos = 10
                creado = False

                while intentos < max_intentos and not creado:
                    intentos += 1
                    try:
                        res = wcapi.post("products", data)
                        if res.status_code in (200, 201):
                            creado = True
                            prod = res.json()
                            summary_creados.append({"nombre": r["nombre"], "id": prod.get("id")})
                            print(f"✅ CREADO -> ID: {prod.get('id')}", flush=True)

                            # Acortar permalink del post
                            try:
                                url_short = acortar_url(prod.get("permalink", ""))
                                if url_short:
                                    wcapi.put(
                                        f"products/{prod.get('id')}",
                                        {"meta_data": [{"key": "url_post_acortada", "value": url_short}]},
                                    )
                            except Exception:
                                pass
                        else:
                            body_preview = (res.text or "").replace("\n", " ")[:250]
                            print(f"⚠️  Woo error {res.status_code}: {body_preview}", flush=True)
                    except Exception as e:
                        print(f"⚠️  Excepción Woo: {e}", flush=True)

                    if (not creado) and (intentos < max_intentos):
                        time.sleep(15)

                if not creado:
                    summary_fallidos.append(r.get("nombre", "desconocido"))
                    print(f"❌ NO SE PUDO CREAR: {r.get('nombre','?')}", flush=True)

        except Exception as e:
            summary_fallidos.append(r.get("nombre", "desconocido"))
            print(f"❌ ERROR en {r.get('nombre','?')}: {e}", flush=True)

       # Resumen
    hoy_fmt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    print(f"\n============================================================", flush=True)
    print(f"📋 RESUMEN DE EJECUCIÓN ({hoy_fmt})", flush=True)
    print(f"============================================================", flush=True)

    print(f"\na) ARTICULOS CREADOS: {len(summary_creados)}", flush=True)
    for item in summary_creados:
        print(f"- {item['nombre']} (ID: {item.get('id')})", flush=True)

    print(f"\nb) ARTICULOS ELIMINADOS (OBSOLETOS): {len(summary_eliminados)}", flush=True)
    for item in summary_eliminados:
        print(f"- {item['nombre']} (ID: {item.get('id')})", flush=True)

    print(f"\nc) ARTICULOS ACTUALIZADOS: {len(summary_actualizados)}", flush=True)
    for item in summary_actualizados:
        cambios = item.get('cambios') or []
        if isinstance(cambios, str):
            cambios = [cambios]
        print(f"- {item['nombre']} (ID: {item.get('id')}): {', '.join(cambios)}", flush=True)

    print(f"\nd) ARTICULOS IGNORADOS (SIN CAMBIOS): {len(summary_ignorados)}", flush=True)
    for item in summary_ignorados:
        print(f"- {item['nombre']} (ID: {item.get('id')})", flush=True)

    # Extras (manteniendo el mismo resumen detallado pedido)
    if summary_duplicados:
        print(f"\nf) DUPLICADOS: {len(summary_duplicados)}", flush=True)
        for nombre in summary_duplicados:
            print(f"- {nombre}", flush=True)

    if summary_fallidos:
        print(f"\ng) FALLIDOS: {len(summary_fallidos)}", flush=True)
        for nombre in summary_fallidos:
            print(f"- {nombre}", flush=True)

    print(f"============================================================", flush=True)

if __name__ == "__main__":
    remotos = obtener_datos_remotos()
    if remotos:
        sincronizar(remotos)
