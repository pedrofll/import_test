import os
import re
import time
import math
import json
import urllib.parse
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from woocommerce import API

# ============================================================
#  SAMSUNG SCRAPER (LISTING -> DETAIL/BUY -> WC SYNC)
# ============================================================
# Objetivo:
#  - Descubrir familias y fichas de móviles Samsung desde la página oficial.
#  - Extraer nombre, RAM, capacidad, precio, imagen y cupón cuando exista.
#  - Sincronizar WooCommerce con altas, actualizaciones y obsoletos.
#  - Mantener la lógica operativa del scraper de Phone House adjunto,
#    adaptada a Samsung y a sus páginas con precio hidratado por JS.
#
# Notas importantes:
#  - Samsung mezcla páginas de familia, fichas de variante y buy pages.
#  - El precio suele llegar renderizado por JS, así que Selenium es clave.
#  - Para evitar duplicados por color, la clave remota ignora el color y usa
#    nombre + RAM + capacidad + fuente.
# ============================================================

DEFAULT_START_URL = "https://www.samsung.com/es/smartphones/all-smartphones/"
START_URL = os.getenv("SOURCE_URL_SAMSUNG", DEFAULT_START_URL).strip() or DEFAULT_START_URL

LISTING_URLS = [
    ("1", START_URL),
    ("2", "https://www.samsung.com/es/smartphones/"),
    ("3", "https://www.samsung.com/es/smartphones/galaxy-a/"),
    ("4", "https://www.samsung.com/es/smartphones/galaxy-s/"),
    ("5", "https://www.samsung.com/es/smartphones/galaxy-z/"),
]

FUENTE = "Samsung"
ID_IMPORTACION = START_URL.rstrip("/")
ENVIADO_DESDE = "España"
ENVIADO_DESDE_TG = "🇪🇸 España"
VERSION = "Versión Global"
CODIGO_DESCUENTO_DEFAULT = "OFERTA: PROMO."
OBJETIVO = 120

AFF_RAW = os.environ.get("AFF_SAMSUNG", "").strip()
if AFF_RAW and not AFF_RAW.startswith("?") and not AFF_RAW.startswith("&"):
    AFF_RAW = "?" + AFF_RAW

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Referer": "https://www.samsung.com/",
}

wcapi = API(
    url=os.environ["WP_URL"],
    consumer_key=os.environ["WP_KEY"],
    consumer_secret=os.environ["WP_SECRET"],
    version="wc/v3",
    timeout=60,
)

summary_creados, summary_eliminados, summary_actualizados = [], [], []
summary_ignorados, summary_fallidos = [], []
summary_duplicados = []

BUY_PAGE_CACHE = {}


# --------------------------
# UTILIDADES
# --------------------------

def normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def mask_url(url: str) -> str:
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


def parse_eur_num(num_txt: str) -> int:
    if not num_txt:
        return 0
    n = str(num_txt).strip().replace(" ", "")
    n = n.replace(".", "").replace(",", ".")
    try:
        return int(round(float(n)))
    except Exception:
        return 0


EURO_AMOUNT_RE = r"(\d{1,3}(?:\.\d{3})*(?:,\d{1,2})?|\d{1,5}(?:[\.,]\d{1,2})?)\s*€"


def parse_eur_all(txt: str):
    if not txt:
        return []
    vals = []
    for m in re.finditer(EURO_AMOUNT_RE, txt, flags=re.I):
        v = parse_eur_num(m.group(1))
        if v > 0:
            vals.append(v)
    return vals


def dedupe_keep_order(seq):
    out = []
    seen = set()
    for x in seq:
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def calcular_precio_original(precio_actual: int, factor: float = 1.20) -> int:
    try:
        pa = int(precio_actual)
    except Exception:
        return 0
    if pa <= 0:
        return 0
    return int(math.ceil(pa * factor))


def acortar_url(url_larga: str) -> str:
    try:
        if not url_larga:
            return ""
        url_encoded = urllib.parse.quote(url_larga, safe="")
        r = requests.get(
            f"https://is.gd/create.php?format=simple&url={url_encoded}",
            headers=HEADERS,
            timeout=10,
        )
        return r.text.strip() if r.status_code == 200 else url_larga
    except Exception:
        return url_larga


def normalizar_nombre_samsung(nombre: str) -> str:
    """Fuerza el nombre a 'Samsung Galaxy ...' y normaliza 5G/4G/TB/GB."""
    t = normalize_spaces(nombre)
    if not t:
        return ""

    # Si viene desde el title, cortamos color / tienda / extras.
    if "|" in t:
        t = normalize_spaces(t.split("|")[0])

    if t.lower().startswith("samsung "):
        base = t[len("Samsung "):]
    else:
        base = t

    words = []
    for w in base.split():
        clean = w.strip()
        if not clean:
            continue
        if re.search(r"\d", clean) and re.search(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]", clean):
            clean = "".join(ch.upper() if ch.isalpha() else ch for ch in clean)
        elif clean.lower() in {"gb", "tb"}:
            clean = clean.upper()
        elif clean.lower() in {"s", "z", "a", "+"}:
            clean = clean.upper()
        else:
            clean = clean[:1].upper() + clean[1:]
        words.append(clean)

    base = " ".join(words)
    if not base.lower().startswith("galaxy "):
        return normalize_spaces(f"Samsung {base}")
    return normalize_spaces(f"Samsung {base}")


def limpiar_nombre_para_categoria(nombre: str) -> str:
    return normalize_spaces(nombre)


def parse_capacidad_desde_texto(txt: str) -> str:
    t = normalize_spaces(txt)
    m = re.search(r"\b(64|128|256|512|1024)\s*GB\b", t, flags=re.I)
    if m:
        return f"{m.group(1)}GB"
    m = re.search(r"\b(1|2)\s*TB\b", t, flags=re.I)
    if m:
        return f"{m.group(1)}TB"
    return ""


def parse_memoria_desde_texto(txt: str) -> str:
    t = normalize_spaces(txt)
    m = re.search(r"\b(3|4|6|8|12|16|24)\s*GB\b", t, flags=re.I)
    if m:
        return f"{m.group(1)}GB"
    return ""


def parse_variant_option_text(option_text: str):
    """Soporta '256 GB｜12 GB', '1 TB|16 GB', '256GB/8GB', etc."""
    t = normalize_spaces(option_text).replace("｜", "|")
    cap = ""
    ram = ""

    m = re.search(
        r"(?P<cap>\d{2,4})\s*(?P<cap_unit>GB|TB)\s*[\|\/+]+\s*(?P<ram>\d{1,2})\s*GB",
        t,
        flags=re.I,
    )
    if m:
        cap = f"{m.group('cap')}{m.group('cap_unit').upper()}"
        ram = f"{m.group('ram')}GB"
        return cap, ram

    cap = parse_capacidad_desde_texto(t)
    ram = parse_memoria_desde_texto(t)
    return cap, ram


def extraer_model_code(text: str) -> str:
    m = re.search(r"\bSM-[A-Z0-9]+\b", text or "", flags=re.I)
    return m.group(0).upper() if m else ""


def extract_coupon_from_text(txt: str) -> str:
    t = normalize_spaces(txt)
    patterns = [
        r"(?:usa\s+el\s+)?c[oó]digo\s+([A-Z0-9]{4,20})",
        r"code\s+([A-Z0-9]{4,20})",
    ]
    for pat in patterns:
        m = re.search(pat, t, flags=re.I)
        if m:
            return m.group(1).upper()
    return CODIGO_DESCUENTO_DEFAULT


def extract_image_from_soup(soup: BeautifulSoup, base_url: str) -> str:
    selectors = [
        ('meta', {'property': 'og:image'}, 'content'),
        ('meta', {'name': 'twitter:image'}, 'content'),
        ('link', {'rel': 'image_src'}, 'href'),
    ]
    for tag, attrs, attr in selectors:
        node = soup.find(tag, attrs=attrs)
        if node and node.get(attr):
            return abs_url(base_url, node.get(attr).strip())

    # Fallback: primera imagen no-logo.
    for img in soup.find_all('img'):
        for attr in ('src', 'data-src', 'data-original', 'data-lazy-src'):
            v = (img.get(attr) or '').strip()
            if not v:
                continue
            low = v.lower()
            if 'logo' in low or 'icon' in low or 'sprite' in low:
                continue
            return abs_url(base_url, v)
    return ""


def should_skip_by_name(nombre: str) -> bool:
    u = (nombre or "").upper()
    return any(x in u for x in [" TAB", "IPAD", " PAD"]) or u.startswith("TAB ")


def source_key(nombre: str, memoria: str, capacidad: str, fuente: str = FUENTE) -> str:
    return f"{normalize_spaces(nombre).lower()}|{str(memoria).upper()}|{str(capacidad).upper()}|{fuente.lower()}"


def family_slug_from_name(nombre: str) -> str:
    """Convierte 'Samsung Galaxy Z Flip 7' -> 'galaxy-z-flip7'."""
    t = normalizar_nombre_samsung(nombre).lower().replace("samsung ", "")
    t = normalize_spaces(t)
    if not t.startswith("galaxy "):
        return ""

    t = t.replace(" +", "+")
    parts = t.split()
    if parts[:3] == ["galaxy", "z", "flip"] and len(parts) >= 4:
        return f"galaxy-z-flip{parts[3].replace('+','')}"
    if parts[:3] == ["galaxy", "z", "fold"] and len(parts) >= 4:
        return f"galaxy-z-fold{parts[3].replace('+','')}"
    if len(parts) >= 2:
        slug = "-".join(parts)
        slug = slug.replace("+-", "+")
        return slug
    return ""


def derive_buy_url(detail_url: str, nombre: str) -> str:
    try:
        p = urllib.parse.urlsplit(detail_url)
        segs = [s for s in p.path.split('/') if s]
        # /es/smartphones/galaxy-s26-ultra/
        # /es/smartphones/galaxy-a/galaxy-a56-5g-awesome-graphite-256gb-sm-a566bzkceub/
        if len(segs) >= 3 and segs[0] == 'es' and segs[1] == 'smartphones':
            if len(segs) == 3:
                return f"https://www.samsung.com/es/smartphones/{segs[2]}/buy/"
            if len(segs) >= 4:
                category = segs[2]
                fam = family_slug_from_name(nombre)
                if fam:
                    return f"https://www.samsung.com/es/smartphones/{category}/{fam}/buy/"
        return detail_url.rstrip('/') + '/buy/'
    except Exception:
        return detail_url.rstrip('/') + '/buy/'


def normalize_product_url(url: str) -> str:
    try:
        u = urllib.parse.urlsplit(url)
        clean = f"{u.scheme}://{u.netloc}{u.path}"
        clean = re.sub(r"/+", "/", clean.replace("https:/", "https://"))
        return clean.rstrip("/") + "/"
    except Exception:
        return url


# --------------------------
# SELENIUM
# --------------------------

def get_driver():
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options

    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1440,2600")
    opts.add_argument(
        "--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
    return webdriver.Chrome(options=opts)


def dismiss_overlays(driver):
    from selenium.webdriver.common.by import By

    candidates = [
        "//button[contains(., 'Aceptar')]",
        "//button[contains(., 'Acepto')]",
        "//button[contains(., 'Aceptar todo')]",
        "//button[contains(., 'OK')]",
        "//button[contains(., 'Continuar')]",
        "//button[contains(., 'Sí')]",
        "//button[contains(., 'MAS TARDE')]",
        "//button[contains(., 'MÁS TARDE')]",
        "//a[contains(., 'IR A SAMSUNG.COM')]",
    ]
    for _ in range(3):
        for xp in candidates:
            try:
                els = driver.find_elements(By.XPATH, xp)
                for el in els[:3]:
                    if el.is_displayed():
                        try:
                            driver.execute_script("arguments[0].click();", el)
                            time.sleep(0.8)
                        except Exception:
                            pass
            except Exception:
                pass


def scroll_page(driver, rounds: int = 28):
    last_h = 0
    stable = 0
    for _ in range(rounds):
        try:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1.1)
            h = driver.execute_script("return document.body.scrollHeight")
            if h == last_h:
                stable += 1
            else:
                stable = 0
            last_h = h
            if stable >= 3:
                break
        except Exception:
            break


def get_rendered_html(url: str) -> str:
    driver = get_driver()
    try:
        driver.set_page_load_timeout(45)
        driver.get(url)
        time.sleep(3)
        dismiss_overlays(driver)
        scroll_page(driver)
        return driver.page_source
    finally:
        try:
            driver.quit()
        except Exception:
            pass


# --------------------------
# DESCUBRIMIENTO DE PRODUCTOS
# --------------------------

def is_candidate_product_url(url: str) -> bool:
    try:
        u = urllib.parse.urlsplit(url)
        if u.netloc.lower() != 'www.samsung.com':
            return False
        path = (u.path or '').rstrip('/') + '/'
        low = path.lower()
        if not low.startswith('/es/smartphones/'):
            return False
        if any(x in low for x in [
            '/buy/', '/specs/', '/compare/', '/accessories', '/offer/', '/business/',
            'galaxy-tab', 'galaxy-watch', 'galaxy-buds', 'galaxy-ring', 'galaxy-book',
        ]):
            return False

        rem = low[len('/es/smartphones/'):].strip('/')
        segs = [s for s in rem.split('/') if s]
        if not segs:
            return False

        if len(segs) == 1:
            slug = segs[0]
            if slug in {'all-smartphones', 'smartphones', 'galaxy-a', 'galaxy-s', 'galaxy-z'}:
                return False
            return slug.startswith('galaxy-')

        if len(segs) >= 2:
            if segs[0] in {'galaxy-a', 'galaxy-s', 'galaxy-z'} and segs[1].startswith('galaxy-'):
                return True
        return False
    except Exception:
        return False


def descubrir_urls_producto(html: str, base_url: str):
    soup = BeautifulSoup(html, 'html.parser')
    urls = set()

    for a in soup.find_all('a', href=True):
        href = (a.get('href') or '').strip()
        if not href:
            continue
        url = normalize_product_url(abs_url(base_url, href))
        if is_candidate_product_url(url):
            urls.add(url)

    for m in re.finditer(r'https://www\.samsung\.com/es/smartphones/[^"\'\s<>]+', html, flags=re.I):
        url = normalize_product_url(m.group(0))
        if is_candidate_product_url(url):
            urls.add(url)

    return urls


# --------------------------
# EXTRACCIÓN DETALLE / SPECS
# --------------------------

def fetch_soup(url: str, session: requests.Session) -> BeautifulSoup | None:
    try:
        r = session.get(url, headers=HEADERS, timeout=30)
        if r.status_code != 200:
            return None
        return BeautifulSoup(r.text, 'html.parser')
    except Exception:
        return None


def extraer_specs_inline(soup: BeautifulSoup):
    text = normalize_spaces(soup.get_text(' '))
    memoria = ''
    capacidad = ''

    m_ram = re.search(r"Memoria[_\s]*\(GB\)\s*(\d{1,2})\b", text, flags=re.I)
    if m_ram:
        memoria = f"{m_ram.group(1)}GB"
    else:
        m_ram = re.search(r"(?:RAM|Memoria)\D{0,20}(\d{1,2})\s*GB\b", text, flags=re.I)
        if m_ram:
            memoria = f"{m_ram.group(1)}GB"

    m_cap = re.search(r"Almacenamiento\s*\(GB\)\s*(\d{2,4})\b", text, flags=re.I)
    if m_cap:
        capacidad = f"{m_cap.group(1)}GB"
    else:
        capacidad = parse_capacidad_desde_texto(text)

    return capacidad, memoria


def extract_detail_info(url: str, source_label: str, source_listing: str, session: requests.Session):
    soup = fetch_soup(url, session)
    if soup is None:
        return None

    h1 = soup.find('h1')
    h1_txt = normalize_spaces(h1.get_text(' ', strip=True)) if h1 else ''

    if not h1_txt:
        og = soup.find('meta', attrs={'property': 'og:title'})
        if og and og.get('content'):
            h1_txt = normalize_spaces(og.get('content'))

    nombre = normalizar_nombre_samsung(h1_txt)
    if not nombre or should_skip_by_name(nombre):
        return None

    full_text = normalize_spaces(soup.get_text(' '))
    model_code = extraer_model_code(full_text)
    img = extract_image_from_soup(soup, url)
    capacidad, memoria = extraer_specs_inline(soup)

    # Fallback URL/title para páginas exactas de variante.
    if not capacidad:
        capacidad = parse_capacidad_desde_texto(url + ' ' + h1_txt)
    if not memoria:
        memoria = parse_memoria_desde_texto(url + ' ' + h1_txt)

    exact_variant = False
    low_url = url.lower()
    if model_code or re.search(r"/(?:[^/]*)(64|128|256|512|1024)gb[^/]*/?$", low_url):
        exact_variant = True
    if capacidad and memoria and '/galaxy-' in low_url and '/buy/' not in low_url and '/specs/' not in low_url:
        exact_variant = True

    buy_url = derive_buy_url(url, nombre)

    return {
        'nombre': nombre,
        'memoria': memoria,
        'capacidad': capacidad,
        'img': img,
        'model_code': model_code,
        'exact_variant': exact_variant,
        'url_imp': url,
        'buy_url': buy_url,
        'source_label': source_label,
        'source_listing': source_listing,
    }


# --------------------------
# PARSEO DE PRECIOS EN PÁGINA RENDERIZADA
# --------------------------

def parse_price_info_from_text(txt: str):
    t = normalize_spaces(txt)
    if not t:
        return 0, 0

    original = 0
    m_orig = re.search(r"Precio original[:\s]*([0-9\.\,]+)\s*€", t, flags=re.I)
    if m_orig:
        original = parse_eur_num(m_orig.group(1))

    usable = []
    for m in re.finditer(EURO_AMOUNT_RE, t, flags=re.I):
        val = parse_eur_num(m.group(1))
        if val <= 0:
            continue

        before_short = t[max(0, m.start() - 16):m.start()].lower()
        after_short = t[m.end():min(len(t), m.end() + 12)].lower().lstrip()
        ctx_small = (before_short + ' ' + after_short).strip()

        # Cuotas / financiación inmediatas al importe
        if any(k in ctx_small for k in ['/mes', ' mes', ' tae', 'tae*', 'monthly']) and val < 300:
            continue

        # Precio original explícito
        if 'precio original' in before_short or 'original:' in before_short:
            original = max(original, val)
            continue

        # Importes promocionales / ahorro
        if any(k in before_short for k in ['dto', 'descuento', 'ahorra', 'ahorro', 'save', 'reembolso', 'estrena']):
            continue
        if after_short.startswith('dto') or after_short.startswith('desc') or after_short.startswith('save'):
            continue

        # Trade-in / cashback como importe separado
        if 'trade-in' in before_short or after_short.startswith('trade-in'):
            continue

        if 100 <= val <= 5000:
            usable.append(val)

    usable = dedupe_keep_order(usable)
    current = 0

    if original:
        smaller = [v for v in usable if v <= original]
        if smaller:
            current = min(smaller)
            if current == original and len(smaller) > 1:
                current = sorted(smaller)[0]

    if not current and usable:
        current = min(usable)
        if len(usable) >= 2:
            original = max(usable)

    if current and not original:
        original = calcular_precio_original(current)

    return int(current or 0), int(original or 0)


def collect_candidate_price_texts(driver):
    from selenium.webdriver.common.by import By

    texts = []
    xpaths = [
        "//*[contains(translate(@class,'PRICE','price'),'price')]",
        "//*[contains(translate(@class,'OFFER','offer'),'offer')]",
        "//*[contains(translate(@class,'DISCOUNT','discount'),'discount')]",
        "//*[contains(translate(@id,'PRICE','price'),'price')]",
    ]

    for xp in xpaths:
        try:
            els = driver.find_elements(By.XPATH, xp)
            for el in els:
                try:
                    if not el.is_displayed():
                        continue
                    y = el.location.get('y', 99999)
                    if y > 1400:
                        continue
                    txt = normalize_spaces(el.text)
                    if txt and len(txt) <= 400:
                        texts.append(txt)
                except Exception:
                    pass
        except Exception:
            pass

    try:
        body = normalize_spaces(driver.find_element(By.TAG_NAME, 'body').text)
        h1s = driver.find_elements(By.TAG_NAME, 'h1')
        h1_txt = normalize_spaces(h1s[0].text) if h1s else ''
        if h1_txt and h1_txt in body:
            idx = body.find(h1_txt)
            if idx >= 0:
                texts.append(body[idx:idx + 1800])
        else:
            texts.append(body[:1800])
    except Exception:
        pass

    return dedupe_keep_order([t for t in texts if t])


def extract_price_coupon_from_driver(driver):
    texts = collect_candidate_price_texts(driver)
    joined = " || ".join(texts)
    current, original = parse_price_info_from_text(joined)
    coupon = extract_coupon_from_text(joined)
    return current, original, coupon


# --------------------------
# VARIANTES EN BUY PAGE
# --------------------------

def _find_clickable_texts(driver, regex, y_max=1800):
    from selenium.webdriver.common.by import By

    out = []
    seen = set()
    xpath = "//*[(self::button or self::a or self::label or self::span or self::div or self::li)]"
    try:
        els = driver.find_elements(By.XPATH, xpath)
    except Exception:
        return []

    for el in els:
        try:
            if not el.is_displayed():
                continue
            y = el.location.get('y', 99999)
            if y > y_max:
                continue
            txt = normalize_spaces(el.text)
            if not txt or len(txt) > 40:
                continue
            if not regex.search(txt):
                continue
            key = txt.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append((txt, el))
        except Exception:
            pass
    return out


def _click_element(driver, el):
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        time.sleep(0.5)
        driver.execute_script("arguments[0].click();", el)
        return True
    except Exception:
        try:
            el.click()
            return True
        except Exception:
            return False


def detect_device_options(driver, base_name: str):
    regex = re.compile(r"^Galaxy\s+(?:S|A|Z|M)[A-Za-z0-9+ ]{0,18}$", flags=re.I)
    opts = _find_clickable_texts(driver, regex, y_max=1400)
    cleaned = []
    base_short = normalizar_nombre_samsung(base_name).replace('Samsung ', '')
    for txt, el in opts:
        txt_n = normalizar_nombre_samsung(txt)
        if should_skip_by_name(txt_n):
            continue
        # Si solo está el mismo modelo seleccionado, no lo tratamos como selector múltiple.
        cleaned.append((txt_n, el))

    unique = {}
    for name, el in cleaned:
        unique.setdefault(name, el)

    if len(unique) <= 1:
        return []
    return list(unique.items())


def detect_storage_options(driver):
    regex_combo = re.compile(r"^\d+\s*(?:GB|TB)\s*[\|｜/+]+\s*\d+\s*GB$", flags=re.I)
    regex_single = re.compile(r"^\d+\s*(?:GB|TB)$", flags=re.I)

    opts = _find_clickable_texts(driver, regex_combo, y_max=1700)
    if opts:
        return opts
    return _find_clickable_texts(driver, regex_single, y_max=1700)


def extract_variants_from_buy_page(buy_url: str, base_name: str, fallback_img: str = ""):
    buy_url = normalize_product_url(buy_url)
    if buy_url in BUY_PAGE_CACHE:
        return BUY_PAGE_CACHE[buy_url]

    out = []
    driver = get_driver()
    try:
        print(f"🛒 Analizando buy page: {mask_url(buy_url)}", flush=True)
        driver.set_page_load_timeout(45)
        driver.get(buy_url)
        time.sleep(3)
        dismiss_overlays(driver)
        time.sleep(1.5)

        # base name definitivo desde h1 renderizado
        try:
            from selenium.webdriver.common.by import By
            h1s = driver.find_elements(By.TAG_NAME, 'h1')
            if h1s:
                h1_txt = normalize_spaces(h1s[0].text)
                if h1_txt:
                    base_name = normalizar_nombre_samsung(h1_txt)
        except Exception:
            pass

        coupon_default = CODIGO_DESCUENTO_DEFAULT
        device_opts = detect_device_options(driver, base_name)
        device_groups = [(base_name, None)] if not device_opts else device_opts

        for device_name, device_el in device_groups:
            if device_el is not None:
                _click_element(driver, device_el)
                time.sleep(1.8)

            device_name = normalizar_nombre_samsung(device_name)
            storage_opts = detect_storage_options(driver)

            if not storage_opts:
                cur, orig, coupon = extract_price_coupon_from_driver(driver)
                if cur > 0:
                    out.append({
                        'nombre': device_name,
                        'capacidad': '',
                        'memoria': '',
                        'precio_actual': cur,
                        'precio_original': orig,
                        'codigo_descuento': coupon or coupon_default,
                        'img': fallback_img,
                    })
                continue

            seen_variant_keys = set()
            for opt_text, opt_el in storage_opts:
                cap, ram = parse_variant_option_text(opt_text)
                if not cap and not ram:
                    continue

                if not _click_element(driver, opt_el):
                    continue
                time.sleep(1.8)

                cur, orig, coupon = extract_price_coupon_from_driver(driver)
                if cur <= 0:
                    continue

                k = source_key(device_name, ram, cap, FUENTE)
                if k in seen_variant_keys:
                    continue
                seen_variant_keys.add(k)

                out.append({
                    'nombre': device_name,
                    'capacidad': cap,
                    'memoria': ram,
                    'precio_actual': cur,
                    'precio_original': orig,
                    'codigo_descuento': coupon or coupon_default,
                    'img': fallback_img,
                })

    except Exception as e:
        print(f"⚠️ Error extrayendo variantes Samsung desde {mask_url(buy_url)}: {e}", flush=True)
    finally:
        try:
            driver.quit()
        except Exception:
            pass

    # Deduplicar por source key y preservar el precio más bajo si se repite.
    dedup = {}
    for item in out:
        k = source_key(item['nombre'], item['memoria'], item['capacidad'], FUENTE)
        if k not in dedup:
            dedup[k] = item
        else:
            prev = dedup[k]
            if int(item.get('precio_actual', 10**9)) < int(prev.get('precio_actual', 10**9)):
                dedup[k] = item

    BUY_PAGE_CACHE[buy_url] = list(dedup.values())
    return BUY_PAGE_CACHE[buy_url]


def find_matching_variant(variants, nombre: str, capacidad: str, memoria: str):
    if not variants:
        return None
    target_name = normalizar_nombre_samsung(nombre)
    target_cap = (capacidad or '').upper()
    target_ram = (memoria or '').upper()

    # Exacto por nombre + cap + ram.
    for v in variants:
        if normalizar_nombre_samsung(v.get('nombre', '')) != target_name:
            continue
        if (v.get('capacidad', '') or '').upper() != target_cap:
            continue
        if (v.get('memoria', '') or '').upper() != target_ram:
            continue
        return v

    # Exacto por cap + ram.
    for v in variants:
        if (v.get('capacidad', '') or '').upper() != target_cap:
            continue
        if (v.get('memoria', '') or '').upper() != target_ram:
            continue
        return v

    # Fallback por capacidad.
    for v in variants:
        if target_cap and (v.get('capacidad', '') or '').upper() == target_cap:
            return v

    return None


# --------------------------
# EXTRACCIÓN REMOTA
# --------------------------

def obtener_datos_remotos():
    print("", flush=True)
    print("--- FASE 1: ESCANEANDO SAMSUNG ---", flush=True)
    print(f"URL base: {mask_url(START_URL)}", flush=True)

    session = requests.Session()
    session.headers.update(HEADERS)

    discovered = []
    seen_discovered = set()

    for label, listing_url in LISTING_URLS:
        try:
            print("-" * 60, flush=True)
            print(f"Escaneando listado Samsung: {mask_url(listing_url)}", flush=True)
            html = get_rendered_html(listing_url)
            urls = descubrir_urls_producto(html, listing_url)
            print(f"✅ URLs de producto detectadas: {len(urls)}", flush=True)
            for u in sorted(urls):
                if u in seen_discovered:
                    continue
                seen_discovered.add(u)
                discovered.append((u, label, listing_url))
        except Exception as e:
            print(f"❌ Error leyendo listado Samsung {mask_url(listing_url)}: {e}", flush=True)

    productos_por_clave = {}

    for url, label, listing_url in discovered:
        try:
            print("-" * 60, flush=True)
            print(f"🔎 Inspeccionando: {mask_url(url)}", flush=True)
            info = extract_detail_info(url, label, listing_url, session)
            if not info:
                continue

            nombre = info['nombre']
            if should_skip_by_name(nombre):
                continue

            if info['exact_variant'] and info['capacidad'] and info['memoria']:
                variants = extract_variants_from_buy_page(info['buy_url'], nombre, info['img'])
                v = find_matching_variant(variants, nombre, info['capacidad'], info['memoria'])
                if not v:
                    # Fallback defensivo: si no hay variante exacta, intenta precio directo renderizado en detalle.
                    v = {
                        'nombre': nombre,
                        'capacidad': info['capacidad'],
                        'memoria': info['memoria'],
                        'precio_actual': 0,
                        'precio_original': 0,
                        'codigo_descuento': CODIGO_DESCUENTO_DEFAULT,
                        'img': info['img'],
                    }

                precio_actual = int(v.get('precio_actual') or 0)
                if precio_actual <= 0:
                    print(f"⚠️ Sin precio usable para {nombre} {info['capacidad']} {info['memoria']}. Se ignora.", flush=True)
                    continue

                precio_original = int(v.get('precio_original') or 0) or calcular_precio_original(precio_actual)
                codigo = v.get('codigo_descuento') or CODIGO_DESCUENTO_DEFAULT
                img = v.get('img') or info['img']
                key = source_key(nombre, info['memoria'], info['capacidad'], FUENTE)

                remoto = {
                    'nombre': nombre,
                    'memoria': info['memoria'],
                    'capacidad': info['capacidad'],
                    'precio_actual': precio_actual,
                    'precio_original': precio_original,
                    'img': img,
                    'url_imp': info['url_imp'],
                    'url_oferta_sin_acortar': info['url_imp'],
                    'url_importada_sin_afiliado': info['url_imp'],
                    'buy_url': info['buy_url'],
                    'enviado_desde': ENVIADO_DESDE,
                    'enviado_desde_tg': ENVIADO_DESDE_TG,
                    'fecha': datetime.now().strftime('%d/%m/%Y'),
                    'version': VERSION,
                    'fuente': FUENTE,
                    'codigo_descuento': codigo,
                    'origen_pagina': label,
                    'origen_listado': listing_url,
                    'source_key': key,
                    'model_code': info.get('model_code', ''),
                }

                if key in productos_por_clave:
                    prev = productos_por_clave[key]
                    summary_duplicados.append(f"{nombre} {info['capacidad']} {info['memoria']}")
                    if int(remoto['precio_actual']) < int(prev.get('precio_actual', 10**9)):
                        productos_por_clave[key] = remoto
                else:
                    productos_por_clave[key] = remoto
                continue

            # Páginas de familia / sin variante exacta: iteramos buy page y generamos variantes.
            variants = extract_variants_from_buy_page(info['buy_url'], nombre, info['img'])
            for v in variants:
                v_nombre = normalizar_nombre_samsung(v.get('nombre') or nombre)
                v_cap = (v.get('capacidad') or info.get('capacidad') or '').upper()
                v_ram = (v.get('memoria') or info.get('memoria') or '').upper()
                if not v_cap or not v_ram:
                    continue
                precio_actual = int(v.get('precio_actual') or 0)
                if precio_actual <= 0:
                    continue
                precio_original = int(v.get('precio_original') or 0) or calcular_precio_original(precio_actual)
                key = source_key(v_nombre, v_ram, v_cap, FUENTE)

                remoto = {
                    'nombre': v_nombre,
                    'memoria': v_ram,
                    'capacidad': v_cap,
                    'precio_actual': precio_actual,
                    'precio_original': precio_original,
                    'img': v.get('img') or info['img'],
                    'url_imp': info['url_imp'],
                    'url_oferta_sin_acortar': info['url_imp'],
                    'url_importada_sin_afiliado': info['url_imp'],
                    'buy_url': info['buy_url'],
                    'enviado_desde': ENVIADO_DESDE,
                    'enviado_desde_tg': ENVIADO_DESDE_TG,
                    'fecha': datetime.now().strftime('%d/%m/%Y'),
                    'version': VERSION,
                    'fuente': FUENTE,
                    'codigo_descuento': v.get('codigo_descuento') or CODIGO_DESCUENTO_DEFAULT,
                    'origen_pagina': label,
                    'origen_listado': listing_url,
                    'source_key': key,
                    'model_code': info.get('model_code', ''),
                }

                if key in productos_por_clave:
                    prev = productos_por_clave[key]
                    summary_duplicados.append(f"{v_nombre} {v_cap} {v_ram}")
                    if int(remoto['precio_actual']) < int(prev.get('precio_actual', 10**9)):
                        productos_por_clave[key] = remoto
                else:
                    productos_por_clave[key] = remoto

        except Exception as e:
            print(f"❌ ERROR extrayendo Samsung desde {mask_url(url)}: {e}", flush=True)
            summary_fallidos.append({'nombre': url, 'error': str(e)})

        if len(productos_por_clave) >= OBJETIVO:
            break

    productos = list(productos_por_clave.values())
    print("", flush=True)
    print("📊 RESUMEN EXTRACCIÓN SAMSUNG:", flush=True)
    print(f"   URLs descubiertas: {len(discovered)}", flush=True)
    print(f"   Productos únicos válidos: {len(productos)}", flush=True)
    return productos


# --------------------------
# WOO: CATEGORÍAS / IMÁGENES
# --------------------------

def obtener_todas_las_categorias():
    categorias = []
    page = 1
    while True:
        try:
            res = wcapi.get('products/categories', params={'per_page': 100, 'page': page}).json()
            if not res or 'message' in res or len(res) == 0:
                break
            categorias.extend(res)
            page += 1
        except Exception:
            break
    return categorias


def resolver_jerarquia(nombre_completo, cache_categorias):
    palabras = (nombre_completo or '').split()
    nombre_padre = palabras[0] if palabras else 'Otros'
    nombre_hijo = limpiar_nombre_para_categoria(nombre_completo)

    id_cat_padre = None
    id_cat_hijo = None

    for cat in cache_categorias:
        if cat.get('name', '').lower() == nombre_padre.lower() and cat.get('parent') == 0:
            id_cat_padre = cat.get('id')
            break
    if not id_cat_padre:
        res = wcapi.post('products/categories', {'name': nombre_padre}).json()
        id_cat_padre = res.get('id')
        cache_categorias.append(res)

    for cat in cache_categorias:
        if cat.get('name', '').lower() == nombre_hijo.lower() and cat.get('parent') == id_cat_padre:
            id_cat_hijo = cat.get('id')
            break
    if not id_cat_hijo:
        res = wcapi.post('products/categories', {'name': nombre_hijo, 'parent': id_cat_padre}).json()
        id_cat_hijo = res.get('id')
        cache_categorias.append(res)

    return id_cat_padre, id_cat_hijo


def obtener_imagen_categoria(cache_categorias, cat_id):
    if not cat_id:
        return ''
    for c in cache_categorias:
        if c.get('id') == cat_id:
            img = c.get('image') or {}
            return img.get('src') or ''
    return ''


def actualizar_imagen_categoria(cache_categorias, cat_id, img_src):
    if not cat_id or not img_src:
        return False
    if obtener_imagen_categoria(cache_categorias, cat_id):
        return False
    try:
        res = wcapi.put(f'products/categories/{cat_id}', {'image': {'src': img_src}}).json()
        for i, c in enumerate(cache_categorias):
            if c.get('id') == cat_id:
                cache_categorias[i] = res
                break
        return True
    except Exception:
        return False


# --------------------------
# WOO: SINCRONIZACIÓN
# --------------------------

def cargar_locales_samsung():
    locales = []
    page = 1
    while True:
        try:
            res = wcapi.get('products', params={'per_page': 100, 'page': page, 'status': 'any'}).json()
            if not res or 'message' in res:
                break
            for p in res:
                meta = {m['key']: str(m.get('value', '')) for m in p.get('meta_data', []) if isinstance(m, dict) and m.get('key')}
                if meta.get('importado_de', '').rstrip('/') == ID_IMPORTACION.rstrip('/'):
                    locales.append({'id': p['id'], 'nombre': p.get('name', ''), 'meta': meta})
            if len(res) < 100:
                break
            page += 1
        except Exception:
            break
    return locales


def build_local_key(local):
    meta = local.get('meta', {})
    if meta.get('_odm_source_key'):
        return meta['_odm_source_key']
    return source_key(
        local.get('nombre', ''),
        meta.get('memoria', ''),
        meta.get('capacidad', ''),
        meta.get('fuente', FUENTE),
    )


def sincronizar(remotos):
    print("\n--- FASE 2: SINCRONIZANDO SAMSUNG ---", flush=True)
    cache_categorias = obtener_todas_las_categorias()
    locales = cargar_locales_samsung()

    print(f"📦 Productos Samsung existentes en la web: {len(locales)}", flush=True)
    print(f"📦 Productos remotos Samsung a procesar: {len(remotos)}", flush=True)

    remote_by_key = {r['source_key']: r for r in remotos}
    local_by_key = {build_local_key(l): l for l in locales}

    # 1) Obsoletos
    for key, local in local_by_key.items():
        if key in remote_by_key:
            continue
        try:
            wcapi.delete(f"products/{local['id']}", params={'force': True})
            summary_eliminados.append({'nombre': local['nombre'], 'id': local['id']})
            print(f"🗑️ ELIMINADO (obsoleto) -> {local['nombre']} (ID: {local['id']})", flush=True)
        except Exception as e:
            print(f"❌ Error eliminando obsoleto {local['nombre']}: {e}", flush=True)
            summary_fallidos.append({'nombre': local['nombre'], 'id': local['id'], 'error': str(e)})

    # 2) Crear / actualizar
    for r in remotos:
        try:
            print("-" * 60, flush=True)
            print(f"Detectado {r.get('nombre', '(sin nombre)')}", flush=True)
            print(f"1) Nombre: {r.get('nombre', '')}", flush=True)
            print(f"2) Memoria: {r.get('memoria', '')}", flush=True)
            print(f"3) Capacidad: {r.get('capacidad', '')}", flush=True)
            print(f"4) Versión: {r.get('version', VERSION)}", flush=True)
            print(f"5) Fuente: {r.get('fuente', FUENTE)}", flush=True)
            print(f"6) Precio actual: {r.get('precio_actual', 0)}", flush=True)
            print(f"7) Precio original: {r.get('precio_original', 0)}", flush=True)
            print(f"8) Código de descuento: {r.get('codigo_descuento', CODIGO_DESCUENTO_DEFAULT)}", flush=True)
            print(f"9) URL Imagen: {r.get('img', '')}", flush=True)
            print(f"10) Enlace Importado: {mask_url(r.get('url_imp', ''))}", flush=True)
            print(f"11) Enlace Expandido: {mask_url(r.get('url_oferta_sin_acortar', ''))}", flush=True)
            print(f"12) URL importada sin afiliado: {mask_url(r.get('url_importada_sin_afiliado', ''))}", flush=True)

            url_base = (r.get('url_importada_sin_afiliado') or r.get('url_imp') or '').strip().split('?')[0]
            url_con_afiliado = f"{url_base}{AFF_RAW}" if AFF_RAW else url_base
            url_oferta = acortar_url(url_con_afiliado)
            print(f"13) URL sin acortar con mi afiliado: {mask_url(url_con_afiliado)}", flush=True)
            print(f"14) URL acortada con mi afiliado: {url_oferta}", flush=True)
            print(f"15) Enviado desde: {r.get('enviado_desde', ENVIADO_DESDE)}", flush=True)
            print(f"16) Importado de: {ID_IMPORTACION}", flush=True)
            print("17) Encolado para comparar con base de datos...", flush=True)
            print("-" * 60, flush=True)

            match = local_by_key.get(r['source_key'])
            id_padre, id_hijo = resolver_jerarquia(r['nombre'], cache_categorias)

            img_subcat = obtener_imagen_categoria(cache_categorias, id_hijo)
            if (not img_subcat) and r.get('img'):
                actualizar_imagen_categoria(cache_categorias, id_hijo, r['img'])
                img_subcat = obtener_imagen_categoria(cache_categorias, id_hijo)
            img_final_producto = img_subcat or r.get('img') or ''

            if match:
                meta = match['meta']
                cambios = []
                payload = {'meta_data': []}

                def _num_meta(k):
                    try:
                        return int(round(float(meta.get(k, 0) or 0)))
                    except Exception:
                        return 0

                old_actual = _num_meta('precio_actual')
                old_original = _num_meta('precio_original')
                if int(r['precio_actual']) != old_actual:
                    cambios.append(f"precio_actual: {old_actual}€ -> {r['precio_actual']}€")
                    payload['sale_price'] = str(r['precio_actual'])
                    payload['meta_data'].append({'key': 'precio_actual', 'value': str(r['precio_actual'])})

                if int(r['precio_original']) != old_original:
                    cambios.append(f"precio_original: {old_original}€ -> {r['precio_original']}€")
                    payload['regular_price'] = str(r['precio_original'])
                    payload['meta_data'].append({'key': 'precio_original', 'value': str(r['precio_original'])})

                # Otros metadatos, por si cambian.
                compare_meta = {
                    'codigo_de_descuento': r.get('codigo_descuento', CODIGO_DESCUENTO_DEFAULT),
                    'enviado_desde': r.get('enviado_desde', ENVIADO_DESDE),
                    'enviado_desde_tg': r.get('enviado_desde_tg', ENVIADO_DESDE_TG),
                    'version': r.get('version', VERSION),
                    'imagen_producto': r.get('img', ''),
                    'url_sin_acortar_con_mi_afiliado': url_con_afiliado,
                    'url_oferta': url_oferta,
                    'url_importada_sin_afiliado': url_base,
                    'url_oferta_sin_acortar': r.get('url_oferta_sin_acortar', url_base),
                }
                for k, v in compare_meta.items():
                    if str(meta.get(k, '')) != str(v):
                        cambios.append(f"{k}: {meta.get(k, '')} -> {v}")
                        payload['meta_data'].append({'key': k, 'value': v})

                if cambios:
                    wcapi.put(f"products/{match['id']}", payload)
                    summary_actualizados.append({'nombre': r['nombre'], 'id': match['id'], 'cambios': cambios})
                    print(f"🔄 ACTUALIZADO -> {r['nombre']} (ID: {match['id']})", flush=True)
                else:
                    summary_ignorados.append({'nombre': r['nombre'], 'id': match['id']})
                    print(f"⏭️ SIN CAMBIOS -> {r['nombre']} (ID: {match['id']})", flush=True)
                continue

            # CREAR
            data = {
                'name': r['nombre'],
                'type': 'simple',
                'status': 'publish',
                'regular_price': str(r['precio_original']),
                'sale_price': str(r['precio_actual']),
                'categories': [{'id': id_padre}, {'id': id_hijo}] if id_hijo else ([{'id': id_padre}] if id_padre else []),
                'images': [{'src': img_final_producto}] if img_final_producto else [],
                'meta_data': [
                    {'key': 'nombre_movil_final', 'value': r['nombre']},
                    {'key': 'importado_de', 'value': ID_IMPORTACION},
                    {'key': 'fecha', 'value': r['fecha']},
                    {'key': 'memoria', 'value': r['memoria']},
                    {'key': 'capacidad', 'value': r['capacidad']},
                    {'key': 'fuente', 'value': FUENTE},
                    {'key': 'precio_actual', 'value': str(r['precio_actual'])},
                    {'key': 'precio_original', 'value': str(r['precio_original'])},
                    {'key': 'codigo_de_descuento', 'value': r.get('codigo_descuento', CODIGO_DESCUENTO_DEFAULT)},
                    {'key': 'enviado_desde', 'value': ENVIADO_DESDE},
                    {'key': 'enviado_desde_tg', 'value': ENVIADO_DESDE_TG},
                    {'key': 'enlace_de_compra_importado', 'value': url_base},
                    {'key': 'url_oferta_sin_acortar', 'value': r.get('url_oferta_sin_acortar', url_base)},
                    {'key': 'url_importada_sin_afiliado', 'value': url_base},
                    {'key': 'url_sin_acortar_con_mi_afiliado', 'value': url_con_afiliado},
                    {'key': 'url_oferta', 'value': url_oferta},
                    {'key': 'imagen_producto', 'value': r.get('img', '')},
                    {'key': 'version', 'value': r.get('version', VERSION)},
                    {'key': '_odm_source_key', 'value': r['source_key']},
                    {'key': '_odm_source_model_code', 'value': r.get('model_code', '')},
                    {'key': '_odm_source_listing', 'value': r.get('origen_listado', '')},
                    {'key': '_odm_source_page', 'value': r.get('origen_pagina', '')},
                ],
            }

            intentos = 0
            creado = False
            while intentos < 10 and not creado:
                intentos += 1
                try:
                    res = wcapi.post('products', data)
                    if res.status_code in (200, 201):
                        prod = res.json()
                        new_id = prod.get('id')
                        summary_creados.append({'nombre': r['nombre'], 'id': new_id})
                        print(f"✅ CREADO -> {r['nombre']} (ID: {new_id})", flush=True)

                        try:
                            permalink = prod.get('permalink', '')
                            if permalink:
                                url_short = acortar_url(permalink)
                                wcapi.put(
                                    f"products/{new_id}",
                                    {'meta_data': [{'key': 'url_post_acortada', 'value': url_short}]},
                                )
                        except Exception:
                            pass
                        creado = True
                    else:
                        body_preview = (res.text or '').replace('\n', ' ')[:250]
                        print(f"⚠️ Woo error {res.status_code}: {body_preview}", flush=True)
                except Exception as e:
                    print(f"⚠️ Excepción Woo creando Samsung: {e}", flush=True)

                if (not creado) and (intentos < 10):
                    time.sleep(15)

            if not creado:
                summary_fallidos.append({'nombre': r['nombre'], 'error': 'No se pudo crear en WooCommerce'})
                print(f"❌ NO SE PUDO CREAR: {r['nombre']}", flush=True)

        except Exception as e:
            print(f"❌ ERROR sincronizando Samsung {r.get('nombre', '?')}: {e}", flush=True)
            summary_fallidos.append({'nombre': r.get('nombre', '?'), 'error': str(e)})

    hoy_fmt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print("\n============================================================", flush=True)
    print(f"📋 RESUMEN DE EJECUCIÓN ({hoy_fmt})", flush=True)
    print("============================================================", flush=True)
    print(f"\na) ARTICULOS CREADOS: {len(summary_creados)}", flush=True)
    for item in summary_creados:
        print(f"- {item['nombre']} (ID: {item['id']})", flush=True)
    print(f"\nb) ARTICULOS ELIMINADOS (OBSOLETOS): {len(summary_eliminados)}", flush=True)
    for item in summary_eliminados:
        print(f"- {item['nombre']} (ID: {item['id']})", flush=True)
    print(f"\nc) ARTICULOS ACTUALIZADOS: {len(summary_actualizados)}", flush=True)
    for item in summary_actualizados:
        print(f"- {item['nombre']} (ID: {item['id']}): {', '.join(item['cambios'])}", flush=True)
    print(f"\nd) ARTICULOS IGNORADOS (SIN CAMBIOS): {len(summary_ignorados)}", flush=True)
    for item in summary_ignorados:
        print(f"- {item['nombre']} (ID: {item['id']})", flush=True)
    print(f"\ne) DUPLICADOS DETECTADOS: {len(summary_duplicados)}", flush=True)
    for item in dedupe_keep_order(summary_duplicados):
        print(f"- {item}", flush=True)
    print(f"\nf) FALLIDOS: {len(summary_fallidos)}", flush=True)
    for item in summary_fallidos[:50]:
        if isinstance(item, dict):
            print(f"- {item.get('nombre', '?')}: {item.get('error', '')}", flush=True)
        else:
            print(f"- {item}", flush=True)
    print("============================================================", flush=True)


def main():
    remotos = obtener_datos_remotos()
    if remotos:
        sincronizar(remotos)
    else:
        print('No se han obtenido productos remotos de Samsung.', flush=True)


if __name__ == '__main__':
    main()
