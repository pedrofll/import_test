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
#  SAMSUNG SCRAPER (LISTING-ONLY + WC SYNC)
# ============================================================
# Reglas operativas:
#  - Solo lee https://www.samsung.com/es/smartphones/all-smartphones/
#  - No descubre productos desde fichas; usa cards visibles + JSON-LD del listing.
#  - La URL base importada se guarda SIN /buy/?...
#  - La URL expandida conserva el enlace real del boton Comprar (si existe).
#  - El afiliado usa AFF_SAMSUNG sobre la URL base.
#  - La imagen del producto usa SOLO la imagen existente en la subcategoria exacta.
#    Si la subcategoria no existe o no tiene imagen propia, no se pone ninguna.
# ============================================================

DEFAULT_START_URL = "https://www.samsung.com/es/smartphones/all-smartphones/"
START_URL = (os.getenv("SOURCE_URL_SAMSUNG") or DEFAULT_START_URL).strip() or DEFAULT_START_URL
START_URL = START_URL.rstrip("/") + "/"

FUENTE = "Samsung"
ID_IMPORTACION = START_URL.rstrip("/")
ENVIADO_DESDE = "España"
ENVIADO_DESDE_TG = "🇪🇸 España"
VERSION = "Versión Global"
CODIGO_DESCUENTO_DEFAULT = "OFERTA: PROMO."

AFF_SAMSUNG = (os.getenv("AFF_SAMSUNG") or "").strip()
if AFF_SAMSUNG and not AFF_SAMSUNG.startswith("?") and not AFF_SAMSUNG.startswith("&"):
    AFF_SAMSUNG = "?" + AFF_SAMSUNG

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
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

summary_creados = []
summary_eliminados = []
summary_actualizados = []
summary_ignorados = []
summary_fallidos = []
summary_duplicados = []

EURO_AMOUNT_RE = r"(\d{1,3}(?:\.\d{3})*(?:,\d{1,2})?|\d{1,5}(?:[\.,]\d{1,2})?)\s*€"
CAP_RE = re.compile(r"\b(64|128|256|512|1024)\s*GB\b|\b(1|2)\s*TB\b", re.I)

# Mapa de RAM verificado con la documentacion oficial actual de Samsung ES.
RAM_BY_NAME_CAP = {
    ("Samsung Galaxy S26", "256GB"): "12GB",
    ("Samsung Galaxy S26", "512GB"): "12GB",
    ("Samsung Galaxy S26+", "256GB"): "12GB",
    ("Samsung Galaxy S26+", "512GB"): "12GB",
    ("Samsung Galaxy S26 Ultra", "256GB"): "12GB",
    ("Samsung Galaxy S26 Ultra", "512GB"): "12GB",
    ("Samsung Galaxy S26 Ultra", "1TB"): "16GB",
    ("Samsung Galaxy Z Fold7", "256GB"): "12GB",
    ("Samsung Galaxy Z Fold7", "512GB"): "12GB",
    ("Samsung Galaxy Z Fold7", "1TB"): "16GB",
    ("Samsung Galaxy Z Flip7", "256GB"): "12GB",
    ("Samsung Galaxy Z Flip7", "512GB"): "12GB",
    ("Samsung Galaxy Z Flip7 FE", "128GB"): "8GB",
    ("Samsung Galaxy Z Flip7 FE", "256GB"): "8GB",
    ("Samsung Galaxy S25", "128GB"): "12GB",
    ("Samsung Galaxy S25", "256GB"): "12GB",
    ("Samsung Galaxy S25", "512GB"): "12GB",
    ("Samsung Galaxy S25+", "256GB"): "12GB",
    ("Samsung Galaxy S25+", "512GB"): "12GB",
    ("Samsung Galaxy S25 Ultra", "256GB"): "12GB",
    ("Samsung Galaxy S25 Ultra", "512GB"): "12GB",
    ("Samsung Galaxy S25 Ultra", "1TB"): "12GB",
    ("Samsung Galaxy S25 FE", "128GB"): "8GB",
    ("Samsung Galaxy S25 FE", "256GB"): "8GB",
    ("Samsung Galaxy S25 FE", "512GB"): "8GB",
    ("Samsung Galaxy A57 5G", "512GB"): "12GB",
    ("Samsung Galaxy A37 5G", "256GB"): "8GB",
    ("Samsung Galaxy A26 5G", "256GB"): "8GB",
    ("Samsung Galaxy A17 5G", "256GB"): "8GB",
    ("Samsung Galaxy A17", "256GB"): "8GB",
    ("Samsung Galaxy A16", "256GB"): "8GB",
    ("Samsung Galaxy S25 Edge", "512GB"): "12GB",
}


def normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def mask_url(url: str) -> str:
    try:
        u = urllib.parse.urlsplit(url)
        base = f"{u.scheme}://{u.netloc}{u.path}"
        return base + ("?***" if u.query else "")
    except Exception:
        return "***"


def parse_eur_num(num_txt: str) -> int:
    if not num_txt:
        return 0
    n = str(num_txt).strip().replace(" ", "")
    n = n.replace(".", "").replace(",", ".")
    try:
        return int(round(float(n)))
    except Exception:
        return 0


def parse_eur_all(txt: str):
    if not txt:
        return []
    vals = []
    for m in re.finditer(EURO_AMOUNT_RE, txt, flags=re.I):
        v = parse_eur_num(m.group(1))
        if v > 0:
            vals.append(v)
    return vals


def abs_url(base: str, href: str) -> str:
    try:
        if not href:
            return ""
        if href.startswith("//"):
            href = "https:" + href
        return urllib.parse.urljoin(base, href)
    except Exception:
        return href or ""


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


def calcular_precio_original(precio_actual: int, factor: float = 1.20) -> int:
    try:
        pa = int(precio_actual)
    except Exception:
        return 0
    if pa <= 0:
        return 0
    return int(math.ceil(pa * factor))


def unir_afiliado(url_base: str, aff: str) -> str:
    base = (url_base or "").strip().replace("&amp;", "&")
    a = (aff or "").strip()
    if not base or not a:
        return base
    if a.lower().startswith("http"):
        return a
    if a.startswith("?"):
        return base + ("&" + a[1:] if "?" in base else a)
    if a.startswith("&"):
        return base + (a if "?" in base else "?" + a[1:])
    return base + (("&" if "?" in base else "?") + a)


def should_skip_by_name(nombre: str) -> bool:
    u = (nombre or "").upper()
    return any(x in u for x in [" TAB", "IPAD", " PAD"]) or u.startswith("TAB ")


def is_generic_name(nombre: str) -> bool:
    t = normalizar_nombre_samsung(nombre)
    base = t.replace("Samsung ", "")
    generic = {
        "Galaxy",
        "Galaxy A",
        "Galaxy S",
        "Galaxy Z",
        "Samsung Galaxy",
        "Samsung Galaxy A",
        "Samsung Galaxy S",
        "Samsung Galaxy Z",
    }
    return (not t) or (t in generic) or (base in generic)


def normalizar_nombre_samsung(nombre: str) -> str:
    t = normalize_spaces(nombre)
    if not t:
        return ""

    t = re.sub(r"\bExclusivo Online\b", "", t, flags=re.I)
    t = re.sub(r"\bComprar\b.*$", "", t, flags=re.I)
    t = re.sub(r"\b(64|128|256|512|1024)\s*GB\b", "", t, flags=re.I)
    t = re.sub(r"\b(1|2)\s*TB\b", "", t, flags=re.I)
    t = normalize_spaces(t)

    if t.lower().startswith("samsung "):
        t = t[len("Samsung "):]
    if not t.lower().startswith("galaxy "):
        t = "Galaxy " + t

    words = []
    for raw in t.split():
        low = raw.lower()
        if low == "galaxy":
            words.append("Galaxy")
        elif low in {"z", "s", "a"}:
            words.append(low.upper())
        elif low == "fe":
            words.append("FE")
        elif low == "5g":
            words.append("5G")
        elif re.fullmatch(r"s\d+\+?", low):
            words.append(raw[0].upper() + raw[1:])
        elif re.fullmatch(r"a\d+", low):
            words.append(raw[0].upper() + raw[1:])
        elif re.fullmatch(r"fold\d+", low):
            words.append("Fold" + raw[len("fold"):])
        elif re.fullmatch(r"flip\d+", low):
            words.append("Flip" + raw[len("flip"):])
        elif low in {"ultra", "edge", "plus"}:
            words.append(low.title())
        elif re.fullmatch(r"\d+(gb|tb)", low):
            num = re.match(r"\d+", low).group(0)
            unit = low[len(num):].upper()
            words.append(num + unit)
        else:
            words.append(raw[:1].upper() + raw[1:])

    out = normalize_spaces(" ".join(words))
    if not out.lower().startswith("galaxy "):
        out = "Galaxy " + out
    return normalize_spaces("Samsung " + out)


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


def source_key(nombre: str, memoria: str, capacidad: str, fuente: str = FUENTE) -> str:
    return f"{normalize_spaces(nombre).lower()}|{str(memoria).upper()}|{str(capacidad).upper()}|{fuente.lower()}"


def canonical_samsung_import_url(url: str) -> str:
    try:
        raw = normalize_spaces(url)
        if not raw or raw.lower().startswith("javascript"):
            return ""
        u = urllib.parse.urlsplit(raw)
        scheme = u.scheme or "https"
        netloc = u.netloc or "www.samsung.com"
        path = (u.path or "").rstrip("/")
        path = re.sub(r"/buy$", "", path, flags=re.I)
        return f"{scheme}://{netloc}{path}"
    except Exception:
        return (url or "").strip()


def extraer_model_code(url: str) -> str:
    try:
        q = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
        vals = q.get("modelCode") or q.get("modelcode") or []
        if vals:
            return vals[0].strip().upper()
    except Exception:
        pass
    m = re.search(r"\bSM-[A-Z0-9]+\b", url or "", flags=re.I)
    return m.group(0).upper() if m else ""


def infer_memoria_samsung_desde_listing(nombre: str, capacidad: str) -> str:
    return RAM_BY_NAME_CAP.get((normalizar_nombre_samsung(nombre), (capacidad or "").upper()), "")


def clean_image_ref(url: str) -> str:
    try:
        if not url:
            return ""
        u = urllib.parse.urlsplit(url)
        return f"{u.netloc}{u.path}".lower().rstrip("/")
    except Exception:
        return (url or "").lower().split("?")[0].rstrip("/")


def same_image_ref(url_a: str, url_b: str) -> bool:
    a = clean_image_ref(url_a)
    b = clean_image_ref(url_b)
    if not a or not b:
        return False
    if a == b:
        return True
    return a.split("/")[-1] == b.split("/")[-1]


# --------------------------
# SELENIUM (solo listing principal)
# --------------------------

def get_driver():
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options

    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1440,2800")
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
                            time.sleep(0.5)
                        except Exception:
                            pass
            except Exception:
                pass


def scroll_page(driver, rounds: int = 12):
    last_h = 0
    stable = 0
    for _ in range(rounds):
        try:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1.0)
            h = driver.execute_script("return document.body.scrollHeight")
            if h == last_h:
                stable += 1
            else:
                stable = 0
            last_h = h
            if stable >= 2:
                break
        except Exception:
            break


# --------------------------
# PARSEO JSON-LD DEL LISTING
# --------------------------

def parse_jsonld_listing_items(html: str):
    soup = BeautifulSoup(html, "html.parser")
    out = []

    def _iter_nodes(data):
        if isinstance(data, dict):
            yield data
            for v in data.values():
                yield from _iter_nodes(v)
        elif isinstance(data, list):
            for v in data:
                yield from _iter_nodes(v)

    for sc in soup.find_all("script", attrs={"type": re.compile(r"ld\+json", re.I)}):
        raw = (sc.string or sc.get_text() or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        nodes = list(_iter_nodes(data))
        for node in nodes:
            if not isinstance(node, dict):
                continue
            if str(node.get("@type") or "") != "Product":
                continue
            name = normalizar_nombre_samsung(node.get("name") or "")
            url = abs_url(START_URL, str(node.get("url") or "").strip())
            image = abs_url(START_URL, str(node.get("image") or "").strip())
            offers = node.get("offers") or {}
            price = 0
            if isinstance(offers, dict):
                price = parse_eur_num(str(offers.get("price") or ""))
            if not name:
                continue
            out.append({
                "name": name,
                "price": price,
                "buy_url": url,
                "base_url": canonical_samsung_import_url(url),
                "image": image,
                "model_code": extraer_model_code(url),
            })

    dedup = {}
    for item in out:
        key = (item["name"], item["price"], item["buy_url"])
        dedup[key] = item
    return list(dedup.values())


# --------------------------
# CARD EXTRACTION
# --------------------------

def collect_listing_card_roots(driver):
    from selenium.webdriver.common.by import By

    roots = []
    seen = set()
    try:
        buy_els = driver.find_elements(By.XPATH, "//*[self::a or self::button][contains(normalize-space(.), 'Comprar')]")
    except Exception:
        buy_els = []

    for btn in buy_els:
        try:
            if not btn.is_displayed():
                continue
        except Exception:
            continue

        current = btn
        chosen = None
        for _ in range(12):
            try:
                current = current.find_element(By.XPATH, "./..")
            except Exception:
                break
            txt = normalize_spaces(getattr(current, "text", "") or "")
            if not txt:
                continue
            if "Galaxy" not in txt or "€" not in txt:
                continue
            if not CAP_RE.search(txt):
                continue
            chosen = current
            break

        if not chosen:
            continue

        try:
            txt = normalize_spaces(chosen.text)
            key = normalize_spaces(txt[:220]).lower()
            if key in seen:
                continue
            seen.add(key)
            roots.append(chosen)
        except Exception:
            pass
    return roots


def card_lines(card):
    try:
        raw = card.text or ""
    except Exception:
        raw = ""
    out = []
    for line in raw.splitlines():
        line = normalize_spaces(line)
        if line:
            out.append(line)
    return out


def extract_title_from_card(card):
    lines = card_lines(card)
    candidates = []
    for line in lines:
        if "Galaxy" not in line:
            continue
        if "Comprar" in line:
            continue
        if "€" in line:
            continue
        if len(line) > 90:
            continue
        candidates.append(line)
    if candidates:
        return candidates[0]

    text = normalize_spaces(" ".join(lines))
    m = re.search(
        r"((?:Samsung\s+)?Galaxy\s+(?:Z\s+)?(?:Flip\d+|Fold\d+|S\d+\+?|A\d+)(?:\s+Ultra|\s+Edge|\s+FE)?(?:\s+5G)?)",
        text,
        flags=re.I,
    )
    return normalize_spaces(m.group(1)) if m else ""


def extract_selected_capacity_from_card(card):
    lines = card_lines(card)
    for line in lines:
        cap = parse_capacidad_desde_texto(line)
        if cap:
            return cap
    text = normalize_spaces(" ".join(lines))
    return parse_capacidad_desde_texto(text)


def extract_card_price_info(card):
    text = normalize_spaces(" ".join(card_lines(card)))
    prices = [v for v in parse_eur_all(text) if v >= 50]
    if not prices:
        return 0, 0
    cur = min(prices)
    orig = max(prices)
    if orig < cur:
        orig = cur
    if orig == cur:
        orig = calcular_precio_original(cur)
    return cur, orig


def extract_buy_url_from_card(card):
    from selenium.webdriver.common.by import By

    # Prioridad: enlaces reales del boton comprar.
    xpaths = [
        ".//*[@href]",
        ".//*[@data-href]",
        ".//*[@data-url]",
        ".//*[@onclick]",
    ]
    for xp in xpaths:
        try:
            els = card.find_elements(By.XPATH, xp)
        except Exception:
            els = []
        for el in els:
            candidates = []
            for attr in ("href", "data-href", "data-url", "onclick"):
                try:
                    v = (el.get_attribute(attr) or "").strip()
                except Exception:
                    v = ""
                if v:
                    candidates.append(v)
            for v in candidates:
                m = re.search(r'https://www\.samsung\.com/es/smartphones/[^"\'\s]+', v, flags=re.I)
                if m:
                    return abs_url(START_URL, m.group(0))
                if "/es/smartphones/" in v:
                    return abs_url(START_URL, v)
    return ""


# --------------------------
# JSON-LD MATCHING
# --------------------------

def item_name_key(nombre: str) -> str:
    t = normalizar_nombre_samsung(nombre).lower()
    t = t.replace("samsung ", "")
    t = t.replace("exclusivo online", "")
    return normalize_spaces(t)


def select_jsonld_match(card_name: str, current_price: int, items):
    if not items:
        return None

    key = item_name_key(card_name)
    candidates = [it for it in items if item_name_key(it["name"]) == key]
    if not candidates and is_generic_name(card_name):
        if current_price > 0:
            candidates = list(items)

    if not candidates:
        return None

    if current_price > 0:
        candidates = sorted(candidates, key=lambda it: abs(int(it.get("price") or 0) - current_price))
    return candidates[0]


# --------------------------
# EXTRACCION PRINCIPAL
# --------------------------

def extract_products_from_main_listing(listing_url: str):
    driver = get_driver()
    productos = []
    seen_keys = set()

    try:
        print(f"🪄 Samsung listing-only: leyendo solo la página principal {mask_url(listing_url)}", flush=True)
        driver.set_page_load_timeout(45)
        driver.get(listing_url)
        time.sleep(3)
        dismiss_overlays(driver)
        scroll_page(driver, rounds=14)
        dismiss_overlays(driver)
        time.sleep(1)

        html = driver.page_source
        jsonld_items = parse_jsonld_listing_items(html)
        print(f"✅ Items JSON-LD Samsung detectados: {len(jsonld_items)}", flush=True)

        roots = collect_listing_card_roots(driver)
        print(f"✅ Cards Samsung detectadas en listing: {len(roots)}", flush=True)

        for card in roots:
            raw_title = extract_title_from_card(card)
            nombre = normalizar_nombre_samsung(raw_title)
            capacidad = extract_selected_capacity_from_card(card)
            precio_actual, precio_original = extract_card_price_info(card)
            buy_url = extract_buy_url_from_card(card)

            jsonld_match = select_jsonld_match(nombre, precio_actual, jsonld_items)
            if (not nombre or is_generic_name(nombre)) and jsonld_match:
                nombre = jsonld_match["name"]
            if not buy_url and jsonld_match:
                buy_url = jsonld_match.get("buy_url", "")
            if precio_actual <= 0 and jsonld_match and int(jsonld_match.get("price") or 0) > 0:
                precio_actual = int(jsonld_match["price"])
                precio_original = precio_original or calcular_precio_original(precio_actual)

            nombre = normalizar_nombre_samsung(nombre)
            if not nombre or is_generic_name(nombre):
                continue
            if should_skip_by_name(nombre):
                continue
            if not capacidad:
                print(f"⚠️ Card Samsung sin capacidad visible para {nombre}. Se ignora.", flush=True)
                continue
            if precio_actual <= 0:
                print(f"⚠️ Card Samsung sin precio usable para {nombre} {capacidad}. Se ignora.", flush=True)
                continue

            memoria = infer_memoria_samsung_desde_listing(nombre, capacidad)
            if not memoria:
                print(f"⚠️ Card Samsung sin RAM resoluble para {nombre} {capacidad}. Se ignora.", flush=True)
                continue

            url_expandida = buy_url or ""
            url_base = canonical_samsung_import_url(url_expandida)
            model_code = extraer_model_code(url_expandida)
            url_afiliada = unir_afiliado(url_base, AFF_SAMSUNG) if url_base else ""
            url_corta = acortar_url(url_afiliada) if url_afiliada else ""

            key = source_key(nombre, memoria, capacidad, FUENTE)
            if key in seen_keys:
                summary_duplicados.append(f"{nombre} {capacidad} {memoria}")
                continue
            seen_keys.add(key)

            productos.append({
                "nombre": nombre,
                "memoria": memoria,
                "capacidad": capacidad,
                "precio_actual": int(precio_actual),
                "precio_original": int(precio_original or calcular_precio_original(precio_actual)),
                "img": "",
                "url_imp": url_base,
                "url_oferta_sin_acortar": url_expandida,
                "url_importada_sin_afiliado": url_base,
                "url_sin_acortar_con_mi_afiliado": url_afiliada,
                "url_oferta": url_corta,
                "enviado_desde": ENVIADO_DESDE,
                "enviado_desde_tg": ENVIADO_DESDE_TG,
                "fecha": datetime.now().strftime("%d/%m/%Y"),
                "version": VERSION,
                "fuente": FUENTE,
                "codigo_descuento": CODIGO_DESCUENTO_DEFAULT,
                "origen_pagina": "1",
                "origen_listado": listing_url,
                "source_key": key,
                "model_code": model_code,
            })
    except Exception as e:
        print(f"❌ Error renderizando listing Samsung: {e}", flush=True)
    finally:
        try:
            driver.quit()
        except Exception:
            pass
    return productos


def obtener_datos_remotos():
    print("", flush=True)
    print("--- FASE 1: ESCANEANDO SAMSUNG ---", flush=True)
    print(f"URL base: {mask_url(START_URL)}", flush=True)

    remotos = extract_products_from_main_listing(START_URL)
    productos = []
    by_key = {}
    for r in remotos:
        key = r["source_key"]
        if key in by_key:
            summary_duplicados.append(f"{r['nombre']} {r['capacidad']} {r['memoria']}")
            if int(r.get("precio_actual", 10**9)) < int(by_key[key].get("precio_actual", 10**9)):
                by_key[key] = r
        else:
            by_key[key] = r
    productos = list(by_key.values())

    print("📊 RESUMEN EXTRACCIÓN SAMSUNG:", flush=True)
    print("   URLs descubiertas: 1 (listing principal)", flush=True)
    print(f"   Productos únicos válidos: {len(productos)}", flush=True)
    return productos


# --------------------------
# WOO: CATEGORIAS / IMAGENES
# --------------------------

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


def find_category(cache_categorias, name: str, parent_id: int):
    name_norm = normalize_spaces(name).lower()
    for cat in cache_categorias:
        if normalize_spaces(cat.get("name", "")).lower() == name_norm and int(cat.get("parent") or 0) == int(parent_id or 0):
            return cat
    return None


def resolver_jerarquia(nombre_completo, cache_categorias):
    palabras = (nombre_completo or "").split()
    nombre_padre = palabras[0] if palabras else "Otros"
    nombre_hijo = limpiar_nombre_para_categoria(nombre_completo)

    padre = find_category(cache_categorias, nombre_padre, 0)
    if not padre:
        padre = wcapi.post("products/categories", {"name": nombre_padre}).json()
        cache_categorias.append(padre)
    id_padre = int(padre.get("id") or 0)

    hijo = find_category(cache_categorias, nombre_hijo, id_padre)
    if not hijo:
        hijo = wcapi.post("products/categories", {"name": nombre_hijo, "parent": id_padre}).json()
        cache_categorias.append(hijo)
    id_hijo = int(hijo.get("id") or 0)

    return id_padre, id_hijo


def image_of_category(cat: dict) -> str:
    if not cat:
        return ""
    img = cat.get("image") or {}
    return (img.get("src") or "").strip()


def obtener_imagen_subcategoria_exacta(cache_categorias, id_padre: int, nombre_hijo: str) -> str:
    padre = None
    hijo = None
    for cat in cache_categorias:
        if int(cat.get("id") or 0) == int(id_padre or 0):
            padre = cat
            break
    hijo = find_category(cache_categorias, nombre_hijo, id_padre)
    if not hijo:
        return ""
    img_hijo = image_of_category(hijo)
    if not img_hijo:
        return ""
    img_padre = image_of_category(padre)
    if img_padre and same_image_ref(img_hijo, img_padre):
        return ""
    return img_hijo


# --------------------------
# WOO: SINCRONIZACION
# --------------------------

def cargar_locales_samsung():
    locales = []
    page = 1
    while True:
        try:
            res = wcapi.get("products", params={"per_page": 100, "page": page, "status": "any"}).json()
            if not res or "message" in res:
                break
            for p in res:
                meta = {
                    m["key"]: str(m.get("value", ""))
                    for m in p.get("meta_data", [])
                    if isinstance(m, dict) and m.get("key")
                }
                if meta.get("importado_de", "").rstrip("/") == ID_IMPORTACION.rstrip("/"):
                    locales.append({"id": p["id"], "nombre": p.get("name", ""), "meta": meta})
            if len(res) < 100:
                break
            page += 1
        except Exception:
            break
    return locales


def build_local_key(local):
    meta = local.get("meta", {})
    if meta.get("_odm_source_key"):
        return meta["_odm_source_key"]
    return source_key(
        local.get("nombre", ""),
        meta.get("memoria", ""),
        meta.get("capacidad", ""),
        meta.get("fuente", FUENTE),
    )


def sincronizar(remotos):
    print("--- FASE 2: SINCRONIZANDO SAMSUNG ---", flush=True)
    cache_categorias = obtener_todas_las_categorias()
    locales = cargar_locales_samsung()
    print(f"📦 Productos Samsung existentes en la web: {len(locales)}", flush=True)
    print(f"📦 Productos remotos Samsung a procesar: {len(remotos)}", flush=True)

    remote_by_key = {r["source_key"]: r for r in remotos}
    local_by_key = {build_local_key(l): l for l in locales}

    # Obsoletos
    for key, local in local_by_key.items():
        if key in remote_by_key:
            continue
        try:
            wcapi.delete(f"products/{local['id']}", params={"force": True})
            summary_eliminados.append({"nombre": local["nombre"], "id": local["id"]})
            print(f"🗑️ ELIMINADO (obsoleto) -> {local['nombre']} (ID: {local['id']})", flush=True)
        except Exception as e:
            print(f"❌ Error eliminando obsoleto {local['nombre']}: {e}", flush=True)
            summary_fallidos.append({"nombre": local["nombre"], "id": local["id"], "error": str(e)})

    for r in remotos:
        try:
            id_padre, id_hijo = resolver_jerarquia(r["nombre"], cache_categorias)
            img_final_producto = obtener_imagen_subcategoria_exacta(cache_categorias, id_padre, limpiar_nombre_para_categoria(r["nombre"]))

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
            print(f"9) URL Imagen: {img_final_producto}", flush=True)
            print(f"10) Enlace Importado: {r.get('url_imp', '')}", flush=True)
            print(f"11) Enlace Expandido: {r.get('url_oferta_sin_acortar', '')}", flush=True)
            print(f"12) URL importada sin afiliado: {r.get('url_importada_sin_afiliado', '')}", flush=True)
            print(f"13) URL sin acortar con mi afiliado: {r.get('url_sin_acortar_con_mi_afiliado', '')}", flush=True)
            print(f"14) URL acortada con mi afiliado: {r.get('url_oferta', '')}", flush=True)
            print(f"15) Enviado desde: {r.get('enviado_desde', ENVIADO_DESDE)}", flush=True)
            print(f"15) Importado de: {ID_IMPORTACION}", flush=True)
            print("16) Encolado para comparar con base de datos...", flush=True)
            print("-" * 60, flush=True)

            match = local_by_key.get(r["source_key"])
            categories_payload = [{"id": id_padre}, {"id": id_hijo}] if id_hijo else ([{"id": id_padre}] if id_padre else [])

            if match:
                meta = match["meta"]
                cambios = []
                payload = {"meta_data": []}

                def _num_meta(k):
                    try:
                        return int(round(float(meta.get(k, 0) or 0)))
                    except Exception:
                        return 0

                old_actual = _num_meta("precio_actual")
                old_original = _num_meta("precio_original")
                if int(r["precio_actual"]) != old_actual:
                    cambios.append(f"precio_actual: {old_actual}€ -> {r['precio_actual']}€")
                    payload["sale_price"] = str(r["precio_actual"])
                    payload["meta_data"].append({"key": "precio_actual", "value": str(r["precio_actual"])})

                if int(r["precio_original"]) != old_original:
                    cambios.append(f"precio_original: {old_original}€ -> {r['precio_original']}€")
                    payload["regular_price"] = str(r["precio_original"])
                    payload["meta_data"].append({"key": "precio_original", "value": str(r["precio_original"])})

                compare_meta = {
                    "codigo_de_descuento": r.get("codigo_descuento", CODIGO_DESCUENTO_DEFAULT),
                    "enviado_desde": r.get("enviado_desde", ENVIADO_DESDE),
                    "enviado_desde_tg": r.get("enviado_desde_tg", ENVIADO_DESDE_TG),
                    "version": r.get("version", VERSION),
                    "imagen_producto": img_final_producto,
                    "url_sin_acortar_con_mi_afiliado": r.get("url_sin_acortar_con_mi_afiliado", ""),
                    "url_oferta": r.get("url_oferta", ""),
                    "url_importada_sin_afiliado": r.get("url_importada_sin_afiliado", ""),
                    "url_oferta_sin_acortar": r.get("url_oferta_sin_acortar", ""),
                    "enlace_de_compra_importado": r.get("url_imp", ""),
                }
                for k, v in compare_meta.items():
                    if str(meta.get(k, "")) != str(v):
                        cambios.append(f"{k}: {meta.get(k, '')} -> {v}")
                        payload["meta_data"].append({"key": k, "value": v})

                if str(meta.get("imagen_producto", "")) != str(img_final_producto):
                    payload["images"] = [{"src": img_final_producto}] if img_final_producto else []

                if cambios:
                    payload["categories"] = categories_payload
                    wcapi.put(f"products/{match['id']}", payload)
                    summary_actualizados.append({"nombre": r["nombre"], "id": match["id"], "cambios": cambios})
                    print(f"🔄 ACTUALIZADO -> {r['nombre']} (ID: {match['id']})", flush=True)
                else:
                    summary_ignorados.append({"nombre": r["nombre"], "id": match["id"]})
                    print(f"⏭️ SIN CAMBIOS -> {r['nombre']} (ID: {match['id']})", flush=True)
                continue

            data = {
                "name": r["nombre"],
                "type": "simple",
                "status": "publish",
                "regular_price": str(r["precio_original"]),
                "sale_price": str(r["precio_actual"]),
                "categories": categories_payload,
                "meta_data": [
                    {"key": "nombre_movil_final", "value": r["nombre"]},
                    {"key": "importado_de", "value": ID_IMPORTACION},
                    {"key": "fecha", "value": r["fecha"]},
                    {"key": "memoria", "value": r["memoria"]},
                    {"key": "capacidad", "value": r["capacidad"]},
                    {"key": "fuente", "value": FUENTE},
                    {"key": "precio_actual", "value": str(r["precio_actual"])},
                    {"key": "precio_original", "value": str(r["precio_original"])},
                    {"key": "codigo_de_descuento", "value": r.get("codigo_descuento", CODIGO_DESCUENTO_DEFAULT)},
                    {"key": "enviado_desde", "value": ENVIADO_DESDE},
                    {"key": "enviado_desde_tg", "value": ENVIADO_DESDE_TG},
                    {"key": "enlace_de_compra_importado", "value": r.get("url_imp", "")},
                    {"key": "url_oferta_sin_acortar", "value": r.get("url_oferta_sin_acortar", "")},
                    {"key": "url_importada_sin_afiliado", "value": r.get("url_importada_sin_afiliado", "")},
                    {"key": "url_sin_acortar_con_mi_afiliado", "value": r.get("url_sin_acortar_con_mi_afiliado", "")},
                    {"key": "url_oferta", "value": r.get("url_oferta", "")},
                    {"key": "imagen_producto", "value": img_final_producto},
                    {"key": "version", "value": r.get("version", VERSION)},
                    {"key": "_odm_source_key", "value": r["source_key"]},
                    {"key": "_odm_source_model_code", "value": r.get("model_code", "")},
                    {"key": "_odm_source_listing", "value": r.get("origen_listado", "")},
                    {"key": "_odm_source_page", "value": r.get("origen_pagina", "")},
                ],
            }
            if img_final_producto:
                data["images"] = [{"src": img_final_producto}]

            intentos = 0
            creado = False
            while intentos < 10 and not creado:
                intentos += 1
                try:
                    res = wcapi.post("products", data)
                    if res.status_code in (200, 201):
                        prod = res.json()
                        new_id = prod.get("id")
                        summary_creados.append({"nombre": r["nombre"], "id": new_id})
                        print(f"✅ CREADO -> {r['nombre']} (ID: {new_id})", flush=True)
                        try:
                            permalink = prod.get("permalink", "")
                            if permalink:
                                url_short = acortar_url(permalink)
                                wcapi.put(
                                    f"products/{new_id}",
                                    {"meta_data": [{"key": "url_post_acortada", "value": url_short}]},
                                )
                        except Exception:
                            pass
                        creado = True
                    else:
                        body_preview = (res.text or "").replace("\n", " ")[:250]
                        print(f"⚠️ Woo error {res.status_code}: {body_preview}", flush=True)
                except Exception as e:
                    print(f"⚠️ Excepción Woo creando Samsung: {e}", flush=True)

                if (not creado) and (intentos < 10):
                    time.sleep(15)

            if not creado:
                summary_fallidos.append({"nombre": r["nombre"], "error": "No se pudo crear en WooCommerce"})
                print(f"❌ NO SE PUDO CREAR: {r['nombre']}", flush=True)

        except Exception as e:
            print(f"❌ ERROR sincronizando Samsung {r.get('nombre', '?')}: {e}", flush=True)
            summary_fallidos.append({"nombre": r.get('nombre', '?'), "error": str(e)})

    hoy_fmt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
    for item in sorted(set(summary_duplicados)):
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
        print("No se han obtenido productos remotos de Samsung.", flush=True)


if __name__ == "__main__":
    main()
