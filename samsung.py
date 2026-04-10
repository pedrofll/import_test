import os
import re
import time
import math
import json
import hashlib
import urllib.parse
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from woocommerce import API

# ============================================================
#  SAMSUNG SCRAPER (LISTING-ONLY)
# ============================================================
# Reglas clave:
#  - Solo usar https://www.samsung.com/es/smartphones/all-smartphones/
#  - No entrar en fichas ni buy pages para extraer datos.
#  - Tomar de cada card visible del listing: nombre, capacidad, precio actual,
#    precio original, enlace de compra real y enlace base sin /buy/?...
#  - Usar SOLO la imagen de la subcategoría exacta. Si no existe o coincide con
#    la imagen de marca/categoría padre, no asignar imagen al producto.
# ============================================================

DEFAULT_START_URL = "https://www.samsung.com/es/smartphones/all-smartphones/"
START_URL = (os.getenv("SOURCE_URL_SAMSUNG") or DEFAULT_START_URL).strip() or DEFAULT_START_URL

FUENTE = "Samsung"
ID_IMPORTACION = START_URL.rstrip("/")
ENVIADO_DESDE = "España"
ENVIADO_DESDE_TG = "🇪🇸 España"
VERSION = "Versión Global"
CODIGO_DESCUENTO_DEFAULT = "OFERTA: PROMO."
OBJETIVO = 120

AFF_SAMSUNG = (os.getenv("AFF_SAMSUNG") or "").strip()

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

summary_creados = []
summary_eliminados = []
summary_actualizados = []
summary_ignorados = []
summary_fallidos = []
summary_duplicados = []


# --------------------------
# UTILIDADES
# --------------------------

def normalize_spaces(s):
    return re.sub(r"\s+", " ", (s or "")).strip()


def mask_url(url):
    try:
        u = urllib.parse.urlsplit(url)
        base = f"{u.scheme}://{u.netloc}{u.path}"
        return base + ("?***" if u.query else "")
    except Exception:
        return "***"


def abs_url(base, href):
    try:
        if href.startswith("//"):
            href = "https:" + href
        return urllib.parse.urljoin(base, href)
    except Exception:
        return href


def parse_eur_num(num_txt):
    if not num_txt:
        return 0
    n = str(num_txt).strip().replace(" ", "")
    n = n.replace(".", "").replace(",", ".")
    try:
        return int(round(float(n)))
    except Exception:
        return 0


EURO_AMOUNT_RE = r"(\d{1,3}(?:\.\d{3})*(?:,\d{1,2})?|\d{1,5}(?:[\.,]\d{1,2})?)\s*€"


def parse_eur_all(txt):
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


def calcular_precio_original(precio_actual, factor=1.20):
    try:
        pa = int(precio_actual)
    except Exception:
        return 0
    if pa <= 0:
        return 0
    return int(math.ceil(pa * factor))


def acortar_url(url_larga):
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


def normalizar_nombre_samsung(nombre):
    t = normalize_spaces(nombre)
    if not t:
        return ""
    t = t.replace("Exclusivo Online", "")
    t = normalize_spaces(t)

    if t.lower().startswith("samsung "):
        base = t[len("Samsung "):]
    else:
        base = t

    out = []
    for w in base.split():
        if re.search(r"\d", w) and re.search(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]", w):
            w2 = "".join(ch.upper() if ch.isalpha() else ch for ch in w)
        elif w.lower() in {"gb", "tb"}:
            w2 = w.upper()
        else:
            w2 = w[:1].upper() + w[1:].lower() if w else w
        out.append(w2)

    base = " ".join(out)
    if not base.lower().startswith("galaxy "):
        return normalize_spaces(f"Samsung {base}")
    return normalize_spaces(f"Samsung {base}")


def should_skip_by_name(nombre):
    u = (nombre or "").upper()
    return any(x in u for x in [" TAB", "IPAD", " PAD"]) or u.startswith("TAB ")


def parse_capacidad_desde_texto(txt):
    t = normalize_spaces(txt)
    m = re.search(r"\b(64|128|256|512|1024)\s*GB\b", t, flags=re.I)
    if m:
        return f"{m.group(1)}GB"
    m = re.search(r"\b(1|2)\s*TB\b", t, flags=re.I)
    if m:
        return f"{m.group(1)}TB"
    return ""


def parse_memoria_desde_texto(txt):
    t = normalize_spaces(txt)
    m = re.search(r"\b(3|4|6|8|12|16|24)\s*GB\b", t, flags=re.I)
    if m:
        return f"{m.group(1)}GB"
    return ""


def base_product_url(url):
    if not url:
        return ""
    try:
        u = urllib.parse.urlsplit(url)
        path = u.path or ""
        path = re.sub(r"/buy/?$", "", path, flags=re.I)
        base = f"{u.scheme}://{u.netloc}{path}".rstrip("/")
        return base
    except Exception:
        return (url or "").split("?")[0].rstrip("/")


def join_affiliate_url(base_url, aff_raw):
    base = (base_url or "").strip()
    aff = (aff_raw or "").strip()
    if not base:
        return ""
    if not aff:
        return base
    if aff.lower().startswith("http"):
        return aff

    # sanea el final para evitar /?/?
    base = re.sub(r"/\?+$", "/", base)
    base = re.sub(r"\?+$", "", base)
    base = re.sub(r"&+$", "", base)

    aff = aff.lstrip("?&")
    if not aff:
        return base

    sep = "&" if "?" in base else "?"
    return f"{base}{sep}{aff}"


def source_key(nombre, memoria, capacidad, fuente=FUENTE):
    return f"{normalize_spaces(nombre).lower()}|{str(memoria).upper()}|{str(capacidad).upper()}|{fuente.lower()}"


# --------------------------
# MAPA LOCAL DE RAM
# --------------------------
# Solo como fallback cuando la card/listing no la muestre de forma fiable.
RAM_MAP = {
    ("SAMSUNG GALAXY S26", "256GB"): "12GB",
    ("SAMSUNG GALAXY S26", "512GB"): "12GB",
    ("SAMSUNG GALAXY S26+", "256GB"): "12GB",
    ("SAMSUNG GALAXY S26+", "512GB"): "12GB",
    ("SAMSUNG GALAXY S26 ULTRA", "256GB"): "12GB",
    ("SAMSUNG GALAXY S26 ULTRA", "512GB"): "12GB",
    ("SAMSUNG GALAXY S26 ULTRA", "1TB"): "16GB",
    ("SAMSUNG GALAXY Z FOLD7", "256GB"): "12GB",
    ("SAMSUNG GALAXY Z FOLD7", "512GB"): "12GB",
    ("SAMSUNG GALAXY Z FOLD7", "1TB"): "16GB",
    ("SAMSUNG GALAXY Z FLIP7", "256GB"): "12GB",
    ("SAMSUNG GALAXY Z FLIP7", "512GB"): "12GB",
    ("SAMSUNG GALAXY Z FLIP7 FE", "128GB"): "8GB",
    ("SAMSUNG GALAXY Z FLIP7 FE", "256GB"): "8GB",
    ("SAMSUNG GALAXY S25", "256GB"): "12GB",
    ("SAMSUNG GALAXY S25", "512GB"): "12GB",
    ("SAMSUNG GALAXY S25+", "256GB"): "12GB",
    ("SAMSUNG GALAXY S25+", "512GB"): "12GB",
    ("SAMSUNG GALAXY S25 ULTRA", "256GB"): "12GB",
    ("SAMSUNG GALAXY S25 ULTRA", "512GB"): "12GB",
    ("SAMSUNG GALAXY S25 ULTRA", "1TB"): "12GB",
    ("SAMSUNG GALAXY S25 FE", "256GB"): "8GB",
    ("SAMSUNG GALAXY S25 FE", "512GB"): "8GB",
    ("SAMSUNG GALAXY A57 5G", "256GB"): "8GB",
    ("SAMSUNG GALAXY A57 5G", "512GB"): "12GB",
    ("SAMSUNG GALAXY A37 5G", "128GB"): "8GB",
    ("SAMSUNG GALAXY A37 5G", "256GB"): "8GB",
    ("SAMSUNG GALAXY A26 5G", "128GB"): "8GB",
    ("SAMSUNG GALAXY A26 5G", "256GB"): "8GB",
    ("SAMSUNG GALAXY A17 5G", "128GB"): "8GB",
    ("SAMSUNG GALAXY A17 5G", "256GB"): "8GB",
    ("SAMSUNG GALAXY A17", "128GB"): "8GB",
    ("SAMSUNG GALAXY A17", "256GB"): "8GB",
    ("SAMSUNG GALAXY A16 5G", "128GB"): "8GB",
    ("SAMSUNG GALAXY A16 5G", "256GB"): "8GB",
    ("SAMSUNG GALAXY A16", "128GB"): "8GB",
    ("SAMSUNG GALAXY A16", "256GB"): "8GB",
}


def resolve_ram(nombre, capacidad):
    return RAM_MAP.get((normalize_spaces(nombre).upper(), (capacidad or "").upper()), "")


# --------------------------
# JSON-LD DEL LISTING
# --------------------------

def parse_jsonld_items(html):
    soup = BeautifulSoup(html, "html.parser")
    items = []
    scripts = soup.find_all("script", attrs={"type": re.compile(r"ld\+json", re.I)})
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
                nodes.extend(node["@graph"])

        for node in nodes:
            if not isinstance(node, dict):
                continue
            if str(node.get("@type") or "") != "ItemList":
                continue
            for li in node.get("itemListElement", []) or []:
                prod = (li or {}).get("item") or {}
                if str(prod.get("@type") or "") != "Product":
                    continue
                name = normalizar_nombre_samsung(prod.get("name") or "")
                url_buy = (prod.get("url") or "").strip()
                detail = base_product_url(prod.get("@id") or url_buy)
                img = abs_url(START_URL, (prod.get("image") or "").strip()) if prod.get("image") else ""
                offer = prod.get("offers") or {}
                price = parse_eur_num(offer.get("price") or "")
                items.append({
                    "name": name,
                    "buy_url": url_buy,
                    "detail_url": detail,
                    "jsonld_price": price,
                    "image": img,
                })
    return items


def build_jsonld_index(items):
    idx = {}
    for item in items:
        key = normalize_spaces(item.get("name") or "").upper()
        idx.setdefault(key, []).append(item)
    return idx


def match_jsonld_record(name, current_price, jsonld_index):
    records = jsonld_index.get(normalize_spaces(name).upper(), [])
    if not records:
        return None
    if current_price:
        by_price = [r for r in records if int(r.get("jsonld_price") or 0) == int(current_price)]
        if by_price:
            return by_price[0]
    # Si no casa por precio, prioriza el primero con buy_url.
    for r in records:
        if r.get("buy_url"):
            return r
    return records[0]


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
    opts.add_argument("--window-size=1440,3200")
    opts.add_argument(
        "--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
    return webdriver.Chrome(options=opts)


def dismiss_overlays(driver):
    from selenium.webdriver.common.by import By

    xpaths = [
        "//button[contains(., 'Aceptar')]",
        "//button[contains(., 'Acepto')]",
        "//button[contains(., 'Aceptar todo')]",
        "//button[contains(., 'OK')]",
        "//button[contains(., 'Continuar')]",
        "//button[contains(., 'Sí')]",
        "//button[contains(., 'MÁS TARDE')]",
        "//button[contains(., 'MAS TARDE')]",
        "//a[contains(., 'IR A SAMSUNG.COM')]",
        "//button[contains(., 'Cerrar')]",
    ]
    for _ in range(3):
        for xp in xpaths:
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


def click_ver_mas(driver, max_clicks=6):
    from selenium.webdriver.common.by import By

    for _ in range(max_clicks):
        clicked = False
        for xp in [
            "//button[contains(., 'Ver más')]",
            "//a[contains(., 'Ver más')]",
            "//*[self::button or self::a][contains(normalize-space(.), 'Ver más')]",
        ]:
            try:
                els = driver.find_elements(By.XPATH, xp)
                for el in els:
                    try:
                        if not el.is_displayed():
                            continue
                        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                        time.sleep(0.3)
                        driver.execute_script("arguments[0].click();", el)
                        time.sleep(1.2)
                        clicked = True
                        break
                    except Exception:
                        pass
                if clicked:
                    break
            except Exception:
                pass
        if not clicked:
            break


def scroll_page(driver, rounds=30):
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
            if stable >= 3:
                break
        except Exception:
            break


def get_rendered_listing_page(url):
    driver = get_driver()
    try:
        driver.set_page_load_timeout(45)
        driver.get(url)
        time.sleep(3)
        dismiss_overlays(driver)
        scroll_page(driver, rounds=18)
        click_ver_mas(driver, max_clicks=8)
        scroll_page(driver, rounds=12)
        return driver, driver.page_source
    except Exception:
        try:
            driver.quit()
        except Exception:
            pass
        raise


# --------------------------
# CARDS DEL LISTING
# --------------------------

def find_product_card_roots(driver):
    from selenium.webdriver.common.by import By

    roots = []
    seen = set()
    candidates = driver.find_elements(By.XPATH, "//*[self::a or self::button][contains(normalize-space(.), 'Comprar')]")
    for btn in candidates:
        node = btn
        chosen = None
        for _ in range(10):
            try:
                node = node.find_element(By.XPATH, "..")
            except Exception:
                break
            try:
                txt = normalize_spaces(node.text)
            except Exception:
                txt = ""
            if not txt or "Galaxy" not in txt:
                continue
            if not re.search(r"\b\d+\s*(GB|TB)\b", txt, flags=re.I):
                continue
            if "Más información" not in txt and "Comprar" not in txt:
                continue
            chosen = node
            break
        if chosen is None:
            continue
        try:
            outer = chosen.get_attribute("outerHTML") or ""
            key = hashlib.sha1(outer.encode("utf-8", errors="ignore")).hexdigest()
            if key in seen:
                continue
            seen.add(key)
            roots.append(chosen)
        except Exception:
            pass
    return roots


def extract_name_from_card(card):
    from selenium.webdriver.common.by import By

    try:
        for xp in [
            ".//h1", ".//h2", ".//h3", ".//h4",
            ".//*[contains(@class,'title')]",
            ".//*[contains(@class,'name')]",
        ]:
            try:
                els = card.find_elements(By.XPATH, xp)
                for el in els:
                    txt = normalize_spaces(el.text)
                    if txt.lower().startswith("galaxy "):
                        return normalizar_nombre_samsung(txt)
            except Exception:
                pass

        lines = [normalize_spaces(x) for x in (card.text or "").splitlines() if normalize_spaces(x)]
        for line in lines:
            if line.lower().startswith("galaxy "):
                return normalizar_nombre_samsung(line)
    except Exception:
        pass
    return ""


def parse_prices_from_card_text(card_text, jsonld_price=0):
    amounts = [x for x in parse_eur_all(card_text) if x > 0]
    amounts = dedupe_keep_order(amounts)
    if not amounts:
        return 0, 0

    # El precio original suele ser el mayor importe de la card.
    original = max(amounts)

    # Filtra importes promocionales muy pequeños (50/60/80/90/100...) si existe
    # un precio real claramente mayor.
    if any(a >= 300 for a in amounts):
        amounts_wo_small = [a for a in amounts if a >= 150]
    else:
        amounts_wo_small = list(amounts)

    current_candidates = [a for a in amounts_wo_small if a != original and a >= max(50, int(original * 0.35))]

    current = 0
    if current_candidates:
        # El precio actual suele ser el menor de los candidatos grandes que no es el original.
        current = min(current_candidates)
    elif jsonld_price and jsonld_price > 0:
        current = int(jsonld_price)
        if current > original:
            original = current
    else:
        # Si solo queda un importe grande, lo tratamos como actual.
        current = max(amounts_wo_small) if amounts_wo_small else original

    if not original:
        original = calcular_precio_original(current)
    if current and original and current > original:
        original = current

    return int(current or 0), int(original or 0)


def find_capacity_elements(card):
    from selenium.webdriver.common.by import By

    found = []
    seen = set()
    xpath = ".//*[self::button or self::a or self::span or self::div or self::label or self::li]"
    try:
        els = card.find_elements(By.XPATH, xpath)
    except Exception:
        return []

    for el in els:
        try:
            if not el.is_displayed():
                continue
            txt = normalize_spaces(el.text)
            if not txt or len(txt) > 20:
                continue
            if not re.fullmatch(r"(64|128|256|512|1024)\s*GB|(1|2)\s*TB", txt, flags=re.I):
                continue
            cap = parse_capacidad_desde_texto(txt)
            if cap in seen:
                continue
            seen.add(cap)
            found.append((cap, el))
        except Exception:
            pass
    return found


def click_el(driver, el):
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        time.sleep(0.2)
        driver.execute_script("arguments[0].click();", el)
        return True
    except Exception:
        try:
            el.click()
            return True
        except Exception:
            return False


def extract_buy_href_from_card(card):
    from selenium.webdriver.common.by import By

    hrefs = []
    try:
        anchors = card.find_elements(By.XPATH, ".//a[@href]")
    except Exception:
        anchors = []

    for a in anchors:
        try:
            href = (a.get_attribute("href") or "").strip()
            if not href or href.lower().startswith("javascript"):
                continue
            if "/buy/" in href or "modelCode=" in href:
                hrefs.append(href)
        except Exception:
            pass

    if hrefs:
        return hrefs[0]

    # Buscar pistas en atributos.
    attrs_to_try = ["data-href", "data-url", "data-link", "onclick"]
    xpath = ".//*"
    try:
        els = card.find_elements(By.XPATH, xpath)
    except Exception:
        els = []
    for el in els:
        for attr in attrs_to_try:
            try:
                val = (el.get_attribute(attr) or "").strip()
            except Exception:
                val = ""
            if not val:
                continue
            m = re.search(r"https://www\.samsung\.com/es/smartphones/[^\"'\s)]+", val, flags=re.I)
            if m:
                return m.group(0)
    return ""


def extract_card_variants(driver, card, jsonld_index):
    name = extract_name_from_card(card)
    if not name or should_skip_by_name(name):
        return []

    capacities = find_capacity_elements(card)
    if not capacities:
        return []

    variants = []
    seen_local = set()

    for capacidad, el in capacities:
        click_el(driver, el)
        time.sleep(0.7)
        try:
            card_text = normalize_spaces(card.text)
        except Exception:
            card_text = ""

        jsonld_rec = match_jsonld_record(name, 0, jsonld_index)
        jsonld_price = int((jsonld_rec or {}).get("jsonld_price") or 0)
        precio_actual, precio_original = parse_prices_from_card_text(card_text, jsonld_price=jsonld_price)
        if precio_actual <= 0:
            print(f"⚠️ Card Samsung sin precio usable para {name} {capacidad}. Se ignora.", flush=True)
            continue

        memoria = resolve_ram(name, capacidad)
        if not memoria:
            print(f"⚠️ Card Samsung sin RAM resoluble para {name} {capacidad}. Se ignora.", flush=True)
            continue

        buy_href = extract_buy_href_from_card(card)
        if not buy_href and jsonld_rec:
            buy_href = jsonld_rec.get("buy_url") or ""

        base_url = base_product_url(buy_href or (jsonld_rec or {}).get("detail_url") or "")
        expanded_url = (buy_href or (jsonld_rec or {}).get("buy_url") or "").strip()
        affiliate_url = join_affiliate_url(base_url, AFF_SAMSUNG)
        short_url = acortar_url(affiliate_url) if affiliate_url else ""

        item = {
            "nombre": name,
            "memoria": memoria,
            "capacidad": capacidad,
            "precio_actual": int(precio_actual),
            "precio_original": int(precio_original or calcular_precio_original(precio_actual)),
            "codigo_descuento": CODIGO_DESCUENTO_DEFAULT,
            "fuente": FUENTE,
            "version": VERSION,
            "enviado_desde": ENVIADO_DESDE,
            "enviado_desde_tg": ENVIADO_DESDE_TG,
            "fecha": datetime.now().strftime("%d/%m/%Y"),
            "importado_de": ID_IMPORTACION,
            "enlace_de_compra_importado": base_url,
            "url_oferta_sin_acortar": expanded_url,
            "url_importada_sin_afiliado": base_url,
            "url_sin_acortar_con_mi_afiliado": affiliate_url,
            "url_oferta": short_url,
            "model_code": urllib.parse.parse_qs(urllib.parse.urlsplit(expanded_url).query).get("modelCode", [""])[0],
        }
        sk = source_key(item["nombre"], item["memoria"], item["capacidad"], FUENTE)
        if sk in seen_local:
            summary_duplicados.append(f"{item['nombre']} {item['capacidad']} {item['memoria']}")
            continue
        seen_local.add(sk)
        variants.append(item)

    return variants


# --------------------------
# CATEGORÍAS / IMAGEN DE SUBCATEGORÍA
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


def get_category_by_id(cache_categorias, cat_id):
    for c in cache_categorias:
        if c.get("id") == cat_id:
            return c
    return None


def get_category_image_src(cat):
    if not cat:
        return ""
    img = cat.get("image") or {}
    return (img.get("src") or "").strip()


def get_subcategory_image_only(cache_categorias, id_padre, id_hijo):
    parent = get_category_by_id(cache_categorias, id_padre)
    child = get_category_by_id(cache_categorias, id_hijo)
    parent_img = get_category_image_src(parent)
    child_img = get_category_image_src(child)
    if not child_img:
        return ""
    if parent_img and child_img == parent_img:
        return ""
    return child_img


# --------------------------
# EXTRACCIÓN REMOTA
# --------------------------

def obtener_datos_remotos():
    print("", flush=True)
    print("--- FASE 1: ESCANEANDO SAMSUNG ---", flush=True)
    print(f"URL base: {mask_url(START_URL)}", flush=True)
    print(f"🪄 Samsung listing-only: leyendo solo la página principal {mask_url(START_URL)}", flush=True)

    driver = None
    try:
        driver, html = get_rendered_listing_page(START_URL)
    except Exception as e:
        print(f"❌ Error renderizando listing Samsung: {e}", flush=True)
        return []

    try:
        jsonld_items = parse_jsonld_items(html)
        jsonld_index = build_jsonld_index(jsonld_items)
        print(f"✅ Items JSON-LD Samsung detectados: {len(jsonld_items)}", flush=True)

        cards = find_product_card_roots(driver)
        print(f"✅ Cards Samsung detectadas en listing: {len(cards)}", flush=True)

        productos_por_clave = {}
        for card in cards:
            for item in extract_card_variants(driver, card, jsonld_index):
                sk = source_key(item["nombre"], item["memoria"], item["capacidad"], FUENTE)
                if sk in productos_por_clave:
                    summary_duplicados.append(f"{item['nombre']} {item['capacidad']} {item['memoria']}")
                    prev = productos_por_clave[sk]
                    # conserva el precio mayor entre actuales válidos? preferimos el menor actual.
                    if int(item.get("precio_actual", 10**9)) < int(prev.get("precio_actual", 10**9)):
                        productos_por_clave[sk] = item
                else:
                    productos_por_clave[sk] = item

        productos = list(productos_por_clave.values())
        print("📊 RESUMEN EXTRACCIÓN SAMSUNG:", flush=True)
        print("   URLs descubiertas: 1 (listing principal)", flush=True)
        print(f"   Productos únicos válidos: {len(productos)}", flush=True)
        return productos
    finally:
        try:
            if driver:
                driver.quit()
        except Exception:
            pass


# --------------------------
# SINCRONIZACIÓN WC
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
                meta = {m["key"]: str(m.get("value", "")) for m in p.get("meta_data", [])}
                if meta.get("importado_de", "") == ID_IMPORTACION:
                    locales.append({"id": p["id"], "nombre": p.get("name", ""), "meta": meta})
            if len(res) < 100:
                break
            page += 1
        except Exception:
            break
    return locales


def sincronizar(remotos):
    print("--- FASE 2: SINCRONIZANDO SAMSUNG ---", flush=True)
    cache_categorias = obtener_todas_las_categorias()
    locales = cargar_locales_samsung()

    print(f"📦 Productos Samsung existentes en la web: {len(locales)}", flush=True)
    print(f"📦 Productos remotos Samsung a procesar: {len(remotos)}", flush=True)

    locales_por_key = {}
    for l in locales:
        meta = l["meta"]
        sk = meta.get("_odm_source_key") or source_key(
            l.get("nombre", ""),
            meta.get("memoria", ""),
            meta.get("capacidad", ""),
            meta.get("fuente", FUENTE),
        )
        locales_por_key[sk] = l

    remotos_por_key = {source_key(r["nombre"], r["memoria"], r["capacidad"], FUENTE): r for r in remotos}

    # Actualizar / ignorar / eliminar
    for sk, local in locales_por_key.items():
        remoto = remotos_por_key.get(sk)
        if not remoto:
            try:
                wcapi.delete(f"products/{local['id']}", params={"force": True})
                summary_eliminados.append({"nombre": local["nombre"], "id": local["id"]})
            except Exception as e:
                summary_fallidos.append({"nombre": local["nombre"], "id": local["id"], "error": str(e)})
            continue

        meta = local["meta"]
        cambios = []
        payload = {"meta_data": []}

        old_current = parse_eur_num(meta.get("precio_actual", "0"))
        old_original = parse_eur_num(meta.get("precio_original", "0"))

        if int(remoto["precio_actual"]) != int(old_current):
            cambios.append(f"precio_actual: {old_current}€ -> {remoto['precio_actual']}€")
            payload["sale_price"] = str(remoto["precio_actual"])
            payload["meta_data"].append({"key": "precio_actual", "value": str(remoto["precio_actual"])})

        if int(remoto["precio_original"]) != int(old_original):
            cambios.append(f"precio_original: {old_original}€ -> {remoto['precio_original']}€")
            payload["regular_price"] = str(remoto["precio_original"])
            payload["meta_data"].append({"key": "precio_original", "value": str(remoto["precio_original"])})

        for key in [
            "enlace_de_compra_importado",
            "url_oferta_sin_acortar",
            "url_importada_sin_afiliado",
            "url_sin_acortar_con_mi_afiliado",
            "url_oferta",
        ]:
            if normalize_spaces(meta.get(key, "")) != normalize_spaces(remoto.get(key, "")):
                cambios.append(f"{key} actualizado")
                payload["meta_data"].append({"key": key, "value": remoto.get(key, "")})

        if cambios:
            try:
                wcapi.put(f"products/{local['id']}", payload)
                summary_actualizados.append({"nombre": local["nombre"], "id": local["id"], "cambios": cambios})
            except Exception as e:
                summary_fallidos.append({"nombre": local["nombre"], "id": local["id"], "error": str(e)})
        else:
            summary_ignorados.append({"nombre": local["nombre"], "id": local["id"]})

    # Crear nuevos
    for remoto in remotos:
        sk = source_key(remoto["nombre"], remoto["memoria"], remoto["capacidad"], FUENTE)
        if sk in locales_por_key:
            continue

        id_padre, id_hijo = resolver_jerarquia(remoto["nombre"], cache_categorias)
        img_final = get_subcategory_image_only(cache_categorias, id_padre, id_hijo)
        remoto["imagen_producto"] = img_final

        print("-" * 60, flush=True)
        print(f"Detectado {remoto['nombre']}", flush=True)
        print(f"1) Nombre: {remoto['nombre']}", flush=True)
        print(f"2) Memoria: {remoto['memoria']}", flush=True)
        print(f"3) Capacidad: {remoto['capacidad']}", flush=True)
        print(f"4) Versión: {remoto['version']}", flush=True)
        print(f"5) Fuente: {remoto['fuente']}", flush=True)
        print(f"6) Precio actual: {remoto['precio_actual']}", flush=True)
        print(f"7) Precio original: {remoto['precio_original']}", flush=True)
        print(f"8) Código de descuento: {remoto['codigo_descuento']}", flush=True)
        print(f"9) URL Imagen: {img_final}", flush=True)
        print(f"10) Enlace Importado: {remoto['enlace_de_compra_importado']}", flush=True)
        print(f"11) Enlace Expandido: {remoto['url_oferta_sin_acortar']}", flush=True)
        print(f"12) URL importada sin afiliado: {remoto['url_importada_sin_afiliado']}", flush=True)
        print(f"13) URL sin acortar con mi afiliado: {mask_url(remoto['url_sin_acortar_con_mi_afiliado'])}", flush=True)
        print(f"14) URL acortada con mi afiliado: {remoto['url_oferta']}", flush=True)
        print(f"15) Enviado desde: {remoto['enviado_desde']}", flush=True)
        print(f"15) Importado de: {remoto['importado_de']}", flush=True)
        print(f"16) Encolado para comparar con base de datos...", flush=True)
        print("-" * 60, flush=True)

        data = {
            "name": remoto["nombre"],
            "type": "simple",
            "status": "publish",
            "regular_price": str(remoto["precio_original"]),
            "sale_price": str(remoto["precio_actual"]),
            "categories": [{"id": id_padre}, {"id": id_hijo}] if id_hijo else ([{"id": id_padre}] if id_padre else []),
            "images": [{"src": img_final}] if img_final else [],
            "meta_data": [
                {"key": "_odm_source_key", "value": sk},
                {"key": "importado_de", "value": remoto["importado_de"]},
                {"key": "fecha", "value": datetime.now().strftime("%d/%m/%Y")},
                {"key": "memoria", "value": remoto["memoria"]},
                {"key": "capacidad", "value": remoto["capacidad"]},
                {"key": "version", "value": remoto["version"]},
                {"key": "fuente", "value": remoto["fuente"]},
                {"key": "precio_actual", "value": str(remoto["precio_actual"])},
                {"key": "precio_original", "value": str(remoto["precio_original"])},
                {"key": "codigo_de_descuento", "value": remoto["codigo_descuento"]},
                {"key": "imagen_producto", "value": img_final},
                {"key": "enlace_de_compra_importado", "value": remoto["enlace_de_compra_importado"]},
                {"key": "url_oferta_sin_acortar", "value": remoto["url_oferta_sin_acortar"]},
                {"key": "url_importada_sin_afiliado", "value": remoto["url_importada_sin_afiliado"]},
                {"key": "url_sin_acortar_con_mi_afiliado", "value": remoto["url_sin_acortar_con_mi_afiliado"]},
                {"key": "url_oferta", "value": remoto["url_oferta"]},
                {"key": "enviado_desde", "value": remoto["enviado_desde"]},
                {"key": "enviado_desde_tg", "value": remoto["enviado_desde_tg"]},
            ],
        }

        intentos = 0
        creado = False
        while intentos < 10 and not creado:
            intentos += 1
            try:
                res = wcapi.post("products", data)
                if res.status_code in (200, 201):
                    prod = res.json()
                    creado = True
                    summary_creados.append({"nombre": remoto["nombre"], "id": prod.get("id")})
                    print(f"✅ CREADO -> {remoto['nombre']} (ID: {prod.get('id')})", flush=True)
                    try:
                        permalink = prod.get("permalink", "")
                        if permalink:
                            short_post = acortar_url(permalink)
                            if short_post:
                                wcapi.put(
                                    f"products/{prod.get('id')}",
                                    {"meta_data": [{"key": "url_post_acortada", "value": short_post}]},
                                )
                    except Exception:
                        pass
                else:
                    body_preview = (res.text or "").replace("\n", " ")[:250]
                    print(f"⚠️ Woo error {res.status_code}: {body_preview}", flush=True)
            except Exception as e:
                print(f"⚠️ Excepción Woo: {e}", flush=True)
            if not creado and intentos < 10:
                time.sleep(15)

        if not creado:
            summary_fallidos.append({"nombre": remoto["nombre"], "error": "No se pudo crear"})
            print(f"❌ NO SE PUDO CREAR: {remoto['nombre']}", flush=True)

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
    for item in summary_duplicados:
        print(f"- {item}", flush=True)
    print(f"\nf) FALLIDOS: {len(summary_fallidos)}", flush=True)
    for item in summary_fallidos:
        if isinstance(item, dict):
            print(f"- {item.get('nombre','?')}", flush=True)
        else:
            print(f"- {item}", flush=True)
    print("============================================================", flush=True)


if __name__ == "__main__":
    remotos = obtener_datos_remotos()
    if remotos:
        sincronizar(remotos)
    else:
        print("No se han obtenido productos remotos de Samsung.", flush=True)
