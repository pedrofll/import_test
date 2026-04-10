import os
import re
import time
import math
import json
import urllib.parse
import hashlib
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from woocommerce import API

# ============================================================
#  SAMSUNG SCRAPER (LISTING-ONLY)
# ============================================================
# Reglas operativas:
#  - SOLO lee https://www.samsung.com/es/smartphones/all-smartphones/
#  - NO entra en fichas ni en buy pages para extraer datos.
#  - Usa el HTML renderizado del listing + JSON-LD embebido.
#  - Toma de cada card: nombre, capacidad seleccionada, precio actual,
#    precio anterior si aparece, imagen y enlace Comprar.
#  - La RAM se resuelve con una tabla local por familia/capacidad.
# ============================================================

DEFAULT_START_URL = "https://www.samsung.com/es/smartphones/all-smartphones/"
START_URL = (os.getenv("SOURCE_URL_SAMSUNG") or DEFAULT_START_URL).strip() or DEFAULT_START_URL

FUENTE = "Samsung"
ID_IMPORTACION = START_URL.rstrip("/")
ENVIADO_DESDE = "España"
ENVIADO_DESDE_TG = "🇪🇸 España"
VERSION = "Versión Global"
CODIGO_DESCUENTO_DEFAULT = "OFERTA: PROMO."

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

AFF_RAW = (os.getenv("AFF_SAMSUNG") or "").strip()

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


# --------------------------
# UTILIDADES
# --------------------------

def normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def parse_eur_num(num_txt: str) -> int:
    if not num_txt:
        return 0
    n = str(num_txt).strip().replace(" ", "")
    n = n.replace(".", "").replace(",", ".")
    try:
        return int(round(float(n)))
    except Exception:
        return 0


def mask_url(url: str) -> str:
    try:
        u = urllib.parse.urlsplit(url)
        base = f"{u.scheme}://{u.netloc}{u.path}"
        return base + ("?***" if u.query else "")
    except Exception:
        return "***"


def abs_url(base: str, href: str) -> str:
    try:
        if not href:
            return ""
        if href.startswith("//"):
            href = "https:" + href
        return urllib.parse.urljoin(base, href)
    except Exception:
        return href


def calcular_precio_original(precio_actual: int, factor: float = 1.20) -> int:
    try:
        pa = int(precio_actual)
    except Exception:
        return 0
    if pa <= 0:
        return 0
    return int(math.ceil(pa * factor))


def should_skip_by_name(nombre: str) -> bool:
    u = f" {normalize_spaces(nombre).upper()} "
    return any(x in u for x in [" TAB ", " IPAD ", " PAD "])


def normalizar_nombre_samsung(nombre: str) -> str:
    t = normalize_spaces(nombre)
    if not t:
        return ""
    if t.lower().startswith("samsung "):
        t = t[len("Samsung "):]
    out = []
    for w in t.split():
        if re.search(r"\d", w) and re.search(r"[A-Za-z]", w):
            w2 = "".join(ch.upper() if ch.isalpha() else ch for ch in w)
        elif w.lower() in {"gb", "tb"}:
            w2 = w.upper()
        else:
            w2 = w[:1].upper() + w[1:]
        out.append(w2)
    base = " ".join(out)
    return normalize_spaces(f"Samsung {base}")


def limpiar_nombre_para_categoria(nombre: str) -> str:
    return normalize_spaces(nombre)


def source_key(nombre: str, memoria: str, capacidad: str, fuente: str = FUENTE) -> str:
    return f"{normalize_spaces(nombre).lower()}|{str(memoria).upper()}|{str(capacidad).upper()}|{fuente.lower()}"


def unir_afiliado(url_base: str, aff: str) -> str:
    base = (url_base or "").strip().replace("&amp;", "&")
    aff = (aff or "").strip()
    if not base or not aff:
        return base
    if aff.startswith("?"):
        return base + ("&" + aff[1:] if "?" in base else aff)
    if aff.startswith("&"):
        return base + (aff if "?" in base else "?" + aff[1:])
    return base + (("&" if "?" in base else "?") + aff)


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


def get_driver():
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options

    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1440,3000")
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
        "//button[contains(., 'Más tarde')]",
        "//button[contains(., 'MÁS TARDE')]",
        "//a[contains(., 'IR A SAMSUNG.COM')]",
    ]
    for _ in range(3):
        for xp in xpaths:
            try:
                for el in driver.find_elements(By.XPATH, xp)[:3]:
                    if el.is_displayed():
                        try:
                            driver.execute_script("arguments[0].click();", el)
                            time.sleep(0.6)
                        except Exception:
                            pass
            except Exception:
                pass


def scroll_page(driver, rounds: int = 18):
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


# --------------------------
# RAM LOCAL POR FAMILIA / CAPACIDAD
# --------------------------

def resolver_ram_desde_listing(nombre: str, capacidad: str, model_code: str = "") -> str:
    n = normalize_spaces(nombre).lower()
    c = (capacidad or "").upper()
    mc = (model_code or "").upper()

    # Serie S26
    if "galaxy s26 ultra" in n:
        if c == "1TB":
            return "16GB"
        if c in {"256GB", "512GB"}:
            return "12GB"
    if "galaxy s26+" in n or "galaxy s26 +" in n:
        if c in {"256GB", "512GB"}:
            return "12GB"
    if re.search(r"\bgalaxy s26\b", n) and "ultra" not in n and "+" not in n:
        if c in {"256GB", "512GB"}:
            return "12GB"

    # Z Fold / Z Flip actuales
    if "galaxy z fold7" in n or "galaxy z fold 7" in n:
        if c == "1TB":
            return "16GB"
        if c in {"256GB", "512GB"}:
            return "12GB"
    if "galaxy z flip7" in n or "galaxy z flip 7" in n:
        if c in {"256GB", "512GB"}:
            return "12GB"

    # Algunos modelos habituales Samsung recientes del listing
    if "galaxy a56" in n:
        if c in {"128GB", "256GB"}:
            return "8GB"
    if "galaxy a36" in n:
        if c in {"128GB", "256GB"}:
            return "8GB"
    if "galaxy a26" in n:
        if c in {"128GB", "256GB"}:
            return "8GB"
    if "galaxy s25 ultra" in n:
        if c == "1TB":
            return "16GB"
        if c in {"256GB", "512GB"}:
            return "12GB"
    if "galaxy s25+" in n or "galaxy s25 +" in n:
        if c in {"256GB", "512GB"}:
            return "12GB"
    if re.search(r"\bgalaxy s25\b", n) and "ultra" not in n and "+" not in n:
        if c in {"128GB", "256GB"}:
            return "12GB"

    # Fallback mínimo por model code conocido de series premium
    if mc.startswith("SM-S948") or mc.startswith("SM-S958"):
        return "12GB" if c != "1TB" else "16GB"
    if mc.startswith("SM-S947") or mc.startswith("SM-S946") or mc.startswith("SM-S945"):
        return "12GB"
    if mc.startswith("SM-F966") or mc.startswith("SM-F956"):
        return "12GB" if c != "1TB" else "16GB"
    if mc.startswith("SM-F766") or mc.startswith("SM-F741") or mc.startswith("SM-F761"):
        return "12GB"

    return ""


# --------------------------
# PARSEO DE LISTING
# --------------------------

def extract_jsonld_products(page_html: str):
    soup = BeautifulSoup(page_html, "html.parser")
    items = []
    for sc in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = (sc.string or sc.get_text() or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue

        blocks = data if isinstance(data, list) else [data]
        for block in blocks:
            if not isinstance(block, dict):
                continue
            if block.get("@type") != "ItemList":
                continue
            for el in block.get("itemListElement") or []:
                if not isinstance(el, dict):
                    continue
                prod = el.get("item") or {}
                if not isinstance(prod, dict):
                    continue
                if prod.get("@type") != "Product":
                    continue
                name = normalizar_nombre_samsung(prod.get("name") or "")
                url = abs_url(START_URL, prod.get("url") or "")
                detail_url = abs_url(START_URL, prod.get("@id") or "")
                image = abs_url(START_URL, prod.get("image") or "")
                offers = prod.get("offers") or {}
                price = parse_eur_num(offers.get("price") or 0)
                model_code = ""
                try:
                    q = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
                    model_code = (q.get("modelCode") or [""])[0].upper()
                except Exception:
                    model_code = ""
                if not name:
                    continue
                items.append({
                    "name": name,
                    "price": price,
                    "buy_url": url,
                    "detail_url": detail_url,
                    "image": image,
                    "model_code": model_code,
                })
    return items


def extract_name_from_card_text(card_text: str) -> str:
    for line in [normalize_spaces(x) for x in (card_text or "").splitlines() if normalize_spaces(x)]:
        if line.lower().startswith("galaxy "):
            return normalizar_nombre_samsung(line)
    m = re.search(r"(Galaxy\s+[A-Za-z0-9+ ]{2,60})", normalize_spaces(card_text), flags=re.I)
    if m:
        return normalizar_nombre_samsung(m.group(1))
    return ""


def extract_card_image(card, base_url: str) -> str:
    try:
        imgs = card.find_elements("xpath", ".//img")
    except Exception:
        imgs = []
    for img in imgs:
        for attr in ["src", "data-src", "data-original", "data-lazy-src"]:
            try:
                v = (img.get_attribute(attr) or "").strip()
            except Exception:
                v = ""
            if not v:
                continue
            low = v.lower()
            if "logo" in low or "icon" in low or "sprite" in low:
                continue
            return abs_url(base_url, v)
    return ""


def extract_urls_from_card(card, base_url: str):
    buy_url = ""
    info_url = ""
    try:
        links = card.find_elements("xpath", ".//a[@href] | .//button[@data-link]")
    except Exception:
        links = []
    for el in links:
        try:
            txt = normalize_spaces(el.text)
        except Exception:
            txt = ""
        href = ""
        try:
            href = (el.get_attribute("href") or el.get_attribute("data-link") or "").strip()
        except Exception:
            href = ""
        if not href:
            continue
        href = abs_url(base_url, href)
        if "Comprar" in txt and not buy_url:
            buy_url = href
        elif ("Más información" in txt or "Ficha" in txt) and not info_url:
            info_url = href
    return info_url, buy_url


def _candidate_capacity_nodes(card):
    try:
        nodes = card.find_elements(
            "xpath",
            ".//*[self::button or self::a or self::span or self::div or self::label][normalize-space(text())]"
        )
    except Exception:
        nodes = []
    out = []
    seen = set()
    for el in nodes:
        try:
            txt = normalize_spaces(el.text)
        except Exception:
            txt = ""
        if not re.fullmatch(r"(?:\d{2,4}\s*GB|\d\s*TB)", txt, flags=re.I):
            continue
        if txt.lower() in seen:
            continue
        seen.add(txt.lower())
        out.append(el)
    return out


def _selection_score(card, el):
    score = 0
    try:
        cls = (el.get_attribute("class") or "").lower()
    except Exception:
        cls = ""
    try:
        aria_pressed = (el.get_attribute("aria-pressed") or "").lower()
    except Exception:
        aria_pressed = ""
    try:
        aria_selected = (el.get_attribute("aria-selected") or "").lower()
    except Exception:
        aria_selected = ""
    try:
        tabindex = (el.get_attribute("tabindex") or "")
    except Exception:
        tabindex = ""

    if any(k in cls for k in ["active", "selected", "current", "checked", "on", "focus"]):
        score += 50
    if aria_pressed == "true":
        score += 50
    if aria_selected == "true":
        score += 50
    if tabindex == "0":
        score += 10

    try:
        styles = card.parent.execute_script(
            "const s=getComputedStyle(arguments[0]); return {"
            "bw: s.borderWidth, bc: s.borderColor, bg: s.backgroundColor, fw: s.fontWeight, c: s.color};",
            el,
        )
    except Exception:
        styles = {}

    bw = str(styles.get("bw", ""))
    bc = str(styles.get("bc", "")).lower()
    bg = str(styles.get("bg", "")).lower()
    fw = str(styles.get("fw", ""))
    if bw and bw != "0px":
        score += 8
    if any(x in bc for x in ["0, 0, 0", "17, 17, 17", "34, 34, 34"]):
        score += 8
    if any(x in bg for x in ["0, 0, 0", "17, 17, 17", "34, 34, 34"]):
        score += 4
    try:
        if int(fw) >= 600:
            score += 5
    except Exception:
        pass
    return score


def extract_selected_capacity_from_card(card):
    nodes = _candidate_capacity_nodes(card)
    if not nodes:
        return ""
    scored = []
    for el in nodes:
        try:
            txt = normalize_spaces(el.text)
        except Exception:
            txt = ""
        if not txt:
            continue
        scored.append((_selection_score(card, el), txt))
    if not scored:
        return ""
    scored.sort(key=lambda x: (-x[0], x[1]))
    cap = normalize_spaces(scored[0][1]).replace(" ", "")
    cap = cap.upper().replace("GB", "GB").replace("TB", "TB")
    return cap


def extract_price_info_from_card_text(card_text: str, fallback_price: int = 0):
    t = normalize_spaces(card_text)
    current = 0
    original = 0

    m_before = re.search(r"Antes\s+([0-9\.\,]+)\s*€", t, flags=re.I)
    if m_before:
        original = parse_eur_num(m_before.group(1))

    prices = []
    for m in re.finditer(r"([0-9]{1,3}(?:\.[0-9]{3})*(?:,[0-9]{1,2})?|[0-9]{1,5}(?:[\.,][0-9]{1,2})?)\s*€", t, flags=re.I):
        val = parse_eur_num(m.group(1))
        if val <= 0:
            continue
        ctx = t[max(0, m.start()-18):min(len(t), m.end()+18)].lower()
        if any(x in ctx for x in ["dto", "descuento", "ahorra", "ahorro", "paypal"]):
            if "antes" not in ctx:
                continue
        prices.append(val)

    prices = [p for p in prices if 100 <= p <= 5000]
    if prices:
        current = prices[0]
        if len(prices) > 1 and not original:
            larger = [p for p in prices if p > current]
            if larger:
                original = max(larger)

    if fallback_price > 0:
        if current <= 0:
            current = fallback_price
        if original <= 0:
            original = max(fallback_price, calcular_precio_original(current))

    if current > 0 and original <= 0:
        original = calcular_precio_original(current)

    return int(current or 0), int(original or 0)


def collect_card_nodes(driver):
    from selenium.webdriver.common.by import By

    seen = set()
    cards = []
    try:
        buy_controls = driver.find_elements(By.XPATH, "//*[self::a or self::button][contains(normalize-space(.), 'Comprar')]")
    except Exception:
        buy_controls = []

    for ctl in buy_controls:
        node = ctl
        for _ in range(10):
            try:
                node = node.find_element(By.XPATH, "./..")
            except Exception:
                break
            try:
                txt = normalize_spaces(node.text)
            except Exception:
                txt = ""
            if not txt or len(txt) < 20 or len(txt) > 1800:
                continue
            if "Galaxy" not in txt or "Comprar" not in txt:
                continue
            sig = f"{int(node.location.get('x', 0))}:{int(node.location.get('y', 0))}:{int(node.size.get('width', 0))}:{int(node.size.get('height', 0))}"
            if sig in seen:
                break
            seen.add(sig)
            cards.append(node)
            break
    cards.sort(key=lambda el: (int(el.location.get("y", 0)), int(el.location.get("x", 0))))
    return cards


def extract_products_from_listing_cards(listing_url: str, source_label: str):
    productos_por_clave = {}
    driver = get_driver()
    try:
        print(f"🪄 Samsung listing-only: leyendo solo la página principal {mask_url(listing_url)}", flush=True)
        driver.set_page_load_timeout(45)
        driver.get(listing_url)
        time.sleep(3)
        dismiss_overlays(driver)
        scroll_page(driver, rounds=18)

        page_html = driver.page_source
        jsonld_items = extract_jsonld_products(page_html)
        jsonld_by_name = {}
        for item in jsonld_items:
            jsonld_by_name.setdefault(item["name"], item)
        print(f"✅ Items JSON-LD Samsung detectados: {len(jsonld_items)}", flush=True)

        cards = collect_card_nodes(driver)
        print(f"✅ Cards Samsung detectadas en listing: {len(cards)}", flush=True)

        for idx, card in enumerate(cards, start=1):
            try:
                card_text = card.text or ""
            except Exception:
                continue

            nombre = extract_name_from_card_text(card_text)
            if not nombre or should_skip_by_name(nombre):
                continue

            meta = jsonld_by_name.get(nombre, {})
            capacidad = extract_selected_capacity_from_card(card)
            if not capacidad:
                print(f"⚠️ Card Samsung sin capacidad visible para {nombre}. Se ignora en modo listing-only.", flush=True)
                continue

            precio_jsonld = int(meta.get("price") or 0)
            precio_actual, precio_original = extract_price_info_from_card_text(card_text, fallback_price=precio_jsonld)
            if precio_actual <= 0:
                print(f"⚠️ Card Samsung sin precio usable para {nombre} {capacidad}. Se ignora.", flush=True)
                continue

            _, buy_url_dom = extract_urls_from_card(card, listing_url)
            buy_url = buy_url_dom or meta.get("buy_url") or meta.get("detail_url") or listing_url
            img = extract_card_image(card, listing_url) or meta.get("image") or ""
            model_code = meta.get("model_code") or ""
            memoria = resolver_ram_desde_listing(nombre, capacidad, model_code)
            if not memoria:
                print(f"⚠️ Card Samsung sin RAM resoluble desde listing para {nombre} {capacidad}. Se ignora en modo listing-only.", flush=True)
                continue

            url_importada_sin_afiliado = buy_url
            url_con_afiliado = unir_afiliado(url_importada_sin_afiliado, AFF_RAW)
            url_oferta = acortar_url(url_con_afiliado) if url_con_afiliado else ""

            key = source_key(nombre, memoria, capacidad, FUENTE)
            remoto = {
                "nombre": nombre,
                "memoria": memoria,
                "capacidad": capacidad,
                "precio_actual": int(precio_actual),
                "precio_original": int(precio_original),
                "img": img,
                "url_imp": url_importada_sin_afiliado,
                "url_oferta_sin_acortar": url_importada_sin_afiliado,
                "url_importada_sin_afiliado": url_importada_sin_afiliado,
                "url_sin_acortar_con_mi_afiliado": url_con_afiliado,
                "url_oferta": url_oferta,
                "enviado_desde": ENVIADO_DESDE,
                "enviado_desde_tg": ENVIADO_DESDE_TG,
                "fecha": datetime.now().strftime("%d/%m/%Y"),
                "version": VERSION,
                "fuente": FUENTE,
                "codigo_descuento": CODIGO_DESCUENTO_DEFAULT,
                "origen_pagina": source_label,
                "origen_listado": listing_url,
                "source_key": key,
                "model_code": model_code,
            }

            if key in productos_por_clave:
                prev = productos_por_clave[key]
                summary_duplicados.append(f"{nombre} {capacidad} {memoria}")
                if int(remoto["precio_actual"]) < int(prev.get("precio_actual", 10**9)):
                    productos_por_clave[key] = remoto
            else:
                productos_por_clave[key] = remoto

    except Exception as e:
        print(f"❌ Error renderizando listing Samsung: {e}", flush=True)
    finally:
        try:
            driver.quit()
        except Exception:
            pass

    return list(productos_por_clave.values())


# --------------------------
# EXTRACCIÓN REMOTA
# --------------------------

def obtener_datos_remotos():
    print("", flush=True)
    print("--- FASE 1: ESCANEANDO SAMSUNG ---", flush=True)
    print(f"URL base: {mask_url(START_URL)}", flush=True)

    productos = extract_products_from_listing_cards(START_URL, "listing-principal")

    print("📊 RESUMEN EXTRACCIÓN SAMSUNG:", flush=True)
    print("   URLs descubiertas: 1 (listing principal)", flush=True)
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
    nombre_hijo = limpiar_nombre_para_categoria(nombre_completo)

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
# WOO: SINCRONIZACIÓN
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
                meta = {m["key"]: str(m.get("value", "")) for m in p.get("meta_data", []) if isinstance(m, dict) and m.get("key")}
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
    print("\n--- FASE 2: SINCRONIZANDO SAMSUNG ---", flush=True)
    cache_categorias = obtener_todas_las_categorias()
    locales = cargar_locales_samsung()

    print(f"📦 Productos Samsung existentes en la web: {len(locales)}", flush=True)
    print(f"📦 Productos remotos Samsung a procesar: {len(remotos)}", flush=True)

    remote_by_key = {r["source_key"]: r for r in remotos}
    local_by_key = {build_local_key(l): l for l in locales}

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
            id_padre, id_hijo = resolver_jerarquia(r["nombre"], cache_categorias)

            img_subcat = obtener_imagen_categoria(cache_categorias, id_hijo)
            if (not img_subcat) and r.get("img"):
                actualizar_imagen_categoria(cache_categorias, id_hijo, r["img"])
                img_subcat = obtener_imagen_categoria(cache_categorias, id_hijo)
            img_final_producto = img_subcat or r.get("img") or ""

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
                    "imagen_producto": r.get("img", ""),
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

                if cambios:
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
                    {"key": "codigo_de_descuento", "value": r.get("codigo_descuento", CODIGO_DESCUENTO_DEFAULT)},
                    {"key": "enviado_desde", "value": ENVIADO_DESDE},
                    {"key": "enviado_desde_tg", "value": ENVIADO_DESDE_TG},
                    {"key": "enlace_de_compra_importado", "value": r.get("url_imp", "")},
                    {"key": "url_oferta_sin_acortar", "value": r.get("url_oferta_sin_acortar", "")},
                    {"key": "url_importada_sin_afiliado", "value": r.get("url_importada_sin_afiliado", "")},
                    {"key": "url_sin_acortar_con_mi_afiliado", "value": r.get("url_sin_acortar_con_mi_afiliado", "")},
                    {"key": "url_oferta", "value": r.get("url_oferta", "")},
                    {"key": "imagen_producto", "value": r.get("img", "")},
                    {"key": "version", "value": r.get("version", VERSION)},
                    {"key": "_odm_source_key", "value": r["source_key"]},
                    {"key": "_odm_source_model_code", "value": r.get("model_code", "")},
                    {"key": "_odm_source_listing", "value": r.get("origen_listado", "")},
                    {"key": "_odm_source_page", "value": r.get("origen_pagina", "")},
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
