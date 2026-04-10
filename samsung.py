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
# - Lee solo https://www.samsung.com/es/smartphones/all-smartphones/
# - Extrae los productos desde la página principal renderizada
# - Sin entrar en fichas de producto ni buy pages
# - Sincroniza WooCommerce con altas, actualizaciones y obsoletos
# ============================================================

DEFAULT_START_URL = "https://www.samsung.com/es/smartphones/all-smartphones/"
START_URL = os.getenv("SOURCE_URL_SAMSUNG", DEFAULT_START_URL).strip() or DEFAULT_START_URL
LISTING_URLS = [("1", START_URL)]

FUENTE = "Samsung"
ID_IMPORTACION = START_URL.rstrip("/")
ENVIADO_DESDE = "España"
ENVIADO_DESDE_TG = "🇪🇸 España"
VERSION = "Versión Global"
CODIGO_DESCUENTO_DEFAULT = "OFERTA: PROMO."
OBJETIVO = 120

AFF_RAW = os.environ.get("AFF_SAMSUNG", "").strip()

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


def unir_afiliado(url_base: str, aff: str) -> str:
    """Concatena AFF_SAMSUNG sin romper la URL base.

    Resultado esperado:
    https://www.samsung.com/es/smartphones/galaxy-z-fold7/?sv1=...
    """
    base = (url_base or "").strip()
    aff = (aff or "").strip()
    if not base or not aff:
        return base
    if aff.lower().startswith("http"):
        return aff
    base = base.split("?")[0].rstrip("/") + "/"
    aff = aff.lstrip("?&")
    return base + "?" + aff


def normalize_detail_base_url(url: str) -> str:
    if not url:
        return ""
    try:
        u = urllib.parse.urlsplit(url)
        path = (u.path or "").rstrip("/")
        path = re.sub(r"/buy$", "", path, flags=re.I)
        return f"{u.scheme}://{u.netloc}{path}"
    except Exception:
        return (url or "").split("?")[0].rstrip("/")


def expanded_from_url(url: str) -> str:
    return (url or "").strip().split("#")[0]


def extract_model_code(text: str) -> str:
    m = re.search(r"\bSM-[A-Z0-9]+\b", text or "", flags=re.I)
    return m.group(0).upper() if m else ""


def parse_capacidad_desde_texto(txt: str) -> str:
    t = normalize_spaces(txt)
    m = re.search(r"\b(64|128|256|512|1024)\s*GB\b", t, flags=re.I)
    if m:
        return f"{m.group(1)}GB"
    m = re.search(r"\b(1|2)\s*TB\b", t, flags=re.I)
    if m:
        return f"{m.group(1)}TB"
    return ""


def should_skip_by_name(nombre: str) -> bool:
    u = (nombre or "").upper()
    return any(x in u for x in [" TAB", "IPAD", " PAD"]) or u.startswith("TAB ")


def _normalize_word(word: str) -> str:
    w = word.strip()
    if not w:
        return ""
    low = w.lower()
    if low in {"samsung", "galaxy"}:
        return w[:1].upper() + w[1:].lower()
    if low in {"gb", "tb", "fe"}:
        return low.upper()
    if low in {"ultra", "plus", "edge", "awesome", "navy", "black", "white", "blue", "icyblue"}:
        return low.title()
    # 5g -> 5G ; 4g -> 4G
    if re.fullmatch(r"\d+[A-Za-z]+", w):
        return "".join(ch.upper() if ch.isalpha() else ch for ch in w)
    # s26, a57, s25+ -> S26 / A57 / S25+
    if re.fullmatch(r"[A-Za-z]{1,3}\d[\dA-Za-z+]*", w):
        prefix = re.match(r"[A-Za-z]+", w).group(0)
        rest = w[len(prefix):]
        if len(prefix) <= 2:
            return prefix.upper() + rest
        return prefix[:1].upper() + prefix[1:].lower() + rest
    # flip7, fold7 -> Flip7/Fold7
    if re.fullmatch(r"[A-Za-z]{4,}\d[\dA-Za-z+]*", w):
        prefix = re.match(r"[A-Za-z]+", w).group(0)
        rest = w[len(prefix):]
        return prefix[:1].upper() + prefix[1:].lower() + rest
    return w[:1].upper() + w[1:].lower()


def normalizar_nombre_samsung(nombre: str) -> str:
    t = normalize_spaces(nombre)
    if not t:
        return ""
    t = re.sub(r"\bExclusivo Online\b", "", t, flags=re.I)
    t = t.replace("()", "")
    t = normalize_spaces(t)
    if t.lower().startswith("samsung "):
        base = t[len("Samsung "):]
    else:
        base = t
    words = [_normalize_word(w) for w in base.split() if _normalize_word(w)]
    base = " ".join(words)
    if not base.lower().startswith("galaxy "):
        base = f"Galaxy {base}" if base else "Galaxy"
    return normalize_spaces(f"Samsung {base}")


def clean_listing_name(raw: str) -> str:
    t = normalize_spaces(raw or "")
    t = re.sub(r"\bExclusivo Online\b", "", t, flags=re.I)
    t = t.replace("()", "").strip()
    t = re.sub(r"\s+", " ", t).strip()
    return normalizar_nombre_samsung(t)


def source_key(nombre: str, memoria: str, capacidad: str, fuente: str = FUENTE) -> str:
    return f"{normalize_spaces(nombre).lower()}|{str(memoria).upper()}|{str(capacidad).upper()}|{fuente.lower()}"


RAM_BY_NAME_CAPACITY = {
    ("samsung galaxy s26", "256GB"): "12GB",
    ("samsung galaxy s26", "512GB"): "12GB",
    ("samsung galaxy s26+", "256GB"): "12GB",
    ("samsung galaxy s26+", "512GB"): "12GB",
    ("samsung galaxy s26 ultra", "256GB"): "12GB",
    ("samsung galaxy s26 ultra", "512GB"): "12GB",
    ("samsung galaxy s26 ultra", "1TB"): "16GB",
    ("samsung galaxy z fold7", "256GB"): "12GB",
    ("samsung galaxy z fold7", "512GB"): "12GB",
    ("samsung galaxy z fold7", "1TB"): "16GB",
    ("samsung galaxy z flip7", "256GB"): "12GB",
    ("samsung galaxy z flip7", "512GB"): "12GB",
    ("samsung galaxy z flip7 fe", "128GB"): "8GB",
    ("samsung galaxy z flip7 fe", "256GB"): "8GB",
    ("samsung galaxy s25", "128GB"): "12GB",
    ("samsung galaxy s25", "256GB"): "12GB",
    ("samsung galaxy s25", "512GB"): "12GB",
    ("samsung galaxy s25+", "256GB"): "12GB",
    ("samsung galaxy s25+", "512GB"): "12GB",
    ("samsung galaxy s25 ultra", "256GB"): "12GB",
    ("samsung galaxy s25 ultra", "512GB"): "12GB",
    ("samsung galaxy s25 ultra", "1TB"): "12GB",
    ("samsung galaxy s25 FE", "128GB"): "8GB",
    ("samsung galaxy s25 FE", "256GB"): "8GB",
    ("samsung galaxy s25 FE", "512GB"): "8GB",
    ("samsung galaxy s25 edge", "256GB"): "12GB",
    ("samsung galaxy s25 edge", "512GB"): "12GB",
    ("samsung galaxy s24", "128GB"): "8GB",
    ("samsung galaxy s24", "256GB"): "8GB",
    ("samsung galaxy s24+", "256GB"): "12GB",
    ("samsung galaxy s24+", "512GB"): "12GB",
    ("samsung galaxy s24 FE", "128GB"): "8GB",
    ("samsung galaxy s24 FE", "256GB"): "8GB",
    ("samsung galaxy z flip6", "256GB"): "12GB",
    ("samsung galaxy z flip6", "512GB"): "12GB",
    ("samsung galaxy z fold6", "256GB"): "12GB",
    ("samsung galaxy z fold6", "512GB"): "12GB",
    ("samsung galaxy z fold6", "1TB"): "16GB",
    ("samsung galaxy a57 5G", "128GB"): "8GB",
    ("samsung galaxy a57 5G", "256GB"): "8GB",
    ("samsung galaxy a57 5G", "512GB"): "12GB",
    ("samsung galaxy a56 5G", "128GB"): "8GB",
    ("samsung galaxy a56 5G", "256GB"): "8GB",
    ("samsung galaxy a37 5G", "256GB"): "8GB",
    ("samsung galaxy a36 5G", "128GB"): "8GB",
    ("samsung galaxy a36 5G", "256GB"): "8GB",
    ("samsung galaxy a26 5G", "128GB"): "6GB",
    ("samsung galaxy a26 5G", "256GB"): "8GB",
    ("samsung galaxy a17 5G", "256GB"): "8GB",
    ("samsung galaxy a17", "256GB"): "8GB",
    ("samsung galaxy a16", "256GB"): "8GB",
}


def resolve_ram_for_listing(nombre: str, capacidad: str, url_hint: str = "") -> str:
    key = (normalize_spaces(nombre), (capacidad or "").upper())
    if key in RAM_BY_NAME_CAPACITY:
        return RAM_BY_NAME_CAPACITY[key]
    name_low = normalize_spaces(nombre).lower()
    hint_low = (url_hint or "").lower()
    if "galaxy-z-fold" in hint_low or "z fold" in name_low:
        return "16GB" if (capacidad or "").upper() == "1TB" else "12GB"
    if "galaxy-z-flip" in hint_low or "z flip" in name_low:
        return "8GB" if "flip7 fe" in name_low else "12GB"
    if " ultra" in name_low:
        return "16GB" if (capacidad or "").upper() == "1TB" else "12GB"
    if "s25 edge" in name_low:
        return "12GB"
    return ""


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
        "--user-agent=Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
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


def scroll_page(driver, rounds: int = 20):
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


# --------------------------
# LISTING-ONLY: JSON-LD + CARDS
# --------------------------

def extract_jsonld_products(html: str):
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for script in soup.find_all("script", attrs={"type": re.compile(r"ld\+json", re.I)}):
        raw = script.string or script.get_text() or ""
        raw = raw.strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue

        stack = [data]
        while stack:
            node = stack.pop()
            if isinstance(node, list):
                stack.extend(node)
                continue
            if not isinstance(node, dict):
                continue
            if isinstance(node.get("itemListElement"), list):
                stack.extend(node.get("itemListElement"))
            if isinstance(node.get("item"), dict):
                stack.append(node.get("item"))
            if isinstance(node.get("@graph"), list):
                stack.extend(node.get("@graph"))

            t = node.get("@type")
            tlist = t if isinstance(t, list) else [t]
            if "Product" not in [str(x) for x in tlist if x]:
                continue

            name = clean_listing_name(node.get("name") or "")
            if not name:
                continue

            offers = node.get("offers") or {}
            if isinstance(offers, list):
                offers = offers[0] if offers else {}
            price = parse_eur_num(str(offers.get("price") or ""))
            raw_url = node.get("url") or node.get("@id") or ""
            expanded = expanded_from_url(abs_url(START_URL, raw_url)) if raw_url else ""
            detail = normalize_detail_base_url(expanded) if expanded else ""
            img = node.get("image") or ""
            if isinstance(img, list):
                img = img[0] if img else ""
            img = abs_url(START_URL, img) if img else ""
            desc = normalize_spaces(node.get("description") or "")
            cap = parse_capacidad_desde_texto(detail + " " + desc + " " + name)
            model_code = extract_model_code(expanded + " " + desc)
            out.append(
                {
                    "name": name,
                    "price": price,
                    "expanded_url": expanded,
                    "detail_url": detail,
                    "image": img,
                    "capacidad": cap,
                    "model_code": model_code,
                }
            )

    by_url = {}
    by_name = {}
    for item in out:
        if item["detail_url"]:
            by_url.setdefault(item["detail_url"], item)
        by_name.setdefault(item["name"].lower(), []).append(item)
    return out, by_url, by_name


def extract_name_from_textblock(text: str) -> str:
    lines = [normalize_spaces(x) for x in re.split(r"[\n\r]+", text) if normalize_spaces(x)]
    for line in lines:
        low = line.lower()
        if "galaxy" not in low:
            continue
        if any(x in low for x in ["comprar", "compara", "ahorra", "entrega y estrena", "descubre", "ver más"]):
            continue
        if any(x in low for x in ["tab", "watch", "buds", "ring", "accesorios"]):
            continue
        line = re.split(r"\b(?:64|128|256|512|1024)\s*GB\b|\b(?:1|2)\s*TB\b", line, maxsplit=1, flags=re.I)[0]
        nombre = clean_listing_name(line)
        if nombre and nombre != "Samsung Galaxy":
            return nombre
    return ""


def price_pair_from_listing_text(text: str, fallback_price: int = 0):
    vals = [v for v in parse_eur_all(text) if 150 <= v <= 5000]
    vals = dedupe_keep_order(vals)
    current = 0
    original = 0
    if vals:
        current = min(vals)
        original = max(vals)
        if original == current:
            original = 0
    if not current and fallback_price:
        current = fallback_price
    if current and not original:
        original = calcular_precio_original(current)
    if original and original < current:
        original = calcular_precio_original(current)
    return int(current or 0), int(original or 0)


def selected_capacity_from_container(container):
    from selenium.webdriver.common.by import By

    storage_re = re.compile(r"^\d+\s*(GB|TB)$", re.I)
    candidates = []
    try:
        els = container.find_elements(By.XPATH, ".//*")
    except Exception:
        return ""

    for el in els:
        try:
            if not el.is_displayed():
                continue
            txt = normalize_spaces(el.text)
            if not txt or len(txt) > 20 or not storage_re.fullmatch(txt):
                continue
            score = 0
            cls = (el.get_attribute("class") or "").lower()
            attrs = " ".join([(el.get_attribute(a) or "") for a in ["aria-selected", "aria-pressed", "selected", "checked"]]).lower()
            if "true" in attrs:
                score += 100
            if any(k in cls for k in ["selected", "active", "checked", "current", "is-selected"]):
                score += 50
            candidates.append((score, txt))
        except Exception:
            pass

    if not candidates:
        return ""
    positives = [c for c in candidates if c[0] > 0]
    if positives:
        positives.sort(key=lambda x: (-x[0], x[1]))
        return parse_capacidad_desde_texto(positives[0][1])
    # Si no hay ninguna marcada como activa/seleccionada, solo aceptamos
    # la capacidad si el bloque ofrece una única opción visible.
    uniques = dedupe_keep_order([txt for _, txt in candidates])
    if len(uniques) == 1:
        return parse_capacidad_desde_texto(uniques[0])
    return ""


def find_urls_in_card(card, fallback: str = ""):
    from selenium.webdriver.common.by import By

    buy = ""
    detail = ""
    try:
        anchors = card.find_elements(By.XPATH, ".//a[@href]")
    except Exception:
        anchors = []

    for a in anchors:
        try:
            href = normalize_spaces(a.get_attribute("href") or "")
            txt = normalize_spaces(a.text or "")
            if not href or "/es/smartphones/" not in href:
                continue
            if "/buy/" in href or "comprar" in txt.lower():
                buy = href
            if not detail:
                detail = normalize_detail_base_url(href)
        except Exception:
            pass

    if buy and not detail:
        detail = normalize_detail_base_url(buy)
    if fallback and not detail:
        detail = normalize_detail_base_url(fallback)
    if fallback and not buy:
        buy = fallback
    return expanded_from_url(buy), normalize_detail_base_url(detail)


def extract_listing_products(listing_url: str):
    from selenium.webdriver.common.by import By

    driver = get_driver()
    products = []
    try:
        driver.set_page_load_timeout(45)
        driver.get(listing_url)
        time.sleep(3)
        dismiss_overlays(driver)
        scroll_page(driver, rounds=20)
        html = driver.page_source
        jsonld_items, jsonld_by_url, jsonld_by_name = extract_jsonld_products(html)
        print(f"✅ Items JSON-LD Samsung detectados: {len(jsonld_items)}", flush=True)

        buy_buttons = []
        for xp in [
            "//a[contains(normalize-space(.), 'Comprar')]",
            "//button[contains(normalize-space(.), 'Comprar')]",
        ]:
            try:
                buy_buttons.extend(driver.find_elements(By.XPATH, xp))
            except Exception:
                pass

        cards = []
        seen_ids = set()
        for btn in buy_buttons:
            try:
                if not btn.is_displayed():
                    continue
                # subimos por los padres hasta encontrar el bloque más cercano con Galaxy y precio
                cur = btn
                container = None
                for _ in range(8):
                    cur = cur.find_element(By.XPATH, "..")
                    txt = normalize_spaces(cur.text)
                    if "galaxy" in txt.lower() and "€" in txt:
                        container = cur
                        break
                if not container:
                    continue
                key = container.id
                if key in seen_ids:
                    continue
                seen_ids.add(key)
                cards.append(container)
            except Exception:
                pass

        print(f"✅ Cards Samsung detectadas en listing: {len(cards)}", flush=True)

        dedup = {}
        for card in cards:
            try:
                text = normalize_spaces(card.text)
                if not text or "galaxy" not in text.lower():
                    continue

                nombre = extract_name_from_textblock(card.text)
                if not nombre or should_skip_by_name(nombre):
                    continue

                buy_url, detail_url = find_urls_in_card(card)
                match = None
                if detail_url and detail_url in jsonld_by_url:
                    match = jsonld_by_url[detail_url]
                if not match:
                    arr = jsonld_by_name.get(nombre.lower(), [])
                    if arr:
                        match = arr[0]
                if match:
                    if not detail_url:
                        detail_url = match.get("detail_url", "")
                    if not buy_url:
                        buy_url = match.get("expanded_url", "")

                detail_url = normalize_detail_base_url(detail_url or buy_url)
                buy_url = expanded_from_url(buy_url or detail_url)
                if not detail_url:
                    continue

                capacidad = selected_capacity_from_container(card)
                if not capacidad:
                    capacidad = parse_capacidad_desde_texto(detail_url + " " + text)
                if not capacidad and match:
                    capacidad = match.get("capacidad", "")
                if not capacidad:
                    continue

                memoria = resolve_ram_for_listing(nombre, capacidad, detail_url or buy_url)
                if not memoria:
                    print(f"⚠️ Card Samsung sin RAM resoluble para {nombre} {capacidad}. Se ignora.", flush=True)
                    continue

                fallback_price = int(match.get("price", 0) or 0) if match else 0
                precio_actual, precio_original = price_pair_from_listing_text(text, fallback_price=fallback_price)
                if precio_actual <= 0:
                    print(f"⚠️ Card Samsung sin precio usable para {nombre} {capacidad}. Se ignora.", flush=True)
                    continue

                model_code = ""
                try:
                    qs = urllib.parse.parse_qs(urllib.parse.urlsplit(buy_url).query)
                    model_code = (qs.get("modelCode") or [""])[0]
                except Exception:
                    model_code = ""
                if not model_code and match:
                    model_code = match.get("model_code", "")

                key = source_key(nombre, memoria, capacidad, FUENTE)
                remote = {
                    "nombre": nombre,
                    "memoria": memoria,
                    "capacidad": capacidad,
                    "precio_actual": int(precio_actual),
                    "precio_original": int(precio_original),
                    "img": "",
                    "url_imp": detail_url,
                    "url_oferta_sin_acortar": buy_url or detail_url,
                    "url_importada_sin_afiliado": detail_url,
                    "buy_url": buy_url or detail_url,
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
                }

                if key in dedup:
                    summary_duplicados.append(f"{nombre} {capacidad} {memoria}")
                    prev = dedup[key]
                    if int(remote["precio_actual"]) < int(prev.get("precio_actual", 10**9)):
                        dedup[key] = remote
                else:
                    dedup[key] = remote
            except Exception:
                continue

        products = list(dedup.values())
    finally:
        try:
            driver.quit()
        except Exception:
            pass
    return products


# --------------------------
# EXTRACCIÓN REMOTA
# --------------------------

def obtener_datos_remotos():
    print("", flush=True)
    print("--- FASE 1: ESCANEANDO SAMSUNG ---", flush=True)
    print(f"URL base: {mask_url(START_URL)}", flush=True)
    print(f"🪄 Samsung listing-only: leyendo solo la página principal {mask_url(START_URL)}", flush=True)
    productos = extract_listing_products(START_URL)
    print("", flush=True)
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


def _norm_image_ref(u: str) -> str:
    try:
        p = urllib.parse.urlsplit(u or "")
        return f"{p.netloc}{p.path}".lower().rstrip("/")
    except Exception:
        return (u or "").lower().rstrip("/")


def _is_brand_like_image(url: str) -> bool:
    low = _norm_image_ref(url)
    if not low:
        return False
    filename = low.rsplit("/", 1)[-1]
    tokens = ["logo", "marca", "brand", "placeholder", "default", "logo_marca", "samsung_logo_marca"]
    return any(tok in filename for tok in tokens)


def seleccionar_imagen_subcategoria(cache_categorias, id_padre, id_hijo):
    """Usa SOLO la imagen propia de la subcategoría exacta.

    Nunca usa la imagen del padre, ni logos de marca, ni imágenes clonadas del padre.
    Si la subcategoría no tiene una imagen válida, devuelve ''.
    """
    if not id_hijo:
        return ""
    img_hijo = obtener_imagen_categoria(cache_categorias, id_hijo)
    if not img_hijo:
        return ""
    img_padre = obtener_imagen_categoria(cache_categorias, id_padre)
    if _is_brand_like_image(img_hijo):
        return ""
    if img_padre and _norm_image_ref(img_hijo) == _norm_image_ref(img_padre):
        return ""
    return img_hijo


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

    # 1) Obsoletos
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

            url_base = (r.get("url_importada_sin_afiliado") or r.get("url_imp") or "").strip().split("?")[0].rstrip("/")
            url_con_afiliado = unir_afiliado(url_base, AFF_RAW) if AFF_RAW else (url_base.rstrip("/") + "/")
            url_oferta = acortar_url(url_con_afiliado)

            match = local_by_key.get(r["source_key"])
            id_padre, id_hijo = resolver_jerarquia(r["nombre"], cache_categorias)
            img_final_producto = seleccionar_imagen_subcategoria(cache_categorias, id_padre, id_hijo)

            print(f"9) URL Imagen: {img_final_producto}", flush=True)
            print(f"10) Enlace Importado: {mask_url(r.get('url_imp', ''))}", flush=True)
            print(f"11) Enlace Expandido: {mask_url(r.get('url_oferta_sin_acortar', ''))}", flush=True)
            print(f"12) URL importada sin afiliado: {mask_url(r.get('url_importada_sin_afiliado', ''))}", flush=True)
            print(f"13) URL sin acortar con mi afiliado: {mask_url(url_con_afiliado)}", flush=True)
            print(f"14) URL acortada con mi afiliado: {url_oferta}", flush=True)
            print(f"15) Enviado desde: {r.get('enviado_desde', ENVIADO_DESDE)}", flush=True)
            print(f"15) Importado de: {ID_IMPORTACION}", flush=True)
            print("16) Encolado para comparar con base de datos...", flush=True)
            print("-" * 60, flush=True)

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
                    "url_sin_acortar_con_mi_afiliado": url_con_afiliado,
                    "url_oferta": url_oferta,
                    "url_importada_sin_afiliado": url_base,
                    "url_oferta_sin_acortar": r.get("url_oferta_sin_acortar", url_base),
                }
                for k, v in compare_meta.items():
                    if str(meta.get(k, "")) != str(v):
                        cambios.append(f"{k}: {meta.get(k, '')} -> {v}")
                        payload["meta_data"].append({"key": k, "value": v})

                current_img_meta = str(meta.get("imagen_producto", "") or "")
                if current_img_meta != str(img_final_producto):
                    payload["images"] = ([{"src": img_final_producto}] if img_final_producto else [])

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
                    {"key": "enlace_de_compra_importado", "value": url_base},
                    {"key": "url_oferta_sin_acortar", "value": r.get("url_oferta_sin_acortar", url_base)},
                    {"key": "url_importada_sin_afiliado", "value": url_base},
                    {"key": "url_sin_acortar_con_mi_afiliado", "value": url_con_afiliado},
                    {"key": "url_oferta", "value": url_oferta},
                    {"key": "imagen_producto", "value": img_final_producto},
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
            summary_fallidos.append({"nombre": r.get("nombre", "?"), "error": str(e)})

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
        print("No se han obtenido productos remotos de Samsung.", flush=True)


if __name__ == "__main__":
    main()
