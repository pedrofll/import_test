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

DEFAULT_START_URL = "https://www.samsung.com/es/smartphones/all-smartphones/"
START_URL = (os.getenv("SOURCE_URL_SAMSUNG") or DEFAULT_START_URL).strip() or DEFAULT_START_URL

FUENTE = "Samsung"
ID_IMPORTACION = START_URL.rstrip("/")
ENVIADO_DESDE = "Espana"
ENVIADO_DESDE_TG = "\U0001F1EA\U0001F1F8 Espana"
VERSION = "Version Global"
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


EURO_AMOUNT_RE = r"(\d{1,3}(?:\.\d{3})*(?:,\d{1,2})?|\d{1,5}(?:[\.,]\d{1,2})?)\s*EUR|" \
                  r"(\d{1,3}(?:\.\d{3})*(?:,\d{1,2})?|\d{1,5}(?:[\.,]\d{1,2})?)\s*€"


def parse_eur_all(txt):
    if not txt:
        return []
    vals = []
    for m in re.finditer(EURO_AMOUNT_RE, txt, flags=re.I):
        g = m.group(1) or m.group(2)
        v = parse_eur_num(g)
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
    t = re.sub(r"\bExclusivo Online\b", "", t, flags=re.I)
    t = t.replace("()", "")
    t = normalize_spaces(t)

    if t.lower().startswith("samsung "):
        base = t[len("Samsung "):]
    else:
        base = t

    out = []
    for w in base.split():
        low = w.lower()
        if re.fullmatch(r"\d+(gb|tb)", low):
            out.append(low.upper())
        elif re.fullmatch(r"\d+[a-z]+", low):
            out.append("".join(ch.upper() if ch.isalpha() else ch for ch in w))
        elif re.fullmatch(r"[a-z]+\d[\da-z+]*", low):
            prefix = re.match(r"[a-z]+", low).group(0)
            rest = w[len(prefix):]
            if prefix in {"s", "a", "z", "m"}:
                out.append(prefix.upper() + rest)
            elif prefix in {"flip", "fold"}:
                out.append(prefix.title() + rest)
            else:
                out.append(prefix[:1].upper() + prefix[1:] + rest)
        elif low in {"ultra", "plus", "edge", "fe", "awesome", "navy", "black", "gray", "grey", "graphite", "silver", "white", "blue", "lavender", "mint", "icyblue", "green"}:
            out.append(low.upper() if low == "fe" else low.title())
        else:
            out.append(w[:1].upper() + w[1:].lower())

    base = normalize_spaces(" ".join(out))
    if not base.lower().startswith("galaxy "):
        base = f"Galaxy {base}"
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


def canonical_samsung_import_url(url):
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


def extraer_model_code(url):
    try:
        q = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
        vals = q.get("modelCode") or q.get("modelcode") or []
        if vals:
            return vals[0].strip().upper()
    except Exception:
        pass
    m = re.search(r"\bSM-[A-Z0-9]+\b", url or "", flags=re.I)
    return m.group(0).upper() if m else ""


def join_affiliate_url(base_url, aff_raw):
    base = canonical_samsung_import_url(base_url)
    aff = (aff_raw or "").strip()
    if not base:
        return ""
    if not aff:
        return base
    if aff.lower().startswith("http"):
        return aff
    aff = aff.lstrip("?&")
    if not aff:
        return base
    return base.rstrip("/") + "/?" + aff


def source_key(nombre, memoria, capacidad, fuente=FUENTE):
    return f"{normalize_spaces(nombre).lower()}|{str(memoria).upper()}|{str(capacidad).upper()}|{fuente.lower()}"


def item_name_key(nombre):
    t = normalizar_nombre_samsung(nombre).lower().replace("samsung ", "")
    t = re.sub(r"\bexclusivo online\b", "", t, flags=re.I)
    t = re.sub(r"[^a-z0-9+]+", "", t)
    return t


RAM_BY_NAME_CAP = {
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
    ("samsung galaxy s25 fe", "128GB"): "8GB",
    ("samsung galaxy s25 fe", "256GB"): "8GB",
    ("samsung galaxy s25 fe", "512GB"): "8GB",
    ("samsung galaxy s25 edge", "256GB"): "12GB",
    ("samsung galaxy s25 edge", "512GB"): "12GB",
    ("samsung galaxy s24", "128GB"): "8GB",
    ("samsung galaxy s24", "256GB"): "8GB",
    ("samsung galaxy s24+", "256GB"): "12GB",
    ("samsung galaxy s24+", "512GB"): "12GB",
    ("samsung galaxy s24 fe", "128GB"): "8GB",
    ("samsung galaxy s24 fe", "256GB"): "8GB",
    ("samsung galaxy z flip6", "256GB"): "12GB",
    ("samsung galaxy z flip6", "512GB"): "12GB",
    ("samsung galaxy z fold6", "256GB"): "12GB",
    ("samsung galaxy z fold6", "512GB"): "12GB",
    ("samsung galaxy z fold6", "1TB"): "16GB",
    ("samsung galaxy a57 5g", "128GB"): "8GB",
    ("samsung galaxy a57 5g", "256GB"): "8GB",
    ("samsung galaxy a57 5g", "512GB"): "12GB",
    ("samsung galaxy a56 5g", "128GB"): "8GB",
    ("samsung galaxy a56 5g", "256GB"): "8GB",
    ("samsung galaxy a37 5g", "256GB"): "8GB",
    ("samsung galaxy a36 5g", "128GB"): "8GB",
    ("samsung galaxy a36 5g", "256GB"): "8GB",
    ("samsung galaxy a26 5g", "128GB"): "6GB",
    ("samsung galaxy a26 5g", "256GB"): "8GB",
    ("samsung galaxy a17 5g", "256GB"): "8GB",
    ("samsung galaxy a17", "256GB"): "8GB",
    ("samsung galaxy a16", "256GB"): "8GB",
}


def resolve_memory(nombre, capacidad):
    key = (normalizar_nombre_samsung(nombre).lower(), (capacidad or "").upper())
    if key in RAM_BY_NAME_CAP:
        return RAM_BY_NAME_CAP[key]
    name_low = normalizar_nombre_samsung(nombre).lower()
    cap_up = (capacidad or "").upper()
    if "z fold" in name_low:
        return "16GB" if cap_up == "1TB" else "12GB"
    if "z flip" in name_low:
        return "8GB" if "fe" in name_low else "12GB"
    if " ultra" in name_low:
        return "16GB" if cap_up == "1TB" else "12GB"
    if "s25 edge" in name_low:
        return "12GB"
    return ""


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
        "//button[contains(., 'Si')]",
        "//button[contains(., 'MAS TARDE')]",
        "//button[contains(., 'MAS TARDE')]",
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


def scroll_page(driver, rounds=18):
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


def get_rendered_html(url):
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


def parse_jsonld_listing_items(html):
    soup = BeautifulSoup(html, "html.parser")
    out = []

    def walk(x):
        if isinstance(x, dict):
            yield x
            for v in x.values():
                for y in walk(v):
                    yield y
        elif isinstance(x, list):
            for v in x:
                for y in walk(v):
                    yield y

    for sc in soup.find_all("script", attrs={"type": re.compile(r"ld\+json", re.I)}):
        raw = (sc.string or sc.get_text() or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        for node in walk(data):
            if not isinstance(node, dict):
                continue
            if str(node.get("@type") or "") != "Product":
                continue
            raw_name = normalize_spaces(node.get("name") or "")
            clean_name = normalizar_nombre_samsung(raw_name)
            if not clean_name or should_skip_by_name(clean_name):
                continue
            raw_url = abs_url(START_URL, str(node.get("url") or "").strip())
            detail_url = canonical_samsung_import_url(raw_url)
            img = node.get("image") or ""
            if isinstance(img, list):
                img = img[0] if img else ""
            img = abs_url(START_URL, img) if img else ""
            offers = node.get("offers") or {}
            if isinstance(offers, list):
                offers = offers[0] if offers else {}
            price = parse_eur_num(str(offers.get("price") or "")) if isinstance(offers, dict) else 0
            text_blob = normalize_spaces(json.dumps(node, ensure_ascii=False))
            out.append({
                "raw_name": raw_name,
                "clean_name": clean_name,
                "exclusive": "exclusivo online" in raw_name.lower(),
                "price": price,
                "buy_url": raw_url,
                "detail_url": detail_url,
                "image": img,
                "model_code": extraer_model_code(raw_url + " " + text_blob),
            })

    dedup = {}
    for item in out:
        key = (item["clean_name"], item["price"], item["buy_url"])
        dedup[key] = item
    return list(dedup.values())


def find_card_blocks(html):
    soup = BeautifulSoup(html, "html.parser")
    blocks = []
    seen = set()
    nodes = []
    for node in soup.find_all(string=re.compile(r"^\s*Comprar\s*$", flags=re.I)):
        if node and node.parent is not None:
            nodes.append(node.parent)

    for node in nodes:
        cur = node
        chosen = None
        for _ in range(8):
            cur = getattr(cur, "parent", None)
            if cur is None:
                break
            txt = normalize_spaces(cur.get_text(" ", strip=True))
            if "Galaxy" not in txt or "€" not in txt:
                continue
            chosen = cur
            break
        if chosen is None:
            continue
        sig = hash(str(chosen)[:3000])
        if sig in seen:
            continue
        seen.add(sig)
        blocks.append(chosen)
    return blocks


def extract_name_from_block(block):
    text = block.get_text("\n", strip=True)
    lines = [normalize_spaces(x) for x in text.splitlines() if normalize_spaces(x)]
    for ln in lines:
        low = ln.lower()
        if not low.startswith("galaxy "):
            continue
        if any(x in low for x in ["tab", "watch", "buds", "book", "ring"]):
            continue
        if re.search(r"\b\d+\s*(?:gb|tb)\b", low, flags=re.I):
            continue
        if "€" in low or "comprar" in low or "comparar" in low or "informacion" in low or "informaci" in low:
            continue
        return normalize_spaces(ln)
    m = re.search(r"\bGalaxy\s+(?:S|A|Z|M)[A-Za-z0-9+ ]{0,28}", text, flags=re.I)
    return normalize_spaces(m.group(0)) if m else ""


def extract_selected_capacity(block):
    candidates = []
    for idx, el in enumerate(block.find_all(True)):
        txt = normalize_spaces(el.get_text(" ", strip=True))
        if not re.fullmatch(r"\d+\s*(?:GB|TB)", txt or "", flags=re.I):
            continue
        cap = parse_capacidad_desde_texto(txt)
        if not cap:
            continue
        score = 0
        attrs = []
        for a in ["class", "aria-selected", "aria-checked", "aria-pressed", "data-selected", "data-current", "tabindex", "selected", "checked"]:
            v = el.get(a)
            if isinstance(v, list):
                v = " ".join([str(x) for x in v])
            attrs.append(str(v or "").lower())
        joined = " ".join(attrs)
        if 'true' in joined:
            score += 10
        if any(k in joined for k in ["selected", "active", "current", "checked", "focus", "on"]):
            score += 5
        if '0' in joined:
            score += 1
        candidates.append((score, idx, cap))

    if not candidates:
        return ""
    positives = [c for c in candidates if c[0] > 0]
    if positives:
        positives.sort(key=lambda x: (x[0], x[1]))
        return positives[-1][2]
    unique = dedupe_keep_order([c[2] for c in candidates])
    if len(unique) == 1:
        return unique[0]
    return ""


def extract_coupon_from_block(block):
    text = normalize_spaces(block.get_text(" ", strip=True))
    m = re.search(r"(?:c[oó]digo|cupon)\s*:?\s*([A-Z0-9]{4,20})", text, flags=re.I)
    if m:
        return m.group(1).upper()
    return CODIGO_DESCUENTO_DEFAULT


def price_pair_from_block(block, matched_json_price=0):
    text = normalize_spaces(block.get_text(" ", strip=True))
    current = 0
    original = 0

    # First, explicit strikethrough price.
    for tag in block.find_all(["s", "del"], limit=10):
        vals = [v for v in parse_eur_all(tag.get_text(" ", strip=True)) if 150 <= v <= 5000]
        if vals:
            original = max(vals)
            break

    # Current price candidates from non-struck nodes.
    candidates = []
    for idx, tag in enumerate(block.find_all(True)):
        if tag.name in {"s", "del"}:
            continue
        txt = normalize_spaces(tag.get_text(" ", strip=True))
        vals = [v for v in parse_eur_all(txt) if 150 <= v <= 5000]
        if not vals:
            continue
        cls = " ".join(tag.get("class", [])) if isinstance(tag.get("class"), list) else str(tag.get("class") or "")
        low = (txt + " " + cls).lower()
        score = 0
        if any(k in low for k in ["price", "precio", "sale", "offer", "current"]):
            score += 4
        if any(k in low for k in ["rebaja", "descuento", "dto", "ahorro"]):
            score -= 3
        if len(txt) <= 24:
            score += 2
        candidates.append((score, idx, vals[0]))

    if candidates:
        candidates.sort(key=lambda x: (x[0], -x[2], -x[1]))
        # Best scored candidate; if tie, choose the smallest reasonable price.
        best_score = candidates[-1][0]
        best_vals = [c[2] for c in candidates if c[0] == best_score]
        current = min(best_vals) if best_vals else 0

    if not current:
        vals = [v for v in parse_eur_all(text) if 150 <= v <= 5000]
        if vals:
            current = min(vals)

    if not original and current:
        vals = [v for v in parse_eur_all(text) if 150 <= v <= 5000 and v > current]
        if vals:
            original = max(vals)

    if matched_json_price and matched_json_price > current and matched_json_price > original:
        original = matched_json_price

    if current and (not original or original <= current):
        original = calcular_precio_original(current)

    return int(current or 0), int(original or 0)


def extract_urls_from_block(block, listing_url):
    detail = ""
    buy = ""
    for a in block.find_all("a", href=True):
        href = abs_url(listing_url, a.get("href", "").strip())
        if not href or "/es/smartphones/" not in href:
            continue
        text = normalize_spaces(a.get_text(" ", strip=True)).lower()
        if "/buy/" in href or "comprar" in text:
            if not buy:
                buy = href
        if not detail:
            detail = canonical_samsung_import_url(href)
    return buy, detail


def match_jsonld_item(card_raw_name, clean_name, current_price, json_items):
    key = item_name_key(clean_name)
    card_exclusive = "exclusivo online" in (card_raw_name or "").lower()
    candidates = [x for x in json_items if item_name_key(x.get("clean_name") or x.get("raw_name") or "") == key]
    if not candidates:
        return None
    same_exclusive = [x for x in candidates if bool(x.get("exclusive")) == card_exclusive]
    if same_exclusive:
        candidates = same_exclusive
    if current_price:
        ge = [x for x in candidates if int(x.get("price") or 0) >= current_price]
        if ge:
            ge.sort(key=lambda x: abs(int(x.get("price") or 0) - int(current_price)))
            return ge[0]
        candidates.sort(key=lambda x: abs(int(x.get("price") or 0) - int(current_price)))
    return candidates[0]


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
    nombre_hijo = normalize_spaces(nombre_completo)
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


def clean_image_ref(url):
    try:
        if not url:
            return ""
        u = urllib.parse.urlsplit(url)
        return f"{u.netloc}{u.path}".lower().rstrip("/")
    except Exception:
        return (url or "").lower().split("?")[0].rstrip("/")


def same_image_ref(url_a, url_b):
    a = clean_image_ref(url_a)
    b = clean_image_ref(url_b)
    if not a or not b:
        return False
    if a == b:
        return True
    return a.split("/")[-1] == b.split("/")[-1]


def is_brand_image(url):
    low = (url or "").lower()
    fname = low.split("/")[-1]
    bad = ["logo", "marca", "samsung_logo", "samsung-logo", "brand", "icon", "placeholder"]
    return any(x in low for x in bad) or any(x in fname for x in bad)


def exact_subcategory_image(cache_categorias, id_padre, id_hijo):
    child = obtener_imagen_categoria(cache_categorias, id_hijo)
    parent = obtener_imagen_categoria(cache_categorias, id_padre)
    if not child:
        return ""
    if is_brand_image(child):
        return ""
    if parent and same_image_ref(child, parent):
        return ""
    return child


def obtener_datos_remotos():
    print("--- FASE 1: ESCANEANDO SAMSUNG ---", flush=True)
    print(f"URL base: {mask_url(START_URL)}", flush=True)
    print(f"Samsung listing-only: leyendo solo la pagina principal {mask_url(START_URL)}", flush=True)

    try:
        html = get_rendered_html(START_URL)
    except Exception as e:
        print(f"Error renderizando listing Samsung: {e}", flush=True)
        return []

    json_items = parse_jsonld_listing_items(html)
    print(f"Items JSON-LD Samsung detectados: {len(json_items)}", flush=True)

    blocks = find_card_blocks(html)
    print(f"Cards Samsung detectadas en listing: {len(blocks)}", flush=True)

    hoy = datetime.now().strftime("%d/%m/%Y")
    productos = {}

    for block in blocks:
        try:
            raw_name = extract_name_from_block(block)
            if not raw_name:
                continue
            clean_name = normalizar_nombre_samsung(raw_name)
            if not clean_name or should_skip_by_name(clean_name):
                continue

            capacidad = extract_selected_capacity(block)
            if not capacidad:
                print(f"Card Samsung sin capacidad resoluble para {clean_name}. Se ignora.", flush=True)
                continue

            matched = match_jsonld_item(raw_name, clean_name, 0, json_items)
            json_price = int(matched.get("price") or 0) if matched else 0
            precio_actual, precio_original = price_pair_from_block(block, matched_json_price=json_price)
            if precio_actual <= 0:
                print(f"Card Samsung sin precio usable para {clean_name} {capacidad}. Se ignora.", flush=True)
                continue

            memoria = resolve_memory(clean_name, capacidad)
            if not memoria:
                print(f"Card Samsung sin RAM resoluble para {clean_name} {capacidad}. Se ignora.", flush=True)
                continue

            buy_from_card, detail_from_card = extract_urls_from_block(block, START_URL)
            buy_url = ""
            import_url = ""
            model_code = ""
            if matched:
                buy_url = matched.get("buy_url") or ""
                import_url = matched.get("detail_url") or ""
                model_code = matched.get("model_code") or ""
            if not buy_url:
                buy_url = buy_from_card
            if not import_url:
                import_url = detail_from_card or canonical_samsung_import_url(buy_url)
            if not model_code:
                model_code = extraer_model_code(buy_url)

            coupon = extract_coupon_from_block(block)
            affiliate_url = join_affiliate_url(import_url, AFF_SAMSUNG) if import_url else ""
            short_url = acortar_url(affiliate_url) if affiliate_url else ""

            if precio_original <= precio_actual:
                precio_original = calcular_precio_original(precio_actual)

            key = source_key(clean_name, memoria, capacidad, FUENTE)
            if key in productos:
                summary_duplicados.append(f"{clean_name} {capacidad} {memoria}".strip())
                if int(precio_actual) < int(productos[key].get("precio_actual", 10**9)):
                    productos[key]["precio_actual"] = int(precio_actual)
                    productos[key]["precio_original"] = int(precio_original)
                continue

            productos[key] = {
                "nombre": clean_name,
                "memoria": memoria,
                "capacidad": capacidad,
                "precio_actual": int(precio_actual),
                "precio_original": int(precio_original),
                "img": "",
                "fecha": hoy,
                "fuente": FUENTE,
                "version": VERSION,
                "codigo_descuento": coupon,
                "enviado_desde": ENVIADO_DESDE,
                "enviado_desde_tg": ENVIADO_DESDE_TG,
                "enlace_de_compra_importado": import_url,
                "url_oferta_sin_acortar": buy_url,
                "url_importada_sin_afiliado": import_url,
                "url_sin_acortar_con_mi_afiliado": affiliate_url,
                "url_oferta": short_url,
                "importado_de": ID_IMPORTACION,
                "source_key": key,
                "model_code": model_code,
                "origen_listado": START_URL,
                "origen_pagina": "1",
            }
        except Exception as e:
            print(f"Error procesando card Samsung: {e}", flush=True)
            summary_fallidos.append({"nombre": "(listing)", "error": str(e)})

    remotos = list(productos.values())
    print("", flush=True)
    print("RESUMEN EXTRACCION SAMSUNG:", flush=True)
    print("   URLs descubiertas: 1 (listing principal)", flush=True)
    print(f"   Productos unicos validos: {len(remotos)}", flush=True)
    return remotos


def cargar_locales_samsung():
    locales = []
    page = 1
    while True:
        try:
            res = wcapi.get("products", params={"per_page": 100, "page": page, "status": "any"}).json()
            if not res or "message" in res:
                break
            for p in res:
                meta = {m.get("key"): str(m.get("value", "")) for m in p.get("meta_data", []) if isinstance(m, dict)}
                if meta.get("importado_de", "").rstrip("/") == ID_IMPORTACION.rstrip("/"):
                    locales.append({"id": p.get("id"), "nombre": p.get("name", ""), "meta": meta})
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
    return source_key(local.get("nombre", ""), meta.get("memoria", ""), meta.get("capacidad", ""), meta.get("fuente", FUENTE))


def sincronizar(remotos):
    print("--- FASE 2: SINCRONIZANDO SAMSUNG ---", flush=True)
    cache_categorias = obtener_todas_las_categorias()
    locales = cargar_locales_samsung()

    print(f"Productos Samsung existentes en la web: {len(locales)}", flush=True)
    print(f"Productos remotos Samsung a procesar: {len(remotos)}", flush=True)

    remotos_by_key = {r["source_key"]: r for r in remotos}
    locales_by_key = {build_local_key(l): l for l in locales}

    for key, local in locales_by_key.items():
        remote = remotos_by_key.get(key)
        if not remote:
            try:
                wcapi.delete(f"products/{local['id']}", params={"force": True})
                summary_eliminados.append({"nombre": local["nombre"], "id": local["id"]})
                print(f"ELIMINADO -> {local['nombre']} (ID: {local['id']})", flush=True)
            except Exception as e:
                summary_fallidos.append({"nombre": local["nombre"], "error": str(e)})
            continue

        meta = local["meta"]
        cambios = []
        payload = {"meta_data": []}

        def add_meta(k, v):
            payload["meta_data"].append({"key": k, "value": v})

        try:
            old_curr = int(float(meta.get("precio_actual", 0) or 0))
        except Exception:
            old_curr = 0
        try:
            old_orig = int(float(meta.get("precio_original", 0) or 0))
        except Exception:
            old_orig = 0

        if int(remote["precio_actual"]) != old_curr:
            cambios.append(f"precio_actual ({old_curr} -> {remote['precio_actual']})")
            payload["sale_price"] = str(remote["precio_actual"])
            add_meta("precio_actual", str(remote["precio_actual"]))
        if int(remote["precio_original"]) != old_orig:
            cambios.append(f"precio_original ({old_orig} -> {remote['precio_original']})")
            payload["regular_price"] = str(remote["precio_original"])
            add_meta("precio_original", str(remote["precio_original"]))

        for mk in [
            "codigo_descuento", "enlace_de_compra_importado", "url_oferta_sin_acortar",
            "url_importada_sin_afiliado", "url_sin_acortar_con_mi_afiliado", "url_oferta",
            "version", "enviado_desde", "enviado_desde_tg"
        ]:
            newv = str(remote.get(mk, ""))
            oldv = str(meta.get(mk, ""))
            if newv != oldv:
                cambios.append(f"{mk} actualizado")
                add_meta(mk, newv)

        id_padre, id_hijo = resolver_jerarquia(remote["nombre"], cache_categorias)
        img_subcat = exact_subcategory_image(cache_categorias, id_padre, id_hijo)
        current_img = str(meta.get("imagen_producto", ""))
        if img_subcat != current_img:
            cambios.append("imagen_producto actualizado")
            add_meta("imagen_producto", img_subcat)
            payload["images"] = [{"src": img_subcat}] if img_subcat else []

        if cambios:
            try:
                wcapi.put(f"products/{local['id']}", payload)
                summary_actualizados.append({"nombre": local["nombre"], "id": local["id"], "cambios": cambios})
                print(f"ACTUALIZADO -> {local['nombre']} (ID: {local['id']})", flush=True)
            except Exception as e:
                summary_fallidos.append({"nombre": local["nombre"], "error": str(e)})
        else:
            summary_ignorados.append({"nombre": local["nombre"], "id": local["id"]})
            print(f"SIN CAMBIOS -> {local['nombre']} (ID: {local['id']})", flush=True)

    for key, r in remotos_by_key.items():
        if key in locales_by_key:
            continue
        try:
            id_padre, id_hijo = resolver_jerarquia(r["nombre"], cache_categorias)
            img_subcat = exact_subcategory_image(cache_categorias, id_padre, id_hijo)

            print("-" * 60, flush=True)
            print(f"Detectado {r['nombre']}", flush=True)
            print(f"1) Nombre: {r['nombre']}", flush=True)
            print(f"2) Memoria: {r['memoria']}", flush=True)
            print(f"3) Capacidad: {r['capacidad']}", flush=True)
            print(f"4) Version: {r['version']}", flush=True)
            print(f"5) Fuente: {r['fuente']}", flush=True)
            print(f"6) Precio actual: {r['precio_actual']}", flush=True)
            print(f"7) Precio original: {r['precio_original']}", flush=True)
            print(f"8) Codigo de descuento: {r['codigo_descuento']}", flush=True)
            print(f"9) URL Imagen: {img_subcat}", flush=True)
            print(f"10) Enlace Importado: {r['enlace_de_compra_importado']}", flush=True)
            print(f"11) Enlace Expandido: {r['url_oferta_sin_acortar']}", flush=True)
            print(f"12) URL importada sin afiliado: {r['url_importada_sin_afiliado']}", flush=True)
            print(f"13) URL sin acortar con mi afiliado: {r['url_sin_acortar_con_mi_afiliado']}", flush=True)
            print(f"14) URL acortada con mi afiliado: {r['url_oferta']}", flush=True)
            print(f"15) Enviado desde: {r['enviado_desde']}", flush=True)
            print(f"15) Importado de: {r['importado_de']}", flush=True)
            print("16) Encolado para comparar con base de datos...", flush=True)
            print("-" * 60, flush=True)

            data = {
                "name": r["nombre"],
                "type": "simple",
                "status": "publish",
                "regular_price": str(r["precio_original"]),
                "sale_price": str(r["precio_actual"]),
                "categories": [{"id": id_padre}, {"id": id_hijo}] if id_hijo else ([{"id": id_padre}] if id_padre else []),
                "images": [{"src": img_subcat}] if img_subcat else [],
                "meta_data": [
                    {"key": "importado_de", "value": ID_IMPORTACION},
                    {"key": "fecha", "value": r["fecha"]},
                    {"key": "memoria", "value": r["memoria"]},
                    {"key": "capacidad", "value": r["capacidad"]},
                    {"key": "version", "value": r["version"]},
                    {"key": "fuente", "value": r["fuente"]},
                    {"key": "precio_actual", "value": str(r["precio_actual"])},
                    {"key": "precio_original", "value": str(r["precio_original"])},
                    {"key": "codigo_de_descuento", "value": r["codigo_descuento"]},
                    {"key": "enlace_de_compra_importado", "value": r["enlace_de_compra_importado"]},
                    {"key": "url_oferta_sin_acortar", "value": r["url_oferta_sin_acortar"]},
                    {"key": "url_importada_sin_afiliado", "value": r["url_importada_sin_afiliado"]},
                    {"key": "url_sin_acortar_con_mi_afiliado", "value": r["url_sin_acortar_con_mi_afiliado"]},
                    {"key": "url_oferta", "value": r["url_oferta"]},
                    {"key": "enviado_desde", "value": r["enviado_desde"]},
                    {"key": "enviado_desde_tg", "value": r["enviado_desde_tg"]},
                    {"key": "imagen_producto", "value": img_subcat},
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
                        print(f"CREADO -> {r['nombre']} (ID: {new_id})", flush=True)
                        try:
                            permalink = prod.get("permalink", "")
                            if permalink:
                                url_short = acortar_url(permalink)
                                wcapi.put(f"products/{new_id}", {"meta_data": [{"key": "url_post_acortada", "value": url_short}]})
                        except Exception:
                            pass
                        creado = True
                    else:
                        body_preview = (res.text or "").replace("\n", " ")[:250]
                        print(f"Woo error {res.status_code}: {body_preview}", flush=True)
                except Exception as e:
                    print(f"Excepcion Woo creando Samsung: {e}", flush=True)
                if (not creado) and (intentos < 10):
                    time.sleep(15)

            if not creado:
                summary_fallidos.append({"nombre": r["nombre"], "error": "No se pudo crear en WooCommerce"})
                print(f"NO SE PUDO CREAR: {r['nombre']}", flush=True)
        except Exception as e:
            summary_fallidos.append({"nombre": r.get("nombre", "?"), "error": str(e)})
            print(f"ERROR sincronizando Samsung {r.get('nombre', '?')}: {e}", flush=True)

    hoy_fmt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("\n============================================================", flush=True)
    print(f"RESUMEN DE EJECUCION ({hoy_fmt})", flush=True)
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
