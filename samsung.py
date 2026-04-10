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
# Reglas operativas aplicadas:
#  - Solo se lee la página principal:
#      https://www.samsung.com/es/smartphones/all-smartphones/
#  - NO se navega a fichas de producto para descubrir productos.
#  - La URL de compra real puede salir de JSON-LD con modelCode, pero:
#      * enlace_de_compra_importado  => URL base SIN /buy/?...
#      * url_importada_sin_afiliado  => URL base SIN /buy/?...
#      * url_oferta_sin_acortar      => URL expandida/original (si existe buy+modelCode)
#      * url_sin_acortar_con_mi_afiliado => URL base + AFF_SAMSUNG
#  - La imagen del producto NO se sube desde Samsung.
#    Se usa la imagen que ya exista en la subcategoría/categoría del producto.
#  - La RAM se resuelve principalmente con mapa local por nombre+capacidad.
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

# Fallback local de RAM por nombre+capacidad.
# Se usa para NO tener que entrar en las páginas de producto.
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
    ("Samsung Galaxy A57 5G", "512GB"): "12GB",
    ("Samsung Galaxy A37 5G", "256GB"): "8GB",
    ("Samsung Galaxy A17 5G", "256GB"): "8GB",
    ("Samsung Galaxy A17", "256GB"): "8GB",
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


def normalizar_nombre_samsung(nombre: str) -> str:
    t = normalize_spaces(nombre)
    if not t:
        return ""
    t = re.sub(r"\bExclusivo Online\b", "", t, flags=re.I)
    t = normalize_spaces(t)
    if t.lower().startswith("samsung "):
        t = t[len("Samsung "):]
    words = []
    for w in t.split():
        if re.search(r"\d", w) and re.search(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]", w):
            w = "".join(ch.upper() if ch.isalpha() else ch for ch in w)
        elif w.lower() in {"gb", "tb"}:
            w = w.upper()
        elif w in {"+"}:
            pass
        else:
            w = w[:1].upper() + w[1:]
        words.append(w)
    out = normalize_spaces(" ".join(words))
    if not out.lower().startswith("galaxy "):
        out = f"Galaxy {out}"
    return normalize_spaces(f"Samsung {out}")


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
    """Convierte buy URL de Samsung a URL base sin /buy/?..."""
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


def item_name_key(nombre: str) -> str:
    t = normalizar_nombre_samsung(nombre).lower()
    t = t.replace("samsung ", "")
    t = t.replace("exclusivo online", "")
    t = normalize_spaces(t)
    return t


def resolve_memory(nombre: str, capacidad: str) -> str:
    return RAM_BY_NAME_CAP.get((normalizar_nombre_samsung(nombre), capacidad), "")


# --------------------------
# SELENIUM (solo para renderizar listing principal)
# --------------------------

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
        "//a[contains(., 'IR A SAMSUNG.COM')]",
        "//button[contains(., 'MÁS TARDE')]",
        "//button[contains(., 'MAS TARDE')]",
        "//button[contains(., 'Sí')]",
    ]
    for _ in range(3):
        for xp in xpaths:
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
# PARSEO JSON-LD DEL LISTING
# --------------------------

def iter_dicts(obj):
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from iter_dicts(v)
    elif isinstance(obj, list):
        for x in obj:
            yield from iter_dicts(x)


def extract_jsonld_products(html: str):
    out = []
    seen = set()
    for m in re.finditer(r"<script[^>]*type=['\"]application/ld\+json['\"][^>]*>(.*?)</script>", html, flags=re.I | re.S):
        raw = (m.group(1) or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        for node in iter_dicts(data):
            t = node.get("@type")
            if isinstance(t, list):
                t = " ".join(str(x) for x in t)
            t = str(t or "")
            if "Product" not in t:
                continue
            name = normalize_spaces(node.get("name") or "")
            if not name:
                continue
            buy_url = normalize_spaces(node.get("url") or "")
            if not buy_url:
                offers = node.get("offers") or {}
                if isinstance(offers, dict):
                    buy_url = normalize_spaces(offers.get("url") or "")
            image = node.get("image") or ""
            if isinstance(image, list) and image:
                image = image[0]
            image = abs_url(START_URL, str(image)) if image else ""
            offers = node.get("offers") or {}
            price = 0
            if isinstance(offers, dict):
                price = parse_eur_num(str(offers.get("price") or ""))
            key = (item_name_key(name), buy_url, price)
            if key in seen:
                continue
            seen.add(key)
            out.append({
                "raw_name": name,
                "clean_name": normalizar_nombre_samsung(name),
                "buy_url": buy_url,
                "import_url": canonical_samsung_import_url(buy_url),
                "image": image,
                "price": price,
                "exclusive": "exclusivo online" in name.lower(),
            })
    return out


# --------------------------
# PARSEO DE CARDS DEL LISTING
# --------------------------

def is_selected_candidate(tag) -> bool:
    try:
        cur = tag
        for _ in range(3):
            if cur is None:
                break
            attrs = []
            for v in cur.attrs.values():
                if isinstance(v, list):
                    attrs.extend(str(x) for x in v)
                else:
                    attrs.append(str(v))
            attr_txt = " ".join(attrs).lower()
            if cur.get("aria-selected") == "true":
                return True
            if cur.get("aria-pressed") == "true":
                return True
            if cur.get("aria-current") == "true":
                return True
            if cur.get("data-selected") == "true":
                return True
            if any(x in attr_txt for x in [
                "selected", "is-selected", "active", "is-active",
                "current", "checked", "focus", "on",
            ]):
                return True
            if cur.find("input", checked=True):
                return True
            cur = cur.parent
    except Exception:
        pass
    return False


def extract_name_from_block(block) -> str:
    # prioridad: headings / tags cortos con 'Galaxy'
    candidates = []
    for tag in block.find_all(["h1", "h2", "h3", "h4", "h5", "strong", "b", "span", "a", "div"], limit=200):
        txt = normalize_spaces(tag.get_text(" ", strip=True))
        if "Galaxy" not in txt:
            continue
        if len(txt) < 6 or len(txt) > 90:
            continue
        if "Comprar" in txt or "Más información" in txt or "Comparar" in txt:
            continue
        if "€" in txt:
            continue
        candidates.append(txt)
    candidates = [c for c in candidates if re.search(r"\bGalaxy\b", c, flags=re.I)]
    candidates = sorted(candidates, key=lambda x: (len(x), x))
    if candidates:
        # preferimos nombres sin basura posterior
        for c in candidates:
            if re.search(r"Galaxy\s+(S|Z|A)", c, flags=re.I):
                return c
        return candidates[0]
    txt = normalize_spaces(block.get_text(" ", strip=True))
    m = re.search(r"(Galaxy\s+[A-Za-z0-9+\- ]{2,60})", txt, flags=re.I)
    return m.group(1).strip() if m else ""



def extract_selected_capacity(block) -> str:
    caps = []
    for tag in block.find_all(["button", "a", "li", "span", "div", "label"], limit=300):
        txt = normalize_spaces(tag.get_text(" ", strip=True))
        cap = parse_capacidad_desde_texto(txt)
        if not cap:
            continue
        if len(txt) > 30:
            continue
        score = 0
        if is_selected_candidate(tag):
            score += 10
        # preferimos textos "limpios" (solo capacidad)
        if re.fullmatch(r"(64|128|256|512|1024)\s*GB|(1|2)\s*TB", txt, flags=re.I):
            score += 3
        caps.append((score, txt, cap))
    if not caps:
        return ""
    caps.sort(key=lambda x: (x[0], -len(x[1])), reverse=True)
    if caps[0][0] > 0:
        return caps[0][2]
    # fallback muy conservador: primer texto corto con capacidad
    return caps[0][2]



def extract_prices_from_block(block):
    text = normalize_spaces(block.get_text(" ", strip=True))
    current = 0
    original = 0

    m_antes = re.search(r"Antes\s*([\d\.,]+)\s*€", text, flags=re.I)
    if m_antes:
        original = parse_eur_num(m_antes.group(1))

    current_candidates = []
    for tag in block.find_all(["span", "div", "p", "strong", "b"], limit=400):
        txt = normalize_spaces(tag.get_text(" ", strip=True))
        if "€" not in txt:
            continue
        if "/mes" in txt.lower():
            continue
        vals = parse_eur_all(txt)
        if not vals:
            continue
        score = 0
        if "antes" in txt.lower():
            score -= 8
        if "ahorra" in txt.lower():
            score -= 6
        if "dto" in txt.lower() or "descuento" in txt.lower():
            score -= 5
        if len(vals) == 1:
            score += 2
        if len(txt) <= 20:
            score += 2
        current_candidates.append((score, vals[0], txt))
    current_candidates.sort(key=lambda x: x[0], reverse=True)
    for score, val, txt in current_candidates:
        if score >= 0:
            current = val
            break
    if current == 0:
        vals = parse_eur_all(text)
        vals = [v for v in vals if v > 0]
        if vals:
            current = vals[0]
    if original == 0:
        # busca elementos tachados
        for tag in block.find_all(["s", "del"], limit=20):
            vals = parse_eur_all(tag.get_text(" ", strip=True))
            if vals:
                original = vals[0]
                break
    if original == 0 and current:
        vals = parse_eur_all(text)
        bigger = [v for v in vals if v > current]
        if bigger:
            original = max(bigger)
    if original == 0 and current:
        original = calcular_precio_original(current)
    return int(current or 0), int(original or 0)



def extract_coupon_from_block(block) -> str:
    text = normalize_spaces(block.get_text(" ", strip=True))
    m = re.search(r"(?:c[oó]digo|cup[oó]n)\s*:?\s*([A-Z0-9]{4,20})", text, flags=re.I)
    if m:
        return m.group(1).upper()
    return CODIGO_DESCUENTO_DEFAULT



def find_card_blocks(html: str):
    soup = BeautifulSoup(html, "html.parser")
    blocks = []
    seen = set()
    buy_nodes = []
    for node in soup.find_all(string=re.compile(r"^\s*Comprar\s*$", flags=re.I)):
        if node and node.parent is not None:
            buy_nodes.append(node.parent)
    for node in buy_nodes:
        cur = node
        chosen = None
        for _ in range(8):
            cur = getattr(cur, "parent", None)
            if cur is None:
                break
            txt = normalize_spaces(cur.get_text(" ", strip=True))
            if "Galaxy" not in txt:
                continue
            if "€" not in txt:
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



def match_jsonld_item(card_raw_name: str, clean_name: str, current_price: int, json_items):
    key = item_name_key(clean_name)
    card_exclusive = "exclusivo online" in (card_raw_name or "").lower()
    candidates = [x for x in json_items if item_name_key(x.get("clean_name") or x.get("raw_name") or "") == key]
    if not candidates:
        return None
    same_exclusive = [x for x in candidates if bool(x.get("exclusive")) == card_exclusive]
    if same_exclusive:
        candidates = same_exclusive
    if current_price:
        candidates = sorted(candidates, key=lambda x: abs(int(x.get("price") or 0) - int(current_price)))
    return candidates[0]


# --------------------------
# WOO CATEGORÍAS / IMAGENES
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


# --------------------------
# FASE 1: EXTRACCIÓN REMOTA
# --------------------------

def obtener_datos_remotos():
    print("--- FASE 1: ESCANEANDO SAMSUNG ---", flush=True)
    print(f"URL base: {mask_url(START_URL)}", flush=True)
    print(f"🪄 Samsung listing-only: leyendo solo la página principal {mask_url(START_URL)}", flush=True)

    try:
        html = get_rendered_html(START_URL)
    except Exception as e:
        print(f"❌ Error renderizando listing Samsung: {e}", flush=True)
        return []

    json_items = extract_jsonld_products(html)
    print(f"✅ Items JSON-LD Samsung detectados: {len(json_items)}", flush=True)

    blocks = find_card_blocks(html)
    print(f"✅ Cards Samsung detectadas en listing: {len(blocks)}", flush=True)

    hoy = datetime.now().strftime("%d/%m/%Y")
    productos_por_clave = {}

    for block in blocks:
        try:
            raw_name = extract_name_from_block(block)
            if not raw_name:
                continue
            clean_name = normalizar_nombre_samsung(raw_name)
            if should_skip_by_name(clean_name):
                continue

            capacidad = extract_selected_capacity(block)
            if not capacidad:
                print(f"⚠️ Card Samsung sin capacidad resoluble para {clean_name}. Se ignora.", flush=True)
                continue

            precio_actual, precio_original = extract_prices_from_block(block)
            if precio_actual <= 0:
                print(f"⚠️ Card Samsung sin precio usable para {clean_name} {capacidad}. Se ignora.", flush=True)
                continue

            memoria = resolve_memory(clean_name, capacidad)
            if not memoria:
                print(f"⚠️ Card Samsung sin RAM resoluble para {clean_name} {capacidad}. Se ignora.", flush=True)
                continue

            coupon = extract_coupon_from_block(block)
            matched = match_jsonld_item(raw_name, clean_name, precio_actual, json_items)
            expanded_url = matched.get("buy_url", "") if matched else ""
            import_url = canonical_samsung_import_url(expanded_url)
            affiliate_url = unir_afiliado(import_url, AFF_SAMSUNG) if import_url else ""
            short_url = acortar_url(affiliate_url) if affiliate_url else ""

            key = source_key(clean_name, memoria, capacidad, FUENTE)
            if key in productos_por_clave:
                summary_duplicados.append(f"{clean_name} {capacidad} {memoria}".strip())
                continue

            productos_por_clave[key] = {
                "nombre": clean_name,
                "memoria": memoria,
                "capacidad": capacidad,
                "precio_actual": int(precio_actual),
                "precio_original": int(precio_original or 0),
                "img": "",  # NO subir imagen remota; se resolverá desde la categoría Woo.
                "fecha": hoy,
                "fuente": FUENTE,
                "version": VERSION,
                "codigo_descuento": coupon,
                "enviado_desde": ENVIADO_DESDE,
                "enviado_desde_tg": ENVIADO_DESDE_TG,
                "enlace_de_compra_importado": import_url,
                "url_oferta_sin_acortar": expanded_url,
                "url_importada_sin_afiliado": import_url,
                "url_sin_acortar_con_mi_afiliado": affiliate_url,
                "url_oferta": short_url,
                "importado_de": ID_IMPORTACION,
                "source_key": key,
            }
        except Exception as e:
            summary_fallidos.append({"nombre": "(listing)", "error": str(e)})
            print(f"⚠️ Error procesando card Samsung: {e}", flush=True)

    productos = list(productos_por_clave.values())
    print("📊 RESUMEN EXTRACCIÓN SAMSUNG:", flush=True)
    print(f"   URLs descubiertas: 1 (listing principal)", flush=True)
    print(f"   Productos únicos válidos: {len(productos)}", flush=True)
    return productos


# --------------------------
# FASE 2: SINCRONIZACIÓN WC
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
                meta = {m.get("key"): str(m.get("value", "")) for m in p.get("meta_data", []) if isinstance(m, dict)}
                if meta.get("importado_de", "").rstrip("/") == ID_IMPORTACION.rstrip("/"):
                    locales.append({
                        "id": p.get("id"),
                        "nombre": p.get("name", ""),
                        "meta": meta,
                    })
            if len(res) < 100:
                break
            page += 1
        except Exception:
            break
    return locales



def sync_key_from_meta(nombre: str, meta: dict) -> str:
    return source_key(
        nombre,
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

    remotos_by_key = {r["source_key"]: r for r in remotos}
    locales_by_key = {sync_key_from_meta(l["nombre"], l["meta"]): l for l in locales}

    # Eliminados / actualizados / ignorados
    for key, local in locales_by_key.items():
        remote = remotos_by_key.get(key)
        if not remote:
            try:
                wcapi.delete(f"products/{local['id']}", params={"force": True})
                summary_eliminados.append({"nombre": local["nombre"], "id": local["id"]})
                print(f"🗑️ ELIMINADO -> {local['nombre']} (ID: {local['id']})", flush=True)
            except Exception as e:
                summary_fallidos.append({"nombre": local["nombre"], "id": local["id"], "error": str(e)})
            continue

        meta = local["meta"]
        cambios = []
        payload = {"meta_data": []}

        def _meta_append(k, v):
            payload["meta_data"].append({"key": k, "value": v})

        try:
            if int(float(meta.get("precio_actual", 0) or 0)) != int(remote["precio_actual"]):
                cambios.append(f"precio_actual ({meta.get('precio_actual')} -> {remote['precio_actual']})")
                payload["sale_price"] = str(remote["precio_actual"])
                _meta_append("precio_actual", str(remote["precio_actual"]))
        except Exception:
            pass
        try:
            if int(float(meta.get("precio_original", 0) or 0)) != int(remote["precio_original"]):
                cambios.append(f"precio_original ({meta.get('precio_original')} -> {remote['precio_original']})")
                payload["regular_price"] = str(remote["precio_original"])
                _meta_append("precio_original", str(remote["precio_original"]))
        except Exception:
            pass

        for k in [
            "url_oferta_sin_acortar",
            "url_importada_sin_afiliado",
            "url_sin_acortar_con_mi_afiliado",
            "url_oferta",
            "enlace_de_compra_importado",
        ]:
            if str(meta.get(k, "")) != str(remote.get(k, "")):
                cambios.append(f"{k} actualizado")
                _meta_append(k, remote.get(k, ""))

        if cambios:
            try:
                wcapi.put(f"products/{local['id']}", payload)
                summary_actualizados.append({"nombre": local["nombre"], "id": local["id"], "cambios": cambios})
                print(f"🔄 ACTUALIZADO -> {local['nombre']} (ID: {local['id']})", flush=True)
            except Exception as e:
                summary_fallidos.append({"nombre": local["nombre"], "id": local["id"], "error": str(e)})
        else:
            summary_ignorados.append({"nombre": local["nombre"], "id": local["id"]})

    # Creaciones nuevas
    for key, r in remotos_by_key.items():
        if key in locales_by_key:
            continue
        try:
            id_padre, id_hijo = resolver_jerarquia(r["nombre"], cache_categorias)
            img_subcat = obtener_imagen_categoria(cache_categorias, id_hijo)
            img_padre = obtener_imagen_categoria(cache_categorias, id_padre)
            img_final = img_subcat or img_padre or ""

            print("-" * 60, flush=True)
            print(f"Detectado {r.get('nombre','(sin nombre)')}", flush=True)
            print(f"1) Nombre: {r.get('nombre','')}", flush=True)
            print(f"2) Memoria: {r.get('memoria','')}", flush=True)
            print(f"3) Capacidad: {r.get('capacidad','')}", flush=True)
            print(f"4) Versión: {r.get('version','')}", flush=True)
            print(f"5) Fuente: {r.get('fuente','')}", flush=True)
            print(f"6) Precio actual: {r.get('precio_actual',0)}", flush=True)
            print(f"7) Precio original: {r.get('precio_original',0)}", flush=True)
            print(f"8) Código de descuento: {r.get('codigo_descuento','')}", flush=True)
            print(f"9) URL Imagen: {img_final or '(vacía)'}", flush=True)
            print(f"10) Enlace Importado: {r.get('enlace_de_compra_importado','')}", flush=True)
            print(f"11) Enlace Expandido: {r.get('url_oferta_sin_acortar','')}", flush=True)
            print(f"12) URL importada sin afiliado: {r.get('url_importada_sin_afiliado','')}", flush=True)
            print(f"13) URL sin acortar con mi afiliado: {r.get('url_sin_acortar_con_mi_afiliado','')}", flush=True)
            print(f"14) URL acortada con mi afiliado: {r.get('url_oferta','')}", flush=True)
            print(f"15) Enviado desde: {r.get('enviado_desde','')}", flush=True)
            print(f"15) Importado de: {r.get('importado_de','')}", flush=True)
            print(f"16) Encolado para comparar con base de datos...", flush=True)
            print("-" * 60, flush=True)

            data = {
                "name": r["nombre"],
                "type": "simple",
                "status": "publish",
                "regular_price": str(r["precio_original"]),
                "sale_price": str(r["precio_actual"]),
                "categories": [{"id": id_padre}, {"id": id_hijo}] if id_hijo else ([{"id": id_padre}] if id_padre else []),
                # Solo usamos la imagen que YA existe en la categoría.
                "images": [{"src": img_final}] if img_final else [],
                "meta_data": [
                    {"key": "nombre_movil_final", "value": r["nombre"]},
                    {"key": "importado_de", "value": r["importado_de"]},
                    {"key": "fecha", "value": r["fecha"]},
                    {"key": "memoria", "value": r["memoria"]},
                    {"key": "capacidad", "value": r["capacidad"]},
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
                    {"key": "imagen_producto", "value": img_final},
                    {"key": "version", "value": r["version"]},
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
                        summary_creados.append({"nombre": r["nombre"], "id": prod.get("id")})
                        print(f"✅ CREADO -> {r['nombre']} (ID: {prod.get('id')})", flush=True)
                        try:
                            url_short = acortar_url(prod.get("permalink", ""))
                            if url_short:
                                wcapi.put(
                                    f"products/{prod.get('id')}",
                                    {"meta_data": [{"key": "url_post_acortada", "value": url_short}]},
                                )
                        except Exception:
                            pass
                        creado = True
                    else:
                        body_preview = (res.text or "").replace("\n", " ")[:250]
                        print(f"⚠️ Woo error {res.status_code}: {body_preview}", flush=True)
                except Exception as e:
                    print(f"⚠️ Excepción Woo: {e}", flush=True)
                if not creado and intentos < 10:
                    time.sleep(15)

            if not creado:
                summary_fallidos.append({"nombre": r["nombre"], "error": "NO SE PUDO CREAR"})
                print(f"❌ NO SE PUDO CREAR: {r['nombre']}", flush=True)
        except Exception as e:
            summary_fallidos.append({"nombre": r.get("nombre", "desconocido"), "error": str(e)})
            print(f"❌ ERROR en {r.get('nombre','?')}: {e}", flush=True)

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
    print("============================================================", flush=True)


if __name__ == "__main__":
    remotos = obtener_datos_remotos()
    if remotos:
        sincronizar(remotos)
    else:
        print("No se han obtenido productos remotos de Samsung.")
