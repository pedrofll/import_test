#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
El Corte Inglés (ECI) - PREVIEW scraper (sin crear productos)

Objetivo:
- Descargar PLP(s) de ECI (Ofertas Límite / Móviles) y extraer:
  - nombre (normalizado: primera letra de cada palabra en mayúscula; tokens alfanuméricos en MAYÚSCULAS: 14T, 5G, G85...)
  - memoria (RAM) y capacidad (ROM) si aparecen en el título (p.e. "8GB + 256GB")
  - precio_actual y precio_original (si existe)
  - imagen_producto (preferencia 600x600 si hay query width/height)
  - url_importada_sin_afiliado (URL limpia sin parámetros)
  - url_sin_acortar_con_mi_afiliado (URL limpia + afiliado)
  - fuente, enviado_desde, version

Notas:
- NO crea productos en WooCommerce.
- Genera logs estilo ODM.

Requisitos:
- requests, bs4, lxml
- (opcional fallback) playwright: playwright + navegador instalado (playwright install --with-deps chromium)
"""

import os
import re
import time
from datetime import datetime
from urllib.parse import urljoin, urlparse, urlunparse, parse_qsl, urlencode

import requests
from bs4 import BeautifulSoup

SCRAPER_VERSION = "ECI_PREVIEW_v2.0_playwright_fallback"

# --- Config ---
PLP_URLS = [
    "https://www.elcorteingles.es/limite-48-horas/electronica/moviles/",
    "https://www.elcorteingles.es/limite-48-horas/electronica/moviles/2/",
    "https://www.elcorteingles.es/limite-48-horas/electronica/moviles-y-smartphones/",
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/",
]

PAUSE_BETWEEN_PRODUCTS = float(os.getenv("REQUEST_PAUSE", "0.8"))

CONNECT_TIMEOUT = float(os.getenv("ECI_CONNECT_TIMEOUT", "12"))
READ_TIMEOUT = float(os.getenv("ECI_READ_TIMEOUT", "40"))
FETCH_RETRIES = int(os.getenv("ECI_FETCH_RETRIES", "3"))
FETCH_SLEEP = float(os.getenv("ECI_FETCH_SLEEP", "8"))

MAX_PRODUCTS = os.getenv("MAX_PRODUCTS", "").strip()
MAX_PRODUCTS = int(MAX_PRODUCTS) if MAX_PRODUCTS.isdigit() else None

AFF_ECI = (os.getenv("AFF_ELCORTEINGLES") or "").strip()
AFILIADO_CONFIGURADO = "SI" if AFF_ECI else "NO"

# Headers "realistas" para reducir bloqueos
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Upgrade-Insecure-Requests": "1",
}

SESSION = requests.Session()
SESSION.headers.update(DEFAULT_HEADERS)


# --- Utilidades ---
def log(msg: str):
    print(msg, flush=True)


def now_fmt():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").replace("\xa0", " ")).strip()


def is_tablet_or_non_phone(name: str) -> bool:
    up = (name or "").upper()
    if "TAB" in up or "IPAD" in up:
        return True
    return False


def normalize_token(token: str) -> str:
    t = token.strip()
    if not t:
        return t
    # Si contiene letras y números -> todo MAYÚSCULAS (ej: g85 -> G85, 14t -> 14T, 5g -> 5G)
    if re.search(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]", t) and re.search(r"\d", t):
        return t.upper()
    # Si es todo mayúsculas (marca) lo pasamos a Title para consistencia
    if t.isupper() and len(t) > 2:
        t = t.lower()
    return t[0].upper() + t[1:]


def normalize_name(name: str) -> str:
    name = clean_text(name)
    if not name:
        return name
    # separar conservando símbolos + y /
    parts = re.split(r"(\s+)", name)
    out = []
    for p in parts:
        if p.isspace():
            out.append(p)
            continue
        # separar tokens por guiones pero preservarlos
        subtoks = re.split(r"(-)", p)
        subt_out = []
        for st in subtoks:
            if st == "-":
                subt_out.append(st)
            else:
                subt_out.append(normalize_token(st))
        out.append("".join(subt_out))
    return "".join(out)


def extract_ram_rom(title: str):
    """
    Extrae RAM y ROM del título tipo:
    - "Samsung Galaxy S25 12GB + 256 GB móvil libre"
    - "HONOR 400 Pro 12 GB + 512 GB móvil libre"
    Devuelve ("12 GB", "256 GB") o (None, None)
    """
    t = (title or "").replace("\xa0", " ")
    found = re.findall(r"(\d+(?:[.,]\d+)?)\s*(TB|GB)\b", t, flags=re.IGNORECASE)
    if len(found) < 2:
        return None, None

    def norm_size(num, unit):
        num = num.replace(",", ".")
        if re.fullmatch(r"\d+\.0", num):
            num = num[:-2]
        unit = unit.upper()
        return f"{num} {unit}"

    ram = norm_size(found[0][0], found[0][1])
    rom = norm_size(found[1][0], found[1][1])
    return ram, rom


def parse_price_to_number(price_str: str):
    s = clean_text(price_str)
    if not s:
        return None
    s = s.replace("€", "").strip()
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


def pick_prices_from_text(txt: str):
    txt = (txt or "").replace("\xa0", " ")
    prices = re.findall(r"\b\d{1,3}(?:\.\d{3})*(?:,\d{2})\s*€\b", txt)
    prices = [p.strip() for p in prices]
    if not prices:
        return None, None

    nums = [parse_price_to_number(p) for p in prices]
    nums = [n for n in nums if n is not None]
    if not nums:
        return None, None

    actual = nums[0]
    original = None
    if len(nums) >= 2:
        bigger = [n for n in nums[1:] if n > actual + 0.01]
        if bigger:
            original = bigger[0]

    def out(n):
        if n is None:
            return None
        return f"{n:.2f}"

    return out(actual), out(original)


def make_600_square(img_url: str) -> str:
    if not img_url:
        return img_url
    try:
        u = urlparse(img_url)
        q = dict(parse_qsl(u.query, keep_blank_values=True))
        changed = False
        if "width" in q:
            q["width"] = "600"
            changed = True
        if "height" in q:
            q["height"] = "600"
            changed = True
        if changed:
            return urlunparse((u.scheme, u.netloc, u.path, u.params, urlencode(q, doseq=True), u.fragment))
        return img_url
    except Exception:
        return img_url


def strip_query(url: str) -> str:
    if not url:
        return url
    u = urlparse(url)
    return urlunparse((u.scheme, u.netloc, u.path, "", "", ""))


def add_affiliate(url_clean: str) -> str:
    if not url_clean:
        return url_clean
    if not AFF_ECI:
        return url_clean

    aff = AFF_ECI
    if not aff.startswith("?"):
        aff = "?" + aff

    base = strip_query(url_clean)
    return base + aff


# --- Fetchers ---
def fetch_with_requests(url: str) -> str:
    last_err = None
    for i in range(1, FETCH_RETRIES + 1):
        try:
            log(f"🌐 GET {url} (requests) intento {i}/{FETCH_RETRIES} timeout=({CONNECT_TIMEOUT},{READ_TIMEOUT})")
            r = SESSION.get(url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
            r.raise_for_status()
            text = r.text
            if text and len(text) > 1000:
                return text
            last_err = RuntimeError("respuesta vacía o demasiado corta")
        except Exception as e:
            last_err = e
            log(f"⚠️  Error fetch (requests) -> {type(e).__name__}: {e}")
        if i < FETCH_RETRIES:
            log(f"⏳ Sleep {FETCH_SLEEP}s")
            time.sleep(FETCH_SLEEP)
    raise last_err


def fetch_with_playwright(url: str) -> str:
    try:
        from playwright.sync_api import sync_playwright
    except Exception as e:
        raise RuntimeError(
            "Playwright no está instalado. Añade 'pip install playwright' y 'playwright install --with-deps chromium'."
        ) from e

    log("🧭 Fallback: Playwright (chromium headless) ...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        context = browser.new_context(locale="es-ES", user_agent=DEFAULT_HEADERS["User-Agent"])
        page = context.new_page()
        page.set_default_navigation_timeout(int((CONNECT_TIMEOUT + READ_TIMEOUT) * 1000))
        page.set_default_timeout(int(READ_TIMEOUT * 1000))
        try:
            page.goto(url, wait_until="domcontentloaded")
            try:
                page.wait_for_selector("li.products_list-item article.product_preview", timeout=int(READ_TIMEOUT * 1000))
            except Exception:
                pass
            page.wait_for_timeout(1500)
            return page.content()
        finally:
            context.close()
            browser.close()


def fetch_any(url: str) -> str:
    try:
        return fetch_with_requests(url)
    except Exception as e_req:
        log(f"🧰 requests falló, probando playwright -> {type(e_req).__name__}: {e_req}")
        return fetch_with_playwright(url)


# --- Parsing ---
def parse_products_from_plp_html(html: str, plp_url: str):
    soup = BeautifulSoup(html, "lxml")
    products = []
    nodes = soup.select("li.products_list-item article.product_preview")
    if not nodes:
        nodes = soup.select("article.product_preview")

    for art in nodes:
        try:
            pid = art.get("id") or ""
            a = art.select_one("h2 a.product_preview-title, h2 a")
            title_raw = clean_text(a.get_text(" ", strip=True)) if a else ""
            href = a.get("href") if a else ""
            url = urljoin("https://www.elcorteingles.es", href) if href else ""
            url_clean = strip_query(url)

            img = art.select_one("img.js_preview_image, picture img, img")
            img_url = img.get("src") if img else ""
            img_url = make_600_square(img_url)

            art_txt = clean_text(art.get_text(" ", strip=True))
            precio_actual, precio_original = pick_prices_from_text(art_txt)

            ram, rom = extract_ram_rom(title_raw)

            if is_tablet_or_non_phone(title_raw):
                continue
            if not ram or not rom:
                continue

            nombre_norm = normalize_name(title_raw)
            categoria = nombre_norm.split(" ")[0] if nombre_norm else ""
            version = "IOS" if categoria.lower() == "iphone" or "iphone" in nombre_norm.lower() else "Versión Global"

            products.append(
                {
                    "id": pid,
                    "nombre": nombre_norm,
                    "categoria": categoria,
                    "subcategoria": nombre_norm,
                    "memoria": ram,
                    "capacidad": rom,
                    "version": version,
                    "fuente": "El Corte Inglés",
                    "enviado_desde": "España",
                    "enviado_desde_tg": "🇪🇸 España",
                    "precio_actual": precio_actual,
                    "precio_original": precio_original,
                    "codigo_de_descuento": "OFERTA: PROMO.",
                    "imagen_producto": img_url,
                    "url_importada_sin_afiliado": url_clean,
                    "url_sin_acortar_con_mi_afiliado": add_affiliate(url_clean),
                    "importado_de": "https://www.elcorteingles.es",
                    "plp_origen": plp_url,
                }
            )
        except Exception:
            continue

    return products


def print_product_log(p):
    log(f"Detectado {p['nombre']}")
    log(f"1) Nombre: {p['nombre']}")
    log(f"2) Memoria: {p['memoria']}")
    log(f"3) Capacidad: {p['capacidad']}")
    log(f"4) Versión: {p['version']}")
    log(f"5) Fuente: {p['fuente']}")
    log(f"6) Precio actual: {p.get('precio_actual') or ''}")
    log(f"7) Precio original: {p.get('precio_original') or ''}")
    log(f"8) Código de descuento: {p['codigo_de_descuento']}")
    log(f"9) Enviado desde: {p['enviado_desde']} ({p['enviado_desde_tg']})")
    log(f"10) URL Imagen (600x600 preferida): {p.get('imagen_producto') or ''}")
    log(f"11) Enlace (sin afiliado): {p.get('url_importada_sin_afiliado') or ''}")
    log(f"12) Enlace (con mi afiliado): {p.get('url_sin_acortar_con_mi_afiliado') or ''}")
    log(f"13) Importado de: {p.get('importado_de')}")
    log(f"14) PLP origen: {p.get('plp_origen')}")
    log("------------------------------------------------------------")


def main():
    log("============================================================")
    log(f"🔎 PREVIEW EL CORTE INGLÉS (SIN CREAR) ({now_fmt()})")
    log("============================================================")
    log(f"SCRAPER_VERSION: {SCRAPER_VERSION}")
    log("PLP_URLS (fallback):")
    for u in PLP_URLS:
        log(f"- {u}")
    log(f"Pausa entre requests: {PAUSE_BETWEEN_PRODUCTS}s")
    log(f"Timeout connect/read (requests): {CONNECT_TIMEOUT}s / {READ_TIMEOUT}s")
    log(f"Reintentos fetch (requests): {FETCH_RETRIES} (sleep {FETCH_SLEEP}s)")
    log(f"Afiliado ECI configurado: {AFILIADO_CONFIGURADO}")
    log(f"MAX_PRODUCTS: {MAX_PRODUCTS if MAX_PRODUCTS is not None else 'SIN LÍMITE'}")
    log("============================================================")

    all_products = []
    last_error = None

    for idx_url, plp in enumerate(PLP_URLS, start=1):
        log("------------------------------------------------------------")
        log(f"🔁 PROBANDO URL {idx_url}/{len(PLP_URLS)}: {plp}")
        try:
            html = fetch_any(plp)
            prods = parse_products_from_plp_html(html, plp)
            log(f"✅ Descarga OK. Productos móviles detectados (con RAM+ROM): {len(prods)}")
            all_products.extend(prods)
            if prods:
                break
        except Exception as e:
            last_error = e
            log(f"❌ Falló URL: {type(e).__name__}: {e}")

    dedup = []
    seen = set()
    for p in all_products:
        k = (p["nombre"], p["memoria"], p["capacidad"])
        if k in seen:
            continue
        seen.add(k)
        dedup.append(p)

    log("------------------------------------------------------------")
    log(f"📦 Productos móviles detectados (deduplicados): {len(dedup)}")
    log("------------------------------------------------------------")

    count = 0
    for p in dedup:
        count += 1
        print_product_log(p)
        if MAX_PRODUCTS is not None and count >= MAX_PRODUCTS:
            log(f"🧯 MAX_PRODUCTS alcanzado ({MAX_PRODUCTS}). Cortando preview.")
            break
        time.sleep(PAUSE_BETWEEN_PRODUCTS)

    log("")
    log("============================================================")
    log(f"📋 RESUMEN DE EJECUCIÓN ({now_fmt()})")
    log("============================================================")
    log(f"Productos detectados (móviles con RAM+ROM): {len(dedup)}")
    if last_error and not dedup:
        log(f"Último error: {type(last_error).__name__}: {last_error}")
    log("============================================================")


if __name__ == "__main__":
    main()
