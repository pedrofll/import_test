#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import sys
import time
import html
import random
import urllib.parse
import subprocess
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup


# =========================
# CONFIG
# =========================
SCRAPER_VERSION = "ECI_PREVIEW_v1.5_multi_plp_ipv4_cookiejar"

BASE_URL = "https://www.elcorteingles.es"

# Puedes sobreescribir con:
#   ECI_PLP_URLS="url1,url2,url3"
# o con:
#   ECI_PLP_URL="una_url"
ECI_PLP_URL = (os.getenv("ECI_PLP_URL") or "").strip()
ECI_PLP_URLS = (os.getenv("ECI_PLP_URLS") or "").strip()

DEFAULT_PLP_URLS = [
    # Más "clásicas"/paginadas de Ofertas Límite (suelen ser más ligeras)
    "https://www.elcorteingles.es/limite-48-horas/electronica/moviles/",
    "https://www.elcorteingles.es/limite-48-horas/electronica/moviles/2/",
    # La que estabas usando (más SPA)
    "https://www.elcorteingles.es/limite-48-horas/electronica/moviles-y-smartphones/",
    # Fallback adicional (no es “limite 48h”, pero útil para verificar scraping)
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/",
]

if ECI_PLP_URLS:
    PLP_URLS = [u.strip() for u in ECI_PLP_URLS.split(",") if u.strip()]
elif ECI_PLP_URL:
    PLP_URLS = [ECI_PLP_URL]
else:
    PLP_URLS = DEFAULT_PLP_URLS


PAUSE_SECONDS = float(os.getenv("PAUSE_SECONDS", "0.8"))

CONNECT_TIMEOUT = float(os.getenv("ECI_CONNECT_TIMEOUT", "12"))
READ_TIMEOUT = float(os.getenv("ECI_READ_TIMEOUT", "40"))

MAX_FETCH_ATTEMPTS = int(os.getenv("ECI_MAX_FETCH_ATTEMPTS", "4"))
RETRY_SLEEP_SECONDS = int(os.getenv("ECI_RETRY_SLEEP_SECONDS", "10"))

# curl max-time independiente (porque a veces requests cae pero curl tarda más)
CURL_MAX_TIME = float(os.getenv("ECI_CURL_MAX_TIME", "55"))

MAX_PRODUCTS = os.getenv("MAX_PRODUCTS", "").strip()
MAX_PRODUCTS_N = int(MAX_PRODUCTS) if MAX_PRODUCTS.isdigit() else None

AFF_ELCORTEINGLES = (os.getenv("AFF_ELCORTEINGLES") or "").strip()

COOKIE_JAR = os.getenv("ECI_COOKIE_JAR", ".eci_cookies.txt").strip()


# =========================
# HTTP SESSION (requests)
# =========================
SESSION = requests.Session()

SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "DNT": "1",
    "Referer": "https://www.elcorteingles.es/",
})


# =========================
# Helpers
# =========================
def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(s: str) -> None:
    print(s, flush=True)


def sleep_polite() -> None:
    time.sleep(PAUSE_SECONDS + random.uniform(0.05, 0.25))


def absolutize_url(href: str) -> str:
    if not href:
        return ""
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("/"):
        return BASE_URL.rstrip("/") + href
    return BASE_URL.rstrip("/") + "/" + href


def normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def title_case_product_name(name: str) -> str:
    """
    - Primera letra de cada palabra en mayúscula
    - Tokens mixtos número+letras (14T, 5G, g85) => letras en mayúscula
    """
    if not name:
        return name
    words = normalize_spaces(name).split(" ")
    out = []
    for w in words:
        token = w.strip()
        if not token:
            continue

        if re.search(r"\d", token) and re.search(r"[A-Za-z]", token):
            out.append("".join(ch.upper() if ch.isalpha() else ch for ch in token))
            continue

        if token.isupper() and len(token) > 3:
            out.append(token.capitalize())
            continue

        out.append(token[:1].upper() + token[1:])
    return " ".join(out)


def extract_ram_rom_from_name(name: str) -> Tuple[Optional[str], Optional[str]]:
    """
    '8GB + 256GB' / '8 GB + 256 GB'
    """
    if not name:
        return None, None

    s = name.replace("\u00a0", " ")
    m = re.search(r"(\d{1,3})\s*GB\s*\+\s*(\d{2,4})\s*GB", s, re.IGNORECASE)
    if m:
        ram = f"{int(m.group(1))} GB"
        rom = f"{int(m.group(2))} GB"
        return ram, rom

    return None, None


def is_tablet_or_non_mobile(name: str) -> bool:
    n = (name or "").upper()
    if "TAB" in n or "IPAD" in n:
        return True
    return False


def build_affiliate_url(product_url: str) -> str:
    """
    - Si AFF_ELCORTEINGLES contiene '{url}' => reemplaza
    - Si parece prefijo tipo '...p=' o '...url=' => concatena url encoded
    - Si viene vacío => devuelve original
    """
    if not product_url:
        return product_url
    if not AFF_ELCORTEINGLES:
        return product_url

    try:
        if "{url}" in AFF_ELCORTEINGLES:
            return AFF_ELCORTEINGLES.replace("{url}", urllib.parse.quote(product_url, safe=""))
        if AFF_ELCORTEINGLES.endswith("=") or "url=" in AFF_ELCORTEINGLES or "p=" in AFF_ELCORTEINGLES:
            return AFF_ELCORTEINGLES + urllib.parse.quote(product_url, safe="")
        return product_url
    except Exception:
        return product_url


def image_to_600(url: str) -> str:
    if not url:
        return url
    u = url
    u = re.sub(r"([?&]width=)\d+", r"\g<1>600", u)
    u = re.sub(r"([?&]height=)\d+", r"\g<1>600", u)
    return u


# =========================
# Fetch strategies
# =========================
def preflight_home_requests() -> None:
    try:
        SESSION.get(BASE_URL, timeout=(CONNECT_TIMEOUT, min(READ_TIMEOUT, 20)), allow_redirects=True)
    except Exception:
        pass


def fetch_with_requests(url: str) -> str:
    r = SESSION.get(url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT), allow_redirects=True)
    r.raise_for_status()
    return r.text


def fetch_with_curl(url: str) -> str:
    """
    Fallback robusto:
    - --http1.1 (ya lo tenías)
    - --ipv4 (evita hangs por IPv6 en runners)
    - cookie jar persistente (a veces WAF/consent)
    - retry nativo de curl
    """
    cmd = [
        "curl", "-sS", "-L",
        "--http1.1",
        "--ipv4",
        "--connect-timeout", str(int(CONNECT_TIMEOUT)),
        "--max-time", str(int(CURL_MAX_TIME)),
        "--retry", "3",
        "--retry-delay", "2",
        "--retry-all-errors",
        "--compressed",
        "-c", COOKIE_JAR,
        "-b", COOKIE_JAR,
        "-H", f"User-Agent: {SESSION.headers.get('User-Agent')}",
        "-H", f"Accept: {SESSION.headers.get('Accept')}",
        "-H", f"Accept-Language: {SESSION.headers.get('Accept-Language')}",
        "-H", f"Referer: {SESSION.headers.get('Referer')}",
        url,
    ]

    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        err = (p.stderr or "").strip().replace("\n", " ")
        raise RuntimeError(f"curl_error rc={p.returncode} stderr={err[:400]}")
    return p.stdout


def fetch_html_one_url(url: str) -> str:
    last_err = None

    log("🧪 Preflight: home (requests)")
    preflight_home_requests()

    for attempt in range(1, MAX_FETCH_ATTEMPTS + 1):
        log(f"🌐 GET {url} (intento {attempt}/{MAX_FETCH_ATTEMPTS}) timeout=({CONNECT_TIMEOUT:.1f},{READ_TIMEOUT:.1f})")

        # 1) requests
        try:
            html_text = fetch_with_requests(url)
            log(f"✅ OK (requests) bytes={len(html_text.encode('utf-8', errors='ignore'))}")
            return html_text
        except Exception as e:
            last_err = e
            log(f"⚠️  Error fetch (requests) -> {type(e).__name__}: {e}")

        # 2) curl fallback (HTTP/1.1 + IPv4 + cookies)
        try:
            log("🧰 Probando fallback: curl --http1.1 --ipv4 (cookiejar) ...")
            html_text = fetch_with_curl(url)
            log(f"✅ OK (curl) bytes={len(html_text.encode('utf-8', errors='ignore'))}")
            return html_text
        except Exception as e2:
            last_err = e2
            log(f"⚠️  Error fetch (curl) -> {type(e2).__name__}: {e2}")

        if attempt < MAX_FETCH_ATTEMPTS:
            log(f"⏳ Sleep {RETRY_SLEEP_SECONDS}s")
            time.sleep(RETRY_SLEEP_SECONDS)

    raise last_err


def fetch_html_any_url(urls: List[str]) -> Tuple[str, str]:
    """
    Prueba varias PLPs (de más ligera a más pesada).
    Devuelve (html, url_ok)
    """
    last_err = None
    for idx, url in enumerate(urls, start=1):
        log("------------------------------------------------------------")
        log(f"🔁 PROBANDO URL {idx}/{len(urls)}: {url}")
        try:
            html_text = fetch_html_one_url(url)
            return html_text, url
        except Exception as e:
            last_err = e
            log(f"❌ Falló URL: {type(e).__name__}: {e}")

    raise last_err


# =========================
# Parsing
# =========================
def parse_plp(html_text: str) -> List[Dict]:
    soup = BeautifulSoup(html_text, "lxml")

    items = []
    for art in soup.select("li.products_list-item article.product_preview"):
        raw_name = art.get("aria-label") or ""
        raw_name = normalize_spaces(raw_name)

        if not raw_name:
            a = art.select_one("h2 a.product_preview-title")
            if a:
                raw_name = normalize_spaces(a.get_text(" ", strip=True))

        if not raw_name:
            continue

        if is_tablet_or_non_mobile(raw_name):
            continue

        ram, rom = extract_ram_rom_from_name(raw_name)
        if not ram or not rom:
            continue

        a = art.select_one("h2 a.product_preview-title")
        href = a.get("href") if a else ""
        url = absolutize_url(href)

        img = art.select_one("img.js_preview_image")
        img_url = html.unescape((img.get("src") if img else "") or "")
        img_url = image_to_600(img_url)

        # Precio: en páginas "clásicas" suele venir en HTML y esto captura bien.
        price_text = normalize_spaces(art.get_text(" ", strip=True))
        price = None
        mprice = re.search(r"(\d{1,4}(?:[.,]\d{2})?)\s*€", price_text)
        if mprice:
            price = mprice.group(1).replace(".", "").replace(",", ".")

        nombre_final = title_case_product_name(raw_name)

        version = "Versión Global"
        if re.search(r"\biphone\b", nombre_final, re.IGNORECASE):
            version = "IOS"

        item = {
            "nombre": nombre_final,
            "memoria": ram,
            "capacidad": rom,
            "version": version,
            "fuente": "El Corte Inglés",
            "enviado_desde": "España",
            "precio_actual": price,
            "precio_original": None,
            "codigo_de_descuento": "OFERTA: PROMO.",
            "imagen_producto": img_url,
            "url_producto": url,
            "url_importada_sin_afiliado": url,
            "url_con_afiliado": build_affiliate_url(url),
            "id_origen": art.get("id") or None,
        }
        items.append(item)

        if MAX_PRODUCTS_N and len(items) >= MAX_PRODUCTS_N:
            break

    return items


# =========================
# MAIN
# =========================
def main() -> None:
    log("============================================================")
    log(f"🔎 PREVIEW EL CORTE INGLÉS (SIN CREAR) ({now_str()})")
    log("============================================================")
    log(f"SCRAPER_VERSION: {SCRAPER_VERSION}")
    log("PLP_URLS (fallback):")
    for u in PLP_URLS:
        log(f"- {u}")
    log(f"Pausa entre requests: {PAUSE_SECONDS}s")
    log(f"Timeout connect/read (requests): {CONNECT_TIMEOUT:.1f}s / {READ_TIMEOUT:.1f}s")
    log(f"curl max-time: {CURL_MAX_TIME:.1f}s | cookiejar: {COOKIE_JAR}")
    log(f"Reintentos fetch por URL: {MAX_FETCH_ATTEMPTS} (sleep {RETRY_SLEEP_SECONDS}s)")
    log(f"Afiliado ECI configurado: {'SI' if bool(AFF_ELCORTEINGLES) else 'NO'}")
    log(f"MAX_PRODUCTS: {MAX_PRODUCTS_N if MAX_PRODUCTS_N else 'SIN LÍMITE'}")
    log("============================================================")

    summary_detectados = []
    url_ok = None
    items = []

    try:
        html_text, url_ok = fetch_html_any_url(PLP_URLS)
        sleep_polite()
        items = parse_plp(html_text)
    except Exception as e:
        log(f"❌ ERROR al descargar/parsear TODAS las PLPs: {type(e).__name__}: {e}")
        items = []

    log("============================================================")
    log(f"✅ URL usada finalmente: {url_ok if url_ok else 'NINGUNA (todo falló)'}")
    log(f"📦 Productos móviles detectados (con RAM+ROM): {len(items)}")
    log("------------------------------------------------------------")

    for p in items:
        summary_detectados.append(p["nombre"])

        log(f"Detectado {p['nombre']}")
        log(f"1) Nombre: {p['nombre']}")
        log(f"2) Memoria: {p['memoria']}")
        log(f"3) Capacidad: {p['capacidad']}")
        log(f"4) Versión: {p['version']}")
        log(f"5) Fuente: {p['fuente']}")
        log(f"6) Precio actual: {p['precio_actual'] if p['precio_actual'] else 'NO DETECTADO (JS o no visible)'}")
        log(f"7) Precio original: {p['precio_original'] if p['precio_original'] else 'NO DETECTADO'}")
        log(f"8) Código de descuento: {p['codigo_de_descuento']}")
        log(f"9) Enviado desde: {p['enviado_desde']}")
        log(f"10) URL Imagen: {p['imagen_producto'] if p['imagen_producto'] else 'N/D'}")
        log(f"11) Enlace Producto: {p['url_producto'] if p['url_producto'] else 'N/D'}")
        log(f"12) URL importada sin afiliado: {p['url_importada_sin_afiliado'] if p['url_importada_sin_afiliado'] else 'N/D'}")
        log(f"13) URL con mi afiliado: {p['url_con_afiliado'] if p['url_con_afiliado'] else 'N/D'}")
        if p.get("id_origen"):
            log(f"14) ID origen (ECI): {p['id_origen']}")
        log("------------------------------------------------------------")

    log("\n============================================================")
    log(f"📋 RESUMEN DE EJECUCIÓN ({now_str()})")
    log("============================================================")
    log(f"\nA) DETECTADOS: {len(summary_detectados)}")
    for n in summary_detectados[:200]:
        log(f"- {n}")
    if len(summary_detectados) > 200:
        log(f"... (+{len(summary_detectados) - 200} más)")
    log("============================================================")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("⛔ Cancelado por usuario")
        sys.exit(1)
