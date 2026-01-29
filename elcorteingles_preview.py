#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import sys
import time
import json
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
SCRAPER_VERSION = "ECI_PREVIEW_v1.3_curl_fallback"

PLP_URL = os.getenv(
    "ECI_PLP_URL",
    "https://www.elcorteingles.es/limite-48-horas/electronica/moviles-y-smartphones/"
).strip()

PAUSE_SECONDS = float(os.getenv("PAUSE_SECONDS", "0.8"))

CONNECT_TIMEOUT = float(os.getenv("ECI_CONNECT_TIMEOUT", "12"))
READ_TIMEOUT = float(os.getenv("ECI_READ_TIMEOUT", "40"))

MAX_FETCH_ATTEMPTS = int(os.getenv("ECI_MAX_FETCH_ATTEMPTS", "4"))
RETRY_SLEEP_SECONDS = int(os.getenv("ECI_RETRY_SLEEP_SECONDS", "10"))

MAX_PRODUCTS = os.getenv("MAX_PRODUCTS", "").strip()
MAX_PRODUCTS_N = int(MAX_PRODUCTS) if MAX_PRODUCTS.isdigit() else None

AFF_ELCORTEINGLES = (os.getenv("AFF_ELCORTEINGLES") or os.getenv("AFF_ELCORTEINGLES", "")).strip()

BASE_URL = "https://www.elcorteingles.es"


# =========================
# HTTP SESSION (requests)
# =========================
SESSION = requests.Session()

# Headers "browser-ish". Importante: NO pedir br para evitar problemas de decode.
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
})


# =========================
# Helpers
# =========================
def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(s: str) -> None:
    print(s, flush=True)


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
    Regla general: primera letra de cada palabra en mayúscula.
    Además, si hay tokens mixtos número+letras (14T, 5G, 4G) => letras en mayúsculas.
    """
    if not name:
        return name
    words = normalize_spaces(name).split(" ")
    out = []
    for w in words:
        # Mantener separadores tipo "+" como token normal.
        token = w.strip()
        if not token:
            continue

        # Si token es algo como "5g" / "14t" / "4g", subir letras
        m = re.fullmatch(r"(\d+)([a-zA-Z]+)", token)
        if m:
            out.append(m.group(1) + m.group(2).upper())
            continue

        # Si token es todo mayúsculas tipo "HONOR" lo normalizamos a "Honor"
        # excepto si es muy corto y parece sigla.
        if token.isupper() and len(token) > 3:
            out.append(token.capitalize())
            continue

        # Capitalización estándar
        out.append(token[:1].upper() + token[1:])
    return " ".join(out)


def extract_ram_rom_from_name(name: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Busca patrones tipo:
      '8GB + 256GB'
      '8 GB + 256 GB'
      '12GB+512GB'
    """
    if not name:
        return None, None

    s = name.replace("\u00a0", " ")
    # RAM + ROM
    m = re.search(r"(\d{1,3})\s*GB\s*\+\s*(\d{2,4})\s*GB", s, re.IGNORECASE)
    if m:
        ram = f"{int(m.group(1))} GB"
        rom = f"{int(m.group(2))} GB"
        return ram, rom

    return None, None


def is_tablet_or_non_mobile(name: str) -> bool:
    """
    Tus reglas: no importar tablets ni iPad ni si falta RAM/ROM.
    """
    n = (name or "").upper()
    if "TAB" in n or "IPAD" in n:
        return True
    return False


def build_affiliate_url(product_url: str) -> str:
    """
    Construcción flexible:
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
        # si es un prefijo típico
        if AFF_ELCORTEINGLES.endswith("=") or "url=" in AFF_ELCORTEINGLES or "p=" in AFF_ELCORTEINGLES:
            return AFF_ELCORTEINGLES + urllib.parse.quote(product_url, safe="")
        # si nos pasan una url completa de tracking sin plantilla, no inventamos
        return product_url
    except Exception:
        return product_url


def image_to_600(url: str) -> str:
    """
    En ECI suele venir:
      ...?impolicy=Resize&width=640&height=640
    Lo pasamos a 600x600.
    """
    if not url:
        return url
    u = url
    u = re.sub(r"([?&]width=)\d+", r"\g<1>600", u)
    u = re.sub(r"([?&]height=)\d+", r"\g<1>600", u)
    return u


# =========================
# Fetch strategies
# =========================
def fetch_with_requests(url: str) -> str:
    r = SESSION.get(url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT), allow_redirects=True)
    r.raise_for_status()
    return r.text


def fetch_with_curl(url: str) -> str:
    """
    Fallback robusto: curl suele pasar donde python-requests falla por fingerprint TLS/WAF.
    """
    # Importante: --compressed para manejar gzip/deflate
    # -L follow redirects, -s silent pero mantenemos errores con -S
    cmd = [
        "curl", "-sS", "-L",
        "--max-time", str(int(READ_TIMEOUT)),
        "--connect-timeout", str(int(CONNECT_TIMEOUT)),
        "--compressed",
        "-H", SESSION.headers.get("User-Agent", ""),
        url
    ]

    # Curl no acepta "User-Agent: ..." como header suelto si viene sin "User-Agent:"
    # Construimos bien:
    cmd = [
        "curl", "-sS", "-L",
        "--max-time", str(int(READ_TIMEOUT)),
        "--connect-timeout", str(int(CONNECT_TIMEOUT)),
        "--compressed",
        "-H", f"User-Agent: {SESSION.headers.get('User-Agent')}",
        "-H", f"Accept: {SESSION.headers.get('Accept')}",
        "-H", f"Accept-Language: {SESSION.headers.get('Accept-Language')}",
        url,
    ]

    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"curl_error rc={p.returncode} stderr={p.stderr.strip()[:300]}")
    return p.stdout


def fetch_html(url: str) -> str:
    last_err = None
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

        # 2) fallback curl
        try:
            log("🧰 Probando fallback: curl ...")
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


# =========================
# Parsing
# =========================
def parse_plp(html_text: str) -> List[Dict]:
    soup = BeautifulSoup(html_text, "lxml")

    items = []
    # Estructura típica: li.products_list-item > article.product_preview
    for li in soup.select("li.products_list-item article.product_preview"):
        # Nombre
        name = li.get("aria-label") or ""
        name = normalize_spaces(name)
        if not name:
            # fallback h2 a
            a = li.select_one("h2 a.product_preview-title")
            if a:
                name = normalize_spaces(a.get_text(" ", strip=True))

        if not name:
            continue

        # URL
        a = li.select_one("h2 a.product_preview-title")
        href = a.get("href") if a else ""
        url = absolutize_url(href)

        # Imagen
        img = li.select_one("img.js_preview_image")
        img_url = img.get("src") if img else ""
        img_url = html.unescape(img_url or "")
        img_url_600 = image_to_600(img_url)

        # RAM/ROM
        ram, rom = extract_ram_rom_from_name(name)

        # Precio (la PLP a veces lo carga por JS; intentamos capturar algo si viene)
        price_text = li.get_text(" ", strip=True)
        price_text = normalize_spaces(price_text)
        price = None
        mprice = re.search(r"(\d{1,4}(?:[.,]\d{2})?)\s*€", price_text)
        if mprice:
            price = mprice.group(1).replace(".", "").replace(",", ".")  # "1.099,00" -> "1099.00"
            # dejamos price como str

        # filtros de negocio
        if is_tablet_or_non_mobile(name):
            continue
        if not ram or not rom:
            # tu regla: si no tiene memoria y capacidad, no es móvil (para importar)
            continue

        nombre_final = title_case_product_name(name)

        # Version (reglas: tienda España -> versión global salvo iPhone)
        version = "Versión Global"
        if nombre_final.upper().startswith("IPHONE") or " IPHONE " in (" " + nombre_final.upper() + " "):
            version = "IOS"

        item = {
            "nombre": nombre_final,
            "memoria": ram,
            "capacidad": rom,
            "precio_actual": price,
            "precio_original": None,
            "codigo_de_descuento": "OFERTA: PROMO.",
            "fuente": "El Corte Inglés",
            "enviado_desde": "España",
            "imagen_producto": img_url_600 or img_url,
            "url_producto": url,
            "url_importada_sin_afiliado": url,
            "url_con_afiliado": build_affiliate_url(url),
            "id_origen": li.get("id") or None,
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
    log(f"PLP: {PLP_URL}")
    log(f"Pausa entre requests: {PAUSE_SECONDS}s")
    log(f"Timeout connect/read: {CONNECT_TIMEOUT:.1f}s / {READ_TIMEOUT:.1f}s")
    log(f"Reintentos fetch: {MAX_FETCH_ATTEMPTS} (sleep {RETRY_SLEEP_SECONDS}s)")
    log(f"Afiliado ECI configurado: {'SI' if bool(AFF_ELCORTEINGLES) else 'NO'}")
    log(f"MAX_PRODUCTS: {MAX_PRODUCTS_N if MAX_PRODUCTS_N else 'SIN LÍMITE'}")
    log("============================================================")

    summary_detectados = []

    try:
        html_text = fetch_html(PLP_URL)
        time.sleep(PAUSE_SECONDS)
        items = parse_plp(html_text)
    except Exception as e:
        log(f"❌ ERROR al descargar/parsear PLP: {type(e).__name__}: {e}")
        items = []

    log(f"📦 Productos móviles detectados (con RAM+ROM): {len(items)}")
    log("------------------------------------------------------------")

    for p in items:
        summary_detectados.append(p["nombre"])

        log(f"Detectado {p['nombre']}")
        log(f"1) Nombre: {p['nombre']}")
        log(f"2) Memoria: {p['memoria']}")
        log(f"3) Capacidad: {p['capacidad']}")
        log(f"4) Versión: {('IOS' if p['nombre'].upper().startswith('IPHONE') else 'Versión Global')}")
        log(f"5) Fuente: {p['fuente']}")
        log(f"6) Precio actual: {p['precio_actual'] if p['precio_actual'] else 'NO DETECTADO (JS)'}")
        log(f"7) Precio original: {p['precio_original'] if p['precio_original'] else 'NO DETECTADO'}")
        log(f"8) Código de descuento: {p['codigo_de_descuento']}")
        log(f"9) Enviado desde: {p['enviado_desde']}")
        log(f"10) URL Imagen: {p['imagen_producto']}")
        log(f"11) Enlace Producto: {p['url_producto']}")
        log(f"12) URL importada sin afiliado: {p['url_importada_sin_afiliado']}")
        log(f"13) URL con mi afiliado: {p['url_con_afiliado']}")
        if p.get("id_origen"):
            log(f"14) ID origen (ECI): {p['id_origen']}")
        log("------------------------------------------------------------")

    hoy_fmt = now_str()
    log("\n============================================================")
    log(f"📋 RESUMEN DE EJECUCIÓN ({hoy_fmt})")
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
