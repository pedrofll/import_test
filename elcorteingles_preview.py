#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import time
import random
import html
from datetime import datetime
from urllib.parse import urljoin, urlparse, urlunparse, parse_qsl, urlencode

import requests
from bs4 import BeautifulSoup


# ============================================================
# CONFIG
# ============================================================
SCRAPER_VERSION = "ECI_PREVIEW_v1.2_runner_fix"

PLP_URL = "https://www.elcorteingles.es/limite-48-horas/electronica/moviles-y-smartphones/"
BASE_URL = "https://www.elcorteingles.es"

PAUSA_REQUESTS = float(os.getenv("PAUSA_REQUESTS", "0.8"))

# Estándar ODM: reintentos y pausas
MAX_FETCH_ATTEMPTS = int(os.getenv("ECI_MAX_FETCH_ATTEMPTS", "10"))
RETRY_SLEEP_SECONDS = int(os.getenv("ECI_RETRY_SLEEP_SECONDS", "15"))

# Timeout separado connect/read (ECI en Actions a veces tarda mucho)
CONNECT_TIMEOUT = float(os.getenv("ECI_CONNECT_TIMEOUT", "12"))
READ_TIMEOUT = float(os.getenv("ECI_READ_TIMEOUT", "120"))

# Limitar productos en preview (opcional)
MAX_PRODUCTS = int(os.getenv("MAX_PRODUCTS", "0"))  # 0 = sin límite

# Afiliado: tu secret en GitHub Actions se llama AFF_ELCORTEINGLES
AFF_ECI = (os.getenv("AFF_ELCORTEINGLES") or "").strip()

FUENTE = "El Corte Inglés"
ENVIADO_DESDE = "España"
IMPORTADO_DE = "https://www.elcorteingles.es"

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "DNT": "1",
        "Referer": "https://www.elcorteingles.es/",
    }
)


# ============================================================
# HELPERS
# ============================================================
RE_RAM_ROM = re.compile(r"(\d{1,3})\s*(GB|TB)\s*\+\s*(\d{2,4})\s*(GB|TB)", re.IGNORECASE)
RE_TABLET = re.compile(r"\b(TAB|IPAD)\b", re.IGNORECASE)


def now_fmt():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def sleep_polite():
    time.sleep(PAUSA_REQUESTS + random.uniform(0.05, 0.25))


def strip_query(url: str) -> str:
    if not url:
        return url
    p = urlparse(url)
    return urlunparse((p.scheme, p.netloc, p.path, p.params, "", ""))


def with_affiliate(url_clean: str, aff: str) -> str:
    aff = (aff or "").strip()
    if not url_clean or not aff:
        return url_clean
    # aff puede venir como "utm=..." o como "?utm=..."
    if not aff.startswith("?"):
        aff = "?" + aff.lstrip("&?")
    return url_clean + aff


def image_to_600(url_img: str) -> str:
    """Fuerza width/height a 600 si vienen en query."""
    if not url_img:
        return url_img
    p = urlparse(url_img)
    qs = dict(parse_qsl(p.query, keep_blank_values=True))
    # ECI usa impolicy=Resize&width=640&height=640 (o 1200)
    qs["width"] = "600"
    qs["height"] = "600"
    qs.setdefault("impolicy", "Resize")
    return urlunparse((p.scheme, p.netloc, p.path, p.params, urlencode(qs, doseq=True), p.fragment))


def titlecase_keep_alnum(s: str) -> str:
    """
    - Primera letra de cada palabra en mayúscula
    - Si hay números + letras: letras en mayúscula (g85 -> G85, 5g -> 5G, 14t -> 14T)
    """
    s = re.sub(r"\s+", " ", (s or "").strip())

    def fix_token(tok: str) -> str:
        if not tok:
            return tok
        if re.search(r"\d", tok) and re.search(r"[A-Za-z]", tok):
            return "".join(ch.upper() if ch.isalpha() else ch for ch in tok)
        if tok.isupper():
            return tok
        return tok[:1].upper() + tok[1:].lower()

    parts = s.split(" ")
    out = []
    for w in parts:
        # respeta separadores internos con guion
        subs = w.split("-")
        subs = [fix_token(x) for x in subs]
        out.append("-".join(subs))
    return " ".join(out)


def parse_ram_rom(title: str):
    m = RE_RAM_ROM.search(title or "")
    if not m:
        return None, None
    ram_n, ram_u, rom_n, rom_u = m.group(1), m.group(2).upper(), m.group(3), m.group(4).upper()
    return f"{int(ram_n)} {ram_u}", f"{int(rom_n)} {rom_u}"


def clean_base_name(raw_title: str) -> str:
    t = html.unescape(raw_title or "")
    # quita "8GB + 256GB"
    t = RE_RAM_ROM.sub("", t)
    # quita textos típicos
    t = re.sub(r"\bm[oó]vil\s+libre\b", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\bsmartphone\b", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\btel[eé]fono\s+m[oó]vil\b", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\s+", " ", t).strip()
    return titlecase_keep_alnum(t)


def is_valid_mobile(raw_title: str) -> bool:
    if RE_TABLET.search(raw_title or ""):
        return False
    ram, rom = parse_ram_rom(raw_title or "")
    return bool(ram and rom)


def compute_version(nombre: str) -> str:
    if re.search(r"\biphone\b", nombre or "", re.IGNORECASE):
        return "IOS"
    return "Versión Global"


# ============================================================
# ROBUST HTTP (10 intentos / 15s)
# ============================================================
def fetch_html(url: str) -> str:
    last_err = None
    for attempt in range(1, MAX_FETCH_ATTEMPTS + 1):
        try:
            r = SESSION.get(url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT), allow_redirects=True)
            r.raise_for_status()
            return r.text
        except Exception as e:
            last_err = e
            print(f"⚠️  Error fetch (intento {attempt}/{MAX_FETCH_ATTEMPTS}) -> {type(e).__name__}: {e}")
            if attempt < MAX_FETCH_ATTEMPTS:
                time.sleep(RETRY_SLEEP_SECONDS)
    raise last_err


# ============================================================
# SCRAPE
# ============================================================
def scrape_plp() -> list[dict]:
    html_txt = fetch_html(PLP_URL)
    soup = BeautifulSoup(html_txt, "html.parser")

    cards = soup.select("li.products_list-item article.product_preview[id]")
    if not cards:
        cards = soup.select("article.product_preview[id]")

    items = []
    for art in cards:
        pid = (art.get("id") or "").strip()

        a = art.select_one("h2 a.product_preview-title") or art.select_one("a.product_preview-title")
        raw_title = ""
        href = ""
        if a:
            raw_title = a.get("title") or a.get_text(" ", strip=True)
            href = a.get("href") or ""

        if not raw_title:
            raw_title = art.get("aria-label") or ""

        raw_title = re.sub(r"\s+", " ", raw_title).strip()

        # url detalle: data-url fallback
        if not href:
            divlink = art.select_one("[data-url]")
            if divlink:
                href = divlink.get("data-url") or ""
        href = href.strip()
        url_producto = urljoin(BASE_URL, href) if href else ""

        # imagen
        img = art.select_one("img.js_preview_image")
        img_url = img.get("src") if img else ""
        if not img_url:
            vimg = art.select_one("[data-variant-image-src]")
            img_url = vimg.get("data-variant-image-src") if vimg else ""
        img_url = image_to_600((img_url or "").strip())

        if not is_valid_mobile(raw_title):
            continue

        ram, rom = parse_ram_rom(raw_title)
        nombre = clean_base_name(raw_title)

        items.append(
            {
                "pid": pid,
                "raw_title": raw_title,
                "nombre": nombre,
                "memoria": ram,
                "capacidad": rom,
                "url_producto": url_producto,
                "img": img_url,
            }
        )

        if MAX_PRODUCTS and len(items) >= MAX_PRODUCTS:
            break

    return items


# ============================================================
# LOGS
# ============================================================
def log_producto(p: dict):
    version = compute_version(p["nombre"])
    codigo_descuento = "OFERTA: PROMO."

    url_importada = p["url_producto"]
    url_importada_sin_afiliado = strip_query(url_importada)
    url_con_mi_afiliado = with_affiliate(url_importada_sin_afiliado, AFF_ECI)

    print(f"Detectado {p['nombre']}")
    print(f"1) Nombre: {p['nombre']}")
    print(f"2) Memoria: {p['memoria']}")
    print(f"3) Capacidad: {p['capacidad']}")
    print(f"4) Versión: {version}")
    print(f"5) Fuente: {FUENTE}")
    print(f"6) Precio actual: N/D (PLP carga por JS)")
    print(f"7) Precio original: N/D (PLP carga por JS)")
    print(f"8) Código de descuento: {codigo_descuento}")
    print(f"9) URL Imagen: {p['img'] if p['img'] else 'N/D'}")
    print(f"10) Enlace Importado: {url_importada if url_importada else 'N/D'}")
    print(f"11) Enlace Expandido: {url_importada if url_importada else 'N/D'}")
    print(f"12) URL importada sin afiliado: {url_importada_sin_afiliado if url_importada_sin_afiliado else 'N/D'}")
    print(f"13) URL sin acortar con mi afiliado: {url_con_mi_afiliado if url_con_mi_afiliado else 'N/D'}")
    print(f"14) Enviado desde: {ENVIADO_DESDE}")
    print(f"15) Importado_de: {IMPORTADO_DE}")
    print(f"16) PID (control interno): {p['pid'] if p['pid'] else 'N/D'}")
    print("-" * 60)


def main():
    print("============================================================")
    print(f"🔎 PREVIEW EL CORTE INGLÉS (SIN CREAR) ({now_fmt()})")
    print("============================================================")
    print(f"SCRAPER_VERSION: {SCRAPER_VERSION}")
    print(f"PLP: {PLP_URL}")
    print(f"Pausa entre requests: {PAUSA_REQUESTS}s")
    print(f"Timeout connect/read: {CONNECT_TIMEOUT}s / {READ_TIMEOUT}s")
    print(f"Reintentos fetch: {MAX_FETCH_ATTEMPTS} (sleep {RETRY_SLEEP_SECONDS}s)")
    print(f"Afiliado ECI configurado: {'SI' if bool(AFF_ECI) else 'NO'}")
    print(f"MAX_PRODUCTS: {MAX_PRODUCTS if MAX_PRODUCTS else 'SIN LÍMITE'}")
    print("============================================================")

    summary_creados = []
    summary_eliminados = []
    summary_actualizados = []
    summary_ignorados = []

    try:
        items = scrape_plp()
    except Exception as e:
        print(f"❌ ERROR al descargar/parsear PLP: {type(e).__name__}: {e}")
        items = []

    print(f"📦 Productos móviles detectados (con RAM+ROM): {len(items)}")
    print("------------------------------------------------------------")

    for p in items:
        log_producto(p)
        summary_ignorados.append({"nombre": p["nombre"], "id": p["pid"] or "N/A"})
        sleep_polite()

    print("\n============================================================")
    print(f"📋 RESUMEN DE EJECUCIÓN ({now_fmt()})")
    print("============================================================")
    print(f"\na) ARTICULOS CREADOS: {len(summary_creados)}")
    for item in summary_creados:
        print(f"- {item['nombre']} (ID: {item['id']})")

    print(f"\nb) ARTICULOS ELIMINADOS (OBSOLETOS): {len(summary_eliminados)}")
    for item in summary_eliminados:
        print(f"- {item['nombre']} (ID: {item['id']})")

    print(f"\nc) ARTICULOS ACTUALIZADOS: {len(summary_actualizados)}")
    for item in summary_actualizados:
        print(f"- {item['nombre']} (ID: {item['id']}): {', '.join(item['cambios'])}")

    print(f"\nd) ARTICULOS IGNORADOS (SIN CAMBIOS): {len(summary_ignorados)}")
    for item in summary_ignorados[:20]:
        print(f"- {item['nombre']} (ID: {item['id']})")
    if len(summary_ignorados) > 20:
        print(f"... ({len(summary_ignorados) - 20} más)")

    print("============================================================")


if __name__ == "__main__":
    main()
