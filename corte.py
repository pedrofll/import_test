"""
Scraper (DRY RUN) para El Corte Inglés — móviles y smartphones

Objetivo (fase 1):
- Escanear páginas 1..10 del listado
- Extraer: nombre, RAM, capacidad, precio_actual, precio_original, img_url, url_producto (sin query)
- Construir URL con afiliado usando secreto AFF_ELCORTEINGLES
- NO crea/actualiza/elimina productos en WooCommerce (solo logs)

Cambios Anti-Bloqueo:
- Headers de Chrome Windows 10 completo.
- Uso de Session para cookies.
- Timeouts ajustados.
"""

import os
import re
import time
import random
from dataclasses import dataclass
from typing import List, Optional, Tuple
from urllib.parse import urljoin, urlparse, parse_qsl, urlencode, urlunparse

import requests
from bs4 import BeautifulSoup


# =========================
# Configuración / Variables
# =========================

DEFAULT_URLS = [
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/",
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/2/",
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/3/",
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/4/",
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/5/",
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/6/",
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/7/",
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/8/",
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/9/",
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/10/",
]

# Opcional: lista completa por variable tipo "url1,url2,url3"
CORTEINGLES_URLS_RAW = os.environ.get("CORTEINGLES_URLS", "").strip()

# Opcional: URL base (página 1) para derivar la lista 1..10
START_URL_CORTEINGLES = os.environ.get("START_URL_CORTEINGLES", "").strip()

# Afiliado (secreto).
AFF_ELCORTEINGLES = os.environ.get("AFF_ELCORTEINGLES", "").strip()

# Modo: solo logs
DRY_RUN = True

TIMEOUT = 20  # Reducido un poco para fallar rápido si bloquean

# HEADERS "CAMUFLADOS" (Simulando Windows 10 / Chrome Real)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.google.es/",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "cross-site",
    "Sec-Fetch-User": "?1",
    "sec-ch-ua": '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}

def mask_url(u: str) -> str:
    """Devuelve la URL sin query ni fragmento."""
    if not u:
        return ""
    try:
        p = urlparse(u)
        return urlunparse((p.scheme, p.netloc, p.path, "", "", ""))
    except Exception:
        return u


def build_urls_paginas() -> List[str]:
    if CORTEINGLES_URLS_RAW:
        return [u.strip() for u in CORTEINGLES_URLS_RAW.split(",") if u.strip()]

    if START_URL_CORTEINGLES:
        base = START_URL_CORTEINGLES.rstrip("/")
        if base.endswith("moviles-y-smartphones"):
            page1 = base + "/"
            return [page1] + [f"{page1}{i}/" for i in range(2, 11)]
        return [START_URL_CORTEINGLES]

    return DEFAULT_URLS


URLS_PAGINAS = build_urls_paginas()
BASE_URL = "https://www.elcorteingles.es"
ID_IMPORTACION = f"{BASE_URL}/electronica/moviles-y-smartphones/"


# ===========
# Modelo dato
# ===========

@dataclass
class ProductoECI:
    nombre: str
    memoria: str
    capacidad: str
    version: str
    precio_actual: float
    precio_original: float
    enviado_desde: str
    origen_pagina: str
    img: str
    url_imp: str
    url_exp: str
    url_importada_sin_afiliado: str
    url_sin_acortar_con_mi_afiliado: str
    url_oferta: str
    page_id: str


# =========================
# Parsing de nombre / specs
# =========================

RE_GB = re.compile(r"(\d{1,3})\s*GB", re.IGNORECASE)
RE_RAM_PLUS = re.compile(r"(\d{1,3})\s*GB\s*\+\s*(\d{1,4})\s*GB", re.IGNORECASE)
RE_12GB_512GB = re.compile(r"(\d{1,3})\s*GB\s*[+xX]\s*(\d{1,4})\s*GB", re.IGNORECASE)
RE_COMPACT_8_256 = re.compile(r"\b(\d{1,2})\s*\+\s*(\d{2,4})\s*GB\b", re.IGNORECASE)

RE_MOBILE_LIBRE = re.compile(r"\bm[oó]vil\s+libre\b", re.IGNORECASE)
RE_PATROCINADO = re.compile(r"\bpatrocinado\b", re.IGNORECASE)


def normalizar_espacios(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def titulo_limpio(titulo: str) -> str:
    t = normalizar_espacios(titulo)
    t = RE_PATROCINADO.sub("", t)
    t = RE_MOBILE_LIBRE.sub("", t)
    return normalizar_espacios(t)


def extraer_ram_rom(titulo: str) -> Optional[Tuple[str, str]]:
    m = RE_RAM_PLUS.search(titulo)
    if m:
        return f"{m.group(1)}GB", f"{m.group(2)}GB"

    m = RE_12GB_512GB.search(titulo)
    if m:
        return f"{m.group(1)}GB", f"{m.group(2)}GB"

    m = RE_COMPACT_8_256.search(titulo)
    if m:
        return f"{m.group(1)}GB", f"{m.group(2)}GB"

    gbs = RE_GB.findall(titulo)
    if len(gbs) >= 2:
        return f"{gbs[0]}GB", f"{gbs[1]}GB"

    return None


def extraer_nombre(titulo: str, ram: str) -> str:
    # Corta antes del primer RAM detectado
    ram_pat = re.escape(ram.replace("GB", "")) + r"\s*GB"
    m = re.search(ram_pat, titulo, flags=re.IGNORECASE)
    if m:
        base = titulo[: m.start()].strip(" -–—,:;")
        return normalizar_espacios(base)
    return normalizar_espacios(titulo)


# ==========
# Precios
# ==========

def parse_precio(texto: str) -> Optional[float]:
    if not texto:
        return None
    s = texto.replace("\xa0", " ").replace("€", "").strip()
    s = s.replace(".", "").replace(",", ".")
    s = re.sub(r"[^\d.]", "", s)
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


# ==========
# Imagen URL
# ==========

def normalizar_url_imagen_600(img_url: str) -> str:
    if not img_url:
        return ""
    img_url = img_url.replace("&amp;", "&")
    try:
        p = urlparse(img_url)
        q = dict(parse_qsl(p.query, keep_blank_values=True))
        q["impolicy"] = q.get("impolicy", "Resize")
        q["width"] = "600"
        q["height"] = "600"
        return urlunparse((p.scheme, p.netloc, p.path, "", urlencode(q, doseq=True), ""))
    except Exception:
        return img_url


# ======================
# Afiliado / URL producto
# ======================

def limpiar_url_producto(url_rel_o_abs: str) -> str:
    if not url_rel_o_abs:
        return ""
    abs_url = urljoin(BASE_URL, url_rel_o_abs)
    p = urlparse(abs_url)
    return urlunparse((p.scheme, p.netloc, p.path, "", "", ""))


def afiliado_to_query(aff: str) -> str:
    if not aff:
        return ""
    a = aff.strip()
    if re.fullmatch(r"\d+", a):
        return f"aff_id={a}"
    if a.startswith("?") or a.startswith("&"):
        return a[1:]
    return a


def build_url_con_afiliado(url_sin: str, aff: str) -> str:
    if not url_sin:
        return ""
    q = afiliado_to_query(aff)
    if not q:
        return url_sin
    sep = "&" if "?" in url_sin else "?"
    return url_sin + sep + q


# ==============
# Descarga HTML (CON SESIÓN Y RETRIES)
# ==============

def get_session():
    """Crea una sesión con headers robustos."""
    s = requests.Session()
    s.headers.update(HEADERS)
    return s

def fetch_html(session: requests.Session, url: str) -> str:
    """Descarga usando la sesión persistente."""
    # Pausa aleatoria para parecer humano (2 a 5 segundos)
    time.sleep(random.uniform(2, 5))
    
    r = session.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    return r.text


# ======================
# Extracción de productos
# ======================

def detectar_cards(soup: BeautifulSoup):
    cards = soup.select('div.card[data-synth="LOCATOR_PRODUCT_PREVIEW_LIST"]')
    return cards if cards else soup.select("div.card")


def extraer_img(card: BeautifulSoup) -> str:
    img = card.select_one("img.js_preview_image")
    if img and img.get("src"):
        return normalizar_url_imagen_600(img["src"])
    img = card.select_one("img[data-variant-image-src]")
    if img and img.get("data-variant-image-src"):
        return normalizar_url_imagen_600(img["data-variant-image-src"])
    img = card.select_one("img")
    if img and img.get("src"):
        return normalizar_url_imagen_600(img["src"])
    return ""


def extraer_titulo_y_url(card: BeautifulSoup) -> Tuple[str, str]:
    a = card.select_one("a.product_preview-title")
    if a:
        titulo = a.get("title") or a.get_text(" ", strip=True)
        href = a.get("href") or ""
        return normalizar_espacios(titulo), href
    a = card.select_one("h2 a")
    if a:
        titulo = a.get("title") or a.get_text(" ", strip=True)
        href = a.get("href") or ""
        return normalizar_espacios(titulo), href
    return "", ""


def extraer_precio(card: BeautifulSoup) -> Tuple[Optional[float], Optional[float]]:
    pricing = card.select_one(".js-preview-pricing") or card.select_one(".pricing")
    texts = [normalizar_espacios(t) for t in (pricing.stripped_strings if pricing else card.stripped_strings) if t]
    precios = []
    for t in texts:
        if "€" in t or re.search(r"\d", t):
            p = parse_precio(t)
            if p is not None:
                precios.append(p)
    if not precios:
        return None, None
    p_act = min(precios)
    p_org = max(precios)
    if p_org == p_act:
        p_org = round(p_act * 1.20, 2)
    return p_act, p_org


def obtener_productos(session: requests.Session, url: str, etiqueta_pagina: str) -> List[ProductoECI]:
    html = fetch_html(session, url)
    soup = BeautifulSoup(html, "html.parser")
    productos: List[ProductoECI] = []
    
    cards = detectar_cards(soup)
    if not cards:
        print(f"⚠️  AVISO: No se encontraron tarjetas de producto en {etiqueta_pagina}. Puede que el selector haya cambiado o nos estén dando otro HTML.", flush=True)

    for card in cards:
        titulo, href = extraer_titulo_y_url(card)
        if not titulo or not href:
            continue

        t = titulo_limpio(titulo)
        specs = extraer_ram_rom(t)
        if not specs:
            continue
        ram, rom = specs
        nombre = extraer_nombre(t, ram)

        p_act, p_org = extraer_precio(card)
        if p_act is None:
            continue
        if p_org is None:
            p_org = round(p_act * 1.20, 2)

        url_sin = limpiar_url_producto(href)
        url_con = build_url_con_afiliado(url_sin, AFF_ELCORTEINGLES)
        img_url = extraer_img(card)

        productos.append(
            ProductoECI(
                nombre=nombre,
                memoria=ram,
                capacidad=rom,
                version="Global",
                precio_actual=p_act,
                precio_original=p_org,
                enviado_desde="España",
                origen_pagina=etiqueta_pagina,
                img=img_url,
                url_imp=url_con,
                url_exp=url_con,
                url_importada_sin_afiliado=url_sin,
                url_sin_acortar_con_mi_afiliado=url_con,
                url_oferta=url_con,
                page_id=ID_IMPORTACION,
            )
        )
    return productos


def main() -> int:
    print("--- FASE 1: ESCANEANDO EL CORTE INGLÉS ---", flush=True)
    print(f"Páginas a escanear: {len(URLS_PAGINAS)}", flush=True)
    print(f"DRY_RUN: {DRY_RUN}", flush=True)
    print(f"Page ID (origen): {ID_IMPORTACION}", flush=True)
    print("-" * 60, flush=True)

    # Creamos una sesión persistente para parecer un navegador
    session = get_session()
    
    total = 0
    for i, url in enumerate(URLS_PAGINAS, start=1):
        etiqueta = str(i)
        print(f"Escaneando listado ({i}/{len(URLS_PAGINAS)}): {mask_url(url)}", flush=True)
        try:
            productos = obtener_productos(session, url, etiqueta)
        except Exception as e:
            print(f"❌ ERROR escaneando {mask_url(url)}: {e}", flush=True)
            continue

        print(f"✅ Productos válidos detectados: {len(productos)} (página {etiqueta})", flush=True)
        total += len(productos)

        for r in productos:
            print("-" * 60, flush=True)
            print(f"Detectado {r.nombre}", flush=True)
            print(f"1) Nombre:          {r.nombre}", flush=True)
            print(f"2) Memoria (RAM):   {r.memoria}", flush=True)
            print(f"3) Capacidad:       {r.capacidad}", flush=True)
            print(f"4) Versión ROM:     {r.version}", flush=True)
            print(f"5) Precio Actual:   {r.precio_actual}€", flush=True)
            print(f"6) Precio Original: {r.precio_original}€", flush=True)
            print(f"7) Enviado desde:   {r.enviado_desde}", flush=True)
            print(f"8) Importado de la página: {r.origen_pagina}", flush=True)
            img = r.img or ""
            print(f"9) URL Imagen:      {(img[:140] + '...') if len(img) > 140 else img}", flush=True)
            print(f"10) Enlace Compra:  {mask_url(r.url_importada_sin_afiliado)}", flush=True)
            print("-" * 60, flush=True)

    print("=" * 60, flush=True)
    print("📋 RESUMEN (SOLO LOGS / SIN PUBLICAR)", flush=True)
    print("=" * 60, flush=True)
    print(f"Productos logueados: {total}", flush=True)
    print(f"DRY_RUN: {DRY_RUN}", flush=True)
    print(f"Page ID (origen): {ID_IMPORTACION}", flush=True)
    print("=" * 60, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
