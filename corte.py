"""
Scraper para El Corte Inglés — móviles y smartphones
SOLUCIÓN DIAGNÓSTICA: Ampliación de selectores y volcado de HTML para debug.

Requisitos:
    pip install curl_cffi beautifulsoup4 requests
"""

import os
import re
import time
import json
import random
from dataclasses import dataclass
from typing import List, Optional, Tuple
from urllib.parse import urljoin, urlparse, parse_qsl, urlencode, urlunparse

from bs4 import BeautifulSoup

# Intentamos importar curl_cffi para evadir bloqueos TLS
try:
    from curl_cffi import requests
    USAR_CURL_CFFI = True
except ImportError:
    import requests
    USAR_CURL_CFFI = False
    print("⚠️ ADVERTENCIA: 'curl_cffi' no está instalado. Se usará 'requests' estándar.")

# =========================
# Configuración / Variables
# =========================

DEFAULT_URLS = [
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/",
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/2/",
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/3/",
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/4/",
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/5/",
]

CORTEINGLES_URLS_RAW = os.environ.get("CORTEINGLES_URLS", "").strip()
START_URL_CORTEINGLES = os.environ.get("START_URL_CORTEINGLES", "").strip()
AFF_ELCORTEINGLES = os.environ.get("AFF_ELCORTEINGLES", "").strip()

DRY_RUN = True
TIMEOUT = 45

# Headers para curl_cffi
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

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
# Helpers
# =========================

def mask_url(u: str) -> str:
    if not u: return ""
    try:
        p = urlparse(u)
        return urlunparse((p.scheme, p.netloc, p.path, "", "", ""))
    except: return u

def build_urls_paginas() -> List[str]:
    if CORTEINGLES_URLS_RAW:
        return [u.strip() for u in CORTEINGLES_URLS_RAW.split(",") if u.strip()]
    if START_URL_CORTEINGLES:
        base = START_URL_CORTEINGLES.rstrip("/")
        if base.endswith("moviles-y-smartphones"):
            return [base + "/"] + [f"{base}/{i}/" for i in range(2, 11)]
        return [START_URL_CORTEINGLES]
    return DEFAULT_URLS

URLS_PAGINAS = build_urls_paginas()
BASE_URL = "https://www.elcorteingles.es"
ID_IMPORTACION = f"{BASE_URL}/electronica/moviles-y-smartphones/"

# =========================
# Parsing (Regex)
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
    if m: return f"{m.group(1)}GB", f"{m.group(2)}GB"
    m = RE_12GB_512GB.search(titulo)
    if m: return f"{m.group(1)}GB", f"{m.group(2)}GB"
    m = RE_COMPACT_8_256.search(titulo)
    if m: return f"{m.group(1)}GB", f"{m.group(2)}GB"
    gbs = RE_GB.findall(titulo)
    if len(gbs) >= 2: return f"{gbs[0]}GB", f"{gbs[1]}GB"
    return None

def extraer_nombre(titulo: str, ram: str) -> str:
    ram_pat = re.escape(ram.replace("GB", "")) + r"\s*GB"
    m = re.search(ram_pat, titulo, flags=re.IGNORECASE)
    if m:
        base = titulo[: m.start()].strip(" -–—,:;")
        return normalizar_espacios(base)
    return normalizar_espacios(titulo)

def parse_precio(texto: str) -> Optional[float]:
    if not texto: return None
    s = texto.replace("\xa0", " ").replace("€", "").strip()
    s = s.replace(".", "").replace(",", ".")
    s = re.sub(r"[^\d.]", "", s)
    if not s: return None
    try: return float(s)
    except: return None

def normalizar_url_imagen_600(img_url: str) -> str:
    if not img_url: return ""
    img_url = img_url.replace("&amp;", "&")
    try:
        p = urlparse(img_url)
        q = dict(parse_qsl(p.query, keep_blank_values=True))
        q["impolicy"] = q.get("impolicy", "Resize")
        q["width"] = "600"
        q["height"] = "600"
        return urlunparse((p.scheme, p.netloc, p.path, "", urlencode(q, doseq=True), ""))
    except: return img_url

def limpiar_url_producto(url_rel_o_abs: str) -> str:
    if not url_rel_o_abs: return ""
    abs_url = urljoin(BASE_URL, url_rel_o_abs)
    p = urlparse(abs_url)
    return urlunparse((p.scheme, p.netloc, p.path, "", "", ""))

def build_url_con_afiliado(url_sin: str, aff: str) -> str:
    if not url_sin: return ""
    if not aff: return url_sin
    sep = "&" if "?" in url_sin else "?"
    if re.fullmatch(r"\d+", aff): return f"{url_sin}{sep}aff_id={aff}"
    return f"{url_sin}{sep}{aff.lstrip('?&')}"

# =========================
# Red / Descarga
# =========================

def get_session():
    if USAR_CURL_CFFI:
        return requests.Session(impersonate="chrome110", headers=HEADERS)
    else:
        s = requests.Session()
        s.headers.update(HEADERS)
        return s

def fetch_html(session, url: str) -> str:
    time.sleep(random.uniform(4, 7))
    r = session.get(url, timeout=TIMEOUT)
    if r.status_code == 403:
        raise Exception("Bloqueo 403 (Access Denied).")
    r.raise_for_status()
    return r.text

# =========================
# Lógica Principal
# =========================

def detectar_cards(soup: BeautifulSoup):
    # Intentamos múltiples selectores de contenedor
    # 1. Selector clásico
    cards = soup.select('div.card[data-synth="LOCATOR_PRODUCT_PREVIEW_LIST"]')
    if cards: return cards
    
    # 2. Selector genérico de cards
    cards = soup.select("div.card")
    if cards: return cards
    
    # 3. Selector para estructura grid moderna (ul > li)
    cards = soup.select("ul.products-list li")
    if cards: return cards

    # 4. Busqueda por clases de producto genéricas
    cards = soup.select(".product-preview")
    return cards

def extraer_info_card(card: BeautifulSoup) -> Tuple[str, str, float, float, str]:
    # Título y URL
    tit, href = "", ""
    for sel in ["a.product_preview-title", "h2 a", "a.js-product-link", ".product-name a"]:
        a = card.select_one(sel)
        if a:
            tit = a.get("title") or a.get_text(" ", strip=True)
            href = a.get("href") or ""
            break
            
    # Precios
    p_act, p_org = None, None
    for sel in [".js-preview-pricing", ".pricing", ".product-price"]:
        pricing = card.select_one(sel)
        if pricing:
            texts = [normalizar_espacios(t) for t in pricing.stripped_strings if t]
            precios = []
            for t in texts:
                p = parse_precio(t)
                if p: precios.append(p)
            if precios:
                p_act = min(precios)
                p_org = max(precios)
                break
    
    if p_act and not p_org: p_org = p_act
    if p_act and p_org and p_org == p_act: p_org = round(p_act * 1.2, 2)

    # Imagen
    img_url = ""
    for sel in ["img.js_preview_image", "img[data-variant-image-src]", "img"]:
        img = card.select_one(sel)
        if img:
            src = img.get("src") or img.get("data-variant-image-src")
            if src: 
                img_url = normalizar_url_imagen_600(src)
                break

    return tit, href, p_act, p_org, img_url

def obtener_productos(session, url: str, etiqueta: str) -> List[ProductoECI]:
    html = fetch_html(session, url)
    soup = BeautifulSoup(html, "html.parser")
    
    # DEBUG: Si no encontramos productos, ver qué página nos devolvieron
    cards = detectar_cards(soup)
    
    if not cards:
        print(f"⚠️  DEBUG: No se detectaron tarjetas en {etiqueta}.", flush=True)
        title = soup.title.string.strip() if soup.title else "SIN TITULO"
        print(f"    Título de la página: {title}")
        print(f"    Inicio del HTML: {html[:300].replace(chr(10), ' ')}")
        return []

    productos = []
    for card in cards:
        tit, href, p_act, p_org, img = extraer_info_card(card)
        
        if not tit or not href: continue
        
        t_clean = titulo_limpio(tit)
        specs = extraer_ram_rom(t_clean)
        
        # Filtro: debe tener RAM/ROM para ser móvil válido
        if not specs: continue
        
        ram, rom = specs
        nombre = extraer_nombre(t_clean, ram)
        
        if p_act is None: continue
        
        url_sin = limpiar_url_producto(href)
        url_con = build_url_con_afiliado(url_sin, AFF_ELCORTEINGLES)
        
        productos.append(ProductoECI(
            nombre=nombre, memoria=ram, capacidad=rom, version="Global",
            precio_actual=p_act, precio_original=p_org, enviado_desde="España",
            origen_pagina=etiqueta, img=img, url_imp=url_con, url_exp=url_con,
            url_importada_sin_afiliado=url_sin, url_sin_acortar_con_mi_afiliado=url_con,
            url_oferta=url_con, page_id=ID_IMPORTACION
        ))
    return productos

def main() -> int:
    print("--- FASE 1: ESCANEANDO EL CORTE INGLÉS (MODO DIAGNÓSTICO) ---", flush=True)
    session = get_session()
    
    total = 0
    for i, url in enumerate(URLS_PAGINAS, start=1):
        print(f"Escaneando ({i}/{len(URLS_PAGINAS)}): {mask_url(url)}", flush=True)
        try:
            prods = obtener_productos(session, url, str(i))
        except Exception as e:
            print(f"❌ Error en {mask_url(url)}: {e}", flush=True)
            continue
            
        print(f"✅ Encontrados: {len(prods)}", flush=True)
        total += len(prods)
        
        for p in prods:
            print("-" * 60)
            print(f"Detectado {p.nombre}")
            print(f"1) Nombre: {p.nombre}")
            print(f"2) RAM: {p.memoria} | ROM: {p.capacidad}")
            print(f"3) Precio: {p.precio_actual}€ (Antes: {p.precio_original}€)")
            print(f"4) URL: {mask_url(p.url_importada_sin_afiliado)}")
            print("-" * 60, flush=True)
            
    print(f"\n📋 TOTAL PRODUCTOS ESCANEADOS: {total}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
