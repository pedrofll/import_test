"""
Scraper para El Corte Inglés — Móviles
ESTRATEGIA FINAL: Google Cache Bypass.
El objetivo es evitar el bloqueo de IP de GitHub pidiendo el HTML a Google en lugar de a ECI.
"""

import os
import re
import time
import random
import urllib.parse
from dataclasses import dataclass
from typing import List, Optional, Tuple
from urllib.parse import urljoin, urlparse, parse_qsl, urlencode, urlunparse

from bs4 import BeautifulSoup

# Intentamos importar curl_cffi
try:
    from curl_cffi import requests
    USAR_CURL_CFFI = True
except ImportError:
    import requests
    USAR_CURL_CFFI = False

# =========================
# Configuración
# =========================

# URLs originales de ECI
RUTAS_OBJETIVO = [
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/",
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/2/",
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/3/",
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/4/",
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/5/",
]

AFF_ELCORTEINGLES = os.environ.get("AFF_ELCORTEINGLES", "").strip()
TIMEOUT = 30 
BASE_URL = "https://www.elcorteingles.es"
ID_IMPORTACION = f"{BASE_URL}/electronica/moviles-y-smartphones/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9",
    "Cache-Control": "max-age=0",
}

# ===========
# Modelo
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

# ===========
# Helpers
# ===========
def mask_url(u: str) -> str:
    if not u: return ""
    try:
        p = urlparse(u)
        return urlunparse((p.scheme, p.netloc, p.path, "", "", ""))
    except: return u

def to_google_cache_url(eci_url: str) -> str:
    """Convierte la URL de ECI en una petición a Google Cache."""
    # Eliminamos parámetros extra para limpiar
    clean_url = eci_url.split("?")[0]
    # Codificamos
    encoded = urllib.parse.quote(clean_url)
    # Construimos URL de cache
    # strip=0 para mantener formato, vwsrc=0 para ver renderizado
    return f"http://webcache.googleusercontent.com/search?q=cache:{clean_url}&strip=0&vwsrc=0"

# ===========
# Regex Helpers
# ===========
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
    # En cache las imagenes a veces apuntan a google, intentamos limpiar
    if "googleusercontent" in img_url: return img_url 
    
    img_url = img_url.replace("&amp;", "&")
    # Si es relativa, la hacemos absoluta a ECI
    if img_url.startswith("//"): img_url = "https:" + img_url
    elif img_url.startswith("/"): img_url = BASE_URL + img_url
    
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
    # En Google Cache, los enlaces pueden venir sucios. Limpiamos.
    # Si empieza por /url?q= es una redireccion de google
    if "/url?q=" in url_rel_o_abs:
        try:
            parsed = parse_qsl(urlparse(url_rel_o_abs).query)
            for k, v in parsed:
                if k == 'q': 
                    url_rel_o_abs = v
                    break
        except: pass

    if url_rel_o_abs.startswith("/"):
        abs_url = urljoin(BASE_URL, url_rel_o_abs)
    else:
        abs_url = url_rel_o_abs
        
    p = urlparse(abs_url)
    return urlunparse((p.scheme, p.netloc, p.path, "", "", ""))

def build_url_con_afiliado(url_sin: str, aff: str) -> str:
    if not url_sin: return ""
    if not aff: return url_sin
    sep = "&" if "?" in url_sin else "?"
    if re.fullmatch(r"\d+", aff): return f"{url_sin}{sep}aff_id={aff}"
    return f"{url_sin}{sep}{aff.lstrip('?&')}"

# =========================
# RED (Google Cache Fetcher)
# =========================

def get_session():
    if USAR_CURL_CFFI:
        return requests.Session(impersonate="chrome120", headers=HEADERS)
    else:
        s = requests.Session()
        s.headers.update(HEADERS)
        return s

def fetch_google_cache(session, original_url: str) -> str:
    cache_link = to_google_cache_url(original_url)
    
    # Pausa humana
    time.sleep(random.uniform(3, 7))
    print(f"🕵️  Pidiendo a Google Cache: {original_url} ...")
    
    try:
        # A veces Google Cache redirige o da 404 si no tiene la página
        r = session.get(cache_link, timeout=TIMEOUT)
        
        if r.status_code == 404:
            print(f"❌ Google no tiene esta página en caché (404).")
            return ""
        if r.status_code == 429:
            print(f"❌ Google nos ha limitado (429 Too Many Requests).")
            return ""
            
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"⚠️  Error conectando con Google Cache: {e}")
        return ""

# =========================
# Parsing
# =========================

def detectar_cards(soup: BeautifulSoup):
    # En Google Cache, la estructura se mantiene, pero cuidado con el header de google
    cards = soup.select('div.card') or soup.select('li.products_list-item') or soup.select('.product-preview') or soup.select('.grid-item')
    return cards

def extraer_info_card(card: BeautifulSoup) -> Tuple[str, str, float, float, str]:
    tit, href = "", ""
    # Título
    for sel in ["a.product_preview-title", "h2 a", ".product-name a", "a.js-product-link"]:
        a = card.select_one(sel)
        if a:
            tit = a.get("title") or a.get_text(" ", strip=True)
            href = a.get("href") or ""
            break
            
    # Precio
    p_act, p_org = None, None
    for sel in [".js-preview-pricing", ".pricing", ".price", ".product-price", ".prices-price"]:
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

def obtener_productos(session, url_objetivo: str, etiqueta: str) -> List[ProductoECI]:
    html = fetch_google_cache(session, url_objetivo)
    if not html: return []
    
    soup = BeautifulSoup(html, "html.parser")
    cards = detectar_cards(soup)
    
    if not cards:
        print(f"⚠️  HTML obtenido de Google, pero no veo productos. ¿Cache antigua?", flush=True)
        return []

    productos = []
    for card in cards:
        tit, href, p_act, p_org, img = extraer_info_card(card)
        
        if not tit: continue
        
        t_clean = titulo_limpio(tit)
        specs = extraer_ram_rom(t_clean)
        
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
    print("--- FASE 1: ECI VIA GOOGLE CACHE (BYPASS TOTAL) ---", flush=True)
    session = get_session()
    
    total = 0
    for i, url in enumerate(RUTAS_OBJETIVO, start=1):
        print(f"\n📂 Procesando ({i}/{len(RUTAS_OBJETIVO)})", flush=True)
        try:
            prods = obtener_productos(session, url, str(i))
        except Exception as e:
            print(f"❌ Error procesando {mask_url(url)}: {e}", flush=True)
            continue
            
        print(f"✅ Encontrados en Cache: {len(prods)}", flush=True)
        total += len(prods)
        
        for p in prods:
            print("-" * 60)
            print(f"Detectado {p.nombre}")
            print(f"1) Nombre: {p.nombre}")
            print(f"2) RAM: {p.memoria} | ROM: {p.capacidad}")
            print(f"3) Precio: {p.precio_actual}€")
            print(f"4) URL: {mask_url(p.url_importada_sin_afiliado)}")
            print("-" * 60, flush=True)
            
    print(f"\n📋 TOTAL PRODUCTOS ESCANEADOS: {total}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
