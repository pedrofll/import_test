"""
Scraper para El Corte Inglés — Móviles
ESTRATEGIA: Bing Translate (Frame Jumping).
Soluciona el problema de descargar solo la 'carcasa' del traductor.
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
import warnings

# Ignorar warnings de SSL
warnings.filterwarnings("ignore")

try:
    from curl_cffi import requests
    USAR_CURL_CFFI = True
except ImportError:
    import requests
    USAR_CURL_CFFI = False

# =========================
# CONFIGURACIÓN
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

TIMEOUT = 40
BASE_URL = "https://www.elcorteingles.es"
ID_IMPORTACION = f"{BASE_URL}/electronica/moviles-y-smartphones/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9",
}

# =========================
# MODELO
# =========================
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
# HELPERS
# =========================
def mask_url(u: str) -> str:
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

RE_GB = re.compile(r"(\d{1,3})\s*GB", re.IGNORECASE)
RE_RAM_PLUS = re.compile(r"(\d{1,3})\s*GB\s*\+\s*(\d{1,4})\s*GB", re.IGNORECASE)
RE_12GB_512GB = re.compile(r"(\d{1,3})\s*GB\s*[+xX]\s*(\d{1,4})\s*GB", re.IGNORECASE)
RE_COMPACT_8_256 = re.compile(r"\b(\d{1,2})\s*\+\s*(\d{2,4})\s*GB\b", re.IGNORECASE)
RE_PATROCINADO = re.compile(r"\bpatrocinado\b", re.IGNORECASE)

def normalizar_espacios(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def titulo_limpio(titulo: str) -> str:
    t = normalizar_espacios(titulo)
    t = RE_PATROCINADO.sub("", t)
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
    s = texto.replace("\xa0", " ").replace("€", "").strip().replace(".", "").replace(",", ".")
    try: return float(re.sub(r"[^\d.]", "", s))
    except: return None

def normalizar_url_imagen_600(img_url: str) -> str:
    if not img_url: return ""
    
    # Si viene con prefijo Bing, lo limpiamos
    if "translatetheweb" in img_url:
        # Intentamos buscar si es una URL absoluta encodeada dentro
        pass 

    if img_url.startswith("//"): img_url = "https:" + img_url
    
    try:
        p = urlparse(img_url)
        q = dict(parse_qsl(p.query))
        q["impolicy"] = "Resize"
        q["width"] = "600"
        q["height"] = "600"
        return urlunparse((p.scheme, p.netloc, p.path, "", urlencode(q, doseq=True), ""))
    except: return img_url

def limpiar_url_producto(url_rel_o_abs: str) -> str:
    if not url_rel_o_abs: return ""
    u = url_rel_o_abs
    
    # Limpiar basura de Bing
    if "translatetheweb.com" in u:
        # Bing a veces reescribe los links como: https://www.translatetheweb.com/...&a=https://elcorte...
        # O los deja relativos.
        pass 

    if u.startswith("/"):
        u = urljoin(BASE_URL, u)
    
    # Quitamos query params para dejarla limpia
    return urlunparse(urlparse(u)._replace(query="", fragment=""))

def build_url_con_afiliado(url_sin: str, aff: str) -> str:
    if not url_sin or not aff: return url_sin
    sep = "&" if "?" in url_sin else "?"
    if re.fullmatch(r"\d+", aff): return f"{url_sin}{sep}aff_id={aff}"
    return f"{url_sin}{sep}{aff.lstrip('?&')}"

# =========================
# LÓGICA DE CONEXIÓN (BING FRAME JUMPER)
# =========================

def fetch_html_hybrid(url: str) -> str:
    session = requests.Session(impersonate="chrome120") if USAR_CURL_CFFI else requests.Session()
    session.headers.update(HEADERS)

    # 1. BING TRANSLATE
    bing_url = f"https://www.translatetheweb.com/?from=es&to=es&a={urllib.parse.quote(url)}"
    print(f"   🛡️  Bing Translate (Entrando)...")
    
    try:
        r = session.get(bing_url, timeout=30, verify=False)
        html = r.text
        
        # --- DETECCIÓN DE FRAME ---
        if "<frameset" in html or "<frame " in html:
            print("      ↪️  Detectado marco de Bing. Saltando al contenido...")
            soup_frame = BeautifulSoup(html, "html.parser")
            # Bing suele poner el contenido en un frame llamado "c"
            frame = soup_frame.find("frame", {"name": "c"})
            if frame and frame.get("src"):
                inner_url = frame.get("src")
                # A veces la URL es relativa a bing
                if inner_url.startswith("/"):
                    inner_url = "https://www.translatetheweb.com" + inner_url
                
                print(f"      🚀 Fetching frame interno...")
                r2 = session.get(inner_url, timeout=30, verify=False)
                return r2.text
        
        # Si no hay frames, devolvemos lo que hay (quizas ya es el contenido)
        return html

    except Exception as e:
        print(f"      ❌ Error Bing: {e}")

    # 2. FALLBACK: GATEWAY
    print("   🌐 Fallback: CodeTabs Gateway...")
    try:
        gw_url = f"https://api.codetabs.com/v1/proxy?quest={url}"
        r = requests.get(gw_url, timeout=20, verify=False)
        if r.status_code == 200 and "Access Denied" not in r.text:
            return r.text
    except: pass

    return ""

# =========================
# SCRAPING
# =========================

def detectar_cards(soup: BeautifulSoup):
    cards = soup.select('div.card') 
    if not cards: cards = soup.select('li.products_list-item')
    if not cards: cards = soup.select('.product-preview')
    if not cards: cards = soup.select('.grid-item')
    if not cards: cards = soup.select('.product_tile')
    return cards

def extraer_info_card(card: BeautifulSoup) -> Tuple[str, str, float, float, str]:
    tit, href = "", ""
    
    # En Bing, los atributos a veces cambian (ej: href -> rhref o similar), pero bs4 suele normalizar
    
    for sel in ["a.product_preview-title", "h2 a", ".product-name a", "a.js-product-link", ".product_tile-title"]:
        a = card.select_one(sel)
        if a:
            tit = a.get("title") or a.get_text(" ", strip=True)
            href = a.get("href") or ""
            break
            
    p_act, p_org = None, None
    for sel in [".js-preview-pricing", ".pricing", ".price", ".product-price", ".prices-price", ".product_tile-price"]:
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

    img_url = ""
    for sel in ["img.js_preview_image", "img[data-variant-image-src]", "img", ".product_tile-image"]:
        img = card.select_one(sel)
        if img:
            # Bing a veces cambia src por data-src para lazy loading
            src = img.get("src") or img.get("data-src") or img.get("data-variant-image-src")
            if src: 
                img_url = normalizar_url_imagen_600(src)
                break

    return tit, href, p_act, p_org, img_url

def obtener_productos(url: str, etiqueta: str) -> List[ProductoECI]:
    html = fetch_html_hybrid(url)
    
    if not html: 
        print(f"❌ Fallo al descargar {etiqueta}.")
        return []
    
    soup = BeautifulSoup(html, "html.parser")
    cards = detectar_cards(soup)
    
    if not cards:
        print(f"⚠️  HTML obtenido pero sin productos en {etiqueta}.", flush=True)
        title = soup.title.string.strip() if soup.title else "SIN TÍTULO"
        print(f"   🔎 Título: '{title}' (Probablemente sigamos en el wrapper de Bing)")
        return []

    productos = []
    for card in cards:
        tit, href, p_act, p_org, img = extraer_info_card(card)
        if not tit or not href: continue
        
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
    print("--- FASE 1: ECI (BING FRAME JUMPER) ---", flush=True)
    
    total = 0
    for i, url in enumerate(URLS_PAGINAS, start=1):
        print(f"\n📂 Procesando ({i}/{len(URLS_PAGINAS)}): {mask_url(url)}", flush=True)
        try:
            prods = obtener_productos(url, str(i))
        except Exception as e:
            print(f"❌ Error crítico: {e}", flush=True)
            continue
            
        print(f"✅ Encontrados: {len(prods)}", flush=True)
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
