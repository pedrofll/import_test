"""
Scraper para El Corte Inglés — Móviles
ESTRATEGIA FINAL: Navegación por MARCAS vía Google Cache (Text Mode).
Evita la paginación (que no está cacheada) y el bloqueo de IP.
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

# Ignorar warnings SSL
warnings.filterwarnings("ignore")

try:
    from curl_cffi import requests
    USAR_CURL_CFFI = True
except ImportError:
    import requests
    USAR_CURL_CFFI = False

# =========================
# CONFIGURACIÓN: ESTRATEGIA DE MARCAS
# =========================

# En lugar de paginar (1, 2, 3...), atacamos las Landing Pages de las marcas.
# Estas páginas SÍ suelen estar en la caché de Google.
URLS_MARCAS = [
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/apple/",
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/samsung/",
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/xiaomi/",
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/oppo/",
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/realme/",
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/motorola/",
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/honor/",
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/vivo/",
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/google/",
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/tcl/"
]

AFF_ELCORTEINGLES = os.environ.get("AFF_ELCORTEINGLES", "").strip()
TIMEOUT = 30
BASE_URL = "https://www.elcorteingles.es"
ID_IMPORTACION = f"{BASE_URL}/electronica/moviles-y-smartphones/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
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
    # En Google Cache Text Mode, el precio puede venir sucio
    s = texto.replace("\xa0", " ").replace("€", "").strip()
    s = s.replace(".", "").replace(",", ".") # Formato ES
    try:
        # Extraer solo números y puntos
        clean = re.sub(r"[^\d.]", "", s)
        return float(clean)
    except: return None

def normalizar_url_imagen_600(img_url: str) -> str:
    if not img_url: return ""
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
    if "googleusercontent" in u: return "" # Link interno de cache
    if u.startswith("/"): u = urljoin(BASE_URL, u)
    return urlunparse(urlparse(u)._replace(query="", fragment=""))

def build_url_con_afiliado(url_sin: str, aff: str) -> str:
    if not url_sin or not aff: return url_sin
    sep = "&" if "?" in url_sin else "?"
    if re.fullmatch(r"\d+", aff): return f"{url_sin}{sep}aff_id={aff}"
    return f"{url_sin}{sep}{aff.lstrip('?&')}"

# =========================
# FETCHER: GOOGLE CACHE TEXT MODE
# =========================

def fetch_google_cache(url: str) -> str:
    """Obtiene la versión 'Solo Texto' de la caché para evitar JS/Cookies."""
    
    session = requests.Session(impersonate="chrome110") if USAR_CURL_CFFI else requests.Session()
    session.headers.update(HEADERS)
    
    # strip=1 elimina estilos y scripts (bypass detección bot)
    # vwsrc=0 asegura vista renderizada texto
    clean_url = url.split("?")[0]
    cache_link = f"http://webcache.googleusercontent.com/search?q=cache:{urllib.parse.quote(clean_url)}&strip=1&vwsrc=0"
    
    print(f"   👻 Cache: {mask_url(url)}")
    
    try:
        time.sleep(random.uniform(3, 6)) # Pausa para no saturar a Google
        r = session.get(cache_link, timeout=25, verify=False)
        
        if r.status_code == 200:
            if "No hay caché" in r.text or "404. That’s an error" in r.text:
                print("      ⚠️  Página no cacheada por Google.")
                return ""
            return r.text
        elif r.status_code == 429:
            print("      ⛔ Google 429 (Too Many Requests).")
        else:
            print(f"      ❌ Status {r.status_code}")
            
    except Exception as e:
        print(f"      ❌ Error: {e}")
        
    return ""

# =========================
# PARSER ROBUSTO
# =========================

def detectar_cards(soup: BeautifulSoup):
    # En modo texto de Google Cache, las clases CSS a veces desaparecen o cambian.
    # Buscamos patrones estructurales.
    
    # 1. Buscamos contenedores que tengan precio y titulo
    cards = []
    
    # Estrategia: Buscar todos los divs que podrían ser productos
    # ECI suele usar estructuras repetitivas
    candidates = soup.find_all("div", recursive=True)
    
    for div in candidates:
        # Un producto suele tener un link y un texto con símbolo euro
        has_euro = "€" in div.get_text()
        has_link = div.find("a") is not None
        
        if has_euro and has_link:
            # Filtramos si es demasiado grande (header/footer) o muy pequeño
            txt_len = len(div.get_text())
            if 50 < txt_len < 1000:
                # Comprobación adicional: ¿Tiene palabras clave?
                if "GB" in div.get_text() or "RAM" in div.get_text() or "Pulgadas" in div.get_text():
                    cards.append(div)
                    
    # Si la búsqueda genérica falla, probamos selectores clásicos (por si la cache los conserva)
    if not cards:
        cards = soup.select('.product_tile') or soup.select('.grid-item') or soup.select('div.card')

    return cards

def extraer_info_card(card: BeautifulSoup) -> Tuple[str, str, float, float, str]:
    # En modo texto, la estructura es plana.
    
    # 1. Título y Enlace: Buscar el primer enlace con texto largo
    links = card.find_all("a")
    tit, href = "", ""
    for a in links:
        t = a.get_text(" ", strip=True)
        h = a.get("href") or ""
        # Validar que parece un producto
        if len(t) > 10 and ("movil" in h or "smartphone" in h):
            tit = t
            href = h
            break
            
    # 2. Precio: Buscar texto con €
    p_act, p_org = None, None
    text_content = card.get_text(" ", strip=True)
    
    # Regex para buscar precios: 1.234,99 €
    precios_matches = re.findall(r"(\d{1,3}(?:\.\d{3})*(?:,\d{1,2})?)\s*€", text_content)
    
    valid_prices = []
    for pm in precios_matches:
        v = parse_precio(pm)
        if v and v > 50: # Filtramos accesorios baratos
            valid_prices.append(v)
            
    if valid_prices:
        p_act = min(valid_prices)
        p_org = max(valid_prices)
        if p_act == p_org: p_org = round(p_act * 1.2, 2)
    
    # 3. Imagen (En modo texto strip=1 NO HAY IMAGENES, devolvemos placeholder o vacio)
    img_url = "" 
    # Si queremos imagen, tendríamos que no usar strip=1, pero eso arriesga bloqueo.
    # Priorizamos datos sobre imagen.

    return tit, href, p_act, p_org, img_url

def obtener_productos(url: str, etiqueta: str) -> List[ProductoECI]:
    html = fetch_google_cache(url)
    if not html: return []
    
    soup = BeautifulSoup(html, "html.parser")
    cards = detectar_cards(soup)
    
    if not cards:
        print(f"⚠️  Sin productos detectados en {etiqueta} (Google Cache).")
        return []

    productos = []
    seen_titles = set()
    
    for card in cards:
        tit, href, p_act, p_org, img = extraer_info_card(card)
        
        if not tit or not href: continue
        if tit in seen_titles: continue
        seen_titles.add(tit)
        
        t_clean = titulo_limpio(tit)
        specs = extraer_ram_rom(t_clean)
        
        if not specs: continue 
        
        ram, rom = specs
        nombre = extraer_nombre(t_clean, ram)
        
        if p_act is None: continue
        
        url_sin = limpiar_url_producto(href)
        # Validar URL
        if not url_sin.startswith("http"): continue
        
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
    print("--- FASE 1: ECI (ESTRATEGIA MARCAS + GOOGLE CACHE) ---", flush=True)
    
    total = 0
    # Usamos un subset para no tardar mucho si hay muchas marcas
    for i, url in enumerate(URLS_MARCAS, start=1):
        brand_name = url.split("/")[-2].upper()
        print(f"\n📂 Procesando Marca ({i}/{len(URLS_MARCAS)}): {brand_name}", flush=True)
        try:
            prods = obtener_productos(url, brand_name)
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
