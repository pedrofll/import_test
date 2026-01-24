"""
Scraper para El Corte Inglés — Móviles
ESTRATEGIA DEFINITIVA: Extracción de JSON oculto en Google Cache.
Ignora el HTML visual y extrae directamente los datos crudos del script.
"""

import os
import re
import time
import json
import random
import urllib.parse
from dataclasses import dataclass
from typing import List, Optional, Tuple
from urllib.parse import urljoin, urlparse, parse_qsl, urlencode, urlunparse
from bs4 import BeautifulSoup
import warnings

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
RE_MOBILE_LIBRE = re.compile(r"\bm[oó]vil\s+libre\b", re.IGNORECASE)

def titulo_limpio(titulo: str) -> str:
    t = (titulo or "").strip()
    t = RE_PATROCINADO.sub("", t)
    t = RE_MOBILE_LIBRE.sub("", t) # Eliminamos "móvil libre" que sale mucho en el JSON
    return re.sub(r"\s+", " ", t).strip()

def extraer_ram_rom(titulo: str) -> Optional[Tuple[str, str]]:
    # Primero buscamos combinaciones explícitas "8GB + 256GB"
    m = RE_RAM_PLUS.search(titulo)
    if m: return f"{m.group(1)}GB", f"{m.group(2)}GB"
    
    m = RE_12GB_512GB.search(titulo)
    if m: return f"{m.group(1)}GB", f"{m.group(2)}GB"
    
    m = RE_COMPACT_8_256.search(titulo)
    if m: return f"{m.group(1)}GB", f"{m.group(2)}GB"

    # Si no, buscamos 2 menciones de GB separadas
    gbs = RE_GB.findall(titulo)
    if len(gbs) >= 2: 
        # Asumimos que el menor es RAM y el mayor es ROM
        vals = sorted([int(x) for x in gbs])
        return f"{vals[0]}GB", f"{vals[-1]}GB"
        
    # Si solo hay uno, suele ser ROM. Asumimos RAM estándar si es alta gama? 
    # Mejor ser conservador: si solo hay 1 dato, devolvemos None o intentamos adivinar.
    if len(gbs) == 1:
        # A veces el título solo dice "iPhone 15 128GB". Asumimos que es ROM.
        # RAM es difícil de adivinar sin contexto. Lo dejamos pasar.
        return None

    return None

def extraer_nombre(titulo: str, ram: str) -> str:
    # Quitamos la parte técnica del nombre
    ram_pat = re.escape(ram.replace("GB", "")) + r"\s*GB"
    m = re.search(ram_pat, titulo, flags=re.IGNORECASE)
    if m:
        base = titulo[: m.start()].strip(" -–—,:;")
        return base
    return titulo

def build_url_con_afiliado(url_sin: str, aff: str) -> str:
    if not url_sin or not aff: return url_sin
    sep = "&" if "?" in url_sin else "?"
    if re.fullmatch(r"\d+", aff): return f"{url_sin}{sep}aff_id={aff}"
    return f"{url_sin}{sep}{aff.lstrip('?&')}"

# =========================
# FETCHER
# =========================

def fetch_google_cache(url: str) -> str:
    """Obtiene la caché (Modo Texto) de Google."""
    session = requests.Session(impersonate="chrome110") if USAR_CURL_CFFI else requests.Session()
    session.headers.update(HEADERS)
    
    clean_url = url.split("?")[0]
    # strip=0 para mantener scripts donde pueda estar el JSON
    # vwsrc=0 para ver el renderizado
    cache_link = f"http://webcache.googleusercontent.com/search?q=cache:{urllib.parse.quote(clean_url)}&strip=0&vwsrc=0"
    
    print(f"   👻 Cache: {mask_url(url)}")
    try:
        time.sleep(random.uniform(2, 5))
        r = session.get(cache_link, timeout=25, verify=False)
        if r.status_code == 200:
            return r.text
        elif r.status_code == 404:
            print("      ⚠️  Página no cacheada (404).")
    except Exception as e:
        print(f"      ❌ Error: {e}")
    return ""

# =========================
# PARSER JSON (NUEVO)
# =========================

def extraer_productos_json(html: str, etiqueta: str) -> List[ProductoECI]:
    productos = []
    
    # Buscamos patrones JSON que contengan datos de productos
    # El ejemplo que diste: {"brand":"Samsung", ... "name":"...", "price":{...}}
    
    # Regex para capturar objetos JSON que parezcan productos de ECI
    # Buscamos bloques que tengan "gtin" y "price"
    # Esta regex es aproximada para sacar bloques {} dentro de arrays []
    
    print("      🔍 Buscando datos JSON en el HTML...")
    
    # 1. Intentamos encontrar el script de dataLayer o similar
    # A menudo ECI pone esto en un var data = [...] o similar.
    # Vamos a buscar todas las ocurrencias de objetos JSON válidos.
    
    # Estrategia: Buscar strings que empiecen por {"brand" y terminen en }
    # Usamos finditer para recorrer todo el texto
    
    regex_prod = re.compile(r'\{"brand":"[^"]+".*?"price":\{.*?\}.*?\}', re.DOTALL)
    matches = regex_prod.findall(html)
    
    print(f"      🧩 Encontrados {len(matches)} fragmentos JSON potenciales.")
    
    seen_ids = set()

    for match_str in matches:
        try:
            # A veces el regex captura comas extra al final, intentamos limpiar
            clean_str = match_str.strip().rstrip(",")
            data = json.loads(clean_str)
            
            # Validar que es un producto válido
            if "name" not in data or "price" not in data: continue
            
            # Extraer ID
            pid = data.get("id", "")
            if pid in seen_ids: continue
            seen_ids.add(pid)
            
            # 1. Título
            raw_name = data.get("name", "")
            t_clean = titulo_limpio(raw_name)
            
            # 2. Specs
            specs = extraer_ram_rom(t_clean)
            if not specs: continue
            ram, rom = specs
            
            # 3. Nombre limpio
            nombre_final = extraer_nombre(t_clean, ram)
            
            # 4. Precio
            price_data = data.get("price", {})
            p_act = float(price_data.get("f_price", 0))
            p_org = float(price_data.get("o_price", 0))
            
            if p_act <= 0: continue
            if p_org <= 0 or p_org < p_act: p_org = round(p_act * 1.2, 2)
            
            # 5. URL e Imagen
            # El JSON que pasaste no tiene URL explícita ("url"), hay que construirla o buscarla
            # A veces viene en "url" o "uri". Si no está, construimos una dummy basada en ID para debug
            # O buscamos si hay otro campo.
            
            # Construcción URL ECI: /electronica/moviles-y-smartphones/A12345678-nombre-slug/
            # Como no tenemos el slug fácil, usamos el ID para buscarlo luego o generamos link de búsqueda
            # TRUCO: ECI permite buscar por ID: https://www.elcorteingles.es/electronica/moviles-y-smartphones/?f=id::{ID}
            # O la url canonica si estuviera.
            
            url_producto = ""
            # Intentamos buscar la URL en el objeto si existe (a veces ECI la pone)
            if "url" in data: url_producto = data["url"]
            elif "uri" in data: url_producto = data["uri"]
            
            # Si viene relativa
            if url_producto and url_producto.startswith("/"):
                url_producto = BASE_URL + url_producto
                
            # Si no hay URL, construimos una funcional usando el ID
            if not url_producto and pid:
                # ECI suele usar códigos A...
                code_a = data.get("code_a", "")
                if code_a:
                     # https://www.elcorteingles.es/electronica/A56390869/
                     url_producto = f"https://www.elcorteingles.es/electronica/mp/{code_a}/"
                else:
                     # Fallback
                     url_producto = f"https://www.elcorteingles.es/buscar/?term={pid}"

            # Imagen: data['media']['count'] indica que hay fotos, pero no la URL directa en ese snippet.
            # A veces ECI pone "image_url". Si no está, usamos placeholder.
            img_url = data.get("image", "")
            if not img_url and "media" in data:
                 # Hack: Construir url imagen ECI basada en ID si conocemos el patrón
                 # Patrón habitual: https://sgfm.elcorteingles.es/SGFM/dctm/MEDIA03/202401/15/001057063613046_1__600x600.jpg
                 # Es complejo adivinar. Dejamos vacío o placeholder.
                 img_url = ""

            # Añadir a la lista
            url_con_aff = build_url_con_afiliado(url_producto, AFF_ELCORTEINGLES)
            
            productos.append(ProductoECI(
                nombre=nombre_final,
                memoria=ram,
                capacidad=rom,
                version="Global",
                precio_actual=p_act,
                precio_original=p_org,
                enviado_desde="España",
                origen_pagina=etiqueta,
                img=img_url,
                url_imp=url_con_aff,
                url_exp=url_con_aff,
                url_importada_sin_afiliado=url_producto,
                url_sin_acortar_con_mi_afiliado=url_con_aff,
                url_oferta=url_con_aff,
                page_id=ID_IMPORTACION
            ))

        except Exception as e:
            # print(f"Error parseando bloque JSON: {e}")
            continue
            
    return productos

def main() -> int:
    print("--- FASE 1: ECI (EXTRACCIÓN JSON GOOGLE CACHE) ---", flush=True)
    
    total = 0
    for i, url in enumerate(URLS_MARCAS, start=1):
        brand_name = url.split("/")[-2].upper()
        print(f"\n📂 Procesando Marca ({i}/{len(URLS_MARCAS)}): {brand_name}", flush=True)
        try:
            html = fetch_google_cache(url)
            if not html: continue
            
            prods = extraer_productos_json(html, brand_name)
            
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
            print(f"3) Precio: {p.precio_actual}€ (Antes {p.precio_original})")
            print(f"4) URL: {mask_url(p.url_importada_sin_afiliado)}")
            print("-" * 60, flush=True)
            
    print(f"\n📋 TOTAL PRODUCTOS ESCANEADOS: {total}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
