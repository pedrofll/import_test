"""
Scraper para El Corte Inglés — Móviles
ESTRATEGIA: Paginación (1-10) + Extracción Híbrida (JSON + Atributos DOM).
Más robusto: Busca datos tanto en scripts ocultos como en etiquetas HTML.
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

# Ignorar advertencias SSL
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

BASE_URL = "https://www.elcorteingles.es"
BASE_CAT = "https://www.elcorteingles.es/electronica/moviles-y-smartphones/"
AFF_ELCORTEINGLES = os.environ.get("AFF_ELCORTEINGLES", "").strip()

# Generamos las URLs de la 1 a la 10
URLS_OBJETIVO = [BASE_CAT]  # Página 1
for i in range(2, 11):      # Páginas 2 a 10
    URLS_OBJETIVO.append(f"{BASE_CAT}{i}/")

ID_IMPORTACION = f"{BASE_URL}/electronica/moviles-y-smartphones/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
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
# REGEX & LIMPIEZA
# =========================
RE_GB = re.compile(r"(\d{1,3})\s*GB", re.IGNORECASE)
RE_RAM_PLUS = re.compile(r"(\d{1,3})\s*GB\s*\+\s*(\d{1,4})\s*GB", re.IGNORECASE)
RE_12GB_512GB = re.compile(r"(\d{1,3})\s*GB\s*[+xX]\s*(\d{1,4})\s*GB", re.IGNORECASE)
RE_COMPACT_8_256 = re.compile(r"\b(\d{1,2})\s*\+\s*(\d{2,4})\s*GB\b", re.IGNORECASE)
RE_PATROCINADO = re.compile(r"\bpatrocinado\b", re.IGNORECASE)
RE_MOBILE_LIBRE = re.compile(r"\bm[oó]vil\s+libre\b", re.IGNORECASE)

def titulo_limpio(titulo: str) -> str:
    t = (titulo or "").strip()
    t = RE_PATROCINADO.sub("", t)
    t = RE_MOBILE_LIBRE.sub("", t)
    return re.sub(r"\s+", " ", t).strip()

def extraer_ram_rom(titulo: str) -> Optional[Tuple[str, str]]:
    m = RE_RAM_PLUS.search(titulo)
    if m: return f"{m.group(1)}GB", f"{m.group(2)}GB"
    m = RE_12GB_512GB.search(titulo)
    if m: return f"{m.group(1)}GB", f"{m.group(2)}GB"
    m = RE_COMPACT_8_256.search(titulo)
    if m: return f"{m.group(1)}GB", f"{m.group(2)}GB"
    gbs = RE_GB.findall(titulo)
    if len(gbs) >= 2: 
        vals = sorted([int(x) for x in gbs])
        return f"{vals[0]}GB", f"{vals[-1]}GB"
    return None

def extraer_nombre(titulo: str, ram: str) -> str:
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

def mask_url(u: str) -> str:
    try:
        p = urlparse(u)
        return urlunparse((p.scheme, p.netloc, p.path, "", "", ""))
    except: return u

# =========================
# GOOGLE CACHE FETCHER
# =========================
def fetch_via_google_cache(url: str) -> str:
    session = requests.Session(impersonate="chrome110") if USAR_CURL_CFFI else requests.Session()
    session.headers.update(HEADERS)
    
    # strip=0 (Mantiene HTML completo), vwsrc=0 (Vista renderizada)
    clean_url = url.split("?")[0]
    cache_link = f"http://webcache.googleusercontent.com/search?q=cache:{urllib.parse.quote(clean_url)}&strip=0&vwsrc=0"
    
    print(f"   ☁️  Google Cache: {mask_url(url)}")
    try:
        time.sleep(random.uniform(2, 5))
        r = session.get(cache_link, timeout=25, verify=False)
        if r.status_code == 200:
            if "404." in r.text and "That’s an error" in r.text:
                print("      ⚠️  Página no disponible en caché.")
                return ""
            return r.text
        elif r.status_code == 429:
            print("      ⛔ Google 429 (Too Many Requests).")
        else:
            print(f"      ❌ Error Google: {r.status_code}")
    except Exception as e:
        print(f"      ❌ Excepción: {e}")
    return ""

# =========================
# LÓGICA DE EXTRACCIÓN HÍBRIDA
# =========================

def parse_json_object(json_str: str) -> Optional[dict]:
    try:
        return json.loads(json_str)
    except:
        return None

def extraer_desde_dom(soup: BeautifulSoup) -> List[dict]:
    """Busca atributos data-json en el HTML (Método más fiable en ECI)."""
    raw_data = []
    
    # ECI suele poner el JSON en un atributo 'data-json' dentro de div.product_tile o similar
    # Buscamos cualquier elemento que tenga data-json
    elements = soup.select('[data-json]')
    
    print(f"      🔍 Elementos DOM con data-json: {len(elements)}")
    
    for el in elements:
        j_str = el.get('data-json')
        if j_str:
            d = parse_json_object(j_str)
            if d: raw_data.append(d)
            
    return raw_data

def extraer_desde_script(html: str) -> List[dict]:
    """Busca patrones JSON dentro de scripts."""
    raw_data = []
    
    # Regex relajada: Busca {"id":...,"name":...} sin importar el orden exacto
    # Capturamos bloques que parecen productos
    pattern = re.compile(r'(\{[\s\S]*?"brand"[\s\S]*?"price"[\s\S]*?\})')
    matches = pattern.findall(html)
    
    print(f"      🔍 Bloques JSON en scripts: {len(matches)}")
    
    for m in matches:
        # Limpieza básica
        clean = m.strip().rstrip(',;')
        d = parse_json_object(clean)
        if d: raw_data.append(d)
        
    return raw_data

def procesar_pagina(html: str, etiqueta: str) -> List[ProductoECI]:
    soup = BeautifulSoup(html, "html.parser")
    
    # 1. Intentar extracción DOM (Más limpia)
    data_list = extraer_desde_dom(soup)
    
    # 2. Si falla, extracción bruta de Scripts
    if not data_list:
        data_list = extraer_desde_script(html)
    
    productos = []
    seen_ids = set()
    
    for data in data_list:
        # Validación mínima
        if "name" not in data: continue
        
        # ID único
        pid = data.get("id", str(random.randint(10000,99999)))
        if pid in seen_ids: continue
        seen_ids.add(pid)
        
        # Datos básicos
        raw_name = data.get("name", "")
        t_clean = titulo_limpio(raw_name)
        
        # Filtro: Solo móviles (evitar fundas, accesorios)
        # Si no tiene GB en el nombre, probablemente no sea un móvil válido
        specs = extraer_ram_rom(t_clean)
        if not specs: continue
        ram, rom = specs
        
        nombre_final = extraer_nombre(t_clean, ram)
        
        # Precios (Manejo de estructuras anidadas)
        # A veces data['price'] es un dict, a veces un float directo
        price_info = data.get("price")
        p_act = 0.0
        p_org = 0.0
        
        if isinstance(price_info, dict):
            p_act = float(price_info.get("f_price", 0))
            p_org = float(price_info.get("o_price", 0))
        elif isinstance(price_info, (int, float)):
            p_act = float(price_info)
            
        if p_act <= 0: continue
        if p_org <= 0 or p_org < p_act: p_org = round(p_act * 1.2, 2)
        
        # URL
        url_suffix = data.get("url") or data.get("uri")
        if url_suffix and url_suffix.startswith("/"):
            url_final = BASE_URL + url_suffix
        else:
            # Reconstrucción si falta URL
            code = data.get("code_a") or pid
            url_final = f"https://www.elcorteingles.es/electronica/mp/{code}/"

        # Imagen
        img = data.get("image", "")
        
        # Objeto Final
        url_con_aff = build_url_con_afiliado(url_final, AFF_ELCORTEINGLES)
        
        productos.append(ProductoECI(
            nombre=nombre_final,
            memoria=ram,
            capacidad=rom,
            version="Global",
            precio_actual=p_act,
            precio_original=p_org,
            enviado_desde="España",
            origen_pagina=etiqueta,
            img=img,
            url_imp=url_con_aff,
            url_exp=url_con_aff,
            url_importada_sin_afiliado=url_final,
            url_sin_acortar_con_mi_afiliado=url_con_aff,
            url_oferta=url_con_aff,
            page_id=ID_IMPORTACION
        ))
        
    return productos

def main() -> int:
    print("--- FASE 1: ECI (PAGINACIÓN + EXTRACCIÓN HÍBRIDA) ---", flush=True)
    
    total = 0
    for i, url in enumerate(URLS_OBJETIVO, start=1):
        print(f"\n📂 Procesando Página {i}: {mask_url(url)}", flush=True)
        try:
            html = fetch_via_google_cache(url)
            if not html:
                print("      ⏩ Saltando (Sin HTML).")
                continue
            
            # DEBUG: Comprobamos si nos ha bloqueado Google
            if "Robot" in html or "captcha" in html.lower():
                 print("      ⛔ DETECTADO BLOQUEO DE GOOGLE (Captcha).")
                 continue
                 
            prods = procesar_pagina(html, str(i))
            
            if not prods:
                soup = BeautifulSoup(html, "html.parser")
                titulo = soup.title.string.strip() if soup.title else "Sin Título"
                print(f"      ⚠️  0 productos. Título página: '{titulo}'")
                print(f"      ℹ️  Longitud HTML: {len(html)} caracteres.")

            print(f"      ✅ Encontrados: {len(prods)}")
            total += len(prods)
            
            for p in prods:
                print(f"      📱 {p.nombre} | {p.precio_actual}€")
                
        except Exception as e:
            print(f"      ❌ Error crítico: {e}", flush=True)
            
    print(f"\n📋 TOTAL PRODUCTOS ESCANEADOS: {total}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
