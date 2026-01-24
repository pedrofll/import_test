"""
Scraper para El Corte Inglés — Móviles
ESTRATEGIA: Paginación Numérica (1-10) + Extracción de JSON oculto.
Usa Google Cache para evitar bloqueos y extrae los datos crudos del código fuente.
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
import warnings

# Ignorar advertencias de certificados SSL
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
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# =========================
# MODELO DE DATOS
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
# EXPRESIONES REGULARES (REGEX)
# =========================
RE_GB = re.compile(r"(\d{1,3})\s*GB", re.IGNORECASE)
RE_RAM_PLUS = re.compile(r"(\d{1,3})\s*GB\s*\+\s*(\d{1,4})\s*GB", re.IGNORECASE)
RE_12GB_512GB = re.compile(r"(\d{1,3})\s*GB\s*[+xX]\s*(\d{1,4})\s*GB", re.IGNORECASE)
RE_COMPACT_8_256 = re.compile(r"\b(\d{1,2})\s*\+\s*(\d{2,4})\s*GB\b", re.IGNORECASE)
RE_PATROCINADO = re.compile(r"\bpatrocinado\b", re.IGNORECASE)
RE_MOBILE_LIBRE = re.compile(r"\bm[oó]vil\s+libre\b", re.IGNORECASE)

# Regex para extraer el JSON del producto del código fuente
# Busca patrones como: {"brand":"Samsung", ... "price":{...}}
RE_JSON_PRODUCT = re.compile(r'\{"brand":"[^"]+".*?"price":\{.*?\}.*?\}', re.DOTALL)

# =========================
# FUNCIONES DE LIMPIEZA
# =========================

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
# SISTEMA DE DESCARGA (GOOGLE CACHE)
# =========================

def fetch_via_google_cache(url: str) -> str:
    """Solicita la URL a través de la caché de Google (Modo Texto) para evitar bloqueo IP."""
    session = requests.Session(impersonate="chrome110") if USAR_CURL_CFFI else requests.Session()
    session.headers.update(HEADERS)
    
    # strip=0: Mantenemos el código fuente (scripts) para extraer el JSON
    # vwsrc=0: Vista de código fuente raw
    clean_url = url.split("?")[0]
    cache_link = f"http://webcache.googleusercontent.com/search?q=cache:{urllib.parse.quote(clean_url)}&strip=0&vwsrc=0"
    
    print(f"   ☁️  Google Cache: {mask_url(url)}")
    try:
        time.sleep(random.uniform(2, 5)) # Pausa de cortesía
        r = session.get(cache_link, timeout=25, verify=False)
        
        if r.status_code == 200:
            if "404. That’s an error" in r.text:
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
# EXTRACCIÓN Y PROCESAMIENTO
# =========================

def procesar_pagina(html: str, etiqueta: str) -> List[ProductoECI]:
    productos = []
    
    # Buscamos bloques de texto que parezcan el JSON de productos
    # Formato esperado: {"brand":"...", ... "price":{...}}
    matches = RE_JSON_PRODUCT.findall(html)
    print(f"      🔍 Detectados {len(matches)} bloques de datos JSON.")
    
    seen_ids = set()
    
    for match_str in matches:
        try:
            # Limpiamos posibles comas finales que rompen el JSON
            clean_json = match_str.strip().rstrip(",")
            data = json.loads(clean_json)
            
            # Validación mínima
            if "name" not in data or "price" not in data: continue
            
            # Evitar duplicados en la misma página
            pid = data.get("id", "")
            if pid in seen_ids: continue
            seen_ids.add(pid)
            
            # 1. Extraer Título y Specs
            raw_name = data.get("name", "")
            t_clean = titulo_limpio(raw_name)
            
            specs = extraer_ram_rom(t_clean)
            if not specs: continue # Si no tiene RAM/ROM claras, saltamos
            ram, rom = specs
            
            nombre_final = extraer_nombre(t_clean, ram)
            
            # 2. Extraer Precio
            price_data = data.get("price", {})
            # A veces viene como 'f_price' (final price) o 'o_price' (original)
            p_act = float(price_data.get("f_price", 0))
            p_org = float(price_data.get("o_price", 0))
            
            if p_act <= 0: continue
            if p_org <= 0 or p_org < p_act: 
                p_org = round(p_act * 1.2, 2) # Simulamos precio original si no existe
            
            # 3. Construir URL
            # El JSON a veces no trae la URL completa. Usamos el code_a o id si es necesario
            code_a = data.get("code_a", "")
            if "url" in data:
                url_producto = urljoin(BASE_URL, data["url"])
            elif code_a:
                # Construcción fallback: https://www.elcorteingles.es/electronica/A1234567/
                url_producto = f"https://www.elcorteingles.es/electronica/mp/{code_a}/"
            else:
                # Fallback final: búsqueda por ID
                url_producto = f"https://www.elcorteingles.es/buscar/?term={pid}"
            
            # 4. Imagen
            img_url = data.get("image", "")
            # Si no hay imagen directa, a veces está en media count. Dejamos vacío si no hay URL clara.
            
            # 5. Generar objeto
            url_con_aff = build_url_con_afiliado(url_producto, AFF_ELCORTEINGLES)
            
            p = ProductoECI(
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
            )
            productos.append(p)
            
        except json.JSONDecodeError:
            continue
        except Exception:
            continue
            
    return productos

def main() -> int:
    print("--- FASE 1: ECI (PAGINACIÓN 1-10 + JSON) ---", flush=True)
    
    total = 0
    for i, url in enumerate(URLS_OBJETIVO, start=1):
        print(f"\n📂 Procesando Página {i} de 10...", flush=True)
        
        try:
            html = fetch_via_google_cache(url)
            if not html: 
                print("      ⏩ Saltando página (sin datos HTML).")
                continue
                
            prods = procesar_pagina(html, str(i))
            
            if not prods:
                print("      ⚠️  No se encontraron productos en el JSON.")
            
            print(f"      ✅ Encontrados: {len(prods)}")
            total += len(prods)
            
            for p in prods:
                print("-" * 60)
                print(f"Detectado {p.nombre}")
                print(f"RAM: {p.memoria} | ROM: {p.capacidad}")
                print(f"Precio: {p.precio_actual}€")
                print(f"URL: {mask_url(p.url_importada_sin_afiliado)}")
                print("-" * 60, flush=True)
                
        except Exception as e:
            print(f"      ❌ Error en página {i}: {e}", flush=True)
            
    print(f"\n📋 TOTAL PRODUCTOS ESCANEADOS: {total}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
