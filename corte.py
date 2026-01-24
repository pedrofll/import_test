import os
import re
import time
import json
import random
import sys
from dataclasses import dataclass
from typing import List, Optional, Tuple
from urllib.parse import urlparse, urlunparse, urljoin
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

# ==============================================================================
# 1. MODELO DE DATOS
# ==============================================================================
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

# ==============================================================================
# 2. CONFIGURACIÓN
# ==============================================================================
BASE_URL = "https://www.elcorteingles.es"
BASE_CAT = "https://www.elcorteingles.es/electronica/moviles-y-smartphones/"
AFF_ELCORTEINGLES = os.environ.get("AFF_ELCORTEINGLES", "").strip()
ID_IMPORTACION = f"{BASE_URL}/electronica/moviles-y-smartphones/"

# ==============================================================================
# 3. HELPERS DE EXTRACCIÓN Y LIMPIEZA
# ==============================================================================
RE_GB = re.compile(r"(\d{1,3})\s*GB", re.IGNORECASE)
RE_RAM_PLUS = re.compile(r"(\d{1,3})\s*GB\s*\+\s*(\d{1,4})\s*GB", re.IGNORECASE)

def extraer_ram_rom(titulo: str) -> Optional[Tuple[str, str]]:
    m = RE_RAM_PLUS.search(titulo)
    if m: return f"{m.group(1)}GB", f"{m.group(2)}GB"
    gbs = RE_GB.findall(titulo)
    if len(gbs) >= 2: 
        vals = sorted([int(x) for x in gbs])
        return f"{vals[0]}GB", f"{vals[-1]}GB"
    return None

def titulo_limpio(titulo: str) -> str:
    t = (titulo or "").strip()
    t = re.sub(r"\bpatrocinado\b", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\bm[oó]vil\s+libre\b", "", t, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", t).strip()

def extraer_nombre(titulo: str, ram: str) -> str:
    ram_pat = re.escape(ram.replace("GB", "")) + r"\s*GB"
    m = re.search(ram_pat, titulo, flags=re.IGNORECASE)
    if m: return titulo[: m.start()].strip(" -–—,:;")
    return titulo

# ==============================================================================
# 4. PARSER DE CONTENIDO (DATA-JSON)
# ==============================================================================
def parse_productos_from_html(html: str, etiqueta: str) -> List[ProductoECI]:
    soup = BeautifulSoup(html, "html.parser")
    productos = []
    seen_ids = set()
    
    # ECI inyecta los datos en atributos data-json de los contenedores
    elements = soup.select('[data-json]')
    
    for el in elements:
        try:
            data = json.loads(el.get('data-json'))
            if "name" not in data or "price" not in data: continue
            
            pid = data.get("id", str(random.randint(1000, 9999)))
            if pid in seen_ids: continue
            seen_ids.add(pid)
            
            raw_name = data.get("name", "")
            t_clean = titulo_limpio(raw_name)
            specs = extraer_ram_rom(t_clean)
            if not specs: continue
            
            ram, rom = specs
            nombre_final = extraer_nombre(t_clean, ram)
            
            price_info = data.get("price", {})
            p_act = float(price_info.get("f_price", 0))
            p_org = float(price_info.get("o_price", 0))
            
            if p_act <= 0: continue
            if p_org <= p_act: p_org = round(p_act * 1.15, 2)
            
            url_raw = urljoin(BASE_URL, data.get("url", ""))
            url_con = f"{url_raw}?aff_id={AFF_ELCORTEINGLES}" if AFF_ELCORTEINGLES else url_raw

            productos.append(ProductoECI(
                nombre=nombre_final, memoria=ram, capacidad=rom, version="Global",
                precio_actual=p_act, precio_original=p_org, enviado_desde="España",
                origen_pagina=etiqueta, img=data.get("image", ""), url_imp=url_con, 
                url_exp=url_con, url_importada_sin_afiliado=url_raw, 
                url_sin_acortar_con_mi_afiliado=url_con, url_oferta=url_con, page_id=ID_IMPORTACION
            ))
        except: continue
    return productos

# ==============================================================================
# 5. MOTOR DE NAVEGACIÓN (FOX BYPASS)
# ==============================================================================
def main():
    print("--- 🦊 INICIANDO ESTRATEGIA FIREFOX BYPASS ---", flush=True)
    
    with sync_playwright() as p:
        # Usamos Firefox: suele tener una huella TLS distinta que a veces engaña a Akamai
        print("🚀 Lanzando navegador Firefox...", flush=True)
        browser = p.firefox.launch(headless=True)
        
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0",
            viewport={"width": 1920, "height": 1080},
            extra_http_headers={
                "Accept-Language": "es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3",
                "Referer": "https://www.google.es/"
            }
        )
        
        page = context.new_page()

        # URLs de la 1 a la 5
        urls = [BASE_CAT]
        for i in range(2, 6):
            urls.append(f"{BASE_CAT}{i}/")
        
        total_capturados = 0
        
        for i, url in enumerate(urls, start=1):
            print(f"\n🌍 Intentando acceder a Página {i}: {url}", flush=True)
            try:
                # Usamos domcontentloaded para no esperar a rastreadores/anuncios
                response = page.goto(url, timeout=60000, wait_until="domcontentloaded")
                
                if response and response.status != 200:
                    print(f"      ⚠️  Error HTTP {response.status}. Akamai bloqueó la petición.", flush=True)
                    continue

                # Pausa aleatoria "humana"
                time.sleep(random.uniform(5, 8))
                
                html = page.content()
                
                if "Access Denied" in html:
                    print("      ⛔ BLOQUEO: La página devolvió 'Access Denied'.", flush=True)
                    continue
                
                prods = parse_productos_from_html(html, str(i))
                
                if len(prods) > 0:
                    print(f"      ✅ ¡ÉXITO! Encontrados: {len(prods)} productos.", flush=True)
                    total_capturados += len(prods)
                    # Ejemplo rápido
                    print(f"      📱 Muestra: {prods[0].nombre} | {prods[0].precio_actual}€", flush=True)
                else:
                    print("      ⚠️  Página cargada pero no se detectaron productos JSON.", flush=True)
                    
            except Exception as e:
                print(f"      ❌ Error en página {i}: Timeout de red.", flush=True)
        
        browser.close()
        print(f"\n📋 ESCANEO FINALIZADO. TOTAL PRODUCTOS: {total_capturados}", flush=True)

if __name__ == "__main__":
    main()
