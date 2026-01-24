import os
import re
import time
import json
import random
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
# 2. CONFIGURACIÓN Y CONSTANTES
# ==============================================================================
BASE_URL = "https://www.elcorteingles.es"
BASE_CAT = "https://www.elcorteingles.es/electronica/moviles-y-smartphones/"
AFF_ELCORTEINGLES = os.environ.get("AFF_ELCORTEINGLES", "").strip()
ID_IMPORTACION = f"{BASE_URL}/electronica/moviles-y-smartphones/"

# ==============================================================================
# 3. HELPERS DE LIMPIEZA Y EXTRACCIÓN
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
# 4. PARSER DE HTML (EXTRACCIÓN JSON)
# ==============================================================================
def parse_productos_from_html(html: str, etiqueta: str) -> List[ProductoECI]:
    soup = BeautifulSoup(html, "html.parser")
    productos = []
    seen_ids = set()
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
# 5. LÓGICA PRINCIPAL (PLAYWRIGHT)
# ==============================================================================
def main():
    print("--- 🛡️ INICIANDO SCRAPER CORTE INGLÉS (MODO BLINDADO) ---", flush=True)
    
    with sync_playwright() as p:
        # Bypass HTTP/2 para evitar ERR_HTTP2_PROTOCOL_ERROR
        browser = p.chromium.launch(headless=True, args=[
            "--disable-http2", 
            "--disable-blink-features=AutomationControlled"
        ])
        
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080}
        )
        
        page = context.new_page()
        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        # Generar URLs del 1 al 10
        urls = [BASE_CAT]
        for i in range(2, 11):
            urls.append(f"{BASE_CAT}{i}/")
        
        total_general = 0
        
        for i, url in enumerate(urls, start=1):
            print(f"\n🚀 Procesando Página {i}: {url}", flush=True)
            try:
                # Usamos domcontentloaded para saltar el bloqueo de carga de Akamai
                page.goto(url, timeout=45000, wait_until="domcontentloaded")
                time.sleep(5) # Espera manual para renderizado mínimo
                
                html = page.content()
                
                if "Access Denied" in html:
                    print(f"      ⛔ BLOQUEO: Akamai ha rechazado la IP de GitHub.", flush=True)
                    continue
                
                prods = parse_productos_from_html(html, str(i))
                print(f"      ✅ Encontrados: {len(prods)} productos", flush=True)
                total_general += len(prods)
                
                for p in prods[:3]: # Log de muestra
                    print(f"      📱 {p.nombre} - {p.precio_actual}€", flush=True)
                    
            except Exception as e:
                print(f"      ❌ Error en página {i}: Timeout o bloqueo de red.", flush=True)
        
        browser.close()
        print(f"\n📋 ESCANEO FINALIZADO. Total productos: {total_general}", flush=True)

if __name__ == "__main__":
    main()
