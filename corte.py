import os
import re
import time
import json
import random
import sys # Añadido para forzar salida
from dataclasses import dataclass
from typing import List, Optional, Tuple
from urllib.parse import urlparse, urlunparse, urljoin
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

# =========================
# CONFIGURACIÓN Y MODELO
# =========================
BASE_URL = "https://www.elcorteingles.es"
AFF_ELCORTEINGLES = os.environ.get("AFF_ELCORTEINGLES", "").strip()
ID_IMPORTACION = f"{BASE_URL}/electronica/moviles-y-smartphones/"

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
# HELPERS DE EXTRACCIÓN
# =========================
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
            specs = extraer_ram_rom(raw_name)
            if not specs: continue
            
            price_info = data.get("price", {})
            p_act = float(price_info.get("f_price", 0))
            p_org = float(price_info.get("o_price", 0))
            if p_act <= 0: continue
            if p_org <= p_act: p_org = round(p_act * 1.15, 2)
            
            url_raw = urljoin(BASE_URL, data.get("url", ""))
            url_con = f"{url_raw}?aff_id={AFF_ELCORTEINGLES}" if AFF_ELCORTEINGLES else url_raw

            productos.append(ProductoECI(
                nombre=raw_name, memoria=specs[0], capacidad=specs[1], version="Global",
                precio_actual=p_act, precio_original=p_org, enviado_desde="España",
                origen_pagina=etiqueta, img=data.get("image", ""), url_imp=url_con, 
                url_exp=url_con, url_importada_sin_afiliado=url_raw, 
                url_sin_acortar_con_mi_afiliado=url_con, url_oferta=url_con, page_id=ID_IMPORTACION
            ))
        except: continue
    return productos

# =========================
# MAIN CON PLAYWRIGHT
# =========================
def main():
    # El flush=True es VITAL para ver logs en vivo en GitHub Actions
    print("--- INICIANDO SCRIPT CORTE.PY ---", flush=True)
    
    with sync_playwright() as p:
        print("🚀 Lanzando navegador Chromium...", flush=True)
        
        # Bypass de HTTP/2 y camuflaje
        browser = p.chromium.launch(
            headless=True, 
            args=["--disable-http2", "--disable-blink-features=AutomationControlled"]
        )
        
        print("👤 Creando contexto de usuario...", flush=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 720}
        )
        
        page = context.new_page()
        
        urls = ["https://www.elcorteingles.es/electronica/moviles-y-smartphones/"]
        for i in range(2, 6):
            urls.append(f"https://www.elcorteingles.es/electronica/moviles-y-smartphones/{i}/")
        
        total = 0
        for i, url in enumerate(urls, start=1):
            print(f"\n📂 Intentando conectar a Página {i}...", flush=True)
            try:
                # Aumentamos timeout a 90s por si Akamai es lento
                page.goto(url, timeout=90000, wait_until="networkidle")
                
                print(f"      ⏳ Esperando renderizado de Página {i}...", flush=True)
                time.sleep(random.uniform(5, 10)) 
                
                html = page.content()
                
                if "Access Denied" in html:
                    print("      ⛔ BLOQUEADO: Akamai ha rechazado la IP de GitHub.", flush=True)
                    continue
                
                if "verify you are human" in html.lower():
                    print("      🧩 CAPTCHA: Se requiere verificación humana.", flush=True)
                    continue
                    
                prods = parse_productos_from_html(html, str(i))
                print(f"      ✅ Encontrados: {len(prods)} productos", flush=True)
                total += len(prods)
                
                for p in prods[:2]:
                    print(f"      📱 {p.nombre} | {p.precio_actual}€", flush=True)
                    
            except Exception as e:
                print(f"      ❌ Error en página {i}: {str(e)[:100]}", flush=True)
        
        browser.close()
        print(f"\n📋 PROCESO FINALIZADO. TOTAL: {total}", flush=True)

if __name__ == "__main__":
    main()
