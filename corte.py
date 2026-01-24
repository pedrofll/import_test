"""
Scraper para El Corte Inglés — Móviles
ESTRATEGIA FINAL: Navegación Real con Playwright (Headless Chrome).
Simula un usuario real renderizando JavaScript para saltar protecciones.
"""

import os
import re
import time
import json
import random
from dataclasses import dataclass
from typing import List, Optional, Tuple
from urllib.parse import urlparse, urlunparse, urljoin, urlencode

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

# =========================
# CONFIGURACIÓN
# =========================

BASE_URL = "https://www.elcorteingles.es"
BASE_CAT = "https://www.elcorteingles.es/electronica/moviles-y-smartphones/"
AFF_ELCORTEINGLES = os.environ.get("AFF_ELCORTEINGLES", "").strip()

# Páginas 1 a 5 (Para no sobrecargar en la primera prueba)
URLS_OBJETIVO = [BASE_CAT]
for i in range(2, 6):
    URLS_OBJETIVO.append(f"{BASE_CAT}{i}/")

ID_IMPORTACION = f"{BASE_URL}/electronica/moviles-y-smartphones/"

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
    if m: return titulo[: m.start()].strip(" -–—,:;")
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
# LÓGICA DE EXTRACCIÓN (DATA-JSON)
# =========================

def parse_productos_from_html(html: str, etiqueta: str) -> List[ProductoECI]:
    soup = BeautifulSoup(html, "html.parser")
    productos = []
    seen_ids = set()

    # ECI guarda los datos limpios en atributos 'data-json' dentro de los divs de producto
    elements = soup.select('[data-json]')
    
    print(f"      🔍 Elementos JSON detectados en DOM: {len(elements)}")
    
    for el in elements:
        try:
            raw = el.get('data-json')
            if not raw: continue
            data = json.loads(raw)
            
            # Validaciones
            if "name" not in data or "price" not in data: continue
            
            pid = data.get("id", str(random.randint(10000,99999)))
            if pid in seen_ids: continue
            seen_ids.add(pid)
            
            # Nombre y Specs
            raw_name = data.get("name", "")
            t_clean = titulo_limpio(raw_name)
            specs = extraer_ram_rom(t_clean)
            if not specs: continue
            ram, rom = specs
            nombre_final = extraer_nombre(t_clean, ram)
            
            # Precio
            price_info = data.get("price")
            p_act = 0.0
            p_org = 0.0
            if isinstance(price_info, dict):
                p_act = float(price_info.get("f_price", 0))
                p_org = float(price_info.get("o_price", 0))
            
            if p_act <= 0: continue
            if p_org <= 0 or p_org < p_act: p_org = round(p_act * 1.2, 2)
            
            # URL
            url_suffix = data.get("url") or data.get("uri")
            if url_suffix:
                url_final = urljoin(BASE_URL, url_suffix)
            else:
                code = data.get("code_a") or pid
                url_final = f"https://www.elcorteingles.es/electronica/mp/{code}/"
            
            # Imagen
            img = data.get("image", "")

            # Objeto
            url_con = build_url_con_afiliado(url_final, AFF_ELCORTEINGLES)
            
            productos.append(ProductoECI(
                nombre=nombre_final, memoria=ram, capacidad=rom, version="Global",
                precio_actual=p_act, precio_original=p_org, enviado_desde="España",
                origen_pagina=etiqueta, img=img, url_imp=url_con, url_exp=url_con,
                url_importada_sin_afiliado=url_final, url_sin_acortar_con_mi_afiliado=url_con,
                url_oferta=url_con, page_id=ID_IMPORTACION
            ))

        except Exception:
            continue
            
    return productos

# =========================
# MAIN (PLAYWRIGHT)
# =========================

def main():
    print("--- FASE 1: ECI (NAVEGADOR REAL) ---", flush=True)
    
    with sync_playwright() as p:
        # Lanzamos navegador (Headless = True para servidor)
        print("🚀 Lanzando navegador Chromium...", flush=True)
        browser = p.chromium.launch(headless=True)
        
        # Contexto con User Agent real
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080}
        )
        page = context.new_page()
        
        total = 0
        
        for i, url in enumerate(URLS_OBJETIVO, start=1):
            print(f"\n📂 Navegando a Página {i}: {mask_url(url)}", flush=True)
            
            try:
                # Navegamos directamente a ECI (ya no usamos Google Cache)
                page.goto(url, timeout=60000, wait_until="domcontentloaded")
                
                # Esperamos un poco a que cargue el JS dinámico
                print("      ⏳ Esperando carga dinámica...", flush=True)
                time.sleep(5)
                
                # Intentamos hacer scroll para activar lazy loading
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(2)
                
                html = page.content()
                
                # Verificar bloqueo
                if "Access Denied" in html or "Human Verification" in html:
                    print("      ⛔ BLOQUEO DIRECTO DETECTADO.")
                    continue
                
                prods = parse_productos_from_html(html, str(i))
                
                print(f"      ✅ Encontrados: {len(prods)}", flush=True)
                total += len(prods)
                
                for p in prods:
                    print(f"      📱 {p.nombre} | {p.precio_actual}€")
                
            except Exception as e:
                print(f"      ❌ Error navegación: {e}", flush=True)
        
        browser.close()
        print(f"\n📋 TOTAL PRODUCTOS ESCANEADOS: {total}")

if __name__ == "__main__":
    main()
