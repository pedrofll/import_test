import os, re, time, json, random
from dataclasses import dataclass
from typing import List, Optional, Tuple
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

@dataclass
class ProductoECI:
    nombre: str; memoria: str; capacidad: str; version: str
    precio_actual: float; precio_original: float; enviado_desde: str
    origen_pagina: str; img: str; url_imp: str; url_exp: str
    url_importada_sin_afiliado: str; url_sin_acortar_con_mi_afiliado: str
    url_oferta: str; page_id: str

BASE_URL = "https://www.elcorteingles.es"
BASE_CAT = "https://www.elcorteingles.es/electronica/moviles-y-smartphones/"
AFF_ELCORTEINGLES = os.environ.get("AFF_ELCORTEINGLES", "").strip()

def extraer_specs(titulo: str) -> Tuple[str, str]:
    ram = re.search(r"(\d+)\s*GB\s*\+?\s*RAM", titulo, re.I) or re.search(r"RAM\s*(\d+)\s*GB", titulo, re.I)
    rom = re.search(r"(\d+)\s*GB(?!\s*RAM)", titulo, re.I)
    return (f"{ram.group(1)}GB" if ram else "N/A"), (f"{rom.group(1)}GB" if rom else "N/A")

def parse_productos_agresivo(html: str, etiqueta: str) -> List[ProductoECI]:
    productos = []
    # Intentamos encontrar el gran bloque JSON de la web
    json_match = re.search(r"window\.__PRELOADED_STATE__\s*=\s*({.*?});", html, re.DOTALL)
    
    if not json_match:
        # Intento 2: Buscar en cualquier script que contenga la palabra 'products'
        json_match = re.search(r"\"products\":\s*(\[.*?\]),", html, re.DOTALL)
    
    if json_match:
        try:
            raw_data = json.loads(json_match.group(1))
            # Navegamos por el JSON (la estructura de ECI suele ser profunda)
            items = []
            if "products" in raw_data: items = raw_data["products"]
            elif isinstance(raw_data, list): items = raw_data
            
            for item in items:
                name = item.get("name", "")
                ram, rom = extraer_specs(name)
                price = item.get("price", {})
                p_act = float(price.get("f_price") or price.get("final") or 0)
                p_org = float(price.get("o_price") or price.get("original") or p_act)
                
                url_raw = urljoin(BASE_URL, item.get("url", ""))
                url_con = f"{url_raw}?aff_id={AFF_ELCORTEINGLES}" if AFF_ELCORTEINGLES else url_raw
                
                productos.append(ProductoECI(
                    nombre=name, memoria=ram, capacidad=rom, version="Global",
                    precio_actual=p_act, precio_original=p_org, enviado_desde="España",
                    origen_pagina=etiqueta, img=item.get("image", ""), url_imp=url_con,
                    url_exp=url_con, url_importada_sin_afiliado=url_raw,
                    url_sin_acortar_con_mi_afiliado=url_con, url_oferta=url_con, page_id=BASE_CAT
                ))
        except: pass
    
    # Si el JSON falla, usamos el plan B: Selectores CSS clásicos
    if not productos:
        soup = BeautifulSoup(html, "html.parser")
        for card in soup.select(".product_tile, .product-preview, [data-json]"):
            name_el = card.select_one(".product_tile-title, .title")
            if not name_el: continue
            name = name_el.get_text(strip=True)
            ram, rom = extraer_specs(name)
            productos.append(ProductoECI(
                nombre=name, memoria=ram, capacidad=rom, version="Global",
                precio_actual=0.0, precio_original=0.0, enviado_desde="España",
                origen_pagina=etiqueta, img="", url_imp="", url_exp="",
                url_importada_sin_afiliado="", url_sin_acortar_con_mi_afiliado="",
                url_oferta="", page_id=BASE_CAT
            ))
            
    return productos

def main():
    print("--- 🦊 MODO INFILTRACIÓN: FIREFOX + JSON DEEP SEARCH ---", flush=True)
    with sync_playwright() as p:
        browser = p.firefox.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
            viewport={"width": 1280, "height": 800}
        )
        page = context.new_page()
        
        urls = [BASE_CAT] + [f"{BASE_CAT}{i}/" for i in range(2, 6)]
        total = 0

        for i, url in enumerate(urls, start=1):
            print(f"\n🌍 Página {i}: {url}", flush=True)
            try:
                # Bypass: Fingimos venir de Google para cada página
                page.set_extra_http_headers({"Referer": "https://www.google.es/search?q=el+corte+ingles+moviles"})
                response = page.goto(url, timeout=60000, wait_until="domcontentloaded")
                
                # Simular humano: Scroll y espera
                time.sleep(random.uniform(3, 6))
                page.mouse.wheel(0, 1500)
                time.sleep(2)
                
                html = page.content()
                if "Access Denied" in html:
                    print("      ⛔ Akamai nos ha detectado. Abortando misión.", flush=True)
                    break
                
                prods = parse_productos_agresivo(html, str(i))
                if prods:
                    print(f"      ✅ ¡ÉXITO! Encontrados {len(prods)} productos.", flush=True)
                    total += len(prods)
                    print(f"      📱 Ejemplo: {prods[0].nombre[:40]}... | {prods[0].precio_actual}€", flush=True)
                else:
                    print("      ⚠️ No se detectaron productos. Akamai está sirviendo una página vacía.", flush=True)
                
                # Pausa larga entre páginas para enfriar la IP
                time.sleep(random.uniform(10, 20))
                
            except Exception:
                print(f"      ❌ Timeout en Página {i}. La IP de GitHub está bajo fuego.", flush=True)
        
        browser.close()
        print(f"\n📋 ESCANEO FINALIZADO. Total: {total}", flush=True)

if __name__ == "__main__":
    main()
