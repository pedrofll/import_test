import os
import re
import json
import random
import time
import urllib.parse
from dataclasses import dataclass
from typing import List
from curl_cffi import requests

@dataclass
class ProductoECI:
    nombre: str
    precio: float
    url: str

# =========================
# ESCANEO PROFUNDO (REGEX)
# =========================
def buscar_productos_en_texto(html: str) -> List[ProductoECI]:
    productos = []
    
    # Buscamos el bloque de datos maestro de la página
    # ECI suele inyectar un JSON gigante aquí
    data_match = re.search(r'__PRELOADED_STATE__\s*=\s*({.+?});', html)
    
    if data_match:
        try:
            full_data = json.loads(data_match.group(1))
            # Navegamos por el laberinto del JSON (Catalog -> Products)
            items = []
            # Intentamos varias rutas porque Google Cache a veces las cambia
            catalog = full_data.get("catalog", {})
            items = catalog.get("category", {}).get("products", []) or catalog.get("search", {}).get("products", [])
            
            for item in items:
                p_actual = item.get("price", {}).get("f_price") or item.get("price", {}).get("final")
                if p_actual:
                    productos.append(ProductoECI(
                        nombre=item.get("name", "Móvil"),
                        precio=float(p_actual),
                        url=item.get("url", "")
                    ))
        except:
            pass

    # Si el bloque maestro falla, buscamos fragmentos de data-json sueltos
    if not productos:
        # Buscamos cualquier cosa que parezca un JSON de producto: {"name":"...", "price":...}
        fragments = re.findall(r'data-json="({.+?})"', html)
        for frag in fragments:
            try:
                # El HTML de la caché tiene las comillas escapadas (&quot;)
                clean_frag = frag.replace('&quot;', '"')
                js = json.loads(clean_frag)
                productos.append(ProductoECI(
                    nombre=js.get('name', 'Móvil'),
                    precio=float(js.get('price', {}).get('f_price', 0)),
                    url=js.get('url', '')
                ))
            except:
                continue
                
    return productos

# =========================
# EJECUCIÓN
# =========================
def main():
    print("--- 🔍 MODO DEEP SCAN: BUSCANDO DATOS OCULTOS ---", flush=True)
    
    session = requests.Session(impersonate="chrome110")
    base_cat = "https://www.elcorteingles.es/electronica/moviles-y-smartphones/"
    total = 0

    urls_reales = [base_cat, f"{base_cat}2/", f"{base_cat}3/"]

    for i, url_real in enumerate(urls_reales, start=1):
        # Usamos la versión de Google Cache (Modo normal para mantener scripts)
        cache_url = f"http://webcache.googleusercontent.com/search?q=cache:{urllib.parse.quote(url_real)}"
        
        print(f"\n📂 Analizando Página {i}...", flush=True)
        
        try:
            time.sleep(random.uniform(5, 8))
            res = session.get(cache_url, timeout=30)
            
            if res.status_code == 200:
                # Comprobación de seguridad
                if "Google" in res.text and "cache" in res.text.lower():
                    prods = buscar_productos_en_texto(res.text)
                    
                    if prods:
                        print(f"      ✅ ¡ENCONTRADOS! {len(prods)} productos.")
                        for p in prods[:2]:
                            print(f"      📱 {p.nombre[:40]}... | {p.precio}€")
                        total += len(prods)
                    else:
                        print("      ⚠️ Google devolvió la página pero no veo el bloque de datos.")
                        # DIAGNÓSTICO: ¿Qué tipo de página estamos viendo?
                        if "captcha" in res.text.lower(): print("      🚨 Detectado Captcha de Google.")
                        elif "moviles" in res.text.lower(): print("      ℹ️ Veo la palabra 'móviles', pero el JSON está ausente.")
                else:
                    print("      ❌ Google no devolvió una página de caché válida.")
            else:
                print(f"      ❌ Error HTTP {res.status_code}")
                
        except Exception as e:
            print(f"      ❌ Error: {e}")

    print(f"\n📋 ESCANEO FINALIZADO. Total recuperado: {total}")

if __name__ == "__main__":
    main()
