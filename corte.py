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

def buscar_productos_agresivo(html: str) -> List[ProductoECI]:
    productos = []
    
    # ESTRATEGIA A: Buscar bloques JSON-LD (Lo que Google lee para SEO)
    # Patrón: <script type="application/ld+json"> ... </script>
    scripts_ld = re.findall(r'type="application/ld\+json">({.+?})</script>', html, re.DOTALL)
    
    for script_text in scripts_ld:
        try:
            data = json.loads(script_text)
            # A veces es un solo objeto o una lista de objetos
            items_list = data.get("itemListElement", []) if isinstance(data, dict) else []
            
            for item in items_list:
                # ECI suele meter los productos aquí
                prod_info = item.get("item", {})
                if prod_info:
                    name = prod_info.get("name")
                    # El precio suele estar en "offers"
                    offers = prod_info.get("offers", {})
                    price = offers.get("price") or offers.get("lowPrice")
                    
                    if name and price:
                        productos.append(ProductoECI(
                            nombre=name,
                            precio=float(price),
                            url=prod_info.get("url", "")
                        ))
        except:
            continue

    # ESTRATEGIA B: Si falla, buscamos el bloque "PRELOADED_STATE" pero con Regex flexible
    if not productos:
        # Buscamos cualquier patrón "name":"..." seguido de "price":...
        # Esta es la "fuerza bruta"
        raw_matches = re.findall(r'"name":"([^"]+)"[^}]+?"f_price":([\d\.]+)', html)
        for name, price in raw_matches:
            if name not in [p.nombre for p in productos]: # Evitar duplicados
                productos.append(ProductoECI(nombre=name, precio=float(price), url=""))

    return productos

def main():
    print("--- 🎯 MODO FRANCOTIRADOR: EXTRACCIÓN POR SEO-JSON ---", flush=True)
    
    session = requests.Session(impersonate="chrome110")
    base_cat = "https://www.elcorteingles.es/electronica/moviles-y-smartphones/"
    total = 0

    urls_reales = [base_cat, f"{base_cat}2/", f"{base_cat}3/"]

    for i, url_real in enumerate(urls_reales, start=1):
        # Probamos la caché de Google
        cache_url = f"http://webcache.googleusercontent.com/search?q=cache:{urllib.parse.quote(url_real)}"
        
        print(f"\n📂 Analizando Página {i}...", flush=True)
        
        try:
            time.sleep(random.uniform(4, 7))
            # Añadimos cabeceras de "humano"
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept-Language": "es-ES,es;q=0.9"
            }
            res = session.get(cache_url, headers=headers, timeout=25)
            
            if res.status_code == 200:
                prods = buscar_productos_agresivo(res.text)
                
                if prods:
                    print(f"      ✅ ¡CAPTURA COMPLETADA! {len(prods)} productos encontrados.")
                    for p in prods[:3]:
                        print(f"      📱 {p.nombre[:45]}... -> {p.precio}€")
                    total += len(prods)
                else:
                    print("      ⚠️ Los datos siguen ocultos. Akamai/Google están filtrando los scripts.")
                    # Verificamos si al menos hay HTML real
                    if len(res.text) > 5000:
                        print(f"      ℹ️ El HTML pesa {len(res.text)} bytes. Hay contenido, pero no el JSON esperado.")
            else:
                print(f"      ❌ Error HTTP {res.status_code}")
                
        except Exception as e:
            print(f"      ❌ Error: {e}")

    print(f"\n📋 RESULTADO FINAL: {total} productos.")
    return total

if __name__ == "__main__":
    main()
