import os
import re
import json
import random
import time
from dataclasses import dataclass
from typing import List, Optional
from curl_cffi import requests
from bs4 import BeautifulSoup

@dataclass
class ProductoECI:
    nombre: str
    precio: float
    url: str

# =========================
# EL BUSCADOR DE TESOROS (PARSER)
# =========================
def buscar_datos_en_html(html: str) -> List[ProductoECI]:
    productos = []
    soup = BeautifulSoup(html, "html.parser")
    
    # Intento 1: Buscar en TODOS los scripts de la página
    scripts = soup.find_all("script")
    for script in scripts:
        content = script.string
        if content and ('"products"' in content or '"f_price"' in content):
            try:
                # Limpiamos el contenido para intentar encontrar un JSON válido dentro
                # Buscamos el primer '[' y el último ']' que parezca una lista de productos
                match = re.search(r'\[\s*\{.*"id":.*\}\s*\]', content, re.DOTALL)
                if match:
                    data = json.loads(match.group(0))
                    for item in data:
                        if isinstance(item, dict) and "name" in item:
                            productos.append(ProductoECI(
                                nombre=item.get("name", ""),
                                precio=float(item.get("price", {}).get("f_price", 0)),
                                url=item.get("url", "")
                            ))
            except:
                continue

    # Intento 2: Si el JSON falla, leemos el HTML directamente (Selectores 2024)
    if not productos:
        # ECI suele usar clases como 'product_tile' o 'product-preview'
        cards = soup.select('div[class*="product_tile"], div[class*="product-preview"]')
        for card in cards:
            try:
                name_el = card.select_one('a[data-list-type="product_list"]') or card.select_one('.title')
                price_el = card.select_one('.price.final') or card.select_one('span[class*="current"]')
                
                if name_el and price_el:
                    # Limpiamos el precio (quitar €, puntos y comas)
                    raw_price = re.sub(r'[^\d,]', '', price_el.get_text())
                    final_price = float(raw_price.replace(',', '.'))
                    
                    productos.append(ProductoECI(
                        nombre=name_el.get_text(strip=True),
                        precio=final_price,
                        url=name_el.get('href', '')
                    ))
            except:
                continue

    return productos

# =========================
# NAVEGACIÓN
# =========================
def main():
    print("--- 🔍 MODO RASTREADOR UNIVERSAL ---", flush=True)
    
    # Camuflaje Safari iOS (muy efectivo actualmente)
    session = requests.Session(impersonate="safari_ios")
    
    base_cat = "https://www.elcorteingles.es/electronica/moviles-y-smartphones/"
    total = 0

    for i in range(1, 3): # Probamos las 2 primeras para testear
        url = base_cat if i == 1 else f"{base_cat}{i}/"
        print(f"\n📂 Analizando Página {i}...", flush=True)
        
        headers = {
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": "https://www.google.com/"
        }

        try:
            time.sleep(random.uniform(3, 6))
            res = session.get(url, headers=headers, timeout=25)
            
            if res.status_code == 200:
                if "Access Denied" in res.text:
                    print("      ⛔ Bloqueado por Akamai (IP quemada).")
                    break
                
                prods = buscar_datos_en_html(res.text)
                
                if prods:
                    print(f"      ✅ ¡Éxito! Encontrados: {len(prods)} productos")
                    for p in prods[:2]:
                        print(f"      📱 {p.nombre[:35]}... | {p.precio}€")
                    total += len(prods)
                else:
                    print("      ⚠️ No se detectaron productos. Revisando estructura...")
                    # Debug: imprimimos los primeros 200 caracteres para ver si es una página real
                    print(f"      📄 Inicio del HTML: {res.text[:150].strip()}...")
            else:
                print(f"      ❌ Error HTTP {res.status_code}")
                
        except Exception as e:
            print(f"      ❌ Error: {e}")

    print(f"\n📋 RESULTADO FINAL: {total} productos.")

if __name__ == "__main__":
    main()
