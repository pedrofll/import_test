import os
import json
import time
import random
from dataclasses import dataclass
from typing import List
from curl_cffi import requests

@dataclass
class ProductoECI:
    nombre: str
    precio: float
    url: str
    marca: str

# El ID de la categoría "Smartphones" en ECI es fijo
CATEGORY_ID = "011.12781530031"

def main():
    print("--- 🎯 EXTRACCIÓN DIRECTA POR API (BYPASS AKAMAI) ---", flush=True)
    
    # Impersonate Chrome 120: Es el camuflaje más avanzado disponible
    session = requests.Session(impersonate="chrome120")
    
    total = 0
    # Cada página en ECI tiene 24 productos. 
    # Página 1 (offset 0), Página 2 (offset 24), etc.
    for page_num in range(1, 4): # Probamos las 3 primeras páginas
        offset = (page_num - 1) * 24
        
        # Esta es la URL de la "sangre" de la web: su API interna de catálogo
        api_url = f"https://www.elcorteingles.es/api/catalog/v1/product/list?category={CATEGORY_ID}&limit=24&offset={offset}"
        
        print(f"\n📂 Consultando API - Página {page_num} (Offset {offset})...", flush=True)
        
        headers = {
            "Accept": "application/json",
            "Accept-Language": "es-ES,es;q=0.9",
            "Referer": "https://www.elcorteingles.es/electronica/moviles-y-smartphones/",
            "X-Requested-With": "XMLHttpRequest"
        }

        try:
            # Pausa de seguridad para no levantar sospechas
            time.sleep(random.uniform(3, 6))
            
            response = session.get(api_url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                products = data.get("products", [])
                
                if not products:
                    print("      ⚠️ La API respondió pero no envió productos.")
                    continue
                
                print(f"      ✅ ¡ÉXITO! Recibidos {len(products)} productos.")
                
                for item in products:
                    name = item.get("name", "Desconocido")
                    # El precio final está en price -> f_price
                    price_data = item.get("price", {})
                    price = price_data.get("f_price") or price_data.get("final") or 0
                    
                    url = "https://www.elcorteingles.es" + item.get("url", "")
                    marca = item.get("brand", "Genérica")
                    
                    print(f"      📱 [{marca}] {name[:40]}... | {price}€")
                    total += 1
            else:
                print(f"      ❌ Bloqueo de API: Error {response.status_code}")
                # Si nos da 403, Akamai ha detectado la IP de GitHub Actions
                if response.status_code == 403:
                    print("      🚨 IP de GitHub bloqueada. Necesitamos un Proxy o ScraperAPI.")
                    break
                    
        except Exception as e:
            print(f"      ❌ Error en la conexión: {e}")

    print(f"\n📋 RESULTADO FINAL: {total} productos capturados.")

if __name__ == "__main__":
    main()
