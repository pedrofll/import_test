import os
import re
import json
import random
import time
from dataclasses import dataclass
from typing import List, Optional, Tuple
from curl_cffi import requests

# =========================
# CONFIGURACIÓN
# =========================
BASE_URL = "https://www.elcorteingles.es"
BASE_CAT = "https://www.elcorteingles.es/electronica/moviles-y-smartphones/"
AFF_ELCORTEINGLES = os.environ.get("AFF_ELCORTEINGLES", "").strip()

@dataclass
class ProductoECI:
    nombre: str
    precio: float
    url: str
    img: str

# =========================
# EXTRACCIÓN QUIRÚRGICA
# =========================
def parse_preloaded_state(html: str) -> List[ProductoECI]:
    productos = []
    try:
        # Buscamos el bloque de datos que ECI inyecta en la página
        match = re.search(r'window\.__PRELOADED_STATE__\s*=\s*({.*?});', html, re.DOTALL)
        if not match:
            return []
        
        data = json.loads(match.group(1))
        # Navegamos por el laberinto del JSON de ECI
        # La estructura suele ser: catalog -> category -> products
        products_list = data.get("catalog", {}).get("category", {}).get("products", [])
        
        for item in products_list:
            name = item.get("name", "")
            price_data = item.get("price", {})
            f_price = price_data.get("f_price") or price_data.get("final") or 0
            
            p_url = item.get("url", "")
            if p_url and not p_url.startswith("http"):
                p_url = f"{BASE_URL}{p_url}"
            
            # Añadimos afiliado
            if AFF_ELCORTEINGLES:
                sep = "&" if "?" in p_url else "?"
                p_url = f"{p_url}{sep}aff_id={AFF_ELCORTEINGLES}"

            img_url = ""
            images = item.get("images", [])
            if images:
                img_url = images[0].get("url", "")

            productos.append(ProductoECI(
                nombre=name,
                precio=float(f_price),
                url=p_url,
                img=img_url
            ))
    except Exception as e:
        print(f"      ⚠️ Error procesando JSON: {e}")
    
    return productos

# =========================
# LÓGICA PRINCIPAL
# =========================
def main():
    print("--- 📱 MODO QUIRÚRGICO (IOS IMPERSONATION) ---", flush=True)
    
    # Usamos un "Apretón de manos" de Safari en iOS
    session = requests.Session(impersonate="safari_ios")
    
    total = 0
    # Probamos las 3 primeras páginas
    for i in range(1, 4):
        url = BASE_CAT if i == 1 else f"{BASE_CAT}{i}/"
        print(f"\n📂 Accediendo a Página {i}...", flush=True)
        
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "es-ES,es;q=0.9",
            "Referer": "https://www.google.com/",
        }

        try:
            # Pausa aleatoria para no parecer un script
            time.sleep(random.uniform(2, 5))
            
            res = session.get(url, headers=headers, timeout=20)
            
            if res.status_code == 200:
                if "Access Denied" in res.text:
                    print("      ⛔ Akamai ha detectado el servidor de GitHub.")
                    continue
                
                prods = parse_preloaded_state(res.text)
                print(f"      ✅ Encontrados: {len(prods)} productos")
                
                for p in prods[:2]: # Muestra rápida
                    print(f"      📱 {p.nombre[:40]}... | {p.precio}€")
                
                total += len(prods)
            else:
                print(f"      ❌ Error HTTP {res.status_code}")
                
        except Exception as e:
            print(f"      ❌ Fallo de conexión: {e}")

    print(f"\n📋 TOTAL CAPTURADOS: {total}")

if __name__ == "__main__":
    main()
