import os
import re
import json
import random
import time
import urllib.parse
from dataclasses import dataclass
from typing import List
from curl_cffi import requests
from bs4 import BeautifulSoup

@dataclass
class ProductoECI:
    nombre: str
    precio: float
    url: str

# =========================
# EXTRACCIÓN DE DATOS
# =========================
def extraer_datos_eci(html: str) -> List[ProductoECI]:
    productos = []
    soup = BeautifulSoup(html, "html.parser")
    
    # Buscamos elementos con data-json
    items = soup.select('[data-json]')
    
    for item in items:
        try:
            js = json.loads(item.get('data-json'))
            if 'name' in js and 'price' in js:
                price_data = js['price']
                p_actual = price_data.get('f_price') or price_data.get('final') or 0
                
                productos.append(ProductoECI(
                    nombre=js['name'],
                    precio=float(p_actual),
                    url=js.get('url', '')
                ))
        except:
            continue
    return productos

# =========================
# NAVEGACIÓN VÍA GOOGLE CACHE
# =========================
def main():
    print("--- 🛰️ MODO PUENTE: GOOGLE CACHE BYPASS ---", flush=True)
    
    session = requests.Session(impersonate="chrome110")
    
    base_cat = "https://www.elcorteingles.es/electronica/moviles-y-smartphones/"
    total = 0

    # URLs a procesar
    urls_reales = [base_cat, f"{base_cat}2/", f"{base_cat}3/"]

    for i, url_real in enumerate(urls_reales, start=1):
        # Construimos la URL de la caché de Google
        cache_url = f"http://webcache.googleusercontent.com/search?q=cache:{urllib.parse.quote(url_real)}&strip=0"
        
        print(f"\n📂 Consultando Caché de Página {i}...", flush=True)
        
        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "accept-language": "es-ES,es;q=0.9",
            "referer": "https://www.google.com/"
        }

        try:
            # Pausa aleatoria para no saturar a Google
            time.sleep(random.uniform(5, 10))
            
            res = session.get(cache_url, headers=headers, timeout=30)
            
            if res.status_code == 200:
                # A veces Google nos pide un Captcha si abusamos
                if "detected unusual traffic" in res.text:
                    print("      ⚠️ Google Cache ha detectado tráfico inusual. Pausando...")
                    break
                
                prods = extraer_datos_eci(res.text)
                
                if prods:
                    print(f"      ✅ Éxito: {len(prods)} productos recuperados de la caché.")
                    for p in prods[:2]:
                        print(f"      📱 {p.nombre[:40]}... | {p.precio}€")
                    total += len(prods)
                else:
                    print("      ⚠️ No se encontraron productos en la caché (posible error de renderizado).")
            elif res.status_code == 404:
                print("      ❌ Esta página no está en la caché de Google todavía.")
            else:
                print(f"      ❌ Error en Google Cache: {res.status_code}")
                
        except Exception as e:
            print(f"      ❌ Error de conexión: {e}")

    print(f"\n📋 PROCESO FINALIZADO. Total recuperado: {total}")

if __name__ == "__main__":
    main()
