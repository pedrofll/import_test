import os
import re
import json
import random
import time
from dataclasses import dataclass
from typing import List, Tuple
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
    # Buscamos el JSON de productos que ECI inyecta en la página
    # Suele estar en una variable llamada 'window.__PRELOADED_STATE__' o en data-json
    
    # Intento 1: Atributos data-json (Muy común en sus listados)
    soup = BeautifulSoup(html, "html.parser")
    items = soup.select('[data-json]')
    
    for item in items:
        try:
            js = json.loads(item.get('data-json'))
            if 'name' in js and 'price' in js:
                p_actual = js['price'].get('f_price') or js['price'].get('final') or 0
                productos.append(ProductoECI(
                    nombre=js['name'],
                    precio=float(p_actual),
                    url=js.get('url', '')
                ))
        except:
            continue

    # Intento 2: Búsqueda por Regex si el DOM está vacío
    if not productos:
        matches = re.findall(r'\{"id":"[^"]+","name":"([^"]+)","price":\{"f_price":([\d\.]+)', html)
        for name, price in matches:
            productos.append(ProductoECI(nombre=name, precio=float(price), url=""))

    return productos

# =========================
# NAVEGACIÓN Y BYPASS
# =========================
def main():
    print("--- 🛠️ CORRIGIENDO PROTOCOLO (FORZANDO HTTP/1.1) ---", flush=True)
    
    # IMPORTANTE: Forzamos la versión de HTTP para evitar el Error 92
    session = requests.Session(
        impersonate="chrome110",
        http_version=requests.HttpVersion.v1_1
    )
    
    base_cat = "https://www.elcorteingles.es/electronica/moviles-y-smartphones/"
    total = 0

    # URLs numeradas (1 a 5 para probar)
    urls = [base_cat] + [f"{base_cat}{i}/" for i in range(2, 6)]

    for i, url in enumerate(urls, start=1):
        print(f"\n📂 Cargando Página {i}: {url}", flush=True)
        
        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "accept-language": "es-ES,es;q=0.9",
            "cache-control": "no-cache",
            "pragma": "no-cache",
            "referer": "https://www.google.com/"
        }

        try:
            # Pausa humana aleatoria
            time.sleep(random.uniform(4, 8))
            
            res = session.get(url, headers=headers, timeout=30)
            
            if res.status_code == 200:
                html_content = res.text
                if "Access Denied" in html_content:
                    print("      ⛔ Akamai detectó el bot (Access Denied).")
                    continue
                
                prods = extraer_datos_eci(html_content)
                
                if prods:
                    print(f"      ✅ Éxito: {len(prods)} productos encontrados.")
                    for p in prods[:2]:
                        print(f"      📱 {p.nombre[:40]}... | {p.precio}€")
                    total += len(prods)
                else:
                    print("      ⚠️ Página cargada pero sin datos. Posible cambio de estructura.")
            else:
                print(f"      ❌ Error HTTP {res.status_code}")
                
        except Exception as e:
            print(f"      ❌ Error de conexión: {e}")

    print(f"\n📋 ESCANEO FINALIZADO. Total: {total} productos.")

if __name__ == "__main__":
    main()
