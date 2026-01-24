import os
import re
import json
import random
import time
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
    
    # Buscamos elementos con data-json, que es donde ECI suele esconder los datos
    items = soup.select('[data-json]')
    
    for item in items:
        try:
            js = json.loads(item.get('data-json'))
            if 'name' in js and 'price' in js:
                # El precio puede venir en f_price o final
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
# NAVEGACIÓN Y BYPASS
# =========================
def main():
    print("--- 🛡️ FORZANDO HTTP/1.1 (SINTAXIS CORREGIDA) ---", flush=True)
    
    # En curl_cffi, http_version=1 activa HTTP/1.1
    session = requests.Session(
        impersonate="chrome110",
        http_version=1 
    )
    
    base_cat = "https://www.elcorteingles.es/electronica/moviles-y-smartphones/"
    total = 0

    # Probamos las 3 primeras URLs (sin scroll, carga directa)
    urls = [base_cat, f"{base_cat}2/", f"{base_cat}3/"]

    for i, url in enumerate(urls, start=1):
        print(f"\n📂 Cargando Página {i}: {url}", flush=True)
        
        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "accept-language": "es-ES,es;q=0.9",
            "referer": "https://www.google.com/",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }

        try:
            # Pausa de seguridad
            time.sleep(random.uniform(3, 6))
            
            res = session.get(url, headers=headers, timeout=30)
            
            if res.status_code == 200:
                if "Access Denied" in res.text:
                    print("      ⛔ Akamai ha bloqueado la IP (Access Denied).")
                    continue
                
                prods = extraer_datos_eci(res.text)
                
                if prods:
                    print(f"      ✅ Encontrados: {len(prods)} productos.")
                    for p in prods[:2]: # Muestra rápida
                        print(f"      📱 {p.nombre[:40]}... | {p.precio}€")
                    total += len(prods)
                else:
                    print("      ⚠️ No se detectaron productos en el HTML.")
                    # Guardamos una muestra para debug si falla
                    if len(res.text) > 200:
                        print(f"      📄 Título página: {res.text.split('<title>')[1].split('</title>')[0]}")
            else:
                print(f"      ❌ Error HTTP {res.status_code}")
                
        except Exception as e:
            print(f"      ❌ Error de conexión: {e}")

    print(f"\n📋 PROCESO FINALIZADO. Total acumulado: {total}")

if __name__ == "__main__":
    main()
