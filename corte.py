import os
import re
import time
import json
import random
import requests
from dataclasses import dataclass
from typing import List, Optional, Tuple

# Intentamos usar curl_cffi para eludir el bloqueo de Akamai (TLS Fingerprinting)
try:
    from curl_cffi import requests as crequests
    SESSION = crequests.Session(impersonate="chrome110")
    print("✅ Motor de camuflaje (curl_cffi) activo.", flush=True)
except ImportError:
    SESSION = requests.Session()
    print("⚠️ Usando motor estándar (requests). Instala curl_cffi para mayor éxito.", flush=True)

# ==============================================================================
# 1. MODELO DE DATOS
# ==============================================================================
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

# ==============================================================================
# 2. CONFIGURACIÓN
# ==============================================================================
BASE_URL = "https://www.elcorteingles.es"
AFF_ELCORTEINGLES = os.environ.get("AFF_ELCORTEINGLES", "").strip()
# ID de categoría para móviles en ECI
CATEGORY_ID = "011.12781530031" 

# ==============================================================================
# 3. HELPERS
# ==============================================================================
def extraer_specs(titulo: str) -> Tuple[str, str]:
    ram = re.search(r"(\d+)\s*GB\s*\+?\s*RAM", titulo, re.I) or re.search(r"RAM\s*(\d+)\s*GB", titulo, re.I)
    rom = re.search(r"(\d+)\s*GB(?!\s*RAM)", titulo, re.I)
    return (f"{ram.group(1)}GB" if ram else "N/A"), (f"{rom.group(1)}GB" if rom else "N/A")

def obtener_proxies():
    print("🌐 Buscando nuevas identidades (proxies)...", flush=True)
    try:
        # Obtenemos lista de proxies gratuitos de Proxyscrape
        r = requests.get("https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all", timeout=10)
        if r.status_code == 200:
            p_list = [p.strip() for p in r.text.split('\n') if p.strip()]
            random.shuffle(p_list)
            return p_list
    except Exception as e:
        print(f"❌ Error al obtener proxies: {e}", flush=True)
        return []
    return []

# ==============================================================================
# 4. MOTOR DE CONSULTA API
# ==============================================================================
def fetch_api(url, proxy_list):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Referer": "https://www.elcorteingles.es/electronica/moviles-y-smartphones/",
        "Origin": "https://www.elcorteingles.es"
    }
    
    # Probamos con los proxies hasta que uno funcione
    intentos_proxy = proxy_list[:15] # No probamos todos para no eternizarnos
    for p in intentos_proxy:
        try:
            proxies = {"http": f"http://{p}", "https": f"http://{p}"}
            print(f"   🔄 Probando con IP: {p}...", end="\r", flush=True)
            res = SESSION.get(url, headers=headers, proxies=proxies, timeout=10)
            if res.status_code == 200:
                return res.json()
        except:
            continue
    return None

# ==============================================================================
# 5. LOGICA PRINCIPAL
# ==============================================================================
def main():
    print("--- 🚀 INICIANDO SCRAPER DE API + PROXIES ---", flush=True)
    
    proxy_list = obtener_proxies()
    if not proxy_list:
        print("⚠️ No se pudieron cargar proxies. Intentando conexión directa (riesgo alto)...", flush=True)

    total_productos = 0
    # Escaneamos los primeros 5 bloques (24 productos por bloque = 120 móviles)
    for i in range(0, 5):
        offset = i * 24
        api_url = f"https://www.elcorteingles.es/api/catalog/v1/product/list?category={CATEGORY_ID}&limit=24&offset={offset}"
        
        print(f"\n📂 Consultando Bloque {i+1} (Offset {offset})...", flush=True)
        data = fetch_api(api_url, proxy_list)
        
        if data and "products" in data:
            products = data["products"]
            print(f"      ✅ Recibidos {len(products)} productos.", flush=True)
            
            for item in products:
                name = item.get("name", "Móvil")
                price_info = item.get("price", {})
                
                # Extraer precios
                p_act = float(price_info.get("f_price") or 0)
                p_org = float(price_info.get("o_price") or p_act)
                if p_org <= p_act: p_org = round(p_act * 1.2, 2)
                
                # Extraer RAM/ROM
                ram, rom = extraer_specs(name)
                
                # URL y Afiliado
                url_raw = f"{BASE_URL}{item.get('url')}"
                url_con = f"{url_raw}?aff_id={AFF_ELCORTEINGLES}" if AFF_ELCORTEINGLES else url_raw
                
                # Imagen
                img_data = item.get("images", [{}])
                img_url = img_data[0].get("url") if img_data else ""

                print(f"      📱 {name[:40]}... | {p_act}€", flush=True)
                total_productos += 1
                
            # Pausa humana entre bloques
            time.sleep(random.uniform(3, 7))
        else:
            print(f"      ❌ No se pudo obtener el bloque {i+1}. Akamai bloqueó los proxies.", flush=True)

    print(f"\n📋 PROCESO TERMINADO.", flush=True)
    print(f"--- TOTAL PRODUCTOS CAPTURADOS: {total_productos} ---", flush=True)

if __name__ == "__main__":
    main()
