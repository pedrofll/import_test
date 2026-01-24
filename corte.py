import os, re, time, json, random, requests
from dataclasses import dataclass
from typing import List, Optional, Tuple

# Intentamos usar curl_cffi para eludir el TLS Fingerprinting de Akamai
try:
    from curl_cffi import requests as crequests
    SESSION = crequests.Session(impersonate="chrome110")
    print("✅ curl_cffi cargado: Camuflaje de red activo.")
except ImportError:
    SESSION = requests.Session()
    print("⚠️ curl_cffi no detectado. Usando modo estándar (más riesgo de bloqueo).")

@dataclass
class ProductoECI:
    nombre: str; memoria: str; capacidad: str; version: str
    precio_actual: float; precio_original: float; enviado_desde: str
    origen_pagina: str; img: str; url_imp: str; url_exp: str
    url_importada_sin_afiliado: str; url_sin_acortar_con_mi_afiliado: str
    url_oferta: str; page_id: str

AFF_ELCORTEINGLES = os.environ.get("AFF_ELCORTEINGLES", "").strip()

def obtener_proxies():
    print("🌐 Buscando proxies frescos para cambiar de identidad...")
    try:
        # Obtenemos proxies gratuitos (HTTP/S)
        r = requests.get("https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all", timeout=10)
        if r.status_code == 200:
            p_list = r.text.strip().split("\r\n")
            random.shuffle(p_list)
            return p_list
    except: return []
    return []

def extraer_specs(titulo: str) -> Tuple[str, str]:
    ram = re.search(r"(\d+)\s*GB\s*\+?\s*RAM", titulo, re.I) or re.search(r"RAM\s*(\d+)\s*GB", titulo, re.I)
    rom = re.search(r"(\d+)\s*GB(?!\s*RAM)", titulo, re.I)
    return (f"{ram.group(1)}GB" if ram else "N/A"), (f"{rom.group(1)}GB" if rom else "N/A")

def fetch_api(url, proxy_list):
    # Intentamos con varios proxies hasta que uno nos deje pasar
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Referer": "https://www.elcorteingles.es/electronica/moviles-y-smartphones/"
    }
    
    for p in proxy_list[:20]: # Probamos 20 proxies
        try:
            print(f"   🔄 Probando con IP: {p}...", end="\r")
            proxies = {"http": f"http://{p}", "https": f"http://{p}"}
            res = SESSION.get(url, headers=headers, proxies=proxies, timeout=8)
            if res.status_code == 200:
                return res.json()
        except: continue
    return None

def main():
    print("--- 🚀 MODO API DIRECTA + PROXY ROTATOR ---", flush=True)
    
    proxy_list = obtener_proxies()
    if not proxy_list:
        print("❌ No se pudieron obtener proxies gratuitos. Abortando.")
        return

    total = 0
    # La API de ECI usa desplazamientos de 24 en 24 productos
    for i in range(0, 5): 
        offset = i * 24
        api_url = f"https://www.elcorteingles.es/api/catalog/v1/product/list?category=011.12781530031&limit=24&offset={offset}"
        
        print(f"\n📂 Petición API (Bloque {i+1})...", flush=True)
        data = fetch_api(api_url, proxy_list)
        
        if data and "products" in data:
            products = data["products"]
            print(f"      ✅ ¡ÉXITO! Recibidos {len(products)} productos.", flush=True)
            
            for item in products:
                name = item.get("name", "Móvil sin nombre")
                price = item.get("price", {})
                p_act = float(price.get("f_price") or 0)
                
                print(f"      📱 {name[:35]}... | {p_act}€", flush=True)
                total += 1
            
            time.sleep(random.uniform(2, 5))
        else:
            print("      ❌ Akamai ha bloqueado todos los proxies intentados para este bloque.", flush=True)

    print(f"\n📋 ESCANEO FINALIZADO. Total capturados: {total}", flush=True)

if __name__ == "__main__":
    main()
