import os, re, time, json, random, requests
from dataclasses import dataclass
from typing import List, Optional, Tuple

# Intentamos usar curl_cffi para saltar el TLS Fingerprinting de Akamai
try:
    from curl_cffi import requests as crequests
    SESSION = crequests.Session(impersonate="chrome110")
except ImportError:
    SESSION = requests.Session()

@dataclass
class ProductoECI:
    nombre: str; memoria: str; capacidad: str; version: str
    precio_actual: float; precio_original: float; enviado_desde: str
    origen_pagina: str; img: str; url_imp: str; url_exp: str
    url_importada_sin_afiliado: str; url_sin_acortar_con_mi_afiliado: str
    url_oferta: str; page_id: str

AFF_ELCORTEINGLES = os.environ.get("AFF_ELCORTEINGLES", "").strip()

def obtener_proxies_frescos():
    print("🌐 Buscando proxies limpios para saltar el bloqueo...")
    try:
        # Bajamos una lista de proxies HTTP/S
        r = requests.get("https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all", timeout=10)
        if r.status_code == 200:
            proxies = r.text.strip().split("\r\n")
            random.shuffle(proxies)
            return proxies
    except: return []
    return []

def extraer_specs(titulo: str) -> Tuple[str, str]:
    ram = re.search(r"(\d+)\s*GB\s*\+?\s*RAM", titulo, re.I) or re.search(r"RAM\s*(\d+)\s*GB", titulo, re.I)
    rom = re.search(r"(\d+)\s*GB(?!\s*RAM)", titulo, re.I)
    return (f"{ram.group(1)}GB" if ram else "N/A"), (f"{rom.group(1)}GB" if rom else "N/A")

def fetch_api(url, proxies):
    # Intentamos con varios proxies hasta que uno no dé Timeout
    for p in proxies[:15]: # Probamos los 15 primeros
        try:
            print(f"   🔄 Probando con Proxy: {p}...", end="\r")
            proxy_dict = {"http": f"http://{p}", "https": f"http://{p}"}
            # Cabeceras que usa la web real
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json",
                "Referer": "https://www.elcorteingles.es/electronica/moviles-y-smartphones/"
            }
            res = SESSION.get(url, headers=headers, proxies=proxy_dict, timeout=10)
            if res.status_code == 200:
                return res.text
        except: continue
    return None

def main():
    print("--- 🚀 MODO API DIRECTA + PROXY ROTATOR ---", flush=True)
    proxies = obtener_proxies_frescos()
    if not proxies:
        print("❌ No se pudieron obtener proxies. Abortando.")
        return

    total = 0
    # La API de ECI usa un sistema de 'limit' y 'offset'
    for i in range(0, 5): # Primeras 5 páginas (24 productos por página)
        offset = i * 24
        api_url = f"https://www.elcorteingles.es/api/catalog/v1/product/list?category=011.12781530031&limit=24&offset={offset}"
        
        print(f"\n📂 Petición API (Productos {offset} al {offset+24})...", flush=True)
        raw_json = fetch_api(api_url, proxies)
        
        if raw_json:
            try:
                data = json.loads(raw_json)
                products = data.get("products", [])
                print(f"      ✅ ¡ÉXITO! Recibidos {len(products)} productos.", flush=True)
                
                for item in products:
                    name = item.get("name", "")
                    ram, rom = extraer_specs(name)
                    price = item.get("price", {})
                    p_act = float(price.get("f_price") or 0)
                    
                    print(f"      📱 {name[:35]}... | {p_act}€", flush=True)
                    total += 1
                
                # Pausa para no quemar el proxy
                time.sleep(2)
            except:
                print("      ⚠️ Error al procesar el JSON de la API.", flush=True)
        else:
            print("      ❌ Akamai bloqueó todos los proxies probados para esta página.", flush=True)

    print(f"\n📋 ESCANEO FINALIZADO. Total: {total}", flush=True)

if __name__ == "__main__":
    main()
