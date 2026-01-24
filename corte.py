"""
Scraper de Diagnóstico para El Corte Inglés
OBJETIVO: Ver qué demonios nos devuelve Google Cache (HTML, JSON o Error).
"""

import re
import time
import random
import urllib.parse
from bs4 import BeautifulSoup
import warnings

warnings.filterwarnings("ignore")

try:
    from curl_cffi import requests
    print("✅ curl_cffi cargado correctamente (Modo Camuflaje).")
    USAR_CURL_CFFI = True
except ImportError:
    import requests
    print("⚠️ curl_cffi no instalado. Usando requests estándar.")
    USAR_CURL_CFFI = False

# =========================
# CONFIGURACIÓN
# =========================

# Probamos la Página 1 (que suele estar cacheada) y la Página 2 (que suele fallar)
URLS_PRUEBA = [
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/",
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/2/"
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

def mask_url(u: str) -> str:
    try:
        p = urllib.parse.urlparse(u)
        return urllib.parse.urlunparse((p.scheme, p.netloc, p.path, "", "", ""))
    except: return u

def analizar_html(html: str, fuente: str):
    print(f"\n   🔬 ANALIZANDO CONTENIDO DE {fuente}...")
    
    if not html:
        print("      ❌ El HTML está vacío.")
        return

    soup = BeautifulSoup(html, "html.parser")
    
    # 1. Título de la página
    titulo = soup.title.string.strip() if soup.title else "SIN TÍTULO"
    print(f"      🏷️  Título: '{titulo}'")
    print(f"      📏 Longitud: {len(html)} caracteres")
    
    # 2. Detección de errores comunes
    if "404" in titulo or "No hay caché" in html:
        print("      ⛔ DIAGNÓSTICO: Página no encontrada en Google Cache (404).")
        return
    if "robot" in html.lower() or "captcha" in html.lower():
        print("      ⛔ DIAGNÓSTICO: Bloqueo de Google (Captcha).")
        return

    # 3. Búsqueda de JSON de productos
    # Buscamos la palabra clave "brand":"Samsung" o "brand":"Apple"
    print("      🔍 Buscando huellas de productos...")
    
    if 'brand":"Samsung"' in html:
        print("      ✅ ¡EUREKA! Se detectaron datos de SAMSUNG en el código.")
    elif 'brand":"Apple"' in html:
        print("      ✅ ¡EUREKA! Se detectaron datos de APPLE en el código.")
    else:
        print("      ⚠️  No veo marcas conocidas en el texto plano.")

    # 4. Extracción de muestra de JSON
    # Intentamos sacar un trocito de texto que parezca JSON para ver el formato
    # Buscamos algo que empiece por {"id" o {"brand"
    match = re.search(r'\{"brand":"[^"]+".*?"price":\{.*?\}', html)
    if match:
        print(f"      📝 Muestra de JSON encontrado:\n      {match.group(0)[:200]}...")
    else:
        # Si falla el regex anterior, probamos uno más simple
        print("      ⚠️  Regex estricta falló. Buscando cualquier estructura JSON...")
        match_simple = re.search(r'data-json="([^"]+)"', html)
        if match_simple:
            print(f"      📝 Encontrado atributo data-json (HTML encoding):\n      {match_simple.group(1)[:100]}...")
        else:
            print("      ❌ NO SE ENCUENTRA NINGÚN JSON RECONOCIBLE.")
            # Imprimimos un trozo del body para ver qué hay
            body_text = soup.get_text()[:300].replace("\n", " ")
            print(f"      📄 Texto visible (inicio): {body_text}")

def fetch_google_cache(url: str):
    session = requests.Session(impersonate="chrome110") if USAR_CURL_CFFI else requests.Session()
    session.headers.update(HEADERS)
    
    clean_url = url.split("?")[0]
    # Probamos sin strip para ver todo el código
    cache_link = f"http://webcache.googleusercontent.com/search?q=cache:{urllib.parse.quote(clean_url)}&strip=0&vwsrc=0"
    
    print(f"🌍 Conectando a Google Cache: {mask_url(url)}")
    try:
        r = session.get(cache_link, timeout=20, verify=False)
        print(f"   Estado HTTP: {r.status_code}")
        if r.status_code == 200:
            analizar_html(r.text, "GOOGLE CACHE")
        elif r.status_code == 429:
            print("   ⛔ Google nos está bloqueando (Too Many Requests).")
    except Exception as e:
        print(f"   ❌ Error de conexión: {e}")

def main():
    print("--- INICIANDO DIAGNÓSTICO ---")
    for url in URLS_PRUEBA:
        print("-" * 60)
        fetch_google_cache(url)
        time.sleep(3)
    print("-" * 60)

if __name__ == "__main__":
    main()
