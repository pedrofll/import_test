"""
Scraper para El Corte Inglés — Móviles
ESTRATEGIA FINAL: Rotación de Proxies Públicos + Camuflaje TLS.
Bypassea el bloqueo de IP de GitHub usando intermediarios.
"""

import os
import re
import time
import random
import requests as std_requests  # Para bajar la lista de proxies
from dataclasses import dataclass
from typing import List, Optional, Tuple
from urllib.parse import urljoin, urlparse, parse_qsl, urlencode, urlunparse
from bs4 import BeautifulSoup

# Intentamos importar curl_cffi (Vital para ocultar que somos un bot de python)
try:
    from curl_cffi import requests
    USAR_CURL_CFFI = True
except ImportError:
    import requests
    USAR_CURL_CFFI = False
    print("⚠️ ADVERTENCIA: 'curl_cffi' no instalado. La tasa de éxito bajará mucho.")

# =========================
# CONFIGURACIÓN
# =========================

DEFAULT_URLS = [
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/",
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/2/",
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/3/",
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/4/",
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/5/",
]

CORTEINGLES_URLS_RAW = os.environ.get("CORTEINGLES_URLS", "").strip()
START_URL_CORTEINGLES = os.environ.get("START_URL_CORTEINGLES", "").strip()
AFF_ELCORTEINGLES = os.environ.get("AFF_ELCORTEINGLES", "").strip()
TIMEOUT = 20 

BASE_URL = "https://www.elcorteingles.es"
ID_IMPORTACION = f"{BASE_URL}/electronica/moviles-y-smartphones/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9",
    "Referer": "https://www.google.es/",
}

# =========================
# GESTOR DE PROXIES
# =========================
class ProxyManager:
    def __init__(self):
        self.proxies = []
        self.blacklist = set()
        self.cargar_proxies()

    def cargar_proxies(self):
        print("🌍 Descargando lista de proxies frescos...")
        # Usamos una API pública de proxies HTTP/HTTPS
        urls = [
            "https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=5000&country=all&ssl=all&anonymity=all",
            "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt"
        ]
        
        found = set()
        for u in urls:
            try:
                r = std_requests.get(u, timeout=10)
                if r.status_code == 200:
                    lines = r.text.strip().split('\n')
                    for line in lines:
                        p = line.strip()
                        if p and ":" in p:
                            found.add(p)
            except: pass
        
        self.proxies = list(found)
        # Mezclamos para no usar siempre los mismos
        random.shuffle(self.proxies)
        print(f"✅ Cargados {len(self.proxies)} proxies potenciales.")

    def get_proxy(self):
        # Devuelve un proxy que no esté en la lista negra
        validos = [p for p in self.proxies if p not in self.blacklist]
        if not validos:
            print("⚠️ Se acabaron los proxies. Recargando...")
            self.cargar_proxies()
            self.blacklist = set() # Reset blacklist
            validos = self.proxies
        
        return random.choice(validos[:20]) # Elegimos uno de los 20 primeros

    def report_fail(self, proxy):
        self.blacklist.add(proxy)

proxy_manager = ProxyManager()

# =========================
# MODELO
# =========================
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

# =========================
# HELPERS
# =========================
def mask_url(u: str) -> str:
    try:
        p = urlparse(u)
        return urlunparse((p.scheme, p.netloc, p.path, "", "", ""))
    except: return u

def build_urls_paginas() -> List[str]:
    if CORTEINGLES_URLS_RAW:
        return [u.strip() for u in CORTEINGLES_URLS_RAW.split(",") if u.strip()]
    if START_URL_CORTEINGLES:
        base = START_URL_CORTEINGLES.rstrip("/")
        if base.endswith("moviles-y-smartphones"):
            return [base + "/"] + [f"{base}/{i}/" for i in range(2, 11)]
        return [START_URL_CORTEINGLES]
    return DEFAULT_URLS

URLS_PAGINAS = build_urls_paginas()

# Regex
RE_GB = re.compile(r"(\d{1,3})\s*GB", re.IGNORECASE)
RE_RAM_PLUS = re.compile(r"(\d{1,3})\s*GB\s*\+\s*(\d{1,4})\s*GB", re.IGNORECASE)
RE_12GB_512GB = re.compile(r"(\d{1,3})\s*GB\s*[+xX]\s*(\d{1,4})\s*GB", re.IGNORECASE)
RE_COMPACT_8_256 = re.compile(r"\b(\d{1,2})\s*\+\s*(\
