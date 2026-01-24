"""
Scraper para El Corte Inglés — Móviles
ESTRATEGIA FINAL: Proxies SSL/HTTPS estrictos + CORS Gateways.
Corrige el error de intentar conectar a ECI (HTTPS) con proxies HTTP planos.
"""

import os
import re
import time
import random
import requests as std_requests
from dataclasses import dataclass
from typing import List, Optional, Tuple
from urllib.parse import urljoin, urlparse, parse_qsl, urlencode, urlunparse
from bs4 import BeautifulSoup

# curl_cffi es obligatorio para la huella TLS
try:
    from curl_cffi import requests
    USAR_CURL_CFFI = True
except ImportError:
    import requests
    USAR_CURL_CFFI = False
    print("⚠️ ADVERTENCIA: 'curl_cffi' no instalado. ECI nos detectará rápido.")

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

TIMEOUT = 25
BASE_URL = "https://www.elcorteingles.es"
ID_IMPORTACION = f"{BASE_URL}/electronica/moviles-y-smartphones/"

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Upgrade-Insecure-Requests": "1",
}

# =========================
# GESTOR DE CONEXIÓN HÍBRIDO (PROXIES + GATEWAYS)
# =========================
class NetworkManager:
    def __init__(self):
        self.proxies = []
        self.gateways = [
            "https://api.allorigins.win/raw?url={}",
            "https://api.codetabs.com/v1/proxy?quest={}",
            # "https://corsproxy.io/?{}" # A veces bloqueado, pero útil
        ]
        self.blacklist = set()
        self.cargar_proxies_https()

    def cargar_proxies_https(self):
        print("🌍 Descargando lista de proxies HTTPS/SSL (Estrictos)...")
        # NOTA: Bajamos listas explícitas de HTTPS, no HTTP genérico
        urls = [
            "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-https.txt",
            "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/https.txt",
            "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/https.txt",
            "https://api.proxyscrape.com/v2/?request=getproxies&protocol=https&timeout=4000&country=all&ssl=yes&anonymity=all"
        ]
        
        found = set()
        for u in urls:
            try:
                print(f"   ⬇️  Bajando: {u} ...")
                r = std_requests.get(u, timeout=10)
                if r.status_code == 200:
                    lines = r.text.strip().split('\n')
                    for line in lines:
                        p = line.strip()
                        if p and ":" in p:
                            found.add(p)
            except: pass
        
        self.proxies = list(found)
        random.shuffle(self.proxies)
        print(f"✅ Cargados {len(self.proxies)} proxies HTTPS válidos.")

    def get_proxy(self):
        validos = [p for p in self.proxies if p not in self.blacklist]
        if not validos: return None
        return random.choice(validos[:50]) # Rotar entre los 50 mejores candidatos

    def report_fail(self, proxy):
        self.blacklist.add(proxy)

network = NetworkManager()

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

RE_GB = re.compile(r"(\d{1,3})\s*GB", re.IGNORECASE)
RE_RAM_PLUS = re.compile(r"(\d{1,3})\s*GB\s*\+\s*(\d{1,4})\s*GB", re.IGNORECASE)
RE_12GB_512GB = re.compile(r"(\d{1,3})\s*GB\s*[+xX]\s*(\d{1,4})\s*GB", re.IGNORECASE)
RE_COMPACT_8_256 = re.compile(r"\b(\d{1,2})\s*\+\s*(\d{2,4})\s*GB\b", re.IGNORECASE)
RE_PATROCINADO = re.compile(r"\bpatrocinado\b", re.IGNORECASE)

def normalizar_espacios(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def titulo_limpio(titulo: str) -> str:
    t = normalizar_espacios(titulo)
    t = RE_PATROCINADO.sub("", t)
    return normalizar_espacios(t)

def extraer_ram_rom(titulo: str) -> Optional[Tuple[str, str]]:
    m = RE_RAM_PLUS.search(titulo)
    if m: return f"{m.group(1)}GB", f"{m.group(2)}GB"
    m = RE_12GB_512GB.search(titulo)
    if m: return f"{m.group(1)}GB", f"{m.group(2)}GB"
    m = RE_COMPACT_8_256.search(titulo)
    if m: return f"{m.group(1)}GB", f"{m.group(2)}GB"
    gbs = RE_GB.findall(titulo)
    if len(gbs) >= 2: return f"{gbs[0]}GB", f"{gbs[1]}GB"
    return None

def extraer_nombre(titulo: str, ram: str) -> str:
    ram_pat = re.escape(ram.replace("GB", "")) + r"\s*GB"
    m = re.search(ram_pat, titulo, flags=re.IGNORECASE)
    if m:
        base = titulo[: m.start()].strip(" -–—,:;")
        return normalizar_espacios(base)
    return normalizar_espacios(titulo)

def parse_precio(texto: str) -> Optional[float]:
    if not texto: return None
    s = texto.replace("\xa0", " ").replace("€", "").strip().replace(".", "").replace(",", ".")
    try: return float(re.sub(r"[^\d.]", "", s))
    except: return None

def normalizar_url_imagen_600(img_url: str) -> str:
    if not img_url: return ""
    if img_url.startswith("//"): img_url = "https:" + img_url
    try:
        p = urlparse(img_url)
        q = dict(parse_qsl(p.query))
        q["impolicy"] = "Resize"
        q["width"] = "600"
        q["height"] = "600"
        return urlunparse((p.scheme, p.netloc, p.path, "", urlencode(q, doseq=True), ""))
    except: return img_url

def limpiar_url_producto(url_rel_o_abs: str) -> str:
    if not url_rel_o_abs: return ""
    return urlunparse(urlparse(urljoin(BASE_URL, url_rel_o_abs))._replace(query="", fragment=""))

def build_url_con_afiliado(url_sin: str, aff: str) -> str:
    if not url_sin or not aff: return url_sin
    sep = "&" if "?" in url_sin else "?"
    if re.fullmatch(r"\d+", aff): return f"{url_sin}{sep}aff_id={aff}"
    return f"{url_sin}{sep}{aff.lstrip('?&')}"

# =========================
# LÓGICA DE DESCARGA HÍBRIDA
# =========================

def fetch_html_hybrid(url: str, max_retries=12) -> str:
    """Intenta descargar usando Proxies SSL, y si falla, usa Gateways CORS."""
    
    session = requests.Session(impersonate="chrome120", headers=HEADERS) if USAR_CURL_CFFI else requests.Session()
    if not USAR_CURL_CFFI: session.headers.update(HEADERS)

    # 1. INTENTO CON PROXIES HTTPS
    for i in range(max_retries):
        proxy = network.get_proxy()
        if not proxy: break # Se acabaron

        # Vital: Esquema 'http' para conectar al proxy, pero el proxy debe soportar CONNECT
        # Para librerías modernas, suele ser 'http://' incluso si es proxy https, 
        # pero curl_cffi maneja bien el túnel.
        proxy_dict = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
        
        print(f"   🔄 Proxy SSL Intento {i+1}/{max_retries} | {proxy} ...")
        
        try:
            r = session.get(url, proxies=proxy_dict, timeout=12)
            
            if r.status_code == 200:
                if "moviles" in r.text.lower() or "card" in r.text:
                    return r.text
                else:
                    print("      ⚠️  Proxy devolvió basura.")
                    network.report_fail(proxy)
            else:
                print(f"      ⛔ Bloqueo/Error {r.status_code}")
                network.report_fail(proxy)

        except Exception:
            # print("      ❌ Error conexión") 
            network.report_fail(proxy)
            pass

    print("⚠️ Fallaron los proxies SSL. Activando Gateways CORS...")

    # 2. INTENTO CON GATEWAYS (FALLBACK)
    for gateway_fmt in network.gateways:
        target_url = gateway_fmt.format(url)
        print(f"   🌐 Probando Gateway: {target_url[:50]}...")
        try:
            # No usamos proxy aquí, vamos directo al gateway con nuestra IP (GitHub)
            # El gateway hace de proxy
            r = std_requests.get(target_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
            if r.status_code == 200:
                 if "moviles" in r.text.lower() or "card" in r.text:
                    print("      ✅ Gateway funcionó!")
                    return r.text
        except Exception as e:
            print(f"      ❌ Falló Gateway: {e}")

    return ""

# =========================
# SCRAPING
# =========================

def detectar_cards(soup: BeautifulSoup):
    cards = soup.select('div.card') or soup.select('li.products_list-item') or soup.select('.product-preview') or soup.select('.grid-item')
    return cards

def extraer_info_card(card: BeautifulSoup) -> Tuple[str, str, float, float, str]:
    tit, href = "", ""
    for sel in ["a.product_preview-title", "h2 a", ".product-name a", "a.js-product-link"]:
        a = card.select_one(sel)
        if a:
            tit = a.get("title") or a.get_text(" ", strip=True)
            href = a.get("href") or ""
            break
            
    p_act, p_org = None, None
    for sel in [".js-preview-pricing", ".pricing", ".price", ".product-price", ".prices-price"]:
        pricing = card.select_one(sel)
        if pricing:
            texts = [normalizar_espacios(t) for t in pricing.stripped_strings if t]
            precios = []
            for t in texts:
                p = parse_precio(t)
                if p: precios.append(p)
            if precios:
                p_act = min(precios)
                p_org = max(precios)
                break
    
    if p_act and not p_org: p_org = p_act
    if p_act and p_org and p_org == p_act: p_org = round(p_act * 1.2, 2)

    img_url = ""
    for sel in ["img.js_preview_image", "img[data-variant-image-src]", "img"]:
        img = card.select_one(sel)
        if img:
            src = img.get("src") or img.get("data-variant-image-src")
            if src: 
                img_url = normalizar_url_imagen_600(src)
                break

    return tit, href, p_act, p_org, img_url

def obtener_productos(url: str, etiqueta: str) -> List[ProductoECI]:
    html = fetch_html_hybrid(url)
    if not html: return []
    
    soup = BeautifulSoup(html, "html.parser")
    cards = detectar_cards(soup)
    
    if not cards:
        print(f"⚠️  HTML obtenido pero sin productos en {etiqueta}. El DOM puede haber cambiado.")
        return []

    productos = []
    for card in cards:
        tit, href, p_act, p_org, img = extraer_info_card(card)
        if not tit or not href: continue
        
        t_clean = titulo_limpio(tit)
        specs = extraer_ram_rom(t_clean)
        if not specs: continue 
        
        ram, rom = specs
        nombre = extraer_nombre(t_clean, ram)
        if p_act is None: continue
        
        url_sin = limpiar_url_producto(href)
        url_con = build_url_con_afiliado(url_sin, AFF_ELCORTEINGLES)
        
        productos.append(ProductoECI(
            nombre=nombre, memoria=ram, capacidad=rom, version="Global",
            precio_actual=p_act, precio_original=p_org, enviado_desde="España",
            origen_pagina=etiqueta, img=img, url_imp=url_con, url_exp=url_con,
            url_importada_sin_afiliado=url_sin, url_sin_acortar_con_mi_afiliado=url_con,
            url_oferta=url_con, page_id=ID_IMPORTACION
        ))
    return productos

def main() -> int:
    print("--- FASE 1: ECI (MODO SSL + GATEWAYS) ---", flush=True)
    
    total = 0
    for i, url in enumerate(URLS_PAGINAS, start=1):
        print(f"\n📂 Procesando ({i}/{len(URLS_PAGINAS)}): {mask_url(url)}", flush=True)
        try:
            prods = obtener_productos(url, str(i))
        except Exception as e:
            print(f"❌ Error crítico: {e}", flush=True)
            continue
            
        print(f"✅ Encontrados: {len(prods)}", flush=True)
        total += len(prods)
        
        for p in prods:
            print("-" * 60)
            print(f"Detectado {p.nombre}")
            print(f"1) Nombre: {p.nombre}")
            print(f"2) RAM: {p.memoria} | ROM: {p.capacidad}")
            print(f"3) Precio: {p.precio_actual}€")
            print(f"4) URL: {mask_url(p.url_importada_sin_afiliado)}")
            print("-" * 60, flush=True)
            
    print(f"\n📋 TOTAL PRODUCTOS ESCANEADOS: {total}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
