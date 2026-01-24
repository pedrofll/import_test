"""
Scraper para El Corte Inglés — Móviles
SOLUCIÓN: Camuflaje iPhone (Safari) para evadir bloqueo de IP en GitHub Actions.
"""

import os
import re
import time
import random
from dataclasses import dataclass
from typing import List, Optional, Tuple
from urllib.parse import urljoin, urlparse, parse_qsl, urlencode, urlunparse

from bs4 import BeautifulSoup

# Intentamos importar curl_cffi
try:
    from curl_cffi import requests
    USAR_CURL_CFFI = True
except ImportError:
    import requests
    USAR_CURL_CFFI = False
    print("⚠️ ADVERTENCIA: 'curl_cffi' no está instalado. Fallará casi seguro.")

# =========================
# Configuración
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

TIMEOUT = 60 

# Headers MÍNIMOS (El resto lo pone curl_cffi automáticamente al simular Safari)
# NOTA: No poner User-Agent manual para evitar huellas contradictorias.
HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9",
    "Referer": "https://www.google.com/",
}

# ===========
# Modelo
# ===========
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

# ===========
# Helpers URL
# ===========
def mask_url(u: str) -> str:
    if not u: return ""
    try:
        p = urlparse(u)
        # Devolvemos la URL limpia para logs
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
BASE_URL = "https://www.elcorteingles.es"
ID_IMPORTACION = f"{BASE_URL}/electronica/moviles-y-smartphones/"

# ===========
# Regex Helpers
# ===========
RE_GB = re.compile(r"(\d{1,3})\s*GB", re.IGNORECASE)
RE_RAM_PLUS = re.compile(r"(\d{1,3})\s*GB\s*\+\s*(\d{1,4})\s*GB", re.IGNORECASE)
RE_12GB_512GB = re.compile(r"(\d{1,3})\s*GB\s*[+xX]\s*(\d{1,4})\s*GB", re.IGNORECASE)
RE_COMPACT_8_256 = re.compile(r"\b(\d{1,2})\s*\+\s*(\d{2,4})\s*GB\b", re.IGNORECASE)
RE_MOBILE_LIBRE = re.compile(r"\bm[oó]vil\s+libre\b", re.IGNORECASE)
RE_PATROCINADO = re.compile(r"\bpatrocinado\b", re.IGNORECASE)

def normalizar_espacios(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def titulo_limpio(titulo: str) -> str:
    t = normalizar_espacios(titulo)
    t = RE_PATROCINADO.sub("", t)
    t = RE_MOBILE_LIBRE.sub("", t)
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
    s = texto.replace("\xa0", " ").replace("€", "").strip()
    s = s.replace(".", "").replace(",", ".")
    s = re.sub(r"[^\d.]", "", s)
    if not s: return None
    try: return float(s)
    except: return None

def normalizar_url_imagen_600(img_url: str) -> str:
    if not img_url: return ""
    img_url = img_url.replace("&amp;", "&")
    try:
        p = urlparse(img_url)
        q = dict(parse_qsl(p.query, keep_blank_values=True))
        q["impolicy"] = q.get("impolicy", "Resize")
        q["width"] = "600"
        q["height"] = "600"
        return urlunparse((p.scheme, p.netloc, p.path, "", urlencode(q, doseq=True), ""))
    except: return img_url

def limpiar_url_producto(url_rel_o_abs: str) -> str:
    if not url_rel_o_abs: return ""
    abs_url = urljoin(BASE_URL, url_rel_o_abs)
    p = urlparse(abs_url)
    return urlunparse((p.scheme, p.netloc, p.path, "", "", ""))

def build_url_con_afiliado(url_sin: str, aff: str) -> str:
    if not url_sin: return ""
    if not aff: return url_sin
    sep = "&" if "?" in url_sin else "?"
    if re.fullmatch(r"\d+", aff): return f"{url_sin}{sep}aff_id={aff}"
    return f"{url_sin}{sep}{aff.lstrip('?&')}"

# =========================
# LÓGICA DE REDIRECCIÓN Y CAMUFLAJE
# =========================

def get_session():
    if USAR_CURL_CFFI:
        # CAMBIO CLAVE: Usamos 'safari15_5' en lugar de Chrome. 
        # Apple suele tener IPs más variables y a veces Akamai es más suave.
        print("🍏 Modo Camuflaje: Simulando Safari (iPhone/Mac)")
        return requests.Session(impersonate="safari15_5", headers=HEADERS)
    else:
        s = requests.Session()
        s.headers.update(HEADERS)
        s.headers.update({"User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1"})
        return s

def fetch_html_smart(session, url: str, depth=0) -> str:
    if depth > 3:
        print("❌ Bucle de redirección detectado. Abortando.")
        return ""

    if depth == 0:
        # Pausa inicial aleatoria para parecer humano
        time.sleep(random.uniform(2, 5))
    
    print(f"🌍 Conectando a {mask_url(url)} (Intento {depth+1})...")
    
    try:
        r = session.get(url, timeout=TIMEOUT)
    except Exception as e:
        print(f"❌ Error de conexión: {e}")
        return ""

    if r.status_code == 403:
        # Si falla, imprimimos un aviso pero no crasheamos todo el script
        print(f"🔒 Bloqueo 403 en {mask_url(url)}. La IP de GitHub sigue sucia.")
        return ""
    
    html = r.text
    soup = BeautifulSoup(html, "html.parser")

    # Detectar Meta Refresh (Waiting Room)
    meta_refresh = soup.find("meta", attrs={"http-equiv": lambda x: x and x.lower() == "refresh"})
    
    if meta_refresh:
        content = meta_refresh.get("content", "")
        parts = content.split("URL=")
        if len(parts) > 1:
            try:
                wait_time = int(parts[0].replace(";", "").strip())
            except: 
                wait_time = 5
            
            # Limpiamos la URL destino (quitamos comillas)
            next_url_rel = parts[1].strip("'\" ")
            next_url_abs = urljoin(BASE_URL, next_url_rel)
            
            print(f"🛑 SALA DE ESPERA DETECTADA.")
            print(f"⏳ Esperando {wait_time}s + margen de seguridad...")
            time.sleep(wait_time + 2) # Damos 2 segundos extra
            
            # Recurrimos a la nueva URL
            return fetch_html_smart(session, next_url_abs, depth=depth+1)

    return html

# =========================
# Parsing Productos
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

def obtener_productos(session, url: str, etiqueta: str) -> List[ProductoECI]:
    html = fetch_html_smart(session, url)
    if not html: return []
    
    soup = BeautifulSoup(html, "html.parser")
    cards = detectar_cards(soup)
    
    if not cards:
        print(f"⚠️  DEBUG: HTML descargado pero sin productos en {etiqueta}.", flush=True)
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
    print("--- FASE 1: ESCANEANDO EL CORTE INGLÉS (MODO SAFARI) ---", flush=True)
    session = get_session()
    
    total = 0
    for i, url in enumerate(URLS_PAGINAS, start=1):
        print(f"\n📂 Procesando listado ({i}/{len(URLS_PAGINAS)})", flush=True)
        try:
            prods = obtener_productos(session, url, str(i))
        except Exception as e:
            print(f"❌ Error general en {mask_url(url)}: {e}", flush=True)
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
