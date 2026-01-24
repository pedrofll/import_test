"""
Scraper (DRY RUN) para El Corte Inglés — móviles y smartphones

Objetivo (fase 1):
- Escanear páginas 1..10 del listado
- Extraer: nombre, RAM, capacidad, precio_actual, precio_original, img_url, url_producto (sin query)
- Construir URL con afiliado usando secreto AFF_ELCORTEINGLES
- NO crea/actualiza/elimina productos en WooCommerce (solo logs)

Cambios Anti-Bloqueo:
- Headers de Chrome Windows 10 completo.
- Uso de Session para cookies.
- Timeouts ajustados.
"""

import os
import re
import time
import random
from dataclasses import dataclass
from typing import List, Optional, Tuple
from urllib.parse import urljoin, urlparse, parse_qsl, urlencode, urlunparse

import requests
from bs4 import BeautifulSoup


# =========================
# Configuración / Variables
# =========================

DEFAULT_URLS = [
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/",
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/2/",
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/3/",
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/4/",
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/5/",
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/6/",
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/7/",
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/8/",
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/9/",
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/10/",
]

# Opcional: lista completa por variable tipo "url1,url2,url3"
CORTEINGLES_URLS_RAW = os.environ.get("CORTEINGLES_URLS", "").strip()

# Opcional: URL base (página 1) para derivar la lista 1..10
START_URL_CORTEINGLES = os.environ.get("START_URL_CORTEINGLES", "").strip()

# Afiliado (secreto).
AFF_ELCORTEINGLES = os.environ.get("AFF_ELCORTEINGLES", "").strip()

# Modo: solo logs
DRY_RUN = True

TIMEOUT = 20  # Reducido un poco para fallar rápido si bloquean

# HEADERS "CAMUFLADOS" (Simulando Windows 10 / Chrome Real)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.google.es/",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "cross-site",
    "Sec-Fetch-User": "?1",
    "sec-ch-ua": '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}

def mask_url(u: str) -> str:
    """Devuelve la URL sin query ni fragmento."""
    if not u:
        return ""
    try:
        p = urlparse(u)
        return urlunparse((p.scheme, p.netloc, p.path, "", "", ""))
    except Exception:
        return u


def build_urls_paginas() -> List[str]:
    if CORTEINGLES_URLS_RAW:
        return [u.strip() for u in CORTEINGLES_URLS_RAW.split(",") if u.strip()]

    if START_URL_CORTEINGLES:
        base = START_URL_CORTEINGLES.rstrip("/")
        if base.endswith("moviles-y-smartphones"):
            page1 = base + "/"
            return [page1] + [f"{page1}{i}/" for i in range(2, 11)]
        return [START_URL_CORTEINGLES]

    return DEFAULT_URLS


URLS_PAGINAS = build_urls_paginas()
BASE_URL = "https://www.elcorteingles.es"
ID_IMPORTACION = f"{BASE_URL}/electronica/moviles-y-smartphones/"


# ===========
# Modelo dato
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


# =========================
# Parsing de nombre / specs
# =========================

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
    if m:
        return f"{m.group(1)}GB", f"{m.group(2)}GB"

    m = RE_12GB_512GB.search(titulo)
    if m:
        return f"{m.group(1)}GB", f"{m.group(2)}GB"

    m = RE_COMPACT_8_256.search(titulo)
    if m:
        return f"{m.group(1)}GB", f"{m.group(2)}GB"

    gbs = RE_GB.findall(titulo)
    if len(gbs) >= 2:
        return f"{gbs[0]}GB", f"{gbs[1]}GB"

    return None


def extraer_nombre(titulo: str, ram: str) -> str:
    # Corta antes del primer RAM detectado
    ram_pat = re.escape(ram.replace("GB", "")) + r"\s*GB"
    m = re.search(ram_pat, titulo, flags=re.IGNORECASE)
    if m:
        base = titulo[: m.start()].strip(" -–—,:;")
        return normalizar_espacios(base)
    return normalizar_espacios(titulo)


# ==========
# Precios
# ==========

def parse_precio(texto: str) -> Optional[float]:
    if not texto:
        return None
    s = texto.replace("\xa0", " ").replace("€", "").strip()
    s = s.replace(".", "").replace(",", ".")
    s = re.sub(r"[^\d.]", "", s)
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


# ==========
# Imagen URL
# ==========

def normalizar_url_imagen_600(img_url: str) -> str:
    if not img_url:
        return ""
    img_url = img_url.replace("&amp;", "&")
    try:
        p = urlparse(img_url)
        q = dict(parse_qsl(p.query, keep_blank_values=True))
        q["
