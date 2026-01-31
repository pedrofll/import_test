#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PowerPlanet importer for ofertasdemoviles.com (WooCommerce + ACF meta).

- Only uses listing page: https://www.powerplanetonline.com/es/moviles-mas-vendidos
- Visits each product page detected in that listing to extract data (RAM/ROM, prices, image, etc.)
- Creates/updates WooCommerce external products with ACF fields as meta_data
- Manages duplicates + obsoletes via internal tag + ACF meta importado_de

Run:
  export WP_URL="https://tusitio.com"
  export WP_KEY="ck_..."
  export WP_SECRET="cs_..."
  python powerplanet.py --max-products 0

CLI args override env.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as _dt
import html
import io
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse, urlsplit, urlunsplit

import requests
from bs4 import BeautifulSoup
from PIL import Image


# ============================================================
# CONSTANTES
# ============================================================

BASE_URL = "https://www.powerplanetonline.com"
LIST_URL = "https://www.powerplanetonline.com/es/moviles-mas-vendidos"

# Import metadata
IMPORTADO_DE = LIST_URL
FUENTE_ACF = "powerplanetonline"
TIENDA_ENVIO = "España"
TIENDA_ENVIO_TG = "🇪🇸 España"

# Default coupon text (ACF)
CUPON_DEFAULT = "OFERTA PROMO"

# Internal tag to scope obsoletes to this source
TAG_IMPORT = "__importado_de_powerplanetonline"

# ACF meta keys (IMPORTANT: keep exact spelling)
ACF_MEMORIA = "memoria"
ACF_CAPACIDAD = "capacidad"
ACF_PRECIO_ACTUAL = "precio_actual"
ACF_PRECIO_ORIGIAL = "precio_origial"  # sic
ACF_CODIGO_DESCUENTO = "codigo_de_descuento"
ACF_FUENTE = "fuente"
ACF_IMAGEN_PRODUCTO = "imagen_producto"
ACF_ENVIADO_DESDE = "enviado_desde"
ACF_ENVIADO_DESDE_TG = "enviado_desde_tg"
ACF_VERSION = "version"
ACF_FECHA = "fecha"
ACF_IMPORTADO_DE = "importado_de"
ACF_URL_POST_ACORTADA = "url_post_acortada"

# Image
TARGET_IMG_SIZE = 600  # 600x600

# Title formatting helpers
ACRONYM_WORDS = {
    "5g", "4g", "3g", "2g", "wifi", "nfc", "gps", "oled", "amoled", "lte", "uwb", "ui",
    "ios", "usb", "usb-c", "esim", "dual", "sim", "ai", "ia",
}
MODEL_TOKEN_RE = re.compile(r"^[0-9]+[a-z]+$|^[a-z]+[0-9]+[a-z0-9]*$", re.I)

# Color/variant tokens to strip at end of title (PowerPlanet tends to append)
COLOR_TOKENS = {
    # ES
    "negro", "blanco", "gris", "plata", "azul", "rojo", "verde", "amarillo", "rosa",
    "morado", "violeta", "dorado", "naranja", "beige", "turquesa", "celeste", "cobre",
    "grafito", "marfil", "titanio", "transparente", "transparencia", "marron", "marrón",
    "lila", "crema", "coral", "lima",
    # common marketing shades
    "obsidiana", "subzero", "midnight", "starlight", "natural", "desert", "ocean",
    "hazel", "sage", "lavender", "peach", "aqua",
    # EN
    "black", "white", "gray", "grey", "silver", "blue", "red", "green", "yellow", "pink",
    "purple", "gold", "orange", "beige", "turquoise", "graphite", "titanium", "transparent",
    # weird but seen
    "drill",
}
# Multi-word colors we should strip (must be checked before single-word stripping)
COLOR_PHRASES = [
    "azul claro", "azul oscuro", "verde agua", "verde drill", "rosa claro", "rosa oscuro",
    "titanio negro", "titanio blanco", "titanio natural",
]

# If title contains any of these after a hyphen, strip the hyphen suffix.
STRIP_AFTER_DASH_KEYWORDS = [
    "versión", "version", "internacional", "estado", "renov", "reacond", "desprecint",
    "bater", "pantalla", "ram", "gb", "tb", "ia", "editor", "fotos", "refurb",
]


# ============================================================
# DATOS
# ============================================================

@dataclass
class Offer:
    url: str
    product_id: int
    name_raw: str
    name_clean: str
    memoria: str
    capacidad: str
    version: str
    precio_actual_eur: float
    precio_original_eur: float
    codigo_descuento: str
    image_url: str
    url_oferta: str = ""  # affiliate short
    url_post_acortada: str = ""  # product permalink short (after create)
    enviado_desde: str = TIENDA_ENVIO
    enviado_desde_tg: str = TIENDA_ENVIO_TG
    fuente: str = FUENTE_ACF


# ============================================================
# UTILIDADES DE TEXTO/PRECIOS
# ============================================================

def _strip_accents_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def format_product_title(name: str) -> str:
    """Title-case with special handling for tokens like 14T, 5G, iOS."""
    name = _strip_accents_spaces(name)
    if not name:
        return name

    words = []
    for w in name.split(" "):
        if not w:
            continue
        wl = w.lower()

        # Keep tokens with slash, quotes etc mostly as-is (but normalize case)
        if "/" in w:
            parts = w.split("/")
            parts2 = []
            for p in parts:
                if p.lower() in ACRONYM_WORDS:
                    parts2.append(p.upper() if p.lower() != "ios" else "iOS")
                elif MODEL_TOKEN_RE.match(p):
                    parts2.append(p.upper())
                else:
                    parts2.append(p.capitalize())
            words.append("/".join(parts2))
            continue

        if wl in ACRONYM_WORDS:
            words.append(w.upper() if wl != "ios" else "iOS")
        elif MODEL_TOKEN_RE.match(w):
            words.append(w.upper())
        else:
            words.append(w.capitalize())
    return " ".join(words)

def safe_float_from_price(price: str) -> Optional[float]:
    if price is None:
        return None
    s = str(price).strip()
    if not s:
        return None
    # accept "174,99", "174.99", "174,99€"
    s = s.replace("€", "").replace("\xa0", " ").strip()
    s = s.replace(".", "").replace(",", ".") if re.search(r"\d+,\d{2}", s) else s.replace(",", ".")
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    if not m:
        return None
    try:
        return float(m.group(1))
    except Exception:
        return None

def extract_ram_rom_from_name(name: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Detect RAM/ROM from strings like:
      - "8GB/256GB"
      - "8GB 256GB"
      - "4b128gb" (PowerPlanet slug style)
      - "8 GB de RAM 256 GB"
    Returns ("8GB", "256GB") or (None, None)
    """
    s = name

    # 8GB/256GB
    m = re.search(r"\b(\d+)\s*GB\s*/\s*(\d+)\s*(GB|TB)\b", s, re.I)
    if m:
        ram = f"{int(m.group(1))}GB"
        rom = f"{int(m.group(2))}{m.group(3).upper()}"
        return ram, rom

    # 8GB 256GB
    m = re.search(r"\b(\d+)\s*GB\b.*?\b(\d+)\s*(GB|TB)\b", s, re.I)
    if m:
        ram = f"{int(m.group(1))}GB"
        rom = f"{int(m.group(2))}{m.group(3).upper()}"
        return ram, rom

    # 4b128gb
    m = re.search(r"\b(\d+)\s*[bB]\s*(\d+)\s*(GB|TB)\b", s, re.I)
    if m:
        ram = f"{int(m.group(1))}GB"
        rom = f"{int(m.group(2))}{m.group(3).upper()}"
        return ram, rom

    # "8 GB de RAM" and "256 GB"
    m_ram = re.search(r"\b(\d+)\s*GB\s*de\s*RAM\b", s, re.I)
    m_rom = re.search(r"\b(\d+)\s*(GB|TB)\b(?!\s*de\s*RAM)", s, re.I)
    if m_ram and m_rom:
        ram = f"{int(m_ram.group(1))}GB"
        rom = f"{int(m_rom.group(1))}{m_rom.group(2).upper()}"
        return ram, rom

    return None, None

def _strip_memory_tokens(s: str, ram: Optional[str], rom: Optional[str]) -> str:
    out = s

    # Remove "8GB/256GB"
    out = re.sub(r"\b\d+\s*GB\s*/\s*\d+\s*(GB|TB)\b", "", out, flags=re.I)

    # Remove "8GB 256GB"
    out = re.sub(r"\b\d+\s*GB\b\s+\b\d+\s*(GB|TB)\b", "", out, flags=re.I)

    # Remove "4b128gb"
    out = re.sub(r"\b\d+\s*[bB]\s*\d+\s*(GB|TB)\b", "", out, flags=re.I)

    if ram:
        out = re.sub(rf"\b{re.escape(ram)}\b", "", out, flags=re.I)
    if rom:
        out = re.sub(rf"\b{re.escape(rom)}\b", "", out, flags=re.I)

    # Remove "8 GB de RAM"
    out = re.sub(r"\b\d+\s*GB\s*de\s*RAM\b", "", out, flags=re.I)

    return _strip_accents_spaces(out)

def _strip_dash_suffix(s: str) -> str:
    # Example: "Vivo X200 Fe Negro - Versión Internacional" -> "Vivo X200 Fe Negro"
    parts = re.split(r"\s[-–]\s", s)
    if len(parts) <= 1:
        return s

    tail = " ".join(parts[1:]).lower()
    if any(k in tail for k in STRIP_AFTER_DASH_KEYWORDS):
        return parts[0].strip()
    return s

def _strip_color_suffix(s: str) -> str:
    # Remove known multi-word color phrases first
    out = _strip_accents_spaces(s)

    lowered = out.lower()
    for phrase in COLOR_PHRASES:
        if lowered.endswith(" " + phrase):
            out = out[: -(len(phrase) + 1)]
            out = _strip_accents_spaces(out)
            lowered = out.lower()

    # Then remove last tokens while they match color tokens
    tokens = out.split()
    while tokens:
        last = re.sub(r"[^\wáéíóúüñÁÉÍÓÚÜÑ-]", "", tokens[-1]).lower()
        last = last.replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u").replace("ü", "u").replace("ñ", "n")
        if last in COLOR_TOKENS:
            tokens.pop()
            continue
        # sometimes color is 2 tokens: "Plata Transparente"
        if len(tokens) >= 2:
            last2 = (tokens[-2] + " " + tokens[-1]).lower()
            last2 = last2.replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u").replace("ü", "u").replace("ñ", "n")
            if last2 in [p.replace("á","a").replace("é","e").replace("í","i").replace("ó","o").replace("ú","u").replace("ü","u").replace("ñ","n") for p in COLOR_PHRASES]:
                tokens = tokens[:-2]
                continue
        break

    return _strip_accents_spaces(" ".join(tokens))

def clean_powerplanet_name(name_raw: str, ram: Optional[str], rom: Optional[str]) -> str:
    """
    Goal:
      - Keep only model name (no RAM/ROM, no color, no refurb/international suffixes).
    """
    s = format_product_title(name_raw)

    # If there is a dash suffix with "versión/estado/renovado..." etc, drop it.
    s = _strip_dash_suffix(s)

    # Remove memory/capacity tokens
    s = _strip_memory_tokens(s, ram, rom)

    # Remove common trailing markers (do this BEFORE color stripping so "Negro Renovado" becomes "Negro")
    s = re.sub(r"\b(renovado|reacondicionado|refurbished)\b.*$", "", s, flags=re.I).strip()
    s = re.sub(r"\b(estado\s+excelente|estado\s+bien|estado\s+bueno)\b.*$", "", s, flags=re.I).strip()
    s = re.sub(r"\b(versi[oó]n\s+internacional|version\s+internacional)\b.*$", "", s, flags=re.I).strip()

    # Strip trailing colors / color-like tokens
    s = _strip_color_suffix(s)

    s = _strip_accents_spaces(s)
    return format_product_title(s)


# ============================================================
# HTTP + SCRAPING
# ============================================================

def fetch_html(sess: requests.Session, url: str, timeout: int) -> str:
    r = sess.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    return r.text

def normalize_url_no_qs(url: str) -> str:
    """Remove query + fragment (stabilize URLs)."""
    sp = urlsplit(url)
    return urlunsplit((sp.scheme, sp.netloc, sp.path, "", ""))

def is_in_domain(url: str) -> bool:
    p = urlparse(url)
    return (p.scheme in ("http", "https")) and (p.netloc == urlparse(BASE_URL).netloc)

def looks_like_product_path(path: str) -> bool:
    path_l = path.lower()
    if not path_l.startswith("/es/"):
        return False

    # Exclude known non-product sections
    if path_l.startswith("/es/moviles-") or path_l.startswith("/es/smartphones") or path_l.startswith("/es/telefonos-moviles"):
        return False
    if path_l.startswith("/es/tablet") or path_l.startswith("/es/ipad") or path_l.startswith("/es/wearables"):
        return False

    # Must contain some storage hint in slug
    if re.search(r"(\d+)\s*(gb|tb)", path_l):
        return True
    if re.search(r"\d+b\d+gb", path_l):
        return True
    return False

def extract_listing_product_urls(list_html: str) -> List[str]:
    """
    IMPORTANT:
      - Only extracts product detail URLs from LIST_URL HTML.
      - Does NOT follow any other pages (no categories, no menus).
    Strategy:
      - take all links whose PATH looks like a product detail URL (contains GB/TB patterns)
      - exclude /es/moviles-*, /es/smartphones*, /es/telefonos-moviles*
    """
    soup = BeautifulSoup(list_html, "html.parser")
    urls: Set[str] = set()

    for a in soup.find_all("a", href=True):
        href = a.get("href", "").strip()
        if not href or href.startswith("#"):
            continue
        abs_url = urljoin(BASE_URL, href) if href.startswith("/") else href
        if not abs_url.startswith(BASE_URL):
            continue
        abs_url = normalize_url_no_qs(abs_url)
        if abs_url == LIST_URL:
            continue

        p = urlparse(abs_url)
        if not looks_like_product_path(p.path):
            continue

        urls.add(abs_url)

    return sorted(urls)


def parse_product_data_json(soup: BeautifulSoup) -> Dict:
    """
    On product pages PowerPlanet embeds a JSON in:
      <form class="buyForm ... product-page-form ..." data-product='{...}'>
    Category/list pages may contain many data-product forms; we MUST select product-page-form.
    """
    form = soup.select_one("form.buyForm.product-page-form[data-product], form.product-page-form[data-product]")
    if not form:
        raise ValueError("No product-page-form with data-product found")

    raw = form.get("data-product", "")
    if not raw:
        raise ValueError("Empty data-product JSON")
    raw = html.unescape(raw)

    try:
        data = json.loads(raw)
    except Exception as e:
        raise ValueError(f"Failed to parse data-product JSON: {e}")

    return data

def parse_prices_from_product_json(product_json: Dict) -> Tuple[Optional[float], Optional[float]]:
    """
    Returns (precio_actual, precio_original) as floats in EUR.
    Prefer retailPrice/basePrice (NOT alternative*).
    """
    definition = product_json.get("definition") or {}

    candidates_actual = [
        definition.get("productRetailPrice"),
        definition.get("retailPrice"),
        definition.get("price"),
        product_json.get("definition", {}).get("productRetailPrice"),
    ]
    candidates_original = [
        definition.get("productBasePrice"),
        definition.get("basePrice"),
        product_json.get("definition", {}).get("productBasePrice"),
    ]

    actual = None
    for c in candidates_actual:
        if isinstance(c, (int, float)):
            actual = float(c)
            break
        f = safe_float_from_price(c)
        if f is not None:
            actual = f
            break

    orig = None
    for c in candidates_original:
        if isinstance(c, (int, float)):
            orig = float(c)
            break
        f = safe_float_from_price(c)
        if f is not None:
            orig = f
            break

    return actual, orig

def parse_image_url(soup: BeautifulSoup) -> str:
    img = soup.select_one("#main-image-container img#main-image, img#main-image, img.mainImageTag")
    if not img:
        return ""
    return (img.get("data-original") or img.get("src") or "").strip()

def detect_version(name_clean: str) -> str:
    # Project rule: iPhone => IOS, otherwise Global (for PowerPlanet ES)
    if re.search(r"\biphone\b", name_clean, re.I):
        return "IOS"
    return "Global"

def parse_offer_detail(sess: requests.Session, url: str, timeout: int) -> Offer:
    html_txt = fetch_html(sess, url, timeout=timeout)
    soup = BeautifulSoup(html_txt, "html.parser")

    product_json = parse_product_data_json(soup)
    product_id = int(product_json.get("id") or 0)

    name_raw = str(product_json.get("name") or "").strip()
    if not name_raw:
        # fallback to H1
        h1 = soup.select_one("h1.real-title")
        name_raw = h1.get_text(" ", strip=True) if h1 else ""
    name_raw = format_product_title(name_raw)

    ram, rom = extract_ram_rom_from_name(name_raw)
    if not ram or not rom:
        # extra attempt: from slug
        slug = urlparse(url).path.lower()
        m = re.search(r"-(\d+)gb-(\d+)(gb|tb)", slug, re.I)
        if m:
            ram = f"{int(m.group(1))}GB"
            rom = f"{int(m.group(2))}{m.group(3).upper()}"
    if not ram or not rom:
        raise ValueError("Producto sin RAM/ROM detectables (no se importa)")

    name_clean = clean_powerplanet_name(name_raw, ram, rom)
    version = detect_version(name_clean)

    precio_actual, precio_original = parse_prices_from_product_json(product_json)
    if precio_actual is None or precio_original is None:
        # fallback: parse spans
        p_act = soup.select_one(".data-all-prices .product-price .integerPrice")
        p_orig = soup.select_one(".data-all-prices .product-basePrice .integerPrice")
        precio_actual = safe_float_from_price(p_act.get_text(strip=True) if p_act else "") if precio_actual is None else precio_actual
        precio_original = safe_float_from_price(p_orig.get_text(strip=True) if p_orig else "") if precio_original is None else precio_original

    if precio_actual is None or precio_original is None:
        raise ValueError("No se pudo detectar precio actual/original")

    image_url = parse_image_url(soup)

    # Tablet filter (hard stop) - also covers iPad
    if re.search(r"\b(tab|ipad)\b", name_raw, re.I):
        raise ValueError("Producto tipo tablet detectado (no se importa)")

    return Offer(
        url=url,
        product_id=product_id,
        name_raw=name_raw,
        name_clean=name_clean,
        memoria=ram,
        capacidad=rom,
        version=version,
        precio_actual_eur=float(precio_actual),
        precio_original_eur=float(precio_original),
        codigo_descuento=CUPON_DEFAULT,
        image_url=image_url,
    )


# ============================================================
# IS.GD
# ============================================================

def shorten_isgd(sess: requests.Session, long_url: str, timeout: int) -> str:
    """
    Shorten with is.gd. If it fails, return original URL.
    """
    try:
        api = "https://is.gd/create.php"
        r = sess.get(api, params={"format": "simple", "url": long_url}, timeout=timeout)
        if r.status_code == 200:
            out = r.text.strip()
            if out.startswith("http"):
                return out
    except Exception:
        pass
    return long_url


# ============================================================
# WOOCOMMERCE CLIENT (REST)
# ============================================================

class WCClient:
    def __init__(self, base_url: str, ck: str, cs: str, timeout: int = 60):
        self.base_url = base_url.rstrip("/")
        self.ck = ck
        self.cs = cs
        self.timeout = timeout
        self.sess = requests.Session()
        self.sess.headers.update({"User-Agent": "Mozilla/5.0"})

    def _url(self, path: str) -> str:
        return f"{self.base_url}/wp-json/wc/v3{path}"

    def get(self, path: str, params: Optional[Dict] = None) -> Dict:
        r = self.sess.get(self._url(path), params=params, auth=(self.ck, self.cs), timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def post(self, path: str, payload: Dict) -> Dict:
        r = self.sess.post(self._url(path), json=payload, auth=(self.ck, self.cs), timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def put(self, path: str, payload: Dict) -> Dict:
        r = self.sess.put(self._url(path), json=payload, auth=(self.ck, self.cs), timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def delete(self, path: str, params: Optional[Dict] = None) -> Dict:
        r = self.sess.delete(self._url(path), params=params, auth=(self.ck, self.cs), timeout=self.timeout)
        r.raise_for_status()
        return r.json()


# ============================================================
# WOOCOMMERCE HELPERS
# ============================================================

def _meta_get(meta_data: List[Dict], key: str) -> Optional[str]:
    for m in meta_data or []:
        if m.get("key") == key:
            v = m.get("value")
            return "" if v is None else str(v)
    return None

def _meta_set(meta: List[Dict], key: str, value) -> None:
    meta.append({"key": key, "value": value})

def compute_sku(offer: Offer) -> str:
    # stable SKU per source + product_id + RAM/ROM
    return f"ppo-{offer.product_id}-{offer.memoria.lower()}-{offer.capacidad.lower()}"

def get_brand_from_name(name_clean: str) -> str:
    first = (name_clean.split()[0] if name_clean else "").strip()
    if re.fullmatch(r"iphone", first, re.I) or first.lower().startswith("iphone"):
        return "Apple"
    return first or "Otros"

def wc_find_or_create_tag(wc: WCClient, name: str) -> int:
    # search tag
    page = 1
    while True:
        tags = wc.get("/products/tags", params={"search": name, "per_page": 100, "page": page})
        if not tags:
            break
        for t in tags:
            if t.get("name") == name:
                return int(t["id"])
        if len(tags) < 100:
            break
        page += 1
    created = wc.post("/products/tags", {"name": name})
    return int(created["id"])

def wc_find_category(wc: WCClient, name: str, parent: int = 0) -> Optional[Dict]:
    page = 1
    while True:
        cats = wc.get("/products/categories", params={"search": name, "per_page": 100, "page": page})
        if not cats:
            break
        for c in cats:
            if (c.get("name") == name) and int(c.get("parent") or 0) == int(parent):
                return c
        if len(cats) < 100:
            break
        page += 1
    return None

def wc_get_or_create_category(wc: WCClient, name: str, parent: int = 0) -> int:
    found = wc_find_category(wc, name, parent=parent)
    if found:
        return int(found["id"])
    created = wc.post("/products/categories", {"name": name, "parent": int(parent)})
    return int(created["id"])

def wc_get_category_image_id(wc: WCClient, cat_id: int) -> Optional[int]:
    cat = wc.get(f"/products/categories/{cat_id}")
    image = cat.get("image")
    if image and image.get("id"):
        return int(image["id"])
    return None

def wp_upload_media(wp_url: str, wp_user: str, wp_app_pass: str, img_bytes: bytes, filename: str, timeout: int) -> int:
    """
    Upload media via WP REST (/wp-json/wp/v2/media). Needs Application Password.
    Returns media ID.
    """
    media_endpoint = wp_url.rstrip("/") + "/wp-json/wp/v2/media"
    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "Content-Type": "image/jpeg",
        "User-Agent": "Mozilla/5.0",
    }
    r = requests.post(
        media_endpoint,
        headers=headers,
        data=img_bytes,
        auth=(wp_user, wp_app_pass),
        timeout=timeout,
    )
    r.raise_for_status()
    data = r.json()
    return int(data["id"])

def download_and_resize_to_jpg(sess: requests.Session, img_url: str, timeout: int) -> bytes:
    r = sess.get(img_url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    img = Image.open(io.BytesIO(r.content)).convert("RGB")

    # Make square crop center, then resize
    w, h = img.size
    if w != h:
        side = min(w, h)
        left = (w - side) // 2
        top = (h - side) // 2
        img = img.crop((left, top, left + side, top + side))
    img = img.resize((TARGET_IMG_SIZE, TARGET_IMG_SIZE))

    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=90)
    return buf.getvalue()

def ensure_category_image(
    wc: WCClient,
    wp_url: str,
    wp_user: str,
    wp_app_pass: str,
    sess: requests.Session,
    cat_id: int,
    source_image_url: str,
    timeout: int,
    logger,
) -> Optional[int]:
    """
    If category already has image, return its ID.
    If not, upload resized 600x600 image to WP and set as category image.
    Retries 10x with 15s backoff as requested.

    NOTE: If WP_USER/WP_APP_PASS are not set, we won't upload media.
    """
    existing_id = wc_get_category_image_id(wc, cat_id)
    if existing_id:
        return existing_id

    if not source_image_url:
        return None

    if not (wp_user and wp_app_pass):
        logger("⚠️  Sin WP_APP_PASS: no se puede subir imagen de categoría. Se continuará sin imagen.")
        return None

    for attempt in range(1, 11):
        try:
            img_bytes = download_and_resize_to_jpg(sess, source_image_url, timeout=timeout)
            filename = f"cat_{cat_id}_{TARGET_IMG_SIZE}.jpg"
            media_id = wp_upload_media(wp_url, wp_user, wp_app_pass, img_bytes, filename, timeout=timeout)
            # set cat image
            wc.put(f"/products/categories/{cat_id}", {"image": {"id": media_id}})
            return media_id
        except Exception as e:
            logger(f"⚠️  Imagen categoría fallo intento {attempt}/10: {e}")
            if attempt < 10:
                time.sleep(15)

    return None

def wc_get_all_products_with_tag(wc: WCClient, tag_id: int) -> List[Dict]:
    out = []
    page = 1
    while True:
        prods = wc.get("/products", params={"tag": tag_id, "per_page": 100, "page": page, "status": "any"})
        if not prods:
            break
        out.extend(prods)
        if len(prods) < 100:
            break
        page += 1
    return out

def wc_find_existing_product(wc: WCClient, offer: Offer, tag_id: int) -> Optional[Dict]:
    """
    Match strategy (same as other importers):
      - Scope only products tagged with TAG_IMPORT
      - Compare ACF meta: nombre (title) + memoria + capacidad + fuente + importado_de
    """
    products = wc.get("/products", params={"tag": tag_id, "search": offer.name_clean, "per_page": 100, "status": "any"})
    for p in products or []:
        if format_product_title(p.get("name", "")) != offer.name_clean:
            continue
        meta = p.get("meta_data") or []
        if (_meta_get(meta, ACF_MEMORIA) or "").upper() != offer.memoria.upper():
            continue
        if (_meta_get(meta, ACF_CAPACIDAD) or "").upper() != offer.capacidad.upper():
            continue
        if (_meta_get(meta, ACF_FUENTE) or "").strip().lower() != FUENTE_ACF:
            continue
        if (_meta_get(meta, ACF_IMPORTADO_DE) or "").strip() != IMPORTADO_DE:
            continue
        return p
    return None

def build_product_payload(
    offer: Offer,
    subcat_id: int,
    tag_id: int,
    image_id: Optional[int],
    status: str,
    wp_permalink_short: str = "",
) -> Dict:
    meta: List[Dict] = []
    _meta_set(meta, ACF_MEMORIA, offer.memoria)
    _meta_set(meta, ACF_CAPACIDAD, offer.capacidad)
    _meta_set(meta, ACF_PRECIO_ACTUAL, f"{offer.precio_actual_eur:.2f}€")
    _meta_set(meta, ACF_PRECIO_ORIGIAL, f"{offer.precio_original_eur:.2f}€")
    _meta_set(meta, ACF_CODIGO_DESCUENTO, offer.codigo_descuento or CUPON_DEFAULT)
    _meta_set(meta, ACF_FUENTE, FUENTE_ACF)
    _meta_set(meta, ACF_IMAGEN_PRODUCTO, offer.image_url or "")
    _meta_set(meta, ACF_ENVIADO_DESDE, offer.enviado_desde)
    _meta_set(meta, ACF_ENVIADO_DESDE_TG, offer.enviado_desde_tg)
    _meta_set(meta, ACF_VERSION, offer.version)
    _meta_set(meta, ACF_IMPORTADO_DE, IMPORTADO_DE)
    _meta_set(meta, ACF_FECHA, _dt.datetime.now().strftime("%d/%m/%Y"))
    _meta_set(meta, ACF_URL_POST_ACORTADA, wp_permalink_short or "")

    payload = {
        "name": offer.name_clean,
        "type": "external",
        "status": status,
        "sku": compute_sku(offer),
        "external_url": offer.url_oferta or offer.url,
        "button_text": "Ver oferta",
        "categories": [{"id": int(subcat_id)}],
        "tags": [{"id": int(tag_id)}],
        "meta_data": meta,
        # Prices for Woo sorting/filtering
        "regular_price": f"{offer.precio_original_eur:.2f}",
        "sale_price": f"{offer.precio_actual_eur:.2f}",
    }
    if image_id:
        payload["images"] = [{"id": int(image_id)}]

    return payload

def product_needs_update(existing: Dict, offer: Offer) -> Tuple[bool, List[str]]:
    changes = []
    # Compare prices
    reg = safe_float_from_price(existing.get("regular_price"))
    sale = safe_float_from_price(existing.get("sale_price"))
    if reg is None or abs(reg - offer.precio_original_eur) > 0.009:
        changes.append(f"regular_price {existing.get('regular_price')} -> {offer.precio_original_eur:.2f}")
    if sale is None or abs(sale - offer.precio_actual_eur) > 0.009:
        changes.append(f"sale_price {existing.get('sale_price')} -> {offer.precio_actual_eur:.2f}")

    # Compare coupon meta
    meta = existing.get("meta_data") or []
    if (_meta_get(meta, ACF_CODIGO_DESCUENTO) or "").strip() != (offer.codigo_descuento or CUPON_DEFAULT):
        changes.append("codigo_de_descuento")

    # Compare external URL
    if (existing.get("external_url") or "").strip() != (offer.url_oferta or offer.url):
        changes.append("external_url")

    # Compare image url meta
    if (offer.image_url or "") and (_meta_get(meta, ACF_IMAGEN_PRODUCTO) or "") != (offer.image_url or ""):
        changes.append("imagen_producto")

    return (len(changes) > 0), changes


# ============================================================
# LOGS
# ============================================================

def print_offer_log(offer: Offer, url_no_aff: str, url_my_aff: str, logger):
    logger(f"Detectado {offer.name_clean}")
    logger(f"1) Nombre: {offer.name_clean}")
    logger(f"2) Memoria: {offer.memoria}")
    logger(f"3) Capacidad: {offer.capacidad}")
    logger(f"4) Versión: {offer.version}")
    logger(f"5) Fuente: {offer.fuente}")
    logger(f"6) Precio actual: {offer.precio_actual_eur:.2f}€")
    logger(f"7) Precio original: {offer.precio_original_eur:.2f}€")
    logger(f"8) Código de descuento: {offer.codigo_descuento or CUPON_DEFAULT}")
    logger(f"9) Version: {offer.version}")
    logger(f"10) URL Imagen: {offer.image_url}")
    logger(f"11) Enlace Importado: {offer.url}")
    logger(f"12) Enlace Expandido: {offer.url}")
    logger(f"13) URL importada sin afiliado: {url_no_aff}")
    logger(f"14) URL sin acortar con mi afiliado: {url_my_aff}")
    logger(f"15) URL acortada con mi afiliado: {offer.url_oferta}")
    logger(f"16) Enviado desde: {offer.enviado_desde}")
    logger(f"17) URL post acortada: {offer.url_post_acortada}")
    logger(f"18) Encolado para comparar con base de datos...")
    logger("-" * 60)

def jsonl_write(fp, obj: Dict):
    fp.write(json.dumps(obj, ensure_ascii=False) + "\n")
    fp.flush()


# ============================================================
# RUN
# ============================================================

def run_import(args) -> int:
    def log(msg: str):
        print(msg, flush=True)

    # Read credentials from args/env
    wp_url = (args.wp_url or os.environ.get("WP_URL") or "").strip()
    wc_key = (args.wc_key or os.environ.get("WP_KEY") or os.environ.get("WC_KEY") or "").strip()
    wc_secret = (args.wc_secret or os.environ.get("WP_SECRET") or os.environ.get("WC_SECRET") or "").strip()
    wp_user = (args.wp_user or os.environ.get("WP_USER") or "").strip()
    wp_app_pass = (args.wp_app_pass or os.environ.get("WP_APP_PASS") or "").strip()

    if not wp_url or not wc_key or not wc_secret:
        log("❌ Faltan credenciales WooCommerce. Debes pasar --wp-url/--wc-key/--wc-secret o exportar WP_URL/WP_KEY/WP_SECRET.")
        return 2

    can_upload_media = bool(wp_user and wp_app_pass)

    if not args.dry_run and not can_upload_media:
        log("⚠️  WP_USER/WP_APP_PASS no configurados: se crearán/actualizarán productos, pero NO se subirán imágenes (categoría/producto).")


    # Clients
    wc = WCClient(wp_url, wc_key, wc_secret, timeout=args.timeout)
    sess = requests.Session()
    sess.headers.update({"User-Agent": "Mozilla/5.0"})

    # Preflight
    try:
        wc.get("/products", params={"per_page": 1})
    except Exception as e:
        log(f"❌ No puedo conectar a WooCommerce: {e}")
        return 2

    tag_id = wc_find_or_create_tag(wc, TAG_IMPORT)

    # Listing
    log(f"📌 PowerPlanet: Escaneando SOLO: {LIST_URL}")
    list_html = fetch_html(sess, LIST_URL, timeout=args.timeout)
    product_urls = extract_listing_product_urls(list_html)
    log(f"📌 PowerPlanet: URLs detectadas = {len(product_urls)}")

    if args.max_products and args.max_products > 0:
        product_urls = product_urls[: args.max_products]
        log(f"🔎 Limitado a --max-products {args.max_products}. (Obsoletos NO se eliminan en modo limitado)")

    jsonl_fp = open(args.jsonl, "w", encoding="utf-8") if args.jsonl else None

    # Summary
    summary_creados = []
    summary_eliminados = []
    summary_actualizados = []
    summary_ignorados = []

    current_skus: Set[str] = set()

    for idx, url in enumerate(product_urls, start=1):
        try:
            offer = parse_offer_detail(sess, url, timeout=args.timeout)

            # Affiliate link (PowerPlanet -> direct + short)
            url_no_aff = offer.url
            url_my_aff = offer.url
            offer.url_oferta = shorten_isgd(sess, url_my_aff, timeout=args.timeout)

            # Log info
            print_offer_log(offer, url_no_aff, url_my_aff, log)

            # JSONL record
            if jsonl_fp:
                jsonl_write(jsonl_fp, dataclasses.asdict(offer))

            # SKU
            sku = compute_sku(offer)
            current_skus.add(sku)

            if args.dry_run:
                summary_creados.append({"nombre": offer.name_clean, "id": "DRY"})
                continue

            # Categories
            brand = get_brand_from_name(offer.name_clean)
            cat_brand_id = wc_get_or_create_category(wc, brand, parent=0)
            subcat_name = offer.name_clean
            cat_sub_id = wc_get_or_create_category(wc, subcat_name, parent=cat_brand_id)

            # Ensure subcategory image (and use it for product)
            image_id = ensure_category_image(
                wc=wc,
                wp_url=wp_url,
                wp_user=wp_user,
                wp_app_pass=wp_app_pass,
                sess=sess,
                cat_id=cat_sub_id,
                source_image_url=offer.image_url,
                timeout=args.timeout,
                logger=log,
            )

            existing = wc_find_existing_product(wc, offer, tag_id=tag_id)
            if not existing:
                payload = build_product_payload(
                    offer=offer,
                    subcat_id=cat_sub_id,
                    tag_id=tag_id,
                    image_id=image_id,
                    status=args.status,
                )

                created = wc.post("/products", payload)
                pid = int(created.get("id"))
                # Shorten product permalink and store in ACF
                permalink = created.get("permalink") or ""
                if permalink:
                    offer.url_post_acortada = shorten_isgd(sess, permalink, timeout=args.timeout)
                    wc.put(f"/products/{pid}", {"meta_data": [{"key": ACF_URL_POST_ACORTADA, "value": offer.url_post_acortada}]})

                summary_creados.append({"nombre": offer.name_clean, "id": pid})
            else:
                needs, changes = product_needs_update(existing, offer)
                if not needs:
                    summary_ignorados.append({"nombre": offer.name_clean, "id": int(existing.get("id"))})
                else:
                    pid = int(existing.get("id"))
                    payload = build_product_payload(
                        offer=offer,
                        subcat_id=cat_sub_id,
                        tag_id=tag_id,
                        image_id=image_id,
                        status=args.status,
                        wp_permalink_short=_meta_get(existing.get("meta_data") or [], ACF_URL_POST_ACORTADA) or "",
                    )
                    updated = wc.put(f"/products/{pid}", payload)
                    summary_actualizados.append({"nombre": offer.name_clean, "id": pid, "cambios": changes})

            time.sleep(args.sleep)

        except Exception as e:
            log(f"⚠️  Saltado ({url}): {e}")
            continue

    # Obsoletes: only if not limited and not dry-run
    if (not args.dry_run) and (not args.max_products or args.max_products == 0):
        try:
            existing_products = wc_get_all_products_with_tag(wc, tag_id)
            for p in existing_products:
                sku = (p.get("sku") or "").strip()
                if not sku:
                    continue
                if sku not in current_skus:
                    pid = int(p.get("id"))
                    # Soft delete (trash)
                    wc.delete(f"/products/{pid}", params={"force": bool(args.force_delete_obsoletes)})
                    summary_eliminados.append({"nombre": p.get("name"), "id": pid})
        except Exception as e:
            log(f"⚠️  Fallo gestionando obsoletos: {e}")

    if jsonl_fp:
        jsonl_fp.close()

    # Final summary
    hoy_fmt = _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log("\n============================================================")
    log(f"📋 RESUMEN DE EJECUCIÓN ({hoy_fmt})")
    log("============================================================")
    log(f"\na) ARTICULOS CREADOS: {len(summary_creados)}")
    for item in summary_creados:
        log(f"- {item['nombre']} (ID: {item['id']})")
    log(f"\nb) ARTICULOS ELIMINADOS (OBSOLETOS): {len(summary_eliminados)}")
    for item in summary_eliminados:
        log(f"- {item['nombre']} (ID: {item['id']})")
    log(f"\nc) ARTICULOS ACTUALIZADOS: {len(summary_actualizados)}")
    for item in summary_actualizados:
        log(f"- {item['nombre']} (ID: {item['id']}): {', '.join(item['cambios'])}")
    log(f"\nd) ARTICULOS IGNORADOS (SIN CAMBIOS): {len(summary_ignorados)}")
    for item in summary_ignorados:
        log(f"- {item['nombre']} (ID: {item['id']})")
    log("============================================================")

    return 0


def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Importador PowerPlanet (móviles más vendidos) -> WooCommerce/ACF")
    ap.add_argument("--wp-url", default=os.environ.get("WP_URL", ""), help="URL WordPress (https://dominio)")
    ap.add_argument("--wc-key", default=os.environ.get("WP_KEY", "") or os.environ.get("WC_KEY", ""), help="WooCommerce consumer_key")
    ap.add_argument("--wc-secret", default=os.environ.get("WP_SECRET", "") or os.environ.get("WC_SECRET", ""), help="WooCommerce consumer_secret")
    ap.add_argument("--wp-user", default=os.environ.get("WP_USER", ""), help="WP user (para subir media)")
    ap.add_argument("--wp-app-pass", default=os.environ.get("WP_APP_PASS", ""), help="WP Application Password (para subir media)")
    ap.add_argument("--max-products", type=int, default=0, help="0=sin límite")
    ap.add_argument("--sleep", type=float, default=0.8, help="Pausa entre productos")
    ap.add_argument("--timeout", type=int, default=60, help="Timeout HTTP")
    ap.add_argument("--dry-run", action="store_true", help="No crea/actualiza/borrar; solo logs")
    ap.add_argument("--status", choices=["publish", "draft", "pending", "private"], default="publish")
    ap.add_argument("--force-delete-obsoletes", action="store_true", help="Borra obsoletos permanentemente (force=true). Sin flag, se envían a papelera.")
    ap.add_argument("--jsonl", default="", help="Escribe un JSONL con los productos detectados")
    return ap


def main():
    args = build_arg_parser().parse_args()
    rc = run_import(args)
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
