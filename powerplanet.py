#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PowerPlanet -> WooCommerce importador (solo "móviles más vendidos").

Características clave
- Escanea SOLO: https://www.powerplanetonline.com/es/moviles-mas-vendidos
- No navega a categorías (moviles-xiaomi, moviles-baratos, etc.)
- Limpia el nombre (quita colores/sufijos)
- Extrae RAM/ROM (incluye iPhone: RAM por mapping y ROM por URL)
- Precios sin decimales (truncate) en ACF/meta y también en WC price fields
- Crea/actualiza productos en WooCommerce (tipo external)
- Crea categorías:
  - Principal = marca (1ª palabra, iPhone => Apple)
  - Subcategoría = nombre completo limpio
- Imagen:
  - Usa imagen de la subcategoría como imagen del producto
  - Si la subcategoría no tiene imagen, intenta subirla (10 intentos, 15s) usando WP_USER + WP_APP_PASS
  - Si no hay WP_USER/WP_APP_PASS o hay 401/403, se desactiva subida de imágenes y se sigue importando
- JSONL opcional (--jsonl) para depurar

Dependencias (pip):
  requests beautifulsoup4 pillow woocommerce

Secrets / ENV (recomendado en GitHub Actions):
  WP_URL      -> https://tudominio.com
  WP_KEY      -> consumer key WooCommerce
  WP_SECRET   -> consumer secret WooCommerce
  WP_USER     -> (opcional) usuario WP con permisos de subir medios
  WP_APP_PASS -> (opcional) Application Password de ese usuario

CLI:
  python powerplanet.py --max-products 0 --sleep 0.7 --timeout 25 --status publish
"""

from __future__ import annotations

import argparse
import base64
import datetime as _dt
import json
import os
import re
import time
import unicodedata
from dataclasses import dataclass, asdict
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

from woocommerce import API

try:
    from PIL import Image  # type: ignore
except Exception:
    Image = None  # type: ignore


LIST_URL = "https://www.powerplanetonline.com/es/moviles-mas-vendidos"
BASE_DOMAIN = "https://www.powerplanetonline.com"
SOURCE_NAME = "powerplanetonline"
IMPORTADO_DE = "https://www.powerplanetonline.com/"
DEFAULT_VERSION = "Global"
DEFAULT_COUPON = "OFERTA PROMO"

# --------------------------
# RAM iPhone (según tu mapping)
# --------------------------
IPHONE_RAM_MAP = [
    ("iphone 17 pro max", "12GB"),
    ("iphone 17 pro", "12GB"),
    ("iphone 17 air", "12GB"),
    ("iphone air", "12GB"),
    ("iphone 17", "8GB"),
    ("iphone 16 pro max", "8GB"),
    ("iphone 16 pro", "8GB"),
    ("iphone 16 plus", "8GB"),
    ("iphone 16e", "8GB"),
    ("iphone 16", "8GB"),
    ("iphone 15 pro max", "8GB"),
    ("iphone 15 pro", "8GB"),
    ("iphone 15 plus", "6GB"),
    ("iphone 15", "6GB"),
    ("iphone 14 pro max", "6GB"),
    ("iphone 14 pro", "6GB"),
    ("iphone 14 plus", "6GB"),
    ("iphone 14", "6GB"),
    ("iphone 13 pro max", "6GB"),
    ("iphone 13 pro", "6GB"),
    ("iphone 13 mini", "4GB"),
    ("iphone 13", "4GB"),
    ("iphone 12 pro max", "6GB"),
    ("iphone 12 pro", "6GB"),
    ("iphone 12 mini", "4GB"),
    ("iphone 12", "4GB"),
]

COLOR_SUFFIXES = [
    "negro", "blanco", "azul", "verde", "rojo", "gris", "plata", "dorado", "oro",
    "morado", "rosa", "amarillo", "naranja", "turquesa", "lavanda", "obsidiana",
    "grafito", "purpura", "púrpura", "midnight", "starlight", "space gray", "space grey",
    "titanio", "titanium", "natural", "subzero", "transparente",
]

TRAILING_PHRASES = [
    "version internacional", "versión internacional", "internacional",
    "renovado", "reacondicionado", "refurbished",
    "estado excelente", "estado bueno", "estado muy bueno",
]


# --------------------------
# Utilidades
# --------------------------
def normalize_text(s: str) -> str:
    s = s or ""
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = s.lower().strip()
    s = re.sub(r"\s+", " ", s)
    return s


def abs_url(href: str) -> str:
    href = (href or "").strip()
    if not href:
        return ""
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        return BASE_DOMAIN + href
    return BASE_DOMAIN + "/" + href


def to_int_no_decimals(value: Any) -> int:
    """
    Convierte '249.99€' -> 249 (truncate). Evita float para no tener 998.999 -> 998.
    """
    if value is None:
        return 0
    if isinstance(value, int):
        return value
    # si ya es Decimal
    if isinstance(value, Decimal):
        try:
            return int(value)
        except Exception:
            return 0
    s = str(value).strip()
    s = s.replace("€", "").replace(" ", "").replace(",", ".")
    s = re.sub(r"[^\d.]+", "", s)
    if not s:
        return 0
    try:
        return int(Decimal(s))
    except (InvalidOperation, ValueError):
        # fallback bruto
        try:
            return int(float(s))
        except Exception:
            return 0


def shorten_isgd(url: str, timeout: int = 25) -> str:
    try:
        r = requests.get("https://is.gd/create.php", params={"format": "simple", "url": url}, timeout=timeout)
        if r.status_code == 200:
            t = r.text.strip()
            if t.startswith("http"):
                return t
    except Exception:
        pass
    return url


def clean_name(raw: str) -> str:
    """
    Quita RAM/ROM y colores/sufijos del final.
    """
    s = (raw or "").strip()
    # quitar RAM/ROM del nombre si vienen
    s = re.sub(r"\b\d+\s*GB\b", "", s, flags=re.I)
    s = re.sub(r"\b\d+\s*TB\b", "", s, flags=re.I)
    s = re.sub(r"\s+", " ", s).strip()

    # quitar sufijos tipo "- Versión Internacional"
    s_norm = normalize_text(s)
    for ph in TRAILING_PHRASES:
        phn = normalize_text(ph)
        if s_norm.endswith(phn):
            s = s[: -(len(ph))].strip(" -–—\t")
            s_norm = normalize_text(s)

    # cortar todo lo que venga tras " - "
    s = re.sub(r"\s*-\s*.*$", "", s).strip()

    # quitar colores al final (varios tokens)
    words = s.split()
    colors_norm = {normalize_text(c) for c in COLOR_SUFFIXES}
    while words:
        tail = normalize_text(words[-1])
        if tail in colors_norm:
            words.pop()
        else:
            break
    s = " ".join(words).strip()
    s = re.sub(r"\s+", " ", s).strip()
    return s or raw


def is_iphone(s: str) -> bool:
    return "iphone" in normalize_text(s)


def iphone_ram_for(s: str) -> Optional[str]:
    t = normalize_text(s)
    for key, ram in IPHONE_RAM_MAP:
        if key in t:
            return ram
    return None


def extract_rom_from_slug(url: str) -> Optional[str]:
    """
    De '.../iphone-16e-128gb-blanco...' -> '128GB'
    """
    slug = normalize_text(url)
    m = re.search(r"(\d{2,4})\s*gb", slug)
    if m:
        return f"{m.group(1)}GB"
    m2 = re.search(r"(\d)\s*tb", slug)
    if m2:
        return f"{m2.group(1)}TB"
    return None


def split_ram_rom(text: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Detecta RAM/ROM en '8GB/256GB', '8GB 256GB', '12GB-512GB', '24GB 1TB'...
    """
    t = normalize_text(text).replace(" ", "")
    # 8gb/256gb o 8gb-256gb
    m = re.search(r"(\d{1,2})gb[\/\-](\d{2,4})gb", t, flags=re.I)
    if m:
        return f"{m.group(1)}GB", f"{m.group(2)}GB"
    # 8gb256gb (sin separador)
    m = re.search(r"(\d{1,2})gb(\d{2,4})gb", t, flags=re.I)
    if m:
        return f"{m.group(1)}GB", f"{m.group(2)}GB"
    # 24gb1tb
    m = re.search(r"(\d{1,2})gb(\d)tb", t, flags=re.I)
    if m:
        return f"{m.group(1)}GB", f"{m.group(2)}TB"

    # fallback: tomar 1º GB pequeño como RAM y el mayor como ROM si hay 2+
    nums = [int(x) for x in re.findall(r"(\d{1,4})gb", t)]
    if len(nums) >= 2:
        nums_sorted = sorted(nums)
        ram = nums_sorted[0]
        rom = nums_sorted[-1]
        if ram != rom:
            return f"{ram}GB", f"{rom}GB"

    # solo ROM
    m = re.search(r"(\d{2,4})gb", t, flags=re.I)
    if m:
        return None, f"{m.group(1)}GB"
    m = re.search(r"(\d)tb", t, flags=re.I)
    if m:
        return None, f"{m.group(1)}TB"
    return None, None


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/121.0 Safari/537.36"
        }
    )
    return s


def extract_product_urls(list_html: str) -> List[str]:
    """
    Extrae URLs de producto desde el HTML del listado.
    Filtra para no ir a /es/moviles-*, /es/smartphones, etc.
    """
    soup = BeautifulSoup(list_html, "html.parser")
    urls: List[str] = []

    BAD_PREFIXES = (
        "/es/moviles-",
        "/es/smartphones",
        "/es/telefonos-moviles",
        "/es/moviles",
        "/es/ofertas",
        "/es/reacondicionados",
    )

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href:
            continue
        # solo /es/...
        if not href.startswith("/es/"):
            continue
        # evitar categorías
        if href.startswith(BAD_PREFIXES):
            continue
        u = abs_url(href)
        # heurística: slug de producto (sin más /)
        path = u.split("powerplanetonline.com")[-1]
        if path.count("/") != 2:  # /es/slug
            continue
        slug = path.split("/es/")[-1]
        if not slug or len(slug) < 8:
            continue
        urls.append(u)

    # dedupe preservando orden
    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def extract_image_url(soup: BeautifulSoup) -> str:
    og = soup.select_one('meta[property="og:image"]')
    if og and og.get("content"):
        return abs_url(og["content"])

    # buscar imagen principal por clases comunes
    for sel in ["img#zoom", ".product-image img", ".product-detail-image img", "img.product-image"]:
        # bs4 select devuelve lista; usamos el primero
        el = soup.select_one(sel)
        if el:
            src = el.get("src") or el.get("data-src") or el.get("data-original")
            if src:
                return abs_url(src)

    # fallback: cualquier img con cdnassets
    img = soup.find("img", src=re.compile("cdnassets", re.I))
    if img and img.get("src"):
        return abs_url(img["src"])
    return ""


def extract_prices(soup: BeautifulSoup) -> Tuple[int, int]:
    """
    Devuelve (precio_actual, precio_original) como int sin decimales (truncate).
    """
    # 1) buscar en <del> para precio antiguo
    def _first_amount(text: str) -> Optional[str]:
        m = re.search(r"(\d{1,4}(?:[.,]\d{2})?)\s*€", text)
        if m:
            return m.group(1)
        return None

    old_candidates = soup.select("del, .old-price, .price-old, .precio-anterior, .old")
    old_val = None
    for el in old_candidates:
        v = _first_amount(el.get_text(" ", strip=True))
        if v:
            old_val = v
            break

    cur_candidates = soup.select("ins, .current-price, .price, .precio, .product-price")
    cur_val = None
    for el in cur_candidates:
        # evita que coja el mismo <del> dentro
        if el.name == "del":
            continue
        v = _first_amount(el.get_text(" ", strip=True))
        if v:
            cur_val = v
            break

    # 2) fallback: todas las cantidades del texto
    if not cur_val:
        txt = soup.get_text(" ", strip=True)
        amounts = re.findall(r"(\d{1,4}(?:[.,]\d{2})?)\s*€", txt)
        amounts = [a.replace(",", ".") for a in amounts]
        # dedupe preservando
        uniq = []
        seen = set()
        for a in amounts:
            if a not in seen:
                seen.add(a)
                uniq.append(a)
        if uniq:
            # Heurística: current=min, old=max si hay descuento
            ints = [to_int_no_decimals(a) for a in uniq]
            if ints:
                cur_i = min(ints)
                old_i = max(ints)
                if old_i < cur_i:
                    old_i = cur_i
                return cur_i, old_i

    cur_i = to_int_no_decimals(cur_val)
    old_i = to_int_no_decimals(old_val) if old_val else cur_i
    if old_i < cur_i:
        old_i = cur_i
    return cur_i, old_i


def parse_product_page(sess: requests.Session, url: str, timeout: int) -> Tuple[str, int, int, str]:
    r = sess.get(url, timeout=timeout)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    h1 = soup.select_one("h1")
    raw_name = h1.get_text(" ", strip=True) if h1 else ""
    img_url = extract_image_url(soup)
    p_cur, p_old = extract_prices(soup)
    return raw_name, p_cur, p_old, img_url


def brand_from_name(cleaned_name: str) -> str:
    if is_iphone(cleaned_name):
        return "Apple"
    parts = cleaned_name.split()
    if not parts:
        return "Otros"
    return parts[0]


def build_affiliate_url(url: str, affiliate_query: str) -> str:
    """
    Para PowerPlanet normalmente será la misma URL.
    Si pasas --affiliate-query, se añade si la URL no tiene ya query.
    """
    if not affiliate_query:
        return url
    if "?" in url:
        return url
    if affiliate_query.startswith("?"):
        return url + affiliate_query
    return url + "?" + affiliate_query


@dataclass
class ProductData:
    nombre: str
    memoria: str
    capacidad: str
    version: str
    fuente: str
    precio_actual: int
    precio_original: int
    codigo_de_descuento: str
    url_imagen: str
    enlace_importado: str
    enlace_expandido: str
    url_importada_sin_afiliado: str
    url_sin_acortar_con_mi_afiliado: str
    url_oferta_sin_acortar: str
    url_oferta: str
    enviado_desde: str = "España"
    url_post_acortada: str = ""

    def base_key(self) -> str:
        return normalize_text(f"{self.nombre}|{self.memoria}|{self.capacidad}|{self.fuente}")


def build_product_data(sess: requests.Session, product_url: str, timeout: int, affiliate_query: str) -> Optional[ProductData]:
    raw_name, price_cur, price_old, img_url = parse_product_page(sess, product_url, timeout=timeout)
    cleaned_name = clean_name(raw_name)

    # RAM/ROM desde nombre y/o url
    ram, rom = split_ram_rom(raw_name)
    if not (ram and rom):
        ram2, rom2 = split_ram_rom(product_url)
        ram = ram or ram2
        rom = rom or rom2

    # iPhone: RAM mapping + ROM desde URL
    if is_iphone(cleaned_name) or is_iphone(product_url):
        if not ram:
            ram = iphone_ram_for(cleaned_name) or iphone_ram_for(product_url)
        if not rom:
            rom = extract_rom_from_slug(product_url)

    if not ram or not rom:
        print(f"⚠️  Saltado ({product_url}): Producto sin RAM/ROM detectables (no se importa)")
        return None

    url_importada_sin_afiliado = product_url
    url_sin_acortar_con_mi_afiliado = build_affiliate_url(product_url, affiliate_query)
    url_oferta_sin_acortar = url_sin_acortar_con_mi_afiliado
    url_oferta = shorten_isgd(url_sin_acortar_con_mi_afiliado, timeout=timeout)

    return ProductData(
        nombre=cleaned_name,
        memoria=ram,
        capacidad=rom,
        version=DEFAULT_VERSION,
        fuente=SOURCE_NAME,
        precio_actual=int(price_cur),
        precio_original=int(price_old),
        codigo_de_descuento=DEFAULT_COUPON,
        url_imagen=img_url,
        enlace_importado=url_importada_sin_afiliado,
        enlace_expandido=url_importada_sin_afiliado,
        url_importada_sin_afiliado=url_importada_sin_afiliado,
        url_sin_acortar_con_mi_afiliado=url_sin_acortar_con_mi_afiliado,
        url_oferta_sin_acortar=url_oferta_sin_acortar,
        url_oferta=url_oferta,
        enviado_desde="España",
    )


def print_required_logs(pd: ProductData) -> None:
    print(f"Detectado {pd.nombre}")
    print(f"1) Nombre: {pd.nombre}")
    print(f"2) Memoria: {pd.memoria}")
    print(f"3) Capacidad: {pd.capacidad}")
    print(f"4) Versión: {pd.version}")
    print(f"5) Fuente: {pd.fuente}")
    print(f"6) Precio actual: {pd.precio_actual}€")
    print(f"7) Precio original: {pd.precio_original}€")
    print(f"8) Código de descuento: {pd.codigo_de_descuento}")
    print(f"9) Version: {pd.version}")
    print(f"10) URL Imagen: {pd.url_imagen}")
    print(f"11) Enlace Importado: {pd.enlace_importado}")
    print(f"12) Enlace Expandido: {pd.enlace_expandido}")
    print(f"13) URL importada sin afiliado: {pd.url_importada_sin_afiliado}")
    print(f"14) URL sin acortar con mi afiliado: {pd.url_sin_acortar_con_mi_afiliado}")
    print(f"15) URL acortada con mi afiliado: {pd.url_oferta}")
    print(f"16) Enviado desde: {pd.enviado_desde}")
    print(f"17) URL post acortada: {pd.url_post_acortada}")
    print("18) Encolado para comparar con base de datos...")


# --------------------------
# WooCommerce / WordPress
# --------------------------
def get_wcapi(timeout: int) -> API:
    wp_url = os.environ.get("WP_URL", "").strip()
    wc_key = os.environ.get("WP_KEY", "").strip()
    wc_secret = os.environ.get("WP_SECRET", "").strip()

    if not wp_url or not wc_key or not wc_secret:
        raise SystemExit("ERROR: Faltan secretos ENV: WP_URL, WP_KEY, WP_SECRET")

    return API(
        url=wp_url,
        consumer_key=wc_key,
        consumer_secret=wc_secret,
        version="wc/v3",
        timeout=int(timeout),
    )


def wp_upload_media(image_bytes: bytes, filename: str, timeout: int, disable: Dict[str, bool]) -> Optional[int]:
    """
    Sube media a /wp-json/wp/v2/media con Basic Auth (WP_USER + WP_APP_PASS).
    Devuelve media_id o None. En 401/403 desactiva subida global.
    """
    if disable.get("disabled"):
        return None

    wp_url = os.environ.get("WP_URL", "").strip()
    wp_user = os.environ.get("WP_USER", "").strip()
    wp_app_pass = os.environ.get("WP_APP_PASS", "").strip()
    if not wp_url or not wp_user or not wp_app_pass:
        return None

    media_url = wp_url.rstrip("/") + "/wp-json/wp/v2/media"
    auth = base64.b64encode(f"{wp_user}:{wp_app_pass}".encode("utf-8")).decode("utf-8")
    headers = {
        "Authorization": f"Basic {auth}",
        "Content-Disposition": f'attachment; filename="{filename}"',
        "Content-Type": "image/jpeg",
        "User-Agent": "powerplanet-importer/1.0",
    }
    r = requests.post(media_url, headers=headers, data=image_bytes, timeout=timeout)
    if r.status_code in (401, 403):
        disable["disabled"] = True
        print(f"⚠️  Media upload deshabilitado (HTTP {r.status_code}). Revisa WP_USER/WP_APP_PASS.")
        return None
    r.raise_for_status()
    j = r.json()
    return int(j.get("id")) if j.get("id") else None


def download_and_resize_image(url: str, timeout: int) -> Optional[bytes]:
    if not url:
        return None
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    data = r.content
    if Image is None:
        # sin Pillow: devolver tal cual
        return data

    try:
        from io import BytesIO
        im = Image.open(BytesIO(data)).convert("RGB")
        im = im.resize((600, 600))
        out = BytesIO()
        im.save(out, format="JPEG", quality=90)
        return out.getvalue()
    except Exception:
        return data


def ensure_category(wcapi: API, name: str, parent_id: int = 0) -> Dict[str, Any]:
    """
    Busca por nombre exacto en cache local o crea categoría.
    """
    # Búsqueda por nombre
    # Nota: Woo API no da filtro "name exact", usamos search y filtramos.
    res = wcapi.get("products/categories", params={"search": name, "per_page": 100}).json()
    if isinstance(res, dict) and res.get("message"):
        raise RuntimeError(res.get("message"))

    for c in res:
        if normalize_text(c.get("name", "")) == normalize_text(name) and int(c.get("parent") or 0) == int(parent_id):
            return c

    payload: Dict[str, Any] = {"name": name}
    if parent_id:
        payload["parent"] = parent_id
    created = wcapi.post("products/categories", payload).json()
    if isinstance(created, dict) and created.get("message"):
        raise RuntimeError(created.get("message"))
    return created


def ensure_category_image(
    wcapi: API,
    cat: Dict[str, Any],
    image_url: str,
    timeout: int,
    disable_upload: Dict[str, bool],
) -> Optional[int]:
    """
    Si la categoría ya tiene imagen -> devuelve su id.
    Si no, intenta subir media y asignarla a la categoría. (10 intentos, 15s).
    En 401/403 desactiva subida y devuelve None.
    """
    existing = (cat.get("image") or {}).get("id")
    if existing:
        try:
            return int(existing)
        except Exception:
            pass

    if disable_upload.get("disabled"):
        return None

    wp_user = os.environ.get("WP_USER", "").strip()
    wp_app_pass = os.environ.get("WP_APP_PASS", "").strip()
    wp_url = os.environ.get("WP_URL", "").strip()
    if not (wp_url and wp_user and wp_app_pass):
        # sin credenciales WP: no se sube media
        return None

    if not image_url:
        return None

    last_err = ""
    for attempt in range(1, 11):
        try:
            img_bytes = download_and_resize_image(image_url, timeout=timeout)
            if not img_bytes:
                return None

            media_id = wp_upload_media(img_bytes, filename="cat.jpg", timeout=timeout, disable=disable_upload)
            if not media_id:
                return None

            updated = wcapi.put(f"products/categories/{cat['id']}", {"image": {"id": media_id}}).json()
            if isinstance(updated, dict) and updated.get("message"):
                raise RuntimeError(updated.get("message"))
            return int(media_id)

        except Exception as e:
            last_err = str(e)
            err_low = last_err.lower()
            # auth -> cortar y desactivar
            if "401" in err_low or "403" in err_low or "unauthorized" in err_low or "forbidden" in err_low:
                disable_upload["disabled"] = True
                print(f"⚠️  Imagen categoría: auth inválida (se desactiva). Error: {last_err}")
                return None

            print(f"⚠️  Imagen categoría fallo intento {attempt}/10: {last_err}")
            time.sleep(15)

    print(f"⚠️  Imagen categoría: agotados 10 intentos. Último error: {last_err}")
    return None


def build_product_payload(pd: ProductData, status: str, category_ids: List[int], image_id: Optional[int]) -> Dict[str, Any]:
    # Si hay descuento real, ponemos sale_price
    regular_price = str(pd.precio_original)
    sale_price = str(pd.precio_actual) if pd.precio_actual < pd.precio_original else ""

    meta_data = [
        {"key": "memoria", "value": pd.memoria},
        {"key": "capacidad", "value": pd.capacidad},
        {"key": "version", "value": pd.version},
        {"key": "fuente", "value": pd.fuente},

        # Precios ACF/meta sin decimales
        {"key": "precio_actual", "value": str(pd.precio_actual)},
        {"key": "precio_original", "value": str(pd.precio_original)},
        {"key": "precio_origial", "value": str(pd.precio_original)},  # compatibilidad typo

        {"key": "codigo_de_descuento", "value": pd.codigo_de_descuento},
        {"key": "imagen_producto", "value": pd.url_imagen},

        {"key": "enlace_de_compra_importado", "value": pd.enlace_importado},
        {"key": "url_importada_sin_afiliado", "value": pd.url_importada_sin_afiliado},
        {"key": "url_sin_acortar_con_mi_afiliado", "value": pd.url_sin_acortar_con_mi_afiliado},
        {"key": "url_oferta_sin_acortar", "value": pd.url_oferta_sin_acortar},
        {"key": "url_oferta", "value": pd.url_oferta},
        {"key": "url_acortada_con_mi_afiliado", "value": pd.url_oferta},
        {"key": "url_post_acortada", "value": pd.url_post_acortada},

        {"key": "importado_de", "value": IMPORTADO_DE},
        {"key": "_base_key", "value": pd.base_key()},
    ]

    payload: Dict[str, Any] = {
        "name": pd.nombre,
        "type": "external",
        "status": status,
        "regular_price": regular_price,
        "sale_price": sale_price,
        "external_url": pd.url_oferta_sin_acortar,
        "button_text": "Comprar",
        "categories": [{"id": cid} for cid in category_ids],
        "meta_data": meta_data,
    }

    if image_id:
        payload["images"] = [{"id": int(image_id)}]

    return payload


def product_meta_map(p: Dict[str, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for m in p.get("meta_data", []) or []:
        k = m.get("key")
        if k:
            out[str(k)] = str(m.get("value", ""))
    return out


def find_existing_product(wcapi: API, pd: ProductData) -> Tuple[Optional[Dict[str, Any]], bool]:
    """
    Busca candidato por name (search) y filtra por memoria/capacidad/fuente/importado_de.
    Devuelve (producto, exact_match).
    exact_match compara también precios.
    """
    res = wcapi.get("products", params={"search": pd.nombre, "per_page": 100, "status": "any"}).json()
    if isinstance(res, dict) and res.get("message"):
        raise RuntimeError(res.get("message"))

    tname = normalize_text(pd.nombre)
    for p in res:
        if p.get("status") == "trash":
            continue
        if normalize_text(p.get("name", "")) != tname:
            continue
        meta = product_meta_map(p)
        if normalize_text(meta.get("memoria", "")) != normalize_text(pd.memoria):
            continue
        if normalize_text(meta.get("capacidad", "")) != normalize_text(pd.capacidad):
            continue
        if normalize_text(meta.get("fuente", "")) != normalize_text(pd.fuente):
            continue
        if normalize_text(meta.get("importado_de", "")) != normalize_text(IMPORTADO_DE):
            continue

        # exact match si precios y url_oferta coinciden (mínimo)
        exact = (
            meta.get("precio_actual", "") == str(pd.precio_actual)
            and (meta.get("precio_original", meta.get("precio_origial", "")) == str(pd.precio_original))
            and meta.get("url_oferta_sin_acortar", "") == pd.url_oferta_sin_acortar
        )
        return p, exact

    return None, False


def build_existing_imported_index(wcapi: API) -> Dict[str, int]:
    """
    Para obsoletos: indexa productos existentes importados de PowerPlanet por _base_key.
    """
    base_to_id: Dict[str, int] = {}
    page = 1
    while True:
        res = wcapi.get("products", params={"per_page": 100, "page": page, "status": "any"}).json()
        if isinstance(res, dict) and res.get("message"):
            raise RuntimeError(res.get("message"))
        if not res:
            break
        for p in res:
            if p.get("status") == "trash":
                continue
            meta = product_meta_map(p)
            if normalize_text(meta.get("importado_de", "")) != normalize_text(IMPORTADO_DE):
                continue
            bk = meta.get("_base_key") or ""
            if bk:
                try:
                    base_to_id[bk] = int(p["id"])
                except Exception:
                    pass
        if len(res) < 100:
            break
        page += 1
    return base_to_id


# --------------------------
# Main flow
# --------------------------
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-products", type=int, default=0, help="0=sin límite")
    ap.add_argument("--sleep", type=float, default=0.7, help="pausa entre productos")
    ap.add_argument("--timeout", type=int, default=25)
    # Credenciales opcionales por CLI (si no, usa ENV)
    ap.add_argument("--wp-url", default="", help="Sobrescribe ENV WP_URL")
    ap.add_argument("--wp-key", default="", help="Sobrescribe ENV WP_KEY")
    ap.add_argument("--wp-secret", default="", help="Sobrescribe ENV WP_SECRET")
    ap.add_argument("--wp-user", default="", help="Sobrescribe ENV WP_USER (media upload)")
    ap.add_argument("--wp-app-pass", default="", help="Sobrescribe ENV WP_APP_PASS (media upload)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--status", default="publish", choices=["publish", "draft", "pending", "private"])
    ap.add_argument("--force-delete-obsoletes", action="store_true")
    ap.add_argument("--affiliate-query", default="", help="Query string para afiliado (opcional)")
    ap.add_argument("--jsonl", default="", help="Ruta JSONL de salida (opcional)")
    return ap.parse_args()


def scrape_and_import(args: argparse.Namespace) -> None:
    print(f"📌 PowerPlanet: Escaneando SOLO: {LIST_URL}")

    sess = make_session()
    html = sess.get(LIST_URL, timeout=args.timeout).text
    urls = extract_product_urls(html)
    print(f"📌 PowerPlanet: URLs detectadas = {len(urls)}")

    wcapi = None
    if not args.dry_run:
        try:
            wcapi = get_wcapi(timeout=args.timeout)
        except SystemExit as e:
            # Sin credenciales: seguimos en modo dry-run
            print(str(e))
            wcapi = None
            args.dry_run = True

    # obsoletos
    base_to_id: Dict[str, int] = {}
    if wcapi and args.force_delete_obsoletes:
        base_to_id = build_existing_imported_index(wcapi)

    disable_upload = {"disabled": False}

    created = 0
    updated = 0
    ignored = 0
    deleted = 0

    current_base_keys: set[str] = set()

    jsonl_fh = open(args.jsonl, "a", encoding="utf-8") if args.jsonl else None

    try:
        for idx, url in enumerate(urls, start=1):
            if args.max_products and idx > args.max_products:
                break

            pd = build_product_data(sess, url, timeout=args.timeout, affiliate_query=args.affiliate_query)
            if not pd:
                continue

            current_base_keys.add(pd.base_key())

            print_required_logs(pd)
            print("-" * 60)

            if jsonl_fh:
                jsonl_fh.write(json.dumps(asdict(pd), ensure_ascii=False) + "\n")
                jsonl_fh.flush()

            if args.dry_run or wcapi is None:
                ignored += 1
                time.sleep(args.sleep)
                continue

            # categorías (marca + modelo)
            brand = brand_from_name(pd.nombre)
            brand_cat = ensure_category(wcapi, brand, parent_id=0)
            model_cat = ensure_category(wcapi, pd.nombre, parent_id=int(brand_cat["id"]))

            # imagen subcategoría -> producto
            image_id = ensure_category_image(wcapi, model_cat, pd.url_imagen, timeout=args.timeout, disable_upload=disable_upload)

            # buscar existente
            existing, exact = find_existing_product(wcapi, pd)
            payload = build_product_payload(pd, status=args.status, category_ids=[int(brand_cat["id"]), int(model_cat["id"])], image_id=image_id)

            if existing:
                pid = int(existing["id"])
                if exact:
                    ignored += 1
                else:
                    wcapi.put(f"products/{pid}", payload).json()
                    updated += 1
                # guardar url_post_acortada si podemos leer permalink
                try:
                    refreshed = wcapi.get(f"products/{pid}").json()
                    permalink = refreshed.get("permalink") or ""
                    if permalink:
                        shortp = shorten_isgd(permalink, timeout=args.timeout)
                        pd.url_post_acortada = shortp
                        wcapi.put(f"products/{pid}", {"meta_data": [{"key": "url_post_acortada", "value": shortp}]}).json()
                except Exception:
                    pass
            else:
                created_obj = wcapi.post("products", payload).json()
                if isinstance(created_obj, dict) and created_obj.get("id"):
                    created += 1
                    pid = int(created_obj["id"])
                    # guardar url_post_acortada
                    try:
                        permalink = created_obj.get("permalink") or ""
                        if permalink:
                            shortp = shorten_isgd(permalink, timeout=args.timeout)
                            pd.url_post_acortada = shortp
                            wcapi.put(f"products/{pid}", {"meta_data": [{"key": "url_post_acortada", "value": shortp}]}).json()
                    except Exception:
                        pass

            time.sleep(args.sleep)

        # borrar obsoletos
        if wcapi and args.force_delete_obsoletes and base_to_id:
            for bk, pid in base_to_id.items():
                if bk not in current_base_keys:
                    try:
                        wcapi.delete(f"products/{pid}", params={"force": True}).json()
                        deleted += 1
                    except Exception as e:
                        print(f"⚠️  No se pudo borrar obsoleto {pid}: {e}")

    finally:
        if jsonl_fh:
            jsonl_fh.close()

    # resumen
    ts = _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("=" * 60)
    print(f"📋 RESUMEN DE EJECUCIÓN ({ts})")
    print("=" * 60)
    print(f"a) ARTICULOS CREADOS: {created}")
    print(f"b) ARTICULOS ELIMINADOS (OBSOLETOS): {deleted}")
    print(f"c) ARTICULOS ACTUALIZADOS: {updated}")
    print(f"d) ARTICULOS IGNORADOS (SIN CAMBIOS): {ignored}")
    print("=" * 60)


def main() -> None:
    args = parse_args()

    # CLI overrides -> ENV (para CI/local)
    if args.wp_url:
        os.environ["WP_URL"] = args.wp_url
    if args.wp_key:
        os.environ["WP_KEY"] = args.wp_key
    if args.wp_secret:
        os.environ["WP_SECRET"] = args.wp_secret
    if args.wp_user:
        os.environ["WP_USER"] = args.wp_user
    if args.wp_app_pass:
        os.environ["WP_APP_PASS"] = args.wp_app_pass

    scrape_and_import(args)


if __name__ == "__main__":
    main()
