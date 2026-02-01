#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Importador PowerPlanetOnline (WooCommerce + ACF)

✅ Escanea SOLO: https://www.powerplanetonline.com/es/moviles-mas-vendidos
✅ Crea/actualiza productos externos en WooCommerce.
✅ Guarda campos ACF como meta_data (postmeta).
✅ Sube imagen (600x600) al Media Library (WP REST) para asignarla a la subcategoría y al producto.
✅ Gestión obsoletos (opcional): elimina productos importados de PowerPlanet que ya no estén en el listado.

Variables ACF (meta_data) guardadas:
- memoria, capacidad
- precio_actual (sin decimales)
- precio_original (sin decimales)  + compat: precio_origial
- codigo_de_descuento (siempre: OFERTA PROMO)
- fuente (powerplanetonline)
- imagen_producto (URL original de PowerPlanet)
- enviado_desde (España) + enviado_desde_tg (🇪🇸 España)
- importado_de (https://www.powerplanetonline.com/)
- enlace_de_compra_importado
- url_importada_sin_afiliado
- url_sin_acortar_con_mi_afiliado
- url_oferta_sin_acortar
- url_oferta (is.gd)
- url_post_acortada (is.gd del permalink del producto)
- fecha (dd/mm/YYYY)

Auth:
- WooCommerce: WP_URL + WP_KEY + WP_SECRET
- Media upload: WP_USER + WP_APP_PASS (Application Password). Si no están, crea productos pero NO sube imágenes.
"""

import argparse
import base64
import json
import os
import re
import time
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse, parse_qsl, urlencode, urlunparse

import requests
from bs4 import BeautifulSoup

try:
    from PIL import Image
except Exception as e:  # pragma: no cover
    raise SystemExit("Falta dependencia pillow (PIL). Instala: python -m pip install pillow") from e

try:
    from woocommerce import API
except Exception as e:  # pragma: no cover
    raise SystemExit("Falta dependencia woocommerce. Instala: python -m pip install woocommerce") from e


BASE_URL = "https://www.powerplanetonline.com"
LIST_URL = f"{BASE_URL}/es/moviles-mas-vendidos"
IMPORTADO_DE_VALUE = f"{BASE_URL}/"  # para comparar obsoletos por fuente
SOURCE_VALUE = "powerplanetonline"
DEFAULT_COUPON = "OFERTA PROMO"
DEFAULT_ENVIADO_DESDE = "España"
DEFAULT_ENVIADO_DESDE_TG = "🇪🇸 España"

# --------------------------
# RAM iPhone (si en la ficha no aparece)
# --------------------------
IPHONE_RAM_MAP: List[Tuple[str, str]] = [
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


@dataclass
class ProductData:
    nombre: str
    ram: str
    rom: str
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

    enviado_desde: str
    enviado_desde_tg: str

    importado_de: str


# --------------------------
# Utils
# --------------------------

def normalize_text(s: str) -> str:
    s = s.strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s


def title_case_smart(name: str) -> str:
    """Title case con reglas:
    - primera letra de cada palabra en mayúsculas
    - tokens alfanuméricos tipo 5g, 14t -> letras en MAYUS
    - tokens conocidos (GB, TB, RAM, ROM, PRO, MAX, PLUS, ULTRA, FE, GT, SE, 4G, 5G) en mayúsculas
    """
    specials = {
        "gb", "tb", "ram", "rom", "pro", "max", "plus", "ultra", "fe", "gt", "se", "ios",
        "4g", "5g", "lte", "ai"
    }

    parts = re.split(r"(\s+)", name.strip())
    out: List[str] = []
    for p in parts:
        if p.isspace() or p == "":
            out.append(p)
            continue
        raw = p
        # quitar puntuación lateral pero mantenerla
        m = re.match(r"^([\(\[\{\'\"\-]*)(.*?)([\)\]\}\'\"\-\,\.!:]*)$", raw)
        if m:
            pre, core, suf = m.group(1), m.group(2), m.group(3)
        else:
            pre, core, suf = "", raw, ""

        low = core.lower()

        if low in specials:
            core2 = core.upper()
        elif re.search(r"\d", core) and re.search(r"[a-zA-Z]", core):
            # alfanumérico => letras en mayus
            core2 = re.sub(r"[a-zA-Z]+", lambda mm: mm.group(0).upper(), core)
        else:
            core2 = core[:1].upper() + core[1:].lower() if core else core

        out.append(pre + core2 + suf)

    return "".join(out).strip()


def parse_eur_amount_to_float(s: str) -> Optional[float]:
    if not s:
        return None
    s = s.strip().replace("\xa0", " ")
    m = re.search(r"(\d[\d\.,]*)\s*€", s)
    if not m:
        return None
    num = m.group(1).replace(".", "").replace(",", ".")
    try:
        return float(num)
    except ValueError:
        return None


def to_int_no_decimals(v: Optional[float]) -> int:
    if v is None:
        return 0
    # "quitar decimales" => truncar
    return int(v)


def format_eur_int(v: int) -> str:
    return f"{v}€"


def build_affiliate_url(url: str, affiliate_query: str) -> str:
    """Añade query params de afiliado (si existe)."""
    if not affiliate_query.strip():
        return url

    parsed = urlparse(url)
    current = dict(parse_qsl(parsed.query, keep_blank_values=True))
    extra = dict(parse_qsl(affiliate_query, keep_blank_values=True))
    current.update(extra)
    new_query = urlencode(current, doseq=True)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))


def shorten_isgd(long_url: str, sess: requests.Session, timeout: int = 25) -> str:
    """Acorta con is.gd. Si falla, devuelve la URL original."""
    try:
        r = sess.get("https://is.gd/create.php", params={"format": "simple", "url": long_url}, timeout=timeout)
        r.raise_for_status()
        short = r.text.strip()
        if short.startswith("http"):
            return short
    except Exception:
        pass
    return long_url


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
            "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        }
    )
    return s


def fetch_html(sess: requests.Session, url: str, timeout: int = 25, retries: int = 3) -> str:
    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            r = sess.get(url, timeout=timeout)
            r.raise_for_status()
            r.encoding = r.apparent_encoding or "utf-8"
            return r.text
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(1.2 * attempt)
    raise RuntimeError(f"Error descargando {url}: {last_err}")


# --------------------------
# Limpieza nombre + RAM/ROM
# --------------------------

COLOR_SUFFIXES = [
    "negro", "blanco", "azul", "rojo", "verde", "amarillo", "morado", "violeta",
    "gris", "plata", "dorado", "oro", "rosa", "naranja", "cian", "turquesa",
    "beige", "crema", "grafito", "lavanda", "marfil", "champan", "neblina",
    "obsidiana", "midnight", "starlight", "titanio", "titanium",
]

TRAILING_JUNK_PHRASES = [
    "version internacional",
    "versión internacional",
    "internacional",
    "renovado",
    "reacondicionado",
    "estado excelente",
    "rugged",
    "transparente",
    "subzero",
]


def clean_name(raw: str) -> str:
    """Quita RAM/ROM, colores y sufijos típicos, dejando modelo limpio."""
    s = raw.strip()

    # corta por " - ..."
    s = re.split(r"\s-\s", s, maxsplit=1)[0].strip()

    # quita RAM/ROM tipo 8GB/256GB
    s = re.sub(r"\b\d+\s*GB\s*/\s*\d+\s*(GB|TB)\b", "", s, flags=re.IGNORECASE).strip()

    # quita " 8GB 256GB" si viniera separado
    s = re.sub(r"\b\d+\s*GB\b", "", s, flags=re.IGNORECASE).strip()
    s = re.sub(r"\b\d+\s*(GB|TB)\b", "", s, flags=re.IGNORECASE).strip()

    # normaliza espacios
    s = re.sub(r"\s+", " ", s).strip(" -")

    # elimina frases basura al final
    low = normalize_text(s)
    for ph in TRAILING_JUNK_PHRASES:
        if low.endswith(" " + normalize_text(ph)):
            s = s[: -(len(ph))].strip(" -")
            low = normalize_text(s)

    # elimina colores al final (1 palabra)
    words = s.split()
    while words:
        last = normalize_text(words[-1])
        if last in [normalize_text(c) for c in COLOR_SUFFIXES]:
            words = words[:-1]
            continue
        break

    s = " ".join(words).strip()

    # title case con reglas
    return title_case_smart(s)


def split_ram_rom_from_text(name_or_slug: str) -> Tuple[str, str]:
    """Detecta RAM/ROM en texto o slug.
    Devuelve (ram, rom) como '8GB', '256GB'.
    """
    t = normalize_text(name_or_slug).replace(" ", "")

    # patrón 8gb/256gb
    m = re.search(r"(\d+)(gb|tb)[/\-](\d+)(gb|tb)", t, flags=re.IGNORECASE)
    if m:
        ram = f"{m.group(1)}{m.group(2).upper()}"
        rom = f"{m.group(3)}{m.group(4).upper()}"
        return ram, rom

    # patrón 8gb256gb en slug (a veces)
    m2 = re.search(r"(\d+)(gb|tb)(\d+)(gb|tb)", t, flags=re.IGNORECASE)
    if m2:
        ram = f"{m2.group(1)}{m2.group(2).upper()}"
        rom = f"{m2.group(3)}{m2.group(4).upper()}"
        return ram, rom

    # solo rom: 128gb, 256gb, 1tb
    m3 = re.search(r"(\d+)(gb|tb)", t, flags=re.IGNORECASE)
    if m3:
        return "", f"{m3.group(1)}{m3.group(2).upper()}"

    return "", ""


def iphone_ram_for(name: str) -> str:
    n = normalize_text(name)
    for needle, ram in IPHONE_RAM_MAP:
        if needle in n:
            return ram
    return ""


def is_iphone(name: str) -> bool:
    return "iphone" in normalize_text(name)


# --------------------------
# Scraping
# --------------------------


def extract_product_urls(list_html: str) -> List[str]:
    """Extrae URLs de producto desde el listado (sin navegar a otras categorías)."""
    soup = BeautifulSoup(list_html, "html.parser")
    urls: List[str] = []

    # En cards, suele haber links /es/<slug>
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href.startswith("/es/"):
            continue
        if href.startswith("/es/moviles-mas-vendidos"):
            continue
        # descarta paginación o categorías
        if href in ["/es/smartphones", "/es/telefonos-moviles"]:
            continue
        url = urljoin(BASE_URL, href)
        if url not in urls:
            urls.append(url)

    # filtra para quedarnos con páginas de producto (heurística: suelen tener guiones y no acabar en /es/xxxx general)
    def looks_like_product(u: str) -> bool:
        path = urlparse(u).path
        if path.count("-") < 2:
            return False
        # evita categorías muy genéricas
        bad = [
            "/es/moviles-", "/es/smartphones", "/es/telefonos-moviles", "/es/moviles",
            "/es/oukitel", "/es/ulefone", "/es/realme", "/es/samsung", "/es/xiaomi",
        ]
        if any(path.startswith(b) for b in bad):
            return False
        return True

    urls = [u for u in urls if looks_like_product(u)]
    return urls


def parse_detail(detail_html: str, product_url: str) -> Dict[str, Any]:
    """Extrae nombre, imagen y precios desde la ficha."""
    soup = BeautifulSoup(detail_html, "html.parser")

    # nombre (H1)
    h1 = soup.select_one("h1.real-title")
    name_h1 = h1.get_text(" ", strip=True) if h1 else ""

    # imagen principal
    img = soup.select_one("img#main-image")
    img_url = ""
    if img:
        img_url = (img.get("data-original") or img.get("src") or "").strip()

    # precios robustos: span.product-price + span.product-basePrice
    current = None
    original = None

    # opción A: data-product (JSON)
    form = soup.select_one("form.buyForm")
    if form and form.has_attr("data-product"):
        try:
            data = json.loads(form["data-product"])
            definition = data.get("definition", {})
            # retailPrice = precio con descuento, basePrice = recomendado
            current = float(definition.get("retailPrice")) if definition.get("retailPrice") is not None else None
            original = float(definition.get("basePrice")) if definition.get("basePrice") is not None else None
            # fallback:
            if current is None and definition.get("price") is not None:
                current = float(definition.get("price"))
        except Exception:
            pass

    # opción B: spans ocultos
    if current is None:
        node = soup.select_one(".hidden.data-all-prices .product-price .price")
        if node:
            current = parse_eur_amount_to_float(node.get_text(" ", strip=True))
    if original is None:
        node = soup.select_one(".hidden.data-all-prices .product-basePrice .price")
        if node:
            original = parse_eur_amount_to_float(node.get_text(" ", strip=True))

    # fallback final (si no encontramos): busca primera y segunda cantidad con €
    if current is None or original is None:
        txt = soup.get_text("\n", strip=True)
        euros = [parse_eur_amount_to_float(x) for x in re.findall(r"\d[\d\.,]*\s*€", txt)]
        euros = [e for e in euros if e is not None]
        if euros:
            # típico: current es el menor
            if current is None:
                current = min(euros)
            if original is None and len(euros) >= 2:
                original = max(euros)

    return {
        "name": name_h1 or "",
        "image": img_url,
        "price_current": current,
        "price_original": original,
        "url": product_url,
    }


def classify_mobile(name: str) -> Tuple[bool, str]:
    n = normalize_text(name)
    if " ipad" in f" {n} ":
        return False, "EXCLUDE:name_contains_ipad"
    if " tab" in f" {n} " or "tablet" in n:
        return False, "EXCLUDE:name_contains_tab/tablet"
    if "smartwatch" in n or "smartband" in n or "reloj" in n:
        return False, "EXCLUDE:name_contains_watch/band"
    return True, "INCLUDE:mobile"


# --------------------------
# WooCommerce + WP Media
# --------------------------


def get_wcapi(wp_url: str, wc_key: str, wc_secret: str, timeout: int) -> API:
    return API(
        url=wp_url,
        consumer_key=wc_key,
        consumer_secret=wc_secret,
        version="wc/v3",
        timeout=timeout,
    )


def wp_basic_auth_headers(user: str, app_pass: str) -> Dict[str, str]:
    # WordPress muestra App Password con espacios -> hay que quitarlos
    app_pass_clean = app_pass.replace(" ", "").strip()
    token = base64.b64encode(f"{user}:{app_pass_clean}".encode("utf-8")).decode("ascii")
    return {"Authorization": f"Basic {token}"}


def download_and_resize_image(sess: requests.Session, img_url: str, timeout: int = 25) -> Tuple[bytes, str]:
    """Descarga imagen y la devuelve como JPEG 600x600 (bytes, filename)."""
    r = sess.get(img_url, timeout=timeout)
    r.raise_for_status()

    content_type = r.headers.get("Content-Type", "").lower()
    ext = ".jpg"
    if "png" in content_type:
        ext = ".png"

    img = Image.open(BytesIO(r.content)).convert("RGB")
    img = img.resize((600, 600))

    out = BytesIO()
    img.save(out, format="JPEG", quality=90)
    out_bytes = out.getvalue()

    filename = "powerplanet_600x600.jpg"
    return out_bytes, filename


def wp_upload_media(
    wp_url: str,
    wp_user: str,
    wp_app_pass: str,
    img_bytes: bytes,
    filename: str,
    timeout: int = 60,
) -> int:
    media_url = wp_url.rstrip("/") + "/wp-json/wp/v2/media"

    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "Content-Type": "image/jpeg",
    }
    headers.update(wp_basic_auth_headers(wp_user, wp_app_pass))

    r = requests.post(media_url, headers=headers, data=img_bytes, timeout=timeout)

    if r.status_code in (401, 403):
        raise RuntimeError(
            f"Unauthorized al subir media ({r.status_code}). Revisa WP_USER/WP_APP_PASS y permisos 'upload_files'."
        )

    r.raise_for_status()
    j = r.json()
    media_id = int(j.get("id"))
    return media_id


def get_all_categories(wcapi: API) -> List[Dict[str, Any]]:
    cats: List[Dict[str, Any]] = []
    page = 1
    while True:
        res = wcapi.get("products/categories", params={"per_page": 100, "page": page}).json()
        if not isinstance(res, list) or not res:
            break
        cats.extend(res)
        if len(res) < 100:
            break
        page += 1
    return cats


def find_category_by_name(cats: List[Dict[str, Any]], name: str, parent: int) -> Optional[Dict[str, Any]]:
    n = normalize_text(name)
    for c in cats:
        if int(c.get("parent") or 0) != int(parent):
            continue
        if normalize_text(str(c.get("name") or "")) == n:
            return c
    return None


def get_or_create_category(
    wcapi: API,
    cats_cache: List[Dict[str, Any]],
    name: str,
    parent: int = 0,
) -> Dict[str, Any]:
    existing = find_category_by_name(cats_cache, name, parent)
    if existing:
        return existing

    payload = {"name": name, "parent": parent}
    created = wcapi.post("products/categories", payload).json()
    # refresca cache
    cats_cache.append(created)
    return created


def category_image_id(cat: Dict[str, Any]) -> Optional[int]:
    img = cat.get("image")
    if isinstance(img, dict) and img.get("id"):
        try:
            return int(img["id"])
        except Exception:
            return None
    return None


def ensure_category_image(
    wcapi: API,
    wp_url: str,
    wp_user: str,
    wp_app_pass: str,
    sess: requests.Session,
    cat: Dict[str, Any],
    img_src_url: str,
    max_attempts: int = 10,
    sleep_seconds: int = 15,
) -> Optional[int]:
    current_id = category_image_id(cat)
    if current_id:
        return current_id

    if not wp_user or not wp_app_pass:
        return None

    last_err: Optional[str] = None

    for attempt in range(1, max_attempts + 1):
        try:
            img_bytes, filename = download_and_resize_image(sess, img_src_url)
            media_id = wp_upload_media(wp_url, wp_user, wp_app_pass, img_bytes, filename)

            # asigna a categoría
            updated = wcapi.put(f"products/categories/{cat['id']}", {"image": {"id": media_id}}).json()
            cat.update(updated)
            return media_id
        except Exception as e:
            last_err = str(e)
            msg = f"⚠️  Imagen categoría fallo intento {attempt}/{max_attempts}: {last_err}"
            print(msg)

            # si es auth, no tiene sentido esperar 15s y repetir 10 veces
            if "Unauthorized" in last_err or "401" in last_err or "403" in last_err:
                break

            if attempt < max_attempts:
                time.sleep(sleep_seconds)

    return None


def wc_get_products_by_search(wcapi: API, term: str, per_page: int = 100) -> List[Dict[str, Any]]:
    res = wcapi.get("products", params={"search": term, "per_page": per_page, "status": "any"}).json()
    return res if isinstance(res, list) else []


def meta_get(product: Dict[str, Any], key: str) -> Optional[str]:
    for md in product.get("meta_data", []) or []:
        if md.get("key") == key:
            v = md.get("value")
            return str(v) if v is not None else None
    return None


def meta_set_list(meta_list: List[Dict[str, Any]], key: str, value: Any) -> None:
    meta_list.append({"key": key, "value": value})


def product_key(pd: ProductData) -> str:
    return "|".join([
        normalize_text(pd.nombre),
        normalize_text(pd.ram),
        normalize_text(pd.rom),
        normalize_text(pd.fuente),
        normalize_text(pd.importado_de),
    ])


def find_existing_product(wcapi: API, pd: ProductData) -> Optional[Dict[str, Any]]:
    candidates = wc_get_products_by_search(wcapi, pd.nombre)
    key = product_key(pd)

    for p in candidates:
        if str(p.get("status")) == "trash":
            continue

        nombre_ok = normalize_text(str(p.get("name") or "")) == normalize_text(pd.nombre)
        if not nombre_ok:
            continue

        ram_ok = normalize_text(meta_get(p, "memoria") or "") == normalize_text(pd.ram)
        rom_ok = normalize_text(meta_get(p, "capacidad") or "") == normalize_text(pd.rom)
        fuente_ok = normalize_text(meta_get(p, "fuente") or "") == normalize_text(pd.fuente)
        imp_ok = normalize_text(meta_get(p, "importado_de") or "") == normalize_text(pd.importado_de)

        if "|".join([
            normalize_text(pd.nombre),
            normalize_text(pd.ram),
            normalize_text(pd.rom),
            normalize_text(pd.fuente),
            normalize_text(pd.importado_de),
        ]) == key and ram_ok and rom_ok and fuente_ok and imp_ok:
            return p

    return None


def build_product_payload(
    pd: ProductData,
    cat_id: int,
    image_id: Optional[int],
    status: str,
    *,
    include_fecha: bool,
) -> Dict[str, Any]:
    meta: List[Dict[str, Any]] = []

    # ACF base
    meta_set_list(meta, "memoria", pd.ram)
    meta_set_list(meta, "capacidad", pd.rom)
    meta_set_list(meta, "precio_actual", str(pd.precio_actual))

    # compat: algunos sitios lo llaman 'precio_origial'
    meta_set_list(meta, "precio_original", str(pd.precio_original))
    meta_set_list(meta, "precio_origial", str(pd.precio_original))

    meta_set_list(meta, "codigo_de_descuento", pd.codigo_de_descuento)
    meta_set_list(meta, "fuente", pd.fuente)

    meta_set_list(meta, "imagen_producto", pd.url_imagen)

    meta_set_list(meta, "enviado_desde", pd.enviado_desde)
    meta_set_list(meta, "enviado_desde_tg", pd.enviado_desde_tg)

    meta_set_list(meta, "importado_de", pd.importado_de)

    # URLs
    meta_set_list(meta, "enlace_de_compra_importado", pd.enlace_importado)
    meta_set_list(meta, "url_importada_sin_afiliado", pd.url_importada_sin_afiliado)
    meta_set_list(meta, "url_sin_acortar_con_mi_afiliado", pd.url_sin_acortar_con_mi_afiliado)
    meta_set_list(meta, "url_oferta_sin_acortar", pd.url_oferta_sin_acortar)
    meta_set_list(meta, "url_oferta", pd.url_oferta)

    # fecha solo en creación
    if include_fecha:
        meta_set_list(meta, "fecha", datetime.now().strftime("%d/%m/%Y"))

    payload: Dict[str, Any] = {
        "name": pd.nombre,
        "type": "external",
        "status": status,
        # Para filtros/ordenación: sincronizamos precio_actual con regular_price
        "regular_price": str(pd.precio_actual),
        # Enlace externo: usamos el corto; guardamos el largo en meta.
        "external_url": pd.url_oferta,
        "button_text": "Ver oferta",
        "categories": [{"id": cat_id}],
        "meta_data": meta,
    }

    if image_id:
        payload["images"] = [{"id": image_id}]

    return payload


def update_product_meta_and_price(wcapi: API, product_id: int, pd: ProductData, status: str, image_id: Optional[int]) -> Dict[str, Any]:
    payload = build_product_payload(pd, cat_id=0, image_id=image_id, status=status, include_fecha=False)

    # en update NO queremos sobrescribir categorías a 0
    payload.pop("categories", None)

    res = wcapi.put(f"products/{product_id}", payload).json()
    return res


def set_product_categories(wcapi: API, product_id: int, cat_id: int) -> None:
    wcapi.put(f"products/{product_id}", {"categories": [{"id": cat_id}]}).json()


def set_url_post_acortada(wcapi: API, product_id: int, short_url: str) -> None:
    wcapi.put(
        f"products/{product_id}",
        {"meta_data": [{"key": "url_post_acortada", "value": short_url}]},
    ).json()


def wc_list_imported_products(wcapi: API) -> List[Dict[str, Any]]:
    """Lista productos de esta fuente (PowerPlanet) para gestión de obsoletos."""
    products: List[Dict[str, Any]] = []
    page = 1
    while True:
        res = wcapi.get("products", params={"per_page": 100, "page": page, "status": "any"}).json()
        if not isinstance(res, list) or not res:
            break

        for p in res:
            if str(p.get("status")) == "trash":
                continue
            if normalize_text(meta_get(p, "fuente") or "") != normalize_text(SOURCE_VALUE):
                continue
            if normalize_text(meta_get(p, "importado_de") or "") != normalize_text(IMPORTADO_DE_VALUE):
                continue
            products.append(p)

        if len(res) < 100:
            break
        page += 1

    return products


def wc_delete_product(wcapi: API, product_id: int) -> None:
    wcapi.delete(f"products/{product_id}", params={"force": True}).json()


# --------------------------
# LOGS (formato requerido)
# --------------------------


def print_required_logs(pd: ProductData) -> None:
    print(f"Detectado {pd.nombre}")
    print(f"1) Nombre: {pd.nombre}")
    print(f"2) Memoria: {pd.ram}")
    print(f"3) Capacidad: {pd.rom}")
    print(f"4) Versión: {pd.version}")
    print(f"5) Fuente: {pd.fuente}")
    print(f"6) Precio actual: {format_eur_int(pd.precio_actual)}")
    print(f"7) Precio original: {format_eur_int(pd.precio_original)}")
    print(f"8) Código de descuento: {pd.codigo_de_descuento}")
    print(f"9) Version: {pd.version}")
    print(f"10) URL Imagen: {pd.url_imagen}")
    print(f"11) Enlace Importado: {pd.enlace_importado}")
    print(f"12) Enlace Expandido: {pd.enlace_expandido}")
    print(f"13) URL importada sin afiliado: {pd.url_importada_sin_afiliado}")
    print(f"14) URL sin acortar con mi afiliado: {pd.url_sin_acortar_con_mi_afiliado}")
    print(f"15) URL acortada con mi afiliado: {pd.url_oferta}")
    print(f"16) Enviado desde: {pd.enviado_desde}")
    print(f"17) URL post acortada: ")
    print(f"18) Encolado para comparar con base de datos...")
    print("-" * 60)


# --------------------------
# Main flow
# --------------------------


def build_product_data(
    detail: Dict[str, Any],
    affiliate_query: str,
    sess: requests.Session,
    timeout: int,
) -> Optional[ProductData]:
    raw_name = detail.get("name") or ""
    url = detail.get("url") or ""

    ok, reason = classify_mobile(raw_name)
    if not ok:
        print(f"⚠️  Saltado ({url}): {reason} (no se importa)")
        return None

    # RAM/ROM desde nombre o slug
    slug = urlparse(url).path.split("/")[-1]
    ram, rom = split_ram_rom_from_text(raw_name)
    if not ram or not rom:
        r2, o2 = split_ram_rom_from_text(slug)
        ram = ram or r2
        rom = rom or o2

    # iPhone: RAM por tabla + ROM por slug si falta
    cleaned_name = clean_name(raw_name)
    if is_iphone(cleaned_name):
        if not ram:
            ram = iphone_ram_for(cleaned_name)
        if not rom:
            _, rom2 = split_ram_rom_from_text(slug)
            rom = rom or rom2

        if not ram or not rom:
            print(f"⚠️  Saltado ({url}): Producto sin RAM/ROM detectables (no se importa)")
            return None

        version = "IOS"
    else:
        if not ram or not rom:
            print(f"⚠️  Saltado ({url}): Producto sin RAM/ROM detectables (no se importa)")
            return None
        version = "Global"

    # precios sin decimales
    precio_actual = to_int_no_decimals(detail.get("price_current"))
    precio_original = to_int_no_decimals(detail.get("price_original"))

    # urls
    url_importada_sin_afiliado = url
    url_sin_acortar_con_mi_afiliado = build_affiliate_url(url, affiliate_query)
    url_oferta_sin_acortar = url_sin_acortar_con_mi_afiliado
    url_oferta = shorten_isgd(url_oferta_sin_acortar, sess=sess, timeout=timeout)

    # imagen
    url_imagen = (detail.get("image") or "").strip()

    # categoría Apple si iPhone
    if is_iphone(cleaned_name):
        nombre_final = cleaned_name  # sin "Apple" en el título
    else:
        nombre_final = cleaned_name

    return ProductData(
        nombre=nombre_final,
        ram=ram,
        rom=rom,
        version=version,
        fuente=SOURCE_VALUE,
        precio_actual=precio_actual,
        precio_original=precio_original,
        codigo_de_descuento=DEFAULT_COUPON,
        url_imagen=url_imagen,
        enlace_importado=url,
        enlace_expandido=url,
        url_importada_sin_afiliado=url_importada_sin_afiliado,
        url_sin_acortar_con_mi_afiliado=url_sin_acortar_con_mi_afiliado,
        url_oferta_sin_acortar=url_oferta_sin_acortar,
        url_oferta=url_oferta,
        enviado_desde=DEFAULT_ENVIADO_DESDE,
        enviado_desde_tg=DEFAULT_ENVIADO_DESDE_TG,
        importado_de=IMPORTADO_DE_VALUE,
    )


def category_main_name(product_name: str) -> str:
    # iPhone => Apple
    if is_iphone(product_name):
        return "Apple"
    first = product_name.split()[0] if product_name.strip() else ""
    return title_case_smart(first)


def scrape_and_import(args: argparse.Namespace) -> None:
    sess = make_session()

    print(f"📌 PowerPlanet: Escaneando SOLO: {LIST_URL}")
    list_html = fetch_html(sess, LIST_URL, timeout=args.timeout)
    urls = extract_product_urls(list_html)

    if args.max_products and args.max_products > 0:
        urls = urls[: args.max_products]

    print(f"📌 PowerPlanet: URLs detectadas = {len(urls)}")

    # Woo setup
    wp_url = args.wp_url or os.environ.get("WP_URL", "")
    wc_key = args.wc_key or os.environ.get("WP_KEY", "")
    wc_secret = args.wc_secret or os.environ.get("WP_SECRET", "")

    if not wp_url or not wc_key or not wc_secret:
        raise SystemExit("Faltan credenciales WooCommerce. Define WP_URL, WP_KEY, WP_SECRET (secrets/env o args).")

    wcapi = get_wcapi(wp_url, wc_key, wc_secret, timeout=max(60, args.timeout))

    wp_user = args.wp_user or os.environ.get("WP_USER", "")
    wp_app_pass = args.wp_app_pass or os.environ.get("WP_APP_PASS", "")

    # caches
    cats_cache = get_all_categories(wcapi)

    summary_creados: List[Dict[str, Any]] = []
    summary_eliminados: List[Dict[str, Any]] = []
    summary_actualizados: List[Dict[str, Any]] = []
    summary_ignorados: List[Dict[str, Any]] = []

    current_keys: set = set()

    for url in urls:
        if args.sleep > 0:
            time.sleep(args.sleep)

        detail_html = fetch_html(sess, url, timeout=args.timeout)
        detail = parse_detail(detail_html, url)

        pd = build_product_data(detail, affiliate_query=args.affiliate_query, sess=sess, timeout=args.timeout)
        if not pd:
            continue

        current_keys.add(product_key(pd))

        # LOGS siempre
        print_required_logs(pd)

        if args.dry_run:
            continue

        # categorías: marca (cat main) + subcat = nombre completo
        main_cat_name = category_main_name(pd.nombre)
        main_cat = get_or_create_category(wcapi, cats_cache, main_cat_name, parent=0)

        subcat_name = pd.nombre
        sub_cat = get_or_create_category(wcapi, cats_cache, subcat_name, parent=int(main_cat["id"]))

        # imagen por defecto: la de subcategoria si existe, si no subir y asignar
        img_id = category_image_id(sub_cat)
        if not img_id and pd.url_imagen:
            img_id = ensure_category_image(
                wcapi=wcapi,
                wp_url=wp_url,
                wp_user=wp_user,
                wp_app_pass=wp_app_pass,
                sess=sess,
                cat=sub_cat,
                img_src_url=pd.url_imagen,
                max_attempts=10,
                sleep_seconds=15,
            )

        # existe?
        existing = find_existing_product(wcapi, pd)
        if existing:
            cambios: List[str] = []
            pid = int(existing["id"])

            # comprobar precios
            old_pa = meta_get(existing, "precio_actual") or ""
            old_po = meta_get(existing, "precio_original") or meta_get(existing, "precio_origial") or ""

            if old_pa != str(pd.precio_actual):
                cambios.append(f"precio_actual {old_pa} -> {pd.precio_actual}")
            if old_po != str(pd.precio_original):
                cambios.append(f"precio_original {old_po} -> {pd.precio_original}")

            if cambios or (img_id and not existing.get("images")):
                updated = update_product_meta_and_price(wcapi, pid, pd, status=args.status, image_id=img_id)
                set_product_categories(wcapi, pid, int(sub_cat["id"]))

                # url_post_acortada
                permalink = updated.get("permalink") or existing.get("permalink")
                if permalink:
                    short_post = shorten_isgd(str(permalink), sess=sess, timeout=args.timeout)
                    set_url_post_acortada(wcapi, pid, short_post)

                summary_actualizados.append({"nombre": pd.nombre, "id": pid, "cambios": cambios})
            else:
                summary_ignorados.append({"nombre": pd.nombre, "id": pid})

            continue

        # crear nuevo
        payload = build_product_payload(pd, cat_id=int(sub_cat["id"]), image_id=img_id, status=args.status, include_fecha=True)

        # reintentos creación (hasta 10)
        created = None
        last_err = None
        for attempt in range(1, 11):
            try:
                created = wcapi.post("products", payload).json()
                if created and created.get("id"):
                    break
            except Exception as e:
                last_err = e
            time.sleep(15)

        if not created or not created.get("id"):
            print(f"❌ Error creando producto '{pd.nombre}': {last_err}")
            continue

        pid = int(created["id"])

        # url_post_acortada
        permalink = created.get("permalink")
        if permalink:
            short_post = shorten_isgd(str(permalink), sess=sess, timeout=args.timeout)
            set_url_post_acortada(wcapi, pid, short_post)

        summary_creados.append({"nombre": pd.nombre, "id": pid})

    # obsoletos
    if (not args.dry_run) and args.force_delete_obsoletes and (args.max_products == 0):
        existing_imported = wc_list_imported_products(wcapi)
        for p in existing_imported:
            pid = int(p["id"])
            nombre = str(p.get("name") or "")
            ram = meta_get(p, "memoria") or ""
            rom = meta_get(p, "capacidad") or ""
            fuente = meta_get(p, "fuente") or ""
            importado_de = meta_get(p, "importado_de") or ""
            key = "|".join([normalize_text(nombre), normalize_text(ram), normalize_text(rom), normalize_text(fuente), normalize_text(importado_de)])

            if key not in current_keys:
                try:
                    wc_delete_product(wcapi, pid)
                    summary_eliminados.append({"nombre": nombre, "id": pid})
                except Exception as e:
                    print(f"⚠️  No se pudo eliminar obsoleto ID {pid}: {e}")

    # resumen
    hoy_fmt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("\n============================================================")
    print(f"📋 RESUMEN DE EJECUCIÓN ({hoy_fmt})")
    print("============================================================")

    print(f"\na) ARTICULOS CREADOS: {len(summary_creados)}")
    for item in summary_creados:
        print(f"- {item['nombre']} (ID: {item['id']})")

    print(f"\nb) ARTICULOS ELIMINADOS (OBSOLETOS): {len(summary_eliminados)}")
    for item in summary_eliminados:
        print(f"- {item['nombre']} (ID: {item['id']})")

    print(f"\nc) ARTICULOS ACTUALIZADOS: {len(summary_actualizados)}")
    for item in summary_actualizados:
        cambios = ", ".join(item.get("cambios") or [])
        print(f"- {item['nombre']} (ID: {item['id']}): {cambios}")

    print(f"\nd) ARTICULOS IGNORADOS (SIN CAMBIOS): {len(summary_ignorados)}")
    for item in summary_ignorados:
        print(f"- {item['nombre']} (ID: {item['id']})")

    print("============================================================")


def build_argparser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="PowerPlanetOnline -> WooCommerce (móviles más vendidos)")

    ap.add_argument("--wp-url", default="", help="URL WordPress (si vacío usa env WP_URL)")
    ap.add_argument("--wc-key", default="", help="Woo key (si vacío usa env WP_KEY)")
    ap.add_argument("--wc-secret", default="", help="Woo secret (si vacío usa env WP_SECRET)")

    ap.add_argument("--wp-user", default="", help="WP user para subir medios (si vacío usa env WP_USER)")
    ap.add_argument("--wp-app-pass", default="", help="WP app password para subir medios (env WP_APP_PASS)")

    ap.add_argument("--max-products", type=int, default=0, help="0 = sin límite")
    ap.add_argument("--sleep", type=float, default=0.7, help="segundos entre requests")
    ap.add_argument("--timeout", type=int, default=25, help="timeout por request")

    ap.add_argument("--dry-run", action="store_true", help="solo logs (no crea/actualiza)")
    ap.add_argument("--status", choices=["publish", "draft", "pending", "private"], default="publish")
    ap.add_argument("--force-delete-obsoletes", dest="force_delete_obsoletes", action="store_true")

    ap.add_argument("--affiliate-query", default="", help="querystring para afiliado (opcional)")

    return ap


def main() -> None:
    ap = build_argparser()
    args = ap.parse_args()
    scrape_and_import(args)


if __name__ == "__main__":
    main()
