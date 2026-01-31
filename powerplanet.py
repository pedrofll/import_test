#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# PowerPlanetOnline -> WooCommerce + ACF (importador)
#
# DRY-RUN (sin credenciales):
#   python3 powerplanet.py --max-products 0 --sleep 0.7 --timeout 25 --jsonl powerplanet_dryrun.jsonl
#
# IMPORT REAL (con credenciales + subida de imágenes):
#   python3 powerplanet.py --wp-url https://ofertasdemoviles.com \
#     --wc-key XXX --wc-secret YYY \
#     --wp-user admin --wp-app-pass "xxxx xxxx xxxx xxxx"
#
# o usando ENV (recomendado en CI):
#   export WP_URL="https://ofertasdemoviles.com"
#   export WP_KEY="ck_..."
#   export WP_SECRET="cs_..."
#   python3 powerplanet.py
#
# Requisitos:
#   pip install requests beautifulsoup4 pillow
#   pip install python-woocommerce   # opcional (si se quiere wcapi)
#

import argparse
import html
import json
import os
import re
import time
import unicodedata
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from io import BytesIO
from typing import List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from PIL import Image


# --- CONFIGURACIÓN WORDPRESS ---
# (Pedido: incluir wcapi con ENV WP_URL/WP_KEY/WP_SECRET)
try:
    from woocommerce import API  # type: ignore

    wcapi = API(
        url=os.environ.get("WP_URL", ""),
        consumer_key=os.environ.get("WP_KEY", ""),
        consumer_secret=os.environ.get("WP_SECRET", ""),
        version="wc/v3",
        timeout=60,
    )
except Exception:
    API = None  # type: ignore
    wcapi = None  # type: ignore


# =========================
# CONFIG FUENTE (PowerPlanet)
# =========================
BASE_URL = "https://www.powerplanetonline.com"
LIST_URL = f"{BASE_URL}/es/moviles-mas-vendidos"

FUENTE_RAW = "powerplanetonline"          # log interno
FUENTE_ACF = "powerplanetonline"     # ACF "fuente"
IMPORTADO_DE = LIST_URL                  # ACF "importado_de"
ENVIO = "España"                         # ACF "enviado_desde"
CUPON_DEFAULT = "OFERTA PROMO"          # ACF "codigo_de_descuento"

TAG_IMPORT = "__importado_de_powerplanetonline"  # tag interno para obsoletos


# =========================
# MODELO
# =========================
@dataclass
class Offer:
    source: str
    name_raw: str
    name_clean: str
    url: str

    ram: str
    rom: str
    version: str

    price_eur: Optional[float] = None
    pvr_eur: Optional[float] = None

    image_url: Optional[str] = None
    ref: Optional[str] = None
    product_id: Optional[int] = None

    scraped_at: str = ""


# =========================
# UTILS
# =========================
def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def today_ddmmyyyy() -> str:
    return datetime.now().strftime("%d/%m/%Y")


def normalize_text(s: str) -> str:
    s = s.strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s


def smart_title_token(token: str) -> str:
    # Primera letra mayúscula; si mezcla letras/números, letras en mayúscula (14T, 5G).
    if not token:
        return token
    raw = token.strip()
    parts = re.split(r"(-)", raw)  # preserva guiones
    out_parts = []

    for p in parts:
        if p == "-":
            out_parts.append(p)
            continue

        low = p.lower()
        if low == "iphone":
            out_parts.append("iPhone")
            continue
        if low == "ipad":
            out_parts.append("iPad")
            continue
        if low == "ios":
            out_parts.append("iOS")
            continue

        has_digit = any(ch.isdigit() for ch in p)
        has_alpha = any(ch.isalpha() for ch in p)

        if has_digit and has_alpha:
            out_parts.append("".join(ch.upper() if ch.isalpha() else ch for ch in p))
        else:
            out_parts.append(p[:1].upper() + p[1:].lower())

    return "".join(out_parts)


def format_product_title(name: str) -> str:
    name = re.sub(r"\s+", " ", name.strip())
    return " ".join(smart_title_token(t) for t in name.split(" "))


def safe_float(v) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def format_price_eur(v: Optional[float]) -> str:
    if v is None:
        return "N/A"
    return f"{v:.2f}€"


def extract_ram_rom_from_name(name: str) -> Tuple[str, str]:
    # Soporta: 8GB/256GB | 8GB 256GB | 8gb-256gb | 4b128gb
    if not name:
        return "", ""
    n = name.replace("\xa0", " ")

    patterns = [
        r"\b(\d+)\s*(GB|TB)\s*[/\+\-\|]\s*(\d+)\s*(GB|TB)\b",
        r"\b(\d+)\s*(GB|TB)\s+(\d+)\s*(GB|TB)\b",
        r"\b(\d+)\s*gb\s*(\d+)\s*gb\b",
        r"\b(\d+)\s*b\s*(\d+)\s*(gb|tb)\b",
    ]

    for pat in patterns:
        m = re.search(pat, n, flags=re.IGNORECASE)
        if not m:
            continue
        if pat == patterns[2]:
            return f"{m.group(1)}GB", f"{m.group(2)}GB"
        if pat == patterns[3]:
            return f"{m.group(1)}GB", f"{m.group(2)}{m.group(3).upper()}"
        return f"{m.group(1)}{m.group(2).upper()}", f"{m.group(3)}{m.group(4).upper()}"

    return "", ""



def strip_variant_from_name(name: str) -> str:
    """Normaliza el título para quedarnos SOLO con el modelo (sin RAM/ROM ni colores/estado)."""
    if not name:
        return name

    s = re.sub(r"\s+", " ", name.strip())

    # Quitar sufijos típicos de reacondicionados/estado tras guiones
    s = re.sub(
        r"\s*[-–|]\s*(?:renovad[oa]|reacondicionad[oa]|refurbished|estado|stock)\b.*$",
        "",
        s,
        flags=re.IGNORECASE,
    ).strip()

    # Quitar RAM/ROM
    s = re.sub(
        r"\s*\b\d+\s*(?:GB|TB)\s*[/\+\-\| ]\s*\d+\s*(?:GB|TB)\b\s*",
        " ",
        s,
        flags=re.IGNORECASE,
    )
    s = re.sub(r"\s*\b\d+\s*b\s*\d+\s*(?:gb|tb)\b\s*", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+", " ", s).strip()

    # Recortar si aparece un marcador de estado en medio
    low_s = " " + normalize_text(s) + " "
    for mk in [" renovado", " reacondicionado", " refurbished", " estado "]:
        if mk in low_s:
            idx = low_s.find(mk)
            s = s[: max(0, idx - 1)].strip()
            break

    descriptors = {
        # colores/tonos
        "negro","blanco","azul","rojo","verde","amarillo","morado","violeta","gris","plata","dorado","oro","rosa","naranja","cian","turquesa",
        "beige","crema","grafito","lavanda","marfil","champan","neblina","midnight","starlight","titanio","titanium","marron","marrón",
        # acabados/variantes
        "transparente","transparent","subzero","sub-zero","ice","snow","glacier","sky","ocean","forest","sand","stone",
        "rugged","rugg","rug",
        "edition","limited","special",
        # estados
        "renovado","renovada","reacondicionado","reacondicionada","refurbished","excelente","muy","bueno","estado",
    }

    parts = s.split(" ")

    def norm_tok(t: str) -> str:
        return normalize_text(t.strip(" ,.;:()[]{}"))

    while parts and norm_tok(parts[-1]) in descriptors:
        parts.pop()

    while parts and parts[-1] in {"-", "–", "|"}:
        parts.pop()

    s = " ".join(parts).strip()
    return re.sub(r"\s+", " ", s).strip()



def compute_version(clean_name: str) -> str:
    if "iphone" in normalize_text(clean_name):
        return "IOS"
    return "Global"


def get_brand_from_name(clean_name: str) -> str:
    if normalize_text(clean_name).startswith("iphone"):
        return "Apple"
    return clean_name.split(" ")[0] if clean_name else ""


def shorten_isgd(sess: requests.Session, url: str, timeout: int = 15, retries: int = 5) -> str:
    endpoint = "https://is.gd/create.php"
    for attempt in range(1, retries + 1):
        try:
            r = sess.get(endpoint, params={"format": "simple", "url": url}, timeout=timeout)
            r.raise_for_status()
            short = (r.text or "").strip()
            if short.startswith("http"):
                return short
        except Exception:
            time.sleep(1.2 * attempt)
    return url


def make_session() -> requests.Session:
    sess = requests.Session()
    sess.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0 Safari/537.36"
            ),
            "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        }
    )
    return sess


def fetch_html(sess: requests.Session, url: str, timeout: int = 25, retries: int = 3) -> str:
    last_err = None
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
            else:
                raise RuntimeError(f"Error descargando {url}: {last_err}") from last_err
    raise RuntimeError(f"Error descargando {url}: {last_err}")


# =========================
# SCRAPING
# =========================

def _looks_like_product_anchor(a: BeautifulSoup) -> bool:
    """Filtra SOLO links de producto dentro del listado.
    Regla principal: el texto del link (o alt/title/aria) debe contener RAM/ROM (ej: 8GB/256GB o 24GB/1TB).
    """
    txt = a.get_text(" ", strip=True) or (a.get("title") or "") or (a.get("aria-label") or "")
    if not txt:
        img = a.find("img")
        if img and img.get("alt"):
            txt = img.get("alt") or ""
    txt = re.sub(r"\s+", " ", (txt or "")).strip()
    if not txt:
        return False

    ram, rom = extract_ram_rom_from_name(txt)
    if not (ram and rom):
        return False

    low = normalize_text(txt)
    if "tab" in low or "ipad" in low or "tablet" in low:
        return False
    if "watch" in low or "smartwatch" in low:
        return False
    # Tablets suelen traer pulgadas y/o WiFi
    if ("wifi" in low) and (("''" in txt) or ('"' in txt) or re.search(r"\b\d{1,2}(?:[\.,]\d)?\s*(?:inch|in)\b", low)):
        return False

    return True


def extract_listing_product_urls(list_html: str) -> List[str]:
    """IMPORTANTE: NO salir del listado /es/moviles-mas-vendidos.
    Extrae únicamente URLs de producto detectadas dentro de ese HTML.
    """
    soup = BeautifulSoup(list_html, "html.parser")

    urls: List[str] = []
    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href.startswith("/es/"):
            continue

        # Evitar categorías/menús típicos (por seguridad extra)
        if href.startswith("/es/moviles-") or href.startswith("/es/smartphones") or href.startswith("/es/telefonos-moviles"):
            continue
        if "moviles-mas-vendidos" in href:
            continue
        if "#" in href:
            continue

        if not _looks_like_product_anchor(a):
            continue

        full = urljoin(BASE_URL, href.split("?")[0])
        urls.append(full)

    # dedupe manteniendo orden
    seen = set()
    out = []
    for u in urls:
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out



def parse_product_data_json(soup: BeautifulSoup) -> Optional[dict]:
    form = soup.find("form", attrs={"data-product": True})
    if not form:
        return None
    raw = form.get("data-product")
    if not raw:
        return None
    raw = html.unescape(raw).strip()
    try:
        return json.loads(raw)
    except Exception:
        return None


def parse_prices_from_dom(soup: BeautifulSoup) -> Tuple[Optional[float], Optional[float]]:
    price = pvr = None
    ip = soup.select_one(".data-all-prices .product-price .integerPrice")
    if ip and ip.get("content"):
        price = safe_float(ip.get("content"))
    ib = soup.select_one(".data-all-prices .product-basePrice .integerPrice")
    if ib and ib.get("content"):
        pvr = safe_float(ib.get("content"))
    return price, pvr


def parse_detail(detail_html: str, url: str) -> Offer:
    soup = BeautifulSoup(detail_html, "html.parser")

    data = parse_product_data_json(soup) or {}
    defn = data.get("definition") or {}

    name_raw = (data.get("name") or "").strip()
    if not name_raw:
        h1 = soup.select_one("h1.real-title, h1.h1, h1")
        name_raw = h1.get_text(" ", strip=True) if h1 else ""

    # precios CON IVA (retailPrice/basePrice)
    price = safe_float(defn.get("retailPrice")) or safe_float(defn.get("price")) or safe_float(defn.get("productRetailPrice"))
    pvr = safe_float(defn.get("basePrice")) or safe_float(defn.get("productBasePrice"))

    if price is None or pvr is None:
        dom_price, dom_pvr = parse_prices_from_dom(soup)
        if price is None:
            price = dom_price
        if pvr is None:
            pvr = dom_pvr

    # imagen
    img = soup.select_one("img#main-image") or soup.select_one("img.mainImageTag")
    image_url = None
    if img:
        image_url = (img.get("data-original") or img.get("src") or "").strip() or None

    # RAM/ROM
    ram, rom = extract_ram_rom_from_name(name_raw)
    if not (ram and rom):
        ram, rom = extract_ram_rom_from_name(url)

    name_clean = format_product_title(strip_variant_from_name(name_raw))
    version = compute_version(name_clean)

    # validación móvil (sin RAM/ROM => ignorar)
    if not (ram and rom):
        raise ValueError("Producto sin RAM/ROM detectables (no se importa)")

    return Offer(
        source=FUENTE_RAW,
        name_raw=name_raw,
        name_clean=name_clean,
        url=url,
        ram=ram,
        rom=rom,
        version=version,
        price_eur=price,
        pvr_eur=pvr,
        image_url=image_url,
        ref=str(data.get("sku")) if data.get("sku") else None,
        product_id=int(data.get("id")) if data.get("id") is not None else None,
        scraped_at=now_iso(),
    )


# =========================
# WP / WC CLIENT
# =========================
class WPClient:
    """
    Cliente WooCommerce/WordPress:
      - WooCommerce: usa python-woocommerce (wcapi) si está disponible y hay credenciales.
        Si no, usa requests + basic auth.
      - WP media upload: siempre por requests + application password.
    """

    def __init__(
        self,
        wp_url: str,
        wc_key: str,
        wc_secret: str,
        wp_user: Optional[str] = None,
        wp_app_password: Optional[str] = None,
        timeout: int = 30,
        wcapi_instance=None,
    ):
        self.wp_url = wp_url.rstrip("/")
        self.timeout = timeout

        self.wcapi = wcapi_instance
        if self.wcapi and not (wp_url and wc_key and wc_secret):
            self.wcapi = None

        self.sess_wc = requests.Session()
        self.sess_wc.auth = (wc_key, wc_secret)
        self.sess_wc.headers.update({"User-Agent": "PowerPlanetImporter/1.0"})

        self.sess_wp = requests.Session()
        if wp_user and wp_app_password:
            self.sess_wp.auth = (wp_user, wp_app_password)
        self.sess_wp.headers.update({"User-Agent": "PowerPlanetImporter/1.0"})

    def _url(self, path: str) -> str:
        if not path.startswith("/"):
            path = "/" + path
        return self.wp_url + "/wp-json" + path

    def wc(self, method: str, path: str, params=None, payload=None, retries: int = 3):
        # Preferir wcapi si está disponible (pedido por el usuario)
        if self.wcapi is not None:
            ep = path.lstrip("/")
            last_err = None
            for attempt in range(1, retries + 1):
                try:
                    if method.upper() == "GET":
                        resp = self.wcapi.get(ep, params=params)
                    elif method.upper() == "POST":
                        resp = self.wcapi.post(ep, data=payload)
                    elif method.upper() == "PUT":
                        resp = self.wcapi.put(ep, data=payload)
                    elif method.upper() == "DELETE":
                        resp = self.wcapi.delete(ep, params=params)
                    else:
                        raise ValueError(f"Método no soportado: {method}")

                    if getattr(resp, "status_code", 0) >= 400:
                        raise RuntimeError(f"WCAPI {method} {ep} -> {resp.status_code} {getattr(resp, 'text', '')[:400]}")
                    return resp.json()
                except Exception as e:
                    last_err = e
                    if attempt < retries:
                        time.sleep(1.2 * attempt)
                    else:
                        raise last_err

        # Fallback: requests directo
        url = self._url("/wc/v3" + (path if path.startswith("/") else "/" + path))
        last_err = None
        for attempt in range(1, retries + 1):
            try:
                r = self.sess_wc.request(method, url, params=params, json=payload, timeout=self.timeout)
                if r.status_code >= 400:
                    raise RuntimeError(f"WC {method} {path} -> {r.status_code} {r.text[:400]}")
                return r.json()
            except Exception as e:
                last_err = e
                if attempt < retries:
                    time.sleep(1.2 * attempt)
                else:
                    raise last_err

    def wp_media_upload(self, filename: str, content: bytes, mime: str = "image/jpeg", retries: int = 3) -> dict:
        url = self._url("/wp/v2/media")
        headers = {"Content-Disposition": f'attachment; filename="{filename}"', "Content-Type": mime}
        last_err = None
        for attempt in range(1, retries + 1):
            try:
                r = self.sess_wp.post(url, headers=headers, data=content, timeout=self.timeout)
                if r.status_code >= 400:
                    raise RuntimeError(f"WP media upload -> {r.status_code} {r.text[:400]}")
                return r.json()
            except Exception as e:
                last_err = e
                if attempt < retries:
                    time.sleep(1.2 * attempt)
                else:
                    raise RuntimeError(f"Error subiendo media: {last_err}") from last_err


def wc_get_all(client: WPClient, path: str, params: dict, per_page: int = 100, max_pages: int = 50) -> List[dict]:
    out: List[dict] = []
    page = 1
    while page <= max_pages:
        p = dict(params or {})
        p.update({"per_page": per_page, "page": page})
        items = client.wc("GET", path, params=p)
        if not items:
            break
        out.extend(items)
        if len(items) < per_page:
            break
        page += 1
    return out


def meta_get(product: dict, key: str) -> Optional[str]:
    for m in product.get("meta_data", []) or []:
        if m.get("key") == key:
            v = m.get("value")
            return str(v) if v is not None else None
    return None


def meta_set(meta_list: List[dict], key: str, value) -> None:
    meta_list.append({"key": key, "value": value})


# =========================
# IMAGENES (download + resize 600x600 + upload)
# =========================
def download_image(sess: requests.Session, url: str, timeout: int = 25) -> bytes:
    r = sess.get(url, timeout=timeout, stream=True)
    r.raise_for_status()
    return r.content


def resize_to_600_square(img_bytes: bytes) -> bytes:
    im = Image.open(BytesIO(img_bytes)).convert("RGB")
    w, h = im.size
    side = min(w, h)
    left = (w - side) // 2
    top = (h - side) // 2
    im = im.crop((left, top, left + side, top + side)).resize((600, 600), Image.LANCZOS)
    out = BytesIO()
    im.save(out, format="JPEG", quality=90, optimize=True)
    return out.getvalue()


def upload_image_with_retry(
    client: WPClient,
    sess: requests.Session,
    image_url: str,
    filename_base: str,
    attempts: int = 10,
    sleep_s: int = 15,
) -> Tuple[Optional[int], Optional[str]]:
    last_err = None
    for i in range(1, attempts + 1):
        try:
            raw = download_image(sess, image_url)
            resized = resize_to_600_square(raw)
            media = client.wp_media_upload(filename=f"{filename_base}.jpg", content=resized, mime="image/jpeg", retries=3)
            mid = int(media.get("id")) if media.get("id") is not None else None
            src = media.get("source_url")
            return mid, src
        except Exception as e:
            last_err = e
            if i < attempts:
                time.sleep(sleep_s)
            else:
                print(f"⚠️  Imagen NO subida tras {attempts} intentos: {image_url} -> {last_err}")
                return None, None
    return None, None


# =========================
# WC TAXONOMIAS (cat/subcat/tag)
# =========================
def wc_get_or_create_category(client: WPClient, name: str, parent: int = 0) -> dict:
    existing = client.wc("GET", "/products/categories", params={"search": name, "per_page": 100})
    for t in existing or []:
        if normalize_text(t.get("name", "")) == normalize_text(name) and int(t.get("parent") or 0) == int(parent):
            return t
    return client.wc("POST", "/products/categories", payload={"name": name, "parent": parent})


def wc_get_or_create_tag(client: WPClient, name: str) -> dict:
    existing = client.wc("GET", "/products/tags", params={"search": name, "per_page": 100})
    for t in existing or []:
        if normalize_text(t.get("name", "")) == normalize_text(name):
            return t
    return client.wc("POST", "/products/tags", payload={"name": name})


def wc_set_category_image(client: WPClient, cat_id: int, media_id: int) -> dict:
    return client.wc("PUT", f"/products/categories/{cat_id}", payload={"image": {"id": media_id}})


# =========================
# WC PRODUCTOS
# =========================
def compute_sku(offer: Offer) -> str:
    pid = offer.product_id if offer.product_id is not None else abs(hash(offer.url)) % 10_000_000
    ram = offer.ram.replace(" ", "")
    rom = offer.rom.replace(" ", "")
    return f"ppo-{pid}-{ram}-{rom}".lower()


def find_existing_product(client: WPClient, offer: Offer, sku: str) -> Optional[dict]:
    by_sku = client.wc("GET", "/products", params={"sku": sku, "per_page": 10})
    if by_sku:
        return by_sku[0]

    search = client.wc("GET", "/products", params={"search": offer.name_clean, "per_page": 100})
    for p in search or []:
        if (meta_get(p, "memoria") or "").strip() != offer.ram:
            continue
        if (meta_get(p, "capacidad") or "").strip() != offer.rom:
            continue
        if normalize_text(meta_get(p, "fuente") or "") != normalize_text(FUENTE_ACF):
            continue
        if normalize_text(meta_get(p, "importado_de") or "") != normalize_text(IMPORTADO_DE):
            continue
        return p
    return None


def build_product_payload(
    offer: Offer,
    sku: str,
    external_url: str,
    cat_id: int,
    tag_id: int,
    featured_media_id: Optional[int],
    imagen_producto_url: Optional[str],
    status: str,
) -> dict:
    meta: List[dict] = []
    meta_set(meta, "memoria", offer.ram)
    meta_set(meta, "capacidad", offer.rom)
    meta_set(meta, "precio_actual", f"{offer.price_eur:.2f}" if offer.price_eur is not None else "")
    meta_set(meta, "precio_origial", f"{offer.pvr_eur:.2f}" if offer.pvr_eur is not None else "")
    meta_set(meta, "codigo_de_descuento", CUPON_DEFAULT)
    meta_set(meta, "fuente", FUENTE_ACF)
    meta_set(meta, "imagen_producto", imagen_producto_url or (offer.image_url or ""))
    meta_set(meta, "enviado_desde", ENVIO)
    meta_set(meta, "enviado_desde_tg", "🇪🇸 España")
    meta_set(meta, "version", offer.version)
    meta_set(meta, "importado_de", IMPORTADO_DE)
    meta_set(meta, "fecha", today_ddmmyyyy())
    meta_set(meta, "url_importada_sin_afiliado", offer.url)
    meta_set(meta, "url_sin_acortar_con_mi_afiliado", offer.url)
    meta_set(meta, "url_oferta", external_url)

    payload = {
        "name": offer.name_clean,
        "type": "external",
        "status": status,
        "sku": sku,
        "external_url": external_url,
        "button_text": "Ver oferta",
        "categories": [{"id": cat_id}],
        "tags": [{"id": tag_id}],
        "meta_data": meta,
    }
    if offer.pvr_eur is not None:
        payload["regular_price"] = f"{offer.pvr_eur:.2f}"
    if offer.price_eur is not None:
        payload["sale_price"] = f"{offer.price_eur:.2f}"
    if featured_media_id:
        payload["images"] = [{"id": featured_media_id}]
    return payload


def shorten_wp_permalink(sess: requests.Session, permalink: str) -> str:
    return shorten_isgd(sess, permalink)


def create_or_update_with_retry(
    client: WPClient,
    method: str,
    path: str,
    payload: dict,
    attempts: int = 10,
    sleep_s: int = 15,
) -> dict:
    last_err = None
    for i in range(1, attempts + 1):
        try:
            return client.wc(method, path, payload=payload, retries=3)
        except Exception as e:
            last_err = e
            if i < attempts:
                time.sleep(sleep_s)
            else:
                raise RuntimeError(f"WC {method} {path} falló tras {attempts} intentos: {last_err}") from last_err
    raise RuntimeError(f"WC {method} {path} falló: {last_err}")


def delete_product(client: WPClient, product_id: int, force: bool = False) -> None:
    client.wc("DELETE", f"/products/{product_id}", params={"force": "true" if force else "false"})


# =========================
# LOGS
# =========================
def log_product(offer: Offer, url_oferta: str, url_post_short: str) -> None:
    print(f"Detectado {offer.name_clean}")
    print(f"1) Nombre: {offer.name_clean}")
    print(f"2) Memoria: {offer.ram}")
    print(f"3) Capacidad: {offer.rom}")
    print(f"4) Versión: {offer.version}")
    print(f"5) Fuente: {FUENTE_RAW}")
    print(f"6) Precio actual: {format_price_eur(offer.price_eur)}")
    print(f"7) Precio original: {format_price_eur(offer.pvr_eur)}")
    print(f"8) Código de descuento: {CUPON_DEFAULT}")
    print(f"9) Version: {offer.version}")
    print(f"10) URL Imagen: {offer.image_url or ''}")
    print(f"11) Enlace Importado: {offer.url}")
    print(f"12) Enlace Expandido: {offer.url}")
    print(f"13) URL importada sin afiliado: {offer.url}")
    print(f"14) URL sin acortar con mi afiliado: {offer.url}")
    print(f"15) URL acortada con mi afiliado: {url_oferta}")
    print(f"16) Enviado desde: {ENVIO}")
    print(f"17) URL post acortada: {url_post_short}")
    print(f"18) Encolado para comparar con base de datos...")
    print("-" * 60)


def jsonl_write(fp, offer: Offer, sku: str, url_oferta: str, wp_id: Optional[int] = None, wp_permalink: Optional[str] = None, wp_short: Optional[str] = None):
    row = asdict(offer)
    row.update(
        {
            "sku": sku,
            "url_oferta": url_oferta,
            "wp_id": wp_id,
            "wp_permalink": wp_permalink,
            "wp_short": wp_short,
            "acf": {
                "memoria": offer.ram,
                "capacidad": offer.rom,
                "precio_actual": f"{offer.price_eur:.2f}" if offer.price_eur is not None else "",
                "precio_origial": f"{offer.pvr_eur:.2f}" if offer.pvr_eur is not None else "",
                "codigo_de_descuento": CUPON_DEFAULT,
                "fuente": FUENTE_ACF,
                "imagen_producto": offer.image_url or "",
                "enviado_desde": ENVIO,
                "enviado_desde_tg": "🇪🇸 España",
                "version": offer.version,
                "importado_de": IMPORTADO_DE,
                "fecha": today_ddmmyyyy(),
            },
        }
    )
    fp.write(json.dumps(row, ensure_ascii=False) + "\n")


# =========================
# MAIN IMPORT
# =========================
def run_import(
    wp_url: Optional[str],
    wc_key: Optional[str],
    wc_secret: Optional[str],
    wp_user: Optional[str],
    wp_app_pass: Optional[str],
    max_products: int,
    sleep_seconds: float,
    timeout: int,
    dry_run: bool,
    status: str,
    force_delete_obsoletes: bool,
    jsonl_path: Optional[str],
):
    sess = make_session()

    list_html = fetch_html(sess, LIST_URL, timeout=timeout)
    product_urls = extract_listing_product_urls(list_html)
    if max_products > 0:
        product_urls = product_urls[:max_products]

    print(f"📌 PowerPlanet: URLs detectadas = {len(product_urls)}")

    fp_jsonl = open(jsonl_path, "w", encoding="utf-8") if jsonl_path else None

    client = None
    tag_id = None

    if not dry_run:
        if not (wp_url and wc_key and wc_secret):
            raise SystemExit("Faltan credenciales. Para dry-run: omite credenciales o usa --dry-run.")

        wcapi_inst = wcapi if (wcapi is not None and os.environ.get("WP_URL") and os.environ.get("WP_KEY") and os.environ.get("WP_SECRET")) else None

        client = WPClient(
            wp_url=wp_url,
            wc_key=wc_key,
            wc_secret=wc_secret,
            wp_user=wp_user,
            wp_app_password=wp_app_pass,
            timeout=timeout,
            wcapi_instance=wcapi_inst,
        )
        tag = wc_get_or_create_tag(client, TAG_IMPORT)
        tag_id = int(tag["id"])

    summary_creados = []
    summary_eliminados = []
    summary_actualizados = []
    summary_ignorados = []

    current_skus = set()

    try:
        for url in product_urls:
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

            try:
                detail_html = fetch_html(sess, url, timeout=timeout)
                offer = parse_detail(detail_html, url)
            except Exception as e:
                print(f"⚠️  Saltado ({url}): {e}")
                continue

            sku = compute_sku(offer)
            current_skus.add(sku)
            url_oferta = shorten_isgd(sess, offer.url)

            if dry_run:
                log_product(offer, url_oferta=url_oferta, url_post_short="")
                if fp_jsonl:
                    jsonl_write(fp_jsonl, offer, sku=sku, url_oferta=url_oferta)
                continue

            assert client is not None and tag_id is not None

            brand = format_product_title(get_brand_from_name(offer.name_clean))
            cat_parent = wc_get_or_create_category(client, brand, parent=0)
            parent_id = int(cat_parent["id"])

            subcat_name = offer.name_clean
            cat_sub = wc_get_or_create_category(client, subcat_name, parent=parent_id)
            sub_id = int(cat_sub["id"])

            featured_media_id = None
            imagen_producto_url = None
            existing_img = (cat_sub.get("image") or {}).get("id")
            existing_src = (cat_sub.get("image") or {}).get("src")

            if existing_img:
                featured_media_id = int(existing_img)
                imagen_producto_url = existing_src
            else:
                if offer.image_url and (wp_user and wp_app_pass):
                    filename_base = re.sub(r"[^a-z0-9\-]+", "-", normalize_text(subcat_name)).strip("-")[:80] or "powerplanet"
                    mid, src = upload_image_with_retry(client, sess, offer.image_url, filename_base, attempts=10, sleep_s=15)
                    if mid:
                        wc_set_category_image(client, sub_id, mid)
                        featured_media_id = mid
                        imagen_producto_url = src

            existing = find_existing_product(client, offer, sku)
            payload = build_product_payload(offer, sku, url_oferta, sub_id, tag_id, featured_media_id, imagen_producto_url, status)

            wp_post_short = ""
            wp_permalink = ""
            wp_id = None

            if existing:
                pid = int(existing["id"])
                wp_id = pid
                cambios = []

                old_pact = meta_get(existing, "precio_actual") or ""
                old_pvr = meta_get(existing, "precio_origial") or ""
                new_pact = payload["meta_data"][2]["value"]
                new_pvr = payload["meta_data"][3]["value"]

                if str(old_pact).strip() != str(new_pact).strip():
                    cambios.append(f"precio_actual: {old_pact} -> {new_pact}")
                if str(old_pvr).strip() != str(new_pvr).strip():
                    cambios.append(f"precio_origial: {old_pvr} -> {new_pvr}")

                needs_image = not (existing.get("images") or []) and featured_media_id is not None

                if not cambios and not needs_image:
                    summary_ignorados.append({"nombre": offer.name_clean, "id": pid})
                else:
                    if needs_image and featured_media_id:
                        payload["images"] = [{"id": featured_media_id}]
                        cambios.append("imagen: asignada")

                    create_or_update_with_retry(client, "PUT", f"/products/{pid}", payload)
                    summary_actualizados.append({"nombre": offer.name_clean, "id": pid, "cambios": cambios})

                try:
                    prod = client.wc("GET", f"/products/{pid}")
                    wp_permalink = prod.get("permalink") or ""
                    if wp_permalink:
                        wp_post_short = shorten_wp_permalink(sess, wp_permalink)
                        client.wc("PUT", f"/products/{pid}", payload={"meta_data": [{"key": "url_post_acortada", "value": wp_post_short}]})
                except Exception:
                    wp_permalink = ""
                    wp_post_short = ""

                log_product(offer, url_oferta=url_oferta, url_post_short=wp_post_short)

            else:
                created = create_or_update_with_retry(client, "POST", "/products", payload)
                pid = int(created["id"])
                wp_id = pid
                summary_creados.append({"nombre": offer.name_clean, "id": pid})

                try:
                    wp_permalink = created.get("permalink") or ""
                    if not wp_permalink:
                        prod = client.wc("GET", f"/products/{pid}")
                        wp_permalink = prod.get("permalink") or ""
                    if wp_permalink:
                        wp_post_short = shorten_wp_permalink(sess, wp_permalink)
                        client.wc("PUT", f"/products/{pid}", payload={"meta_data": [{"key": "url_post_acortada", "value": wp_post_short}]})
                except Exception:
                    wp_permalink = ""
                    wp_post_short = ""

                log_product(offer, url_oferta=url_oferta, url_post_short=wp_post_short)

            if fp_jsonl:
                jsonl_write(fp_jsonl, offer, sku=sku, url_oferta=url_oferta, wp_id=wp_id, wp_permalink=wp_permalink, wp_short=wp_post_short)

        if not dry_run and client is not None and tag_id is not None:
            imported_products = wc_get_all(client, "/products", params={"tag": tag_id}, per_page=100, max_pages=50)
            for p in imported_products:
                pid = int(p["id"])
                sku = (p.get("sku") or "").strip().lower()
                if sku and sku not in current_skus:
                    try:
                        delete_product(client, pid, force=force_delete_obsoletes)
                        summary_eliminados.append({"nombre": p.get("name", ""), "id": pid})
                    except Exception as e:
                        print(f"⚠️  No se pudo eliminar obsoleto ID {pid}: {e}")

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
            print(f"- {item['nombre']} (ID: {item['id']}): {', '.join(item['cambios'])}")
        print(f"\nd) ARTICULOS IGNORADOS (SIN CAMBIOS): {len(summary_ignorados)}")
        for item in summary_ignorados:
            print(f"- {item['nombre']} (ID: {item['id']})")
        print("============================================================")

    finally:
        if fp_jsonl:
            fp_jsonl.close()


def main() -> None:
    ap = argparse.ArgumentParser(description="PowerPlanetOnline -> WooCommerce + ACF (importador)")

    ap.add_argument("--wp-url", default=os.environ.get("WP_URL", ""), help="URL base de WordPress, ej: https://ofertasdemoviles.com")
    ap.add_argument("--wc-key", default=os.environ.get("WP_KEY", ""), help="WooCommerce consumer key")
    ap.add_argument("--wc-secret", default=os.environ.get("WP_SECRET", ""), help="WooCommerce consumer secret")

    ap.add_argument("--wp-user", default=os.environ.get("WP_USER", ""), help="WP user (Application Password) para subir imágenes")
    ap.add_argument("--wp-app-pass", default=os.environ.get("WP_APP_PASS", ""), help="WP Application Password para subir imágenes")

    ap.add_argument("--max-products", type=int, default=0, help="0 = sin límite")
    ap.add_argument("--sleep", type=float, default=0.7, help="segundos entre requests")
    ap.add_argument("--timeout", type=int, default=25, help="timeout por request (seg)")
    ap.add_argument("--dry-run", action="store_true", help="solo logs, NO crea/actualiza")
    ap.add_argument("--jsonl", default="", help="ruta para guardar JSONL (opcional)")
    ap.add_argument("--status", default="publish", choices=["publish", "draft", "pending", "private"], help="estado del producto")
    ap.add_argument("--force-delete-obsoletes", action="store_true", help="borrado definitivo de obsoletos (force=true). Si no, van a papelera.")

    args = ap.parse_args()

    have_creds = bool(args.wp_url.strip() and args.wc_key.strip() and args.wc_secret.strip())
    dry_run = bool(args.dry_run or not have_creds)

    run_import(
        wp_url=args.wp_url.strip() or None,
        wc_key=args.wc_key.strip() or None,
        wc_secret=args.wc_secret.strip() or None,
        wp_user=args.wp_user.strip() or None,
        wp_app_pass=args.wp_app_pass.strip() or None,
        max_products=args.max_products,
        sleep_seconds=args.sleep,
        timeout=args.timeout,
        dry_run=dry_run,
        status=args.status,
        force_delete_obsoletes=args.force_delete_obsoletes,
        jsonl_path=args.jsonl.strip() or None,
    )


if __name__ == "__main__":
    main()
