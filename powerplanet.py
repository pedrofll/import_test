#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import hashlib
import json
import os
import re
import sys
import time
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# ============================================================
# CONFIG
# ============================================================

LIST_URL = "https://www.powerplanetonline.com/es/moviles-mas-vendidos"
IMPORT_SOURCE = "powerplanetonline"
IMPORTADO_DE = "https://www.powerplanetonline.com/"
CUPON_DEFAULT = "OFERTA PROMO"  # fijo

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

COLOR_ENDINGS = [
    "xiaomi",  # (por si viniese como "Xiaomi /" raro, no afecta normalmente)
    "negro", "blanco", "azul", "rojo", "verde", "gris", "plata", "dorado",
    "titanio", "amarillo", "marron", "marrón", "violeta", "lila", "rosa",
    "obsidiana", "neblina", "oscuro", "claro",
    "azul neblina", "azul oscuro", "titanio negro",
]

# --------------------------
# RAM iPhone
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


# ============================================================
# DATA
# ============================================================

@dataclass
class Offer:
    nombre: str
    memoria: str
    capacidad: str
    version: str
    fuente: str
    precio_actual: int
    precio_original: int
    codigo_de_descuento: str
    url_imagen: str
    enlace_de_compra_importado: str
    enlace_expandido: str
    url_importada_sin_afiliado: str
    url_sin_acortar_con_mi_afiliado: str
    url_oferta_sin_acortar: str
    url_oferta: str  # (acortada)
    enviado_desde: str


# ============================================================
# HELPERS
# ============================================================

def norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).lower()

def strip_accents_basic(s: str) -> str:
    s = s.replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u")
    s = s.replace("Á", "a").replace("É", "e").replace("Í", "i").replace("Ó", "o").replace("Ú", "u")
    s = s.replace("ñ", "n").replace("Ñ", "n")
    return s

def is_iphone(name: str) -> bool:
    return "iphone" in norm(name)

def iphone_ram_for(name: str) -> Optional[str]:
    n = norm(strip_accents_basic(name))
    for key, ram in IPHONE_RAM_MAP:
        if key in n:
            return ram
    return None

def parse_price_number(txt: str) -> Optional[float]:
    if not txt:
        return None
    t = txt.strip()
    t = re.sub(r"[^\d\.,\-]", "", t)
    if not t:
        return None
    if "." in t and "," in t:
        t = t.replace(".", "").replace(",", ".")
    else:
        if "," in t:
            t = t.replace(",", ".")
    try:
        return float(t)
    except Exception:
        return None

def price_to_int_trunc(p: Optional[float]) -> int:
    if p is None:
        return 0
    try:
        return int(p)  # truncar decimales
    except Exception:
        return 0

def clean_name_remove_ram_rom(name: str) -> str:
    s = re.sub(r"\s*[/\-–—]\s*", " ", (name or "").strip())
    s = re.sub(r"\b\d+\s*GB\s*/\s*\d+\s*(GB|TB)\b", "", s, flags=re.I)
    s = re.sub(r"\b\d+\s*GB\s+\d+\s*(GB|TB)\b", "", s, flags=re.I)
    s = re.sub(r"\b\d+\s*b\s*\d+\s*(GB|TB)\b", "", s, flags=re.I)
    s = re.sub(r"\bversi[oó]n\s+internacional\b", "", s, flags=re.I)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def drop_trailing_color(name: str) -> str:
    s = re.sub(r"\s+", " ", (name or "").strip()).strip(" /-–—|,")
    low = norm(strip_accents_basic(s))

    for c in sorted(COLOR_ENDINGS, key=lambda x: len(x.split()), reverse=True):
        c_low = norm(strip_accents_basic(c))
        if low.endswith(" " + c_low):
            s = s[: max(0, len(s) - len(c))].strip()
            low = norm(strip_accents_basic(s))
            break

    s = s.strip(" /-–—|,")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def extract_storage_from_url_or_name(url: str, name: str) -> Optional[str]:
    u = (url or "").lower()
    m = re.search(r"-(\d+)(gb|tb)(?:-|$)", u)
    if m:
        return f"{m.group(1)}{m.group(2).upper()}"

    candidates = re.findall(r"(\d+)\s*(GB|TB)\b", name or "", flags=re.I)
    if candidates:
        num, unit = candidates[-1]
        return f"{num}{unit.upper()}"
    return None

def extract_ram_rom_from_slug(url: str) -> Tuple[Optional[str], Optional[str]]:
    slug = (urlparse(url).path or "").lower().replace("_", "-")

    m = re.search(r"-(\d{1,2})gb[-/]?(\d{2,4})(gb|tb)\b", slug)
    if m:
        return f"{m.group(1)}GB", f"{m.group(2)}{m.group(3).upper()}"

    m = re.search(r"-(\d{1,2})b(\d{2,4})(gb|tb)\b", slug)
    if m:
        return f"{m.group(1)}GB", f"{m.group(2)}{m.group(3).upper()}"

    return None, None

def extract_ram_rom_from_name(name: str) -> Tuple[Optional[str], Optional[str]]:
    s = (name or "")

    m = re.search(r"\b(\d{1,2})\s*(GB)\s*/\s*(\d{2,4})\s*(GB|TB)\b", s, flags=re.I)
    if m:
        return f"{m.group(1)}GB", f"{m.group(3)}{m.group(4).upper()}"

    m = re.search(r"\b(\d{1,2})\s*(GB)\s+(\d{2,4})\s*(GB|TB)\b", s, flags=re.I)
    if m:
        return f"{m.group(1)}GB", f"{m.group(3)}{m.group(4).upper()}"

    m = re.search(r"\b(\d{1,2})\s*b\s*(\d{2,4})\s*(GB|TB)\b", s, flags=re.I)
    if m:
        return f"{m.group(1)}GB", f"{m.group(2)}{m.group(3).upper()}"

    return None, None

def valid_ram(ram: Optional[str]) -> bool:
    if not ram:
        return False
    m = re.match(r"^(\d+)\s*GB$", ram.strip(), flags=re.I)
    if not m:
        return False
    v = int(m.group(1))
    return 1 <= v <= 32

def valid_cap(cap: Optional[str]) -> bool:
    if not cap:
        return False
    m = re.match(r"^(\d+)\s*(GB|TB)$", cap.strip(), flags=re.I)
    if not m:
        return False
    v = int(m.group(1))
    unit = m.group(2).upper()
    if unit == "TB":
        return 1 <= v <= 8
    return 8 <= v <= 4096


# ============================================================
# LISTING PAGE (NO SALIR DE LIST_URL)
# ============================================================

PRICE_EUR_RE = re.compile(r"(\d{1,3}(?:\.\d{3})*(?:,\d{2})?|\d+(?:,\d{2})?)\s*€")

def extract_prices_from_card(card) -> Tuple[Optional[float], Optional[float]]:
    if not card:
        return None, None

    txt = card.get_text(" ", strip=True)
    hits = PRICE_EUR_RE.findall(txt)
    floats = []
    for h in hits:
        f = parse_price_number(h)
        if f is not None and f > 0:
            floats.append(f)

    if not floats:
        for meta in card.select('meta[itemprop="price"][content]'):
            f = parse_price_number(meta.get("content", ""))
            if f is not None and f > 0:
                floats.append(f)

    if not floats:
        return None, None

    if len(floats) == 1:
        return floats[0], floats[0]

    return min(floats), max(floats)

def extract_name_url_img_from_anchor(a, base_url: str) -> Tuple[str, str, str]:
    href = a.get("href", "")
    url = urljoin(base_url, href)

    name = (a.get("title") or "").strip()
    if not name:
        name = a.get_text(" ", strip=True)
    if not name:
        img = a.find("img")
        if img and img.get("alt"):
            name = img.get("alt", "").strip()

    img_url = ""
    img = a.find("img")
    if img:
        img_url = (img.get("src") or img.get("data-src") or "").strip()
        if img_url:
            img_url = urljoin(base_url, img_url)

    return name.strip(), url, img_url

def iter_product_anchors_from_listing(soup: BeautifulSoup, base_url: str):
    for a in soup.select("a[href^='/es/']"):
        href = a.get("href", "")
        if not href or href == "/es/" or "moviles-mas-vendidos" in href:
            continue

        if not a.find("img"):
            continue

        card = None
        cur = a
        for _ in range(7):
            if cur is None:
                break
            try:
                t = cur.get_text(" ", strip=True)
            except Exception:
                t = ""
            tl = t.lower()
            if "€" in t and ("gb" in tl or "tb" in tl or "iphone" in tl):
                card = cur
                break
            cur = cur.parent

        if card is None:
            continue

        yield a, card

def extract_offers_from_listing(html: str, base_url: str, max_products: int = 0) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    seen_urls = set()
    items = []

    for a, card in iter_product_anchors_from_listing(soup, base_url):
        name, url, img_url = extract_name_url_img_from_anchor(a, base_url)
        p_now, p_old = extract_prices_from_card(card)

        if not name or not url:
            continue
        if p_now is None:
            continue
        if url in seen_urls:
            continue

        seen_urls.add(url)
        items.append({"raw_name": name, "url": url, "img": img_url, "p_now": p_now, "p_old": p_old})

        if max_products and len(items) >= max_products:
            break

    return items


# ============================================================
# SHORTENER
# ============================================================

def isgd_shorten(url: str, timeout: int = 20) -> str:
    try:
        api = "https://is.gd/create.php"
        r = requests.get(api, params={"format": "simple", "url": url}, timeout=timeout, headers={"User-Agent": UA})
        r.raise_for_status()
        short = (r.text or "").strip()
        if short.startswith("http"):
            return short
    except Exception:
        pass
    return url


# ============================================================
# BUILD OFFER
# ============================================================

def build_offer_from_listing_item(item: Dict, timeout: int) -> Optional[Offer]:
    raw_name = item["raw_name"].strip()
    url = item["url"].strip()
    img = (item.get("img") or "").strip()

    p_now_f = item.get("p_now")
    p_old_f = item.get("p_old") if item.get("p_old") is not None else p_now_f

    precio_actual = price_to_int_trunc(p_now_f)
    precio_original = price_to_int_trunc(p_old_f)

    if precio_original and precio_actual and precio_original < precio_actual:
        precio_original = precio_actual

    ram, cap = extract_ram_rom_from_slug(url)
    if not (valid_ram(ram) and valid_cap(cap)):
        ram2, cap2 = extract_ram_rom_from_name(raw_name)
        if valid_ram(ram2) and valid_cap(cap2):
            ram, cap = ram2, cap2

    if is_iphone(raw_name):
        cap_i = extract_storage_from_url_or_name(url, raw_name)
        if cap_i and valid_cap(cap_i):
            cap = cap_i
        ram_i = iphone_ram_for(raw_name)
        if ram_i:
            ram = ram_i

    if not is_iphone(raw_name):
        if not (valid_ram(ram) and valid_cap(cap)):
            return None
    else:
        if not valid_cap(cap):
            return None
        if not valid_ram(ram):
            return None

    name_clean = clean_name_remove_ram_rom(raw_name)
    if is_iphone(name_clean):
        name_clean = re.sub(r"\b\d+\s*(GB|TB)\b", "", name_clean, flags=re.I).strip()

    name_clean = drop_trailing_color(name_clean)
    name_clean = re.sub(r"\s+", " ", name_clean).strip()

    enlace_de_compra_importado = url
    enlace_expandido = url
    url_importada_sin_afiliado = url
    url_sin_acortar_con_mi_afiliado = url
    url_oferta_sin_acortar = url
    url_oferta = isgd_shorten(url, timeout=timeout)

    enviado_desde = "España"

    return Offer(
        nombre=name_clean,
        memoria=ram,
        capacidad=cap,
        version="Global",
        fuente=IMPORT_SOURCE,
        precio_actual=precio_actual,
        precio_original=precio_original,
        codigo_de_descuento=CUPON_DEFAULT,
        url_imagen=img,
        enlace_de_compra_importado=enlace_de_compra_importado,
        enlace_expandido=enlace_expandido,
        url_importada_sin_afiliado=url_importada_sin_afiliado,
        url_sin_acortar_con_mi_afiliado=url_sin_acortar_con_mi_afiliado,
        url_oferta_sin_acortar=url_oferta_sin_acortar,
        url_oferta=url_oferta,
        enviado_desde=enviado_desde,
    )


# ============================================================
# ACF/META (LO QUE SE GUARDARIA)
# ============================================================

def meta_kv(key: str, value) -> dict:
    return {"key": key, "value": value if value is not None else ""}

def build_acf_meta(offer: Offer) -> List[dict]:
    return [
        meta_kv("memoria", offer.memoria),
        meta_kv("capacidad", offer.capacidad),
        meta_kv("version", offer.version),
        meta_kv("fuente", offer.fuente),

        meta_kv("precio_actual", str(int(offer.precio_actual or 0))),
        meta_kv("precio_original", str(int(offer.precio_original or 0))),

        meta_kv("codigo_de_descuento", offer.codigo_de_descuento),

        meta_kv("importado_de", IMPORTADO_DE),

        meta_kv("enlace_de_compra_importado", offer.enlace_de_compra_importado),
        meta_kv("url_importada_sin_afiliado", offer.url_importada_sin_afiliado),
        meta_kv("url_sin_acortar_con_mi_afiliado", offer.url_sin_acortar_con_mi_afiliado),
        meta_kv("url_oferta_sin_acortar", offer.url_oferta_sin_acortar),
        meta_kv("url_oferta", offer.url_oferta),

        meta_kv("url_imagen", offer.url_imagen),

        meta_kv("enviado_desde", offer.enviado_desde),
        meta_kv("enviado_desde_tg", "🇪🇸 España" if offer.enviado_desde == "España" else ""),
    ]

def log_acf_meta(meta: List[dict]) -> None:
    print("🧾 ACF/META que se guardaría:", flush=True)
    for m in meta:
        k = m.get("key", "")
        v = m.get("value", "")
        print(f"   - {k}: {v}", flush=True)


# ============================================================
# SCRAPE (SOLO LIST_URL)
# ============================================================

def scrape(args) -> List[Offer]:
    print(f"📌 PowerPlanet: Escaneando SOLO: {LIST_URL}", flush=True)

    s = requests.Session()
    s.headers.update({"User-Agent": UA})

    r = s.get(LIST_URL, timeout=args.timeout)
    r.raise_for_status()

    items = extract_offers_from_listing(r.text, base_url=LIST_URL, max_products=args.max_products)
    print(f"📌 PowerPlanet: Productos detectados = {len(items)}", flush=True)

    offers: List[Offer] = []

    for it in items:
        offer = build_offer_from_listing_item(it, timeout=args.timeout)
        if not offer:
            print(f"⚠️  Saltado ({it.get('url')}): Producto sin RAM/ROM detectables (no se importa)", flush=True)
            continue

        offers.append(offer)

        print(f"Detectado {offer.nombre}", flush=True)
        print(f"1) Nombre: {offer.nombre}", flush=True)
        print(f"2) Memoria: {offer.memoria}", flush=True)
        print(f"3) Capacidad: {offer.capacidad}", flush=True)
        print(f"4) Versión: {offer.version}", flush=True)
        print(f"5) Fuente: {offer.fuente}", flush=True)
        print(f"6) Precio actual: {offer.precio_actual}€", flush=True)
        print(f"7) Precio original: {offer.precio_original}€", flush=True)
        print(f"8) Código de descuento: {offer.codigo_de_descuento}", flush=True)
        print(f"9) Version: {offer.version}", flush=True)
        print(f"10) URL Imagen: {offer.url_imagen}", flush=True)
        print(f"11) Enlace Importado: {offer.enlace_de_compra_importado}", flush=True)
        print(f"12) Enlace Expandido: {offer.enlace_expandido}", flush=True)
        print(f"13) URL importada sin afiliado: {offer.url_importada_sin_afiliado}", flush=True)
        print(f"14) URL sin acortar con mi afiliado: {offer.url_sin_acortar_con_mi_afiliado}", flush=True)
        print(f"15) URL acortada con mi afiliado: {offer.url_oferta}", flush=True)
        print(f"16) Enviado desde: {offer.enviado_desde}", flush=True)

        if args.log_acf:
            meta = build_acf_meta(offer)
            log_acf_meta(meta)

        print("------------------------------------------------------------", flush=True)

        if args.sleep:
            time.sleep(args.sleep)

    return offers


# ============================================================
# JSONL
# ============================================================

def write_jsonl(path: str, offers: List[Offer]) -> None:
    if not path:
        return
    with open(path, "w", encoding="utf-8") as f:
        for o in offers:
            f.write(json.dumps(asdict(o), ensure_ascii=False) + "\n")


# ============================================================
# CLI
# ============================================================

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--wp-url", default=os.getenv("WP_URL", "").strip(), help="URL base de WordPress")
    ap.add_argument("--wc-key", default=os.getenv("WP_KEY", "").strip(), help="WooCommerce Consumer Key")
    ap.add_argument("--wc-secret", default=os.getenv("WP_SECRET", "").strip(), help="WooCommerce Consumer Secret")
    ap.add_argument("--wp-user", default=os.getenv("WP_USER", "").strip(), help="Usuario WP (media upload)")
    ap.add_argument("--wp-app-pass", default=os.getenv("WP_APP_PASS", "").strip(), help="App Password WP (media upload)")

    ap.add_argument("--max-products", type=int, default=0, help="0 = sin límite")
    ap.add_argument("--sleep", type=float, default=0.7)
    ap.add_argument("--timeout", type=int, default=25)

    ap.add_argument("--no-import", action="store_true", help="NO crea/actualiza productos. Solo logs + jsonl.")
    ap.add_argument("--status", default="publish", choices=["publish", "draft", "pending", "private"])

    ap.add_argument("--jsonl", default="", help="Ruta JSONL de salida (ej: powerplanet.jsonl)")
    ap.add_argument("--log-acf", action="store_true", help="Loguea todas las claves ACF/meta por producto")

    args = ap.parse_args()
    # por defecto: si no especificas --log-acf, lo activamos siempre (lo pediste explícito)
    if "--log-acf" not in sys.argv:
        args.log_acf = True
    return args


def main():
    args = parse_args()

    offers = scrape(args)

    if args.jsonl:
        write_jsonl(args.jsonl, offers)

    if args.no_import:
        print("🧪 MODO --no-import: NO se crean productos. Solo scraping + logs ACF/meta + jsonl (si aplica).", flush=True)
        return

    # Si algún día vuelves a activar import, aquí pondrías el cliente WC.
    print("❌ Import deshabilitado en este archivo. Usa --no-import (por defecto recomendado).", flush=True)
    sys.exit(2)


if __name__ == "__main__":
    main()
