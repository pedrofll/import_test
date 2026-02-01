#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""powerplanet.py

Scraper (dry-run + jsonl) para PowerPlanetOnline.

Objetivos (según requisitos del proyecto):
- Detectar productos en /es/moviles-mas-vendidos
- Extraer ACF: memoria, capacidad, version, fuente, precio_actual, precio_original,
  codigo_de_descuento, imagen_producto y URLs relacionadas.
- Normalizar nombre (modelo) evitando extras (5G/4G y todo lo posterior, colores, ruido).
- Preservar el sufijo " /" SOLO cuando el título original incluye "/" como separador.
- Precios robustos: listing + ficha (si include_details), con fallback por JSON/DOM/regex.
- Logs mostrando variables ACF.

Uso típico:
python powerplanet.py --max-products 0 --sleep 0.7 --timeout 25 --status publish --jsonl powerplanet.jsonl
"""

import argparse
import json
import re
import sys
import time
import unicodedata
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

LISTING_URL = "https://www.powerplanetonline.com/es/moviles-mas-vendidos"
BASE_URL = "https://www.powerplanetonline.com"

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
)

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


# --------------------------
# Helpers
# --------------------------
def _fold(s: str) -> str:
    s = s or ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.lower().strip()


def safe_json_loads(s: str) -> Optional[dict]:
    try:
        return json.loads(s)
    except Exception:
        return None


def safe_float(s: Any) -> Optional[float]:
    """
    Robust number parser for strings that may use:
      - comma decimals: "1.099,99"
      - dot decimals:   "269.99"
      - thousand seps:  "1.099" or "1,099"
    Returns float or None.
    """
    try:
        if s is None:
            return None
        if isinstance(s, (int, float)):
            return float(s)

        raw = str(s).strip()
        if not raw:
            return None

        # keep digits, separators and sign
        cleaned = re.sub(r"[^0-9,\.\-]", "", raw)
        if not cleaned or cleaned in ("-", ",", "."):
            return None

        sign = -1.0 if cleaned.startswith("-") else 1.0
        cleaned = cleaned.lstrip("-")

        has_dot = "." in cleaned
        has_comma = "," in cleaned

        if has_dot and has_comma:
            # decimal separator is the last one
            if cleaned.rfind(".") > cleaned.rfind(","):
                # dot decimal, comma thousands
                cleaned = cleaned.replace(",", "")
            else:
                # comma decimal, dot thousands
                cleaned = cleaned.replace(".", "").replace(",", ".")
        elif has_comma:
            # assume comma decimal if last group looks like cents
            parts = cleaned.split(",")
            if len(parts) == 2 and len(parts[1]) in (1, 2):
                cleaned = parts[0].replace(".", "") + "." + parts[1]
            else:
                cleaned = cleaned.replace(",", "").replace(".", "")
        elif has_dot:
            parts = cleaned.split(".")
            if len(parts) == 2 and len(parts[1]) in (1, 2):
                # dot decimal
                cleaned = cleaned
            else:
                cleaned = cleaned.replace(".", "")
        # else: only digits

        val = float(cleaned)
        return sign * val
    except Exception:
        return None


def coerce_price(v: Any) -> Optional[float]:
    """
    Price parser with cent-normalization:
      - If the parsed number is > 9999, it is assumed to be cents and divided by 100.
    """
    x = safe_float(v)
    if x is None:
        return None
    if x > 9999:
        x = x / 100.0
    return x


def parse_eur_amount(text: str) -> Optional[float]:
    if not text:
        return None
    t = str(text).replace("\xa0", " ").strip()
    # Captura 1.299,99  | 1299,99 | 1299.99 | 1299
    m = re.search(r"(\d{1,3}(?:[.\s]\d{3})*(?:[.,]\d{2})|\d+(?:[.,]\d{2})|\d+)\s*€", t)
    if not m:
        return None
    return safe_float(m.group(1))


def find_all_eur_amounts(text: str) -> List[float]:
    if not text:
        return []
    t = str(text).replace("\xa0", " ")
    out: List[float] = []
    for m in re.finditer(r"(\d{1,3}(?:[.\s]\d{3})*(?:[.,]\d{2})|\d+(?:[.,]\d{2})|\d+)\s*€", t):
        v = safe_float(m.group(1))
        if v is not None:
            out.append(v)
    return out


def format_price(val: Optional[float]) -> str:
    if val is None:
        return "N/A"
    if abs(val - round(val)) < 1e-9:
        return f"{int(round(val))}€"
    return f"{val:.2f}€"


def fetch_html(url: str, timeout: int, user_agent: str) -> str:
    headers = {"User-Agent": user_agent}
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.text


# --------------------------
# Name normalization
# --------------------------
STOP_TOKENS = {
    # colores comunes
    "negro", "blanco", "gris", "plata", "rosa", "dorado", "oro", "verde", "azul", "rojo",
    "marron", "marrón", "violeta", "morado", "amarillo", "beige", "crema", "grafito",
    "lavanda", "marfil", "champan", "champán", "titanio", "titanium", "natural", "desert",
    "obsidiana", "neblina", "midnight", "starlight",
    # modificadores/condición
    "oscuro", "claro", "renovado", "reacondicionado", "reacondicionada", "reacondicionados",
    "reacondicionadas", "desprecintado", "desprecintada", "precintado", "precintada",
    "nuevo", "nueva", "usado", "usada", "estado", "excelente", "bueno", "como",
    "internacional", "global",
    # conectividad/otros
    "wifi", "wi-fi", "lte",
}


def normalize_iphone_name(name: str) -> str:
    # Construye: "Apple Iphone <modelo> [Pro/Max/Plus/Mini/SE/E...]"
    tokens = name.split()
    idx = None
    for i, t in enumerate(tokens):
        if _fold(t) in ("iphone", "iPhone".lower()):
            idx = i
            break
    if idx is None:
        return name.strip()

    model = tokens[idx + 1] if idx + 1 < len(tokens) else ""
    model = re.sub(r"[^\w]+", "", model)

    allowed = {"pro", "max", "plus", "mini", "ultra", "se", "e", "air"}
    suffix: List[str] = []
    j = idx + 2
    while j < len(tokens) and _fold(tokens[j]) in allowed:
        suffix.append(tokens[j].title())
        j += 1

    base = ["Apple", "Iphone"]
    if model:
        base.append(model.upper() if re.fullmatch(r"\d+[A-Za-z]", model) else model)
    base.extend(suffix)
    return " ".join(base).strip()


def strip_variant_and_noise(raw_name: str) -> str:
    s = (raw_name or "").replace("\xa0", " ").strip()
    s = re.sub(r"\s+", " ", s)

    # Si el título usa "/", nos quedamos con el lado izquierdo (modelo) y luego añadiremos " /"
    if "/" in s:
        s = s.split("/", 1)[0].strip()

    # Quitar RAM/ROM tipo 8GB/256GB o 8GB 256GB
    s = re.sub(r"\b\d+\s*(?:GB|TB)\s*[/\s]\s*\d+\s*(?:GB|TB)\b", "", s, flags=re.I)
    # Quitar capacidades sueltas
    s = re.sub(r"\b\d+\s*(?:GB|TB)\b", "", s, flags=re.I)

    # Quitar tamaños de pantalla y similares
    s = re.sub(r"\b\d+(?:[.,]\d+)?\s*(?:''|\"|pulgadas|inch|in)\b", "", s, flags=re.I)

    # Quitar conectividad 5G/4G y todo lo posterior
    parts = re.split(r"\b(?:5G|4G)\b", s, flags=re.I, maxsplit=1)
    s = parts[0].strip()

    # Limpieza de separadores al final
    s = re.sub(r"[\-\|:+]+$", "", s).strip()

    # Quitar tokens finales tipo color/condición
    toks = s.split()
    while toks and _fold(toks[-1]) in STOP_TOKENS:
        toks.pop()
    s = " ".join(toks).strip()
    s = re.sub(r"\s+", " ", s)

    # iPhone: forzar prefijo Apple + eliminar ruido extra
    if "iphone" in _fold(s):
        s = normalize_iphone_name(s)

    return s.strip()


def clean_product_name(raw_name: str) -> str:
    # Preserva sufijo " /" SOLO si el título original tenía el separador " / " (no el "GB/GB").
    had_model_slash = bool(re.search(r"\s/\s", raw_name or "")) or bool(re.search(r"\s/\s*$", raw_name or ""))
    base = strip_variant_and_noise(raw_name)
    if had_model_slash and base:
        base = base.rstrip(" /")
        base = base.strip() + " /"
    return base.strip()


def extract_ram_rom_from_name(name: str) -> Tuple[str, str]:
    s = (name or "").replace("\xa0", " ")
    m = re.search(
        r"\b(\d{1,2})\s*(GB|TB)\s*/\s*(\d{2,4})\s*(GB|TB)\b",
        s,
        flags=re.I,
    )
    if not m:
        # casos raros: 8GB128GB
        m = re.search(r"\b(\d{1,2})\s*(GB|TB)\s*(\d{2,4})\s*(GB|TB)\b", s, flags=re.I)
    if not m:
        return ("", "")
    ram = f"{m.group(1).upper()}{m.group(2).upper()}"
    rom = f"{m.group(3).upper()}{m.group(4).upper()}"
    return ram.replace("TB", "TB"), rom.replace("TB", "TB")


def infer_iphone_ram(model_name: str) -> str:
    k = _fold(model_name)
    for pat, ram in IPHONE_RAM_MAP:
        if pat in k:
            return ram
    return ""


# --------------------------
# Price extraction (listing + detail)
# --------------------------
def extract_prices_from_container(element: BeautifulSoup) -> Tuple[Optional[float], Optional[float]]:
    """
    Devuelve (precio_actual, precio_original).
    Evita coger importes de financiación (€/mes) u otros importes pequeños no relacionados.
    """
    # 1) JSON-LD (si existe)
    for s in element.select('script[type="application/ld+json"]'):
        try:
            raw = s.string or s.get_text(strip=True) or ""
            if not raw:
                continue
            data = json.loads(raw)

            def iter_objs(o):
                if isinstance(o, dict):
                    yield o
                    for v in o.values():
                        yield from iter_objs(v)
                elif isinstance(o, list):
                    for it in o:
                        yield from iter_objs(it)

            for obj in iter_objs(data):
                # offers.price
                offers = obj.get("offers") if isinstance(obj, dict) else None
                if isinstance(offers, dict):
                    p = coerce_price(offers.get("price"))
                    if p is not None:
                        return p, None
                # algunos JSON-LD ponen price directamente
                p = coerce_price(obj.get("price")) if isinstance(obj, dict) else None
                if p is not None:
                    # no hay forma fiable de inferir PVR aquí
                    return p, None
        except Exception:
            pass

    # 2) data-product (cards de listado suelen tenerlo)
    node = element if getattr(element, "attrs", None) and element.has_attr("data-product") else element.find(attrs={"data-product": True})
    if node and node.has_attr("data-product"):
        try:
            d = json.loads(node["data-product"])
            if isinstance(d, dict):
                price = (coerce_price(d.get("retailPrice")) or coerce_price(d.get("price")) or coerce_price(d.get("specialPrice"))
                         or coerce_price(d.get("finalPrice")) or coerce_price(d.get("salePrice")))
                pvr = (coerce_price(d.get("basePrice")) or coerce_price(d.get("pvr")) or coerce_price(d.get("oldPrice"))
                       or coerce_price(d.get("regularPrice")) or coerce_price(d.get("comparePrice")) or coerce_price(d.get("originalPrice")))
                if price is not None:
                    if pvr is not None and pvr < price:
                        pvr, price = price, pvr
                    if pvr is not None and abs(pvr - price) < 0.01:
                        pvr = None
                    return price, pvr
        except Exception:
            pass

    # 3) Fallback: buscar "NNN€" en el texto del contenedor y escoger los 2 importes más altos "coherentes"
    t = element.get_text(" ", strip=True)
    amounts: List[float] = []
    for m in re.finditer(r"(\d[\d.,]*)\s*€", t):
        # Descarta financiación: "€/mes", "al mes", "meses" justo detrás del €
        after = t[m.end():m.end() + 14].lower()
        after_n = re.sub(r"\s+", "", after)
        if after_n.startswith("/mes") or after_n.startswith("mes") or after_n.startswith("almes") or after_n.startswith("meses"):
            continue

        v = coerce_price(m.group(1))
        if v is not None and v > 0:
            amounts.append(v)

    if not amounts:
        return None, None

    uniq = sorted(set(amounts), reverse=True)
    top = uniq[0]

    # elegir segundo importe que no sea un "importe pequeño" (envío/financiación residual)
    second = None
    for v in uniq[1:]:
        if v <= top * 0.99 and v >= top * 0.30:
            second = v
            break

    if second is None:
        return top, None

    pvr = top
    price = second
    if price > pvr:
        pvr, price = price, pvr
    if abs(pvr - price) < 0.01:
        pvr = None
    return price, pvr


def parse_detail_fields(html: str, url: str) -> Dict[str, Optional[float]]:
    """Extrae precios desde ficha: (price_eur, pvr_eur) con fallbacks."""
    soup = BeautifulSoup(html, "html.parser")
    out: Dict[str, Optional[float]] = {"price_eur": None, "pvr_eur": None}

    # 1) data-product JSON (muy fiable)
    data_product = soup.select_one("[data-product]")
    if data_product and data_product.get("data-product"):
        d = safe_json_loads(data_product.get("data-product"))
        if isinstance(d, dict):
            out["price_eur"] = safe_float(d.get("retailPrice")) or safe_float(d.get("price"))
            out["pvr_eur"] = safe_float(d.get("basePrice")) or safe_float(d.get("pvr"))

    # 2) JSON-LD Product
    if out["price_eur"] is None:
        for sc in soup.select('script[type="application/ld+json"]'):
            data = safe_json_loads(sc.get_text(strip=True))
            if not data:
                continue
            items = data if isinstance(data, list) else [data]
            for it in items:
                if not isinstance(it, dict):
                    continue
                if it.get("@type") not in ("Product", "product"):
                    continue
                offers = it.get("offers")
                if isinstance(offers, dict):
                    out["price_eur"] = safe_float(offers.get("price"))
                elif isinstance(offers, list) and offers:
                    out["price_eur"] = safe_float(offers[0].get("price"))
                if out["price_eur"] is not None:
                    break
            if out["price_eur"] is not None:
                break

    # 3) meta/itemprop
    if out["price_eur"] is None:
        meta_price = soup.select_one('meta[itemprop="price"]')
        if meta_price and meta_price.get("content"):
            out["price_eur"] = safe_float(meta_price.get("content"))

    # 4) DOM heurística por euros
    if out["price_eur"] is None or out["pvr_eur"] is None:
        price, pvr = extract_prices_from_container(soup)
        out["price_eur"] = out["price_eur"] or price
        out["pvr_eur"] = out["pvr_eur"] or pvr

    return out


# --------------------------
# Listing extraction
# --------------------------
@dataclass
class Offer:
    title: str
    url: str
    img: str
    price_eur: Optional[float] = None
    pvr_eur: Optional[float] = None


def extract_listing_candidates(html: str) -> List[Offer]:
    soup = BeautifulSoup(html, "html.parser")
    offers: List[Offer] = []

    # 1) Preferir elementos con data-product (muchas tiendas lo usan en listing)
    nodes = soup.select("[data-product]")
    for node in nodes:
        d = safe_json_loads(node.get("data-product", ""))
        if not isinstance(d, dict):
            continue

        title = d.get("name") or d.get("title") or ""
        url = d.get("url") or ""
        img = d.get("imageUrl") or d.get("image") or ""

        if url and url.startswith("/"):
            url = urljoin(BASE_URL, url)
        if img and img.startswith("//"):
            img = "https:" + img
        if img and img.startswith("/"):
            img = urljoin(BASE_URL, img)

        price = (coerce_price(d.get("retailPrice")) or coerce_price(d.get("price")) or coerce_price(d.get("specialPrice"))
                 or coerce_price(d.get("finalPrice")) or coerce_price(d.get("salePrice")))
        pvr = (coerce_price(d.get("basePrice")) or coerce_price(d.get("pvr")) or coerce_price(d.get("oldPrice"))
               or coerce_price(d.get("regularPrice")) or coerce_price(d.get("comparePrice")) or coerce_price(d.get("originalPrice")))

        # fallback por DOM
        if price is None or pvr is None:
            p2, pvr2 = extract_prices_from_container(node)
            price = price or p2
            pvr = pvr or pvr2

        if not title or not url:
            continue

        offers.append(Offer(title=title.strip(), url=url.strip(), img=img.strip(), price_eur=price, pvr_eur=pvr))

    # 2) Fallback: enlaces a producto si no hubo data-product
    if offers:
        # eliminar duplicados por URL
        uniq: Dict[str, Offer] = {}
        for o in offers:
            uniq[o.url] = o
        return list(uniq.values())

    # Enlaces típicos de producto
    for a in soup.select('a[href^="/es/"]'):
        href = a.get("href", "").strip()
        if not href or href.count("/") != 2:
            continue
        url = urljoin(BASE_URL, href)
        title = a.get("title") or a.get_text(" ", strip=True)
        if not title:
            continue
        container = a.find_parent() or a
        img = ""
        imgn = container.select_one("img")
        if imgn:
            img = imgn.get("data-src") or imgn.get("src") or ""
            if img.startswith("//"):
                img = "https:" + img
            if img.startswith("/"):
                img = urljoin(BASE_URL, img)

        price, pvr = extract_prices_from_container(container)
        offers.append(Offer(title=title.strip(), url=url, img=img.strip(), price_eur=price, pvr_eur=pvr))

    # dedup
    uniq: Dict[str, Offer] = {}
    for o in offers:
        uniq[o.url] = o
    return list(uniq.values())


# --------------------------
# URL shortener (is.gd)
# --------------------------
def shorten_isgd(long_url: str, timeout: int = 20) -> str:
    try:
        api = "https://is.gd/create.php"
        r = requests.get(api, params={"format": "simple", "url": long_url}, timeout=timeout)
        if r.ok and r.text.strip().startswith("http"):
            return r.text.strip()
    except Exception:
        pass
    return long_url


# --------------------------
# Dry-run + JSONL
# --------------------------
def print_required_logs(acf: Dict[str, object]) -> None:
    print("------------------------------------------------------------")
    print(f"Detectado {acf.get('nombre')}")
    print(f"1) Nombre: {acf.get('nombre')}")
    print(f"2) Memoria (memoria): {acf.get('memoria')}")
    print(f"3) Capacidad (capacidad): {acf.get('capacidad')}")
    print(f"4) Versión (version): {acf.get('version')}")
    print(f"5) Fuente (fuente): {acf.get('fuente')}")
    print(f"6) Precio actual (precio_actual): {format_price(acf.get('precio_actual'))}")
    print(f"7) Precio original (precio_original): {format_price(acf.get('precio_original'))}")
    print(f"8) Código de descuento (codigo_de_descuento): {acf.get('codigo_de_descuento')}")
    print(f"9) URL Imagen (imagen_producto): {acf.get('imagen_producto')}")
    print(f"10) Enlace Importado (enlace_de_compra_importado): {acf.get('enlace_de_compra_importado')}")
    print(f"11) Enlace Expandido (url_oferta_sin_acortar): {acf.get('url_oferta_sin_acortar')}")
    print(f"12) URL importada sin afiliado (url_importada_sin_afiliado): {acf.get('url_importada_sin_afiliado')}")
    print(f"13) URL sin acortar con mi afiliado (url_sin_acortar_con_mi_afiliado): {acf.get('url_sin_acortar_con_mi_afiliado')}")
    print(f"14) URL acortada con mi afiliado (url_oferta): {acf.get('url_oferta')}")
    print(f"15) Enviado desde (enviado_desde): {acf.get('enviado_desde')}")
    print("16) Encolado para comparar con base de datos...")


def scrape_dryrun(
    max_products: int,
    sleep_s: float,
    timeout: int,
    user_agent: str,
    include_details: bool,
    jsonl_path: Optional[str],
) -> None:
    html = fetch_html(LISTING_URL, timeout=timeout, user_agent=user_agent)
    candidates = extract_listing_candidates(html)

    if max_products == 0:
        limit = len(candidates)
    else:
        limit = min(max_products, len(candidates))

    out_f = open(jsonl_path, "w", encoding="utf-8") if jsonl_path else None

    for i in range(limit):
        offer = candidates[i]
        raw_name = offer.title

        nombre = clean_product_name(raw_name)

        # RAM/ROM desde título (raw)
        ram, rom = extract_ram_rom_from_name(raw_name)

        # iPhone: completar RAM si falta
        if "iphone" in _fold(nombre):
            if not ram:
                ram = infer_iphone_ram(nombre)
            version = "IOS"
            if not nombre.lower().startswith("apple "):
                nombre = "Apple " + nombre
        else:
            version = "Versión Global"

        # precios: listing ya trae; si details habilitado, mejora desde ficha
        price = offer.price_eur
        pvr = offer.pvr_eur

        if include_details:
            try:
                detail_html = fetch_html(offer.url, timeout=timeout, user_agent=user_agent)
                d = parse_detail_fields(detail_html, offer.url)
                price = d.get("price_eur") or price
                pvr = d.get("pvr_eur") or pvr
            except Exception:
                pass

        # descuento -> código promo
        codigo = "OFERTA PROMO"
        if price is not None and pvr is not None and pvr <= price:
            # si no hay descuento real, aún mantenemos el mismo texto por compat
            codigo = "OFERTA PROMO"

        # urls
        enlace_importado = offer.url
        url_expandido = offer.url
        url_sin_afiliado = offer.url
        url_con_afiliado = offer.url  # powerplanet: misma URL (sin params)
        url_corto = shorten_isgd(url_con_afiliado, timeout=timeout)

        acf = {
            "nombre": nombre,
            "memoria": ram,
            "capacidad": rom,
            "version": version,
            "fuente": "powerplanetonline",
            "precio_actual": price,
            "precio_original": pvr,
            "codigo_de_descuento": codigo,
            "imagen_producto": offer.img,
            "enlace_de_compra_importado": enlace_importado,
            "url_oferta_sin_acortar": url_expandido,
            "url_importada_sin_afiliado": url_sin_afiliado,
            "url_sin_acortar_con_mi_afiliado": url_con_afiliado,
            "url_oferta": url_corto,
            "enviado_desde": "España",
        }

        print_required_logs(acf)

        if out_f:
            payload = {
                "acf": acf,
                "_offer": asdict(offer),
                "_raw_title": raw_name,
                "_listing_url": LISTING_URL,
            }
            out_f.write(json.dumps(payload, ensure_ascii=False) + "\n")

        if sleep_s:
            time.sleep(sleep_s)

    if out_f:
        out_f.close()


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="PowerPlanetOnline scraper (dry-run + jsonl)")
    p.add_argument("--max-products", type=int, default=30, help="Máximo de productos (0 = sin límite)")
    p.add_argument("--sleep", type=float, default=0.0, help="Sleep entre productos")
    p.add_argument("--timeout", type=int, default=25, help="Timeout HTTP")
    p.add_argument("--user-agent", type=str, default=DEFAULT_USER_AGENT)
    p.add_argument("--no-details", action="store_true", help="No entrar en fichas de producto")
    p.add_argument("--jsonl", type=str, default=None, help="Ruta de salida JSONL")

    # compat con tu comando (no se usa internamente)
    p.add_argument("--status", type=str, default="publish", help="Compat: status destino")

    return p


def main(argv: Optional[List[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)

    max_products = int(args.max_products)
    if max_products < 0:
        max_products = 0

    scrape_dryrun(
        max_products=max_products,
        sleep_s=float(args.sleep or 0.0),
        timeout=int(args.timeout),
        user_agent=str(args.user_agent),
        include_details=not bool(args.no_details),
        jsonl_path=args.jsonl,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
