#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import math
import re
import time
import unicodedata
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse, parse_qsl, urlencode, urlunparse

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.powerplanetonline.com"
LIST_URL = f"{BASE_URL}/es/moviles-mas-vendidos"

# --- CONSTANTES DE TU PROYECTO (PowerPlanet) ---
FUENTE_POWERPLANET = "powerplanetonline"
ENVIO_POWERPLANET = "España"
CUPON_DEFAULT = "OFERTA PROMO"
IMPORTADO_DE_POWERPLANET = BASE_URL  # ACF: importado_de


@dataclass
class Offer:
    source: str
    name: str
    url: str

    price_eur: Optional[float] = None
    pvr_eur: Optional[float] = None
    discount_pct: Optional[int] = None

    reviews_count: Optional[int] = None
    rating: Optional[float] = None

    brand: Optional[str] = None
    ref: Optional[str] = None
    capacity: Optional[str] = None
    color: Optional[str] = None

    category_path: Optional[str] = None
    image_large: Optional[str] = None
    image_small: Optional[str] = None
    product_id: Optional[int] = None

    scraped_at: str = ""


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def today_ddmmyyyy() -> str:
    """Fecha formato ACF: dd/mm/yyyy."""
    return datetime.now().strftime("%d/%m/%Y")


def normalize_text(s: str) -> str:
    s = s.strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s


def smart_title_token(token: str) -> str:
    """Capitalización especial de tokens.

    - Primera letra en mayúscula.
    - Si mezcla letras/números (14t, 5g), letras en mayúscula => 14T, 5G.
    """
    if not token:
        return token

    raw = token.strip()

    # Preservar separadores internos (muy típico: "Pro+", etc.)
    parts = re.split(r"(-)", raw)
    out_parts: List[str] = []
    for p in parts:
        if p == "-":
            out_parts.append(p)
            continue

        low = p.lower()

        # Excepciones frecuentes
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
    # Normaliza espacios y capitaliza tokens
    name = re.sub(r"\s+", " ", (name or "").strip())
    tokens = name.split(" ") if name else []
    return " ".join(smart_title_token(t) for t in tokens)


def strip_after_4g_5g(nombre_5g: str) -> str:
    """Devuelve el nombre base cortando en el primer token 4G/5G y eliminando todo lo posterior.

    Ej:
      - 'Oppo A5 Pro 5G Marrón' -> 'Oppo A5 Pro'
      - 'Motorola Moto G15 4G'  -> 'Motorola Moto G15'
    """
    if not nombre_5g:
        return nombre_5g

    tokens = nombre_5g.split()
    kept: List[str] = []
    for tok in tokens:
        tok_clean = re.sub(r"[^0-9A-Za-z]+", "", tok).lower()
        if tok_clean in {"4g", "5g"}:
            break
        kept.append(tok)

    base = " ".join(kept).strip()
    return base if base else nombre_5g.strip()


def build_nombre_fields(raw_name: str) -> Tuple[str, str]:
    """Construye:
      - nombre_5g: EXACTAMENTE lo que imprimimos tras 'Detectado ...' (ACF 'nombre_5g')
      - nombre: nombre limpio para Woo (sin 4G/5G y sin el resto de especificaciones)
    """
    nombre_5g = format_product_title(re.sub(r"\s+", " ", (raw_name or "").strip()))

    # Nombre base: cortar en 4G/5G y limpiar variantes habituales (RAM/ROM + color final)
    nombre_base = strip_after_4g_5g(nombre_5g)
    nombre_base = strip_variant_from_name(nombre_base)
    nombre_base = format_product_title(nombre_base)

    return nombre_5g, nombre_base


def safe_float(v) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def parse_eur_amount(s: str) -> Optional[float]:
    if not s:
        return None
    s = s.strip().replace("\xa0", " ")
    m = re.search(r"(\d[\d\.\,]*)\s*€", s)
    if not m:
        return None
    num = m.group(1).replace(".", "").replace(",", ".")
    try:
        return float(num)
    except ValueError:
        return None


def parse_pct(s: str) -> Optional[int]:
    m = re.search(r"-\s*(\d{1,3})\s*%", s)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def parse_int_from(s: str, pattern: str) -> Optional[int]:
    m = re.search(pattern, s, flags=re.IGNORECASE)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def parse_float_from(s: str, pattern: str) -> Optional[float]:
    m = re.search(pattern, s, flags=re.IGNORECASE)
    if not m:
        return None
    val = m.group(1).strip().replace(",", ".")
    try:
        return float(val)
    except ValueError:
        return None


def truncate_price(v: Optional[float]) -> Optional[int]:
    """Trunca el precio eliminando decimales (174.99 -> 174)."""
    if v is None:
        return None
    try:
        return int(math.floor(float(v)))
    except Exception:
        return None


def format_price(v: Optional[float]) -> str:
    """Formato precio sin decimales (TRUNCADO)."""
    iv = truncate_price(v)
    return f"{iv}€" if iv is not None else "N/A"


def extract_ram_rom_from_name(name: str, url: str = "") -> Tuple[str, str]:
    """Extrae RAM/ROM desde nombre y/o URL.

    Soporta:
      - 8GB/256GB, 8GB+256GB, 8GB-256GB
      - 8GB 256GB
      - 8GB256GB
      - 4B128GB (slugs)
      - Fallback por heurística si hay 2+ tokens 'GB/TB'
      - Fallback desde URL tipo ...-8gb-256gb-...
    """
    if not name:
        return "", ""

    n = (name or "").replace("\xa0", " ").strip()

    # 4B128GB (slugs)
    m = re.search(r"\b(\d+)\s*b\s*(\d+)\s*(GB|TB)\b", n, flags=re.IGNORECASE)
    if m:
        ram = f"{m.group(1)}GB"
        rom = f"{m.group(2)}{m.group(3).upper()}"
        return ram, rom

    # 8GB/256GB, 8GB+256GB, 8GB-256GB, 8GB|256GB
    m = re.search(r"(\d+)\s*(GB|TB)\s*[/\+\-\|]\s*(\d+)\s*(GB|TB)", n, flags=re.IGNORECASE)
    if m:
        ram = f"{m.group(1)}{m.group(2).upper()}"
        rom = f"{m.group(3)}{m.group(4).upper()}"
        return ram, rom

    # 8GB 256GB
    m = re.search(r"\b(\d+)\s*(GB|TB)\s+(\d+)\s*(GB|TB)\b", n, flags=re.IGNORECASE)
    if m:
        ram = f"{m.group(1)}{m.group(2).upper()}"
        rom = f"{m.group(3)}{m.group(4).upper()}"
        return ram, rom

    # 8GB256GB (sin separador)
    m = re.search(r"\b(\d+)\s*GB\s*(\d+)\s*(GB|TB)\b", n, flags=re.IGNORECASE)
    if m:
        ram = f"{m.group(1)}GB"
        rom = f"{m.group(2)}{m.group(3).upper()}"
        return ram, rom

    # Fallback URL: ...-8gb-256gb-...
    if url:
        try:
            p = urlparse(url)
            path = (p.path or "").lower()
            m = re.search(r"-(\d+)gb-(\d+)gb(?:-|\b)", path)
            if m:
                return f"{m.group(1)}GB", f"{m.group(2)}GB"
        except Exception:
            pass

    # Heurística: capturar todos los tokens GB/TB y deducir RAM/ROM
    vals_gb: List[int] = []
    for mm in re.finditer(r"\b(\d+)\s*(GB|TB)\b", n, flags=re.IGNORECASE):
        try:
            v = int(mm.group(1))
            unit = (mm.group(2) or "").upper()
            gb = v * 1024 if unit == "TB" else v
            vals_gb.append(gb)
        except Exception:
            continue

    if len(vals_gb) >= 2:
        # RAM suele ser <= 32GB; ROM suele ser >= 64GB
        ram_candidates = [v for v in vals_gb if 1 <= v <= 32]
        rom_candidates = [v for v in vals_gb if v >= 64]

        if ram_candidates and rom_candidates:
            ram = max(ram_candidates)
            rom = max(rom_candidates)
            return f"{ram}GB", f"{rom}GB"

        # fallback general: menor como RAM, mayor como ROM
        vals_sorted = sorted(set(vals_gb))
        if len(vals_sorted) >= 2:
            ram = vals_sorted[0]
            rom = vals_sorted[-1]
            return f"{ram}GB", f"{rom}GB"

    return "", ""


def strip_variant_from_name(name: str) -> str:
    """Quita del nombre:
      - el bloque RAM/ROM (múltiples formatos: 8GB/256GB, 8GB 256GB, 4B128GB, 8GB128GB)
      - y un color final típico (Negro, Azul, etc.)
    """
    if not name:
        return name

    s = re.sub(r"\s+", " ", name.strip())

    # Quitar RAM/ROM (varios formatos)
    for pat in (
        # 8GB/256GB, 8GB+256GB, 8GB-256GB
        r"\s*\b\d+\s*(?:GB|TB)\s*[/\+\-\|]\s*\d+\s*(?:GB|TB)\b\s*",
        # 8GB 256GB
        r"\s*\b\d+\s*(?:GB|TB)\s+\d+\s*(?:GB|TB)\b\s*",
        # 4B128GB (slugs)
        r"\s*\b\d+\s*b\s*\d+\s*(?:GB|TB)\b\s*",
        # 8GB128GB (sin separador explícito)
        r"\s*\b\d+\s*GB\s*\d+\s*(?:GB|TB)\b\s*",
    ):
        s = re.sub(pat, " ", s, flags=re.IGNORECASE)

    s = re.sub(r"\s+", " ", s).strip()

    # Quitar color final (si coincide con lista típica)
    colors = {
        "negro", "blanco", "azul", "rojo", "verde", "amarillo", "morado", "violeta",
        "gris", "plata", "dorado", "oro", "rosa", "naranja", "cian", "turquesa",
        "beige", "crema", "grafito", "lavanda", "marfil", "champan", "neblina",
        "midnight", "starlight", "titanio", "titanium",
        # ejemplo del cliente: "Marrón"
        "marron",
    }
    parts = s.split(" ")
    if parts and normalize_text(parts[-1]) in colors:
        s = " ".join(parts[:-1]).strip()

    return re.sub(r"\s+", " ", s).strip()


def compute_version(clean_name: str) -> str:
    """Reglas de tu proyecto:
      - iPhone => IOS
      - PowerPlanet (tienda España) y no iPhone => Global
    """
    n = normalize_text(clean_name)
    if "iphone" in n:
        return "IOS"
    return "Global"


def build_affiliate_url(url: str, affiliate_query: str) -> str:
    """Añade parámetros de afiliado (string tipo 'utm_source=x&utm_campaign=y').
    Si affiliate_query está vacío, devuelve url sin cambios.
    """
    if not affiliate_query.strip():
        return url

    parsed = urlparse(url)
    current = dict(parse_qsl(parsed.query, keep_blank_values=True))
    extra = dict(parse_qsl(affiliate_query, keep_blank_values=True))
    current.update(extra)

    new_query = urlencode(current, doseq=True)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))


def shorten_isgd(sess: requests.Session, url: str, timeout: int = 15, retries: int = 5) -> str:
    """Acorta con is.gd (format=simple). Si falla, devuelve la URL larga."""
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


def extract_listing_candidates(list_html: str) -> List[Offer]:
    soup = BeautifulSoup(list_html, "html.parser")
    offers: Dict[str, Offer] = {}

    # Heurística: encontrar bloques que contengan "PVR" y extraer nombre/URL/precios
    pvr_nodes = soup.find_all(string=re.compile(r"\bPVR\b", re.IGNORECASE))
    for node in pvr_nodes:
        container = node.parent
        chosen = None
        chosen_container = None

        for _ in range(7):
            if container is None:
                break

            blob = container.get_text(" ", strip=True)
            if "€" not in blob:
                container = container.parent
                continue

            anchors = container.find_all("a", href=True)
            prod_anchors = [
                a
                for a in anchors
                if a["href"].startswith("/es/")
                and "moviles-mas-vendidos" not in a["href"]
                and len(a.get_text(" ", strip=True)) >= 6
            ]
            if prod_anchors:
                a_best = max(prod_anchors, key=lambda a: len(a.get_text(" ", strip=True)))
                chosen = a_best
                chosen_container = container
                break

            container = container.parent

        if not chosen or not chosen_container:
            continue

        url = urljoin(BASE_URL, chosen["href"])
        chosen_text = chosen.get_text(" ", strip=True)
        block_text = chosen_container.get_text(" ", strip=True).replace("\xa0", " ")

        m = re.search(r"PVR\s*([0-9\.\,]+)\s*€\s*([0-9\.\,]+)\s*€", block_text, re.IGNORECASE)
        pvr = price = None
        if m:
            pvr = parse_eur_amount(m.group(1) + "€")
            price = parse_eur_amount(m.group(2) + "€")
        else:
            euros = re.findall(r"\d[\d\.\,]*\s*€", block_text)
            if len(euros) >= 2:
                pvr = parse_eur_amount(euros[0])
                price = parse_eur_amount(euros[1])

        discount = parse_pct(block_text)
        reviews = parse_int_from(block_text, r"\((\d+)\s*opiniones\)")

        offers[url] = Offer(
            source=FUENTE_POWERPLANET,
            name=chosen_text,
            url=url,
            price_eur=price,
            pvr_eur=pvr,
            discount_pct=discount,
            reviews_count=reviews,
            rating=None,
            scraped_at=now_iso(),
        )

    return list(offers.values())


def parse_product_data_json(soup: BeautifulSoup) -> Optional[dict]:
    """Extrae el JSON del atributo data-product (fuente de verdad: nombre/sku/precios)."""
    form = soup.find("form", attrs={"data-product": True})
    if not form:
        return None
    raw = form.get("data-product")
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def parse_detail_fields(detail_html: str) -> Dict[str, Optional[object]]:
    """PowerPlanet: prioriza el JSON data-product para nombre/sku/precios."""
    soup = BeautifulSoup(detail_html, "html.parser")
    out: Dict[str, Optional[object]] = {}

    # 1) Fuente de verdad: data-product JSON
    data = parse_product_data_json(soup)
    if data:
        out["product_id"] = data.get("id")
        out["ref"] = data.get("sku")
        out["name"] = data.get("name")
        out["brand"] = data.get("brandName")

        defn = data.get("definition") or {}
        out["price_eur"] = safe_float(defn.get("price") or defn.get("retailPrice") or defn.get("productRetailPrice"))
        out["pvr_eur"] = safe_float(defn.get("basePrice") or defn.get("productBasePrice"))

        # Nota: mainCategoryName es solo el último segmento
        out["category_path"] = data.get("mainCategoryName")

    # 2) Imagen principal (src o data-original)
    img = soup.select_one("img#main-image") or soup.select_one("img.mainImageTag")
    if img:
        out["image_large"] = (img.get("data-original") or img.get("src") or "").strip() or None

    # 3) Fallbacks por si falla el JSON (muy raro)
    if not out.get("name"):
        h1 = soup.select_one("h1.real-title, h1.h1, h1")
        if h1:
            out["name"] = h1.get_text(" ", strip=True)

    return out


def classify_offer(name: str, category_path: Optional[str], capacity: Optional[str]) -> Tuple[bool, str]:
    n = normalize_text(name)
    cat = normalize_text(category_path) if category_path else ""

    if " ipad" in f" {n} ":
        return False, "EXCLUDE:name_contains_ipad"
    if " tab" in f" {n} " or "tablet" in n:
        return False, "EXCLUDE:name_contains_tab/tablet"
    if "smartwatch" in n or "smartband" in n or "reloj" in n:
        return False, "EXCLUDE:name_contains_watch/band"

    if cat:
        if any(k in cat for k in ["tablet", "wearable", "smartwatch", "smartband"]):
            return False, "EXCLUDE:category_tablet_or_wearable"
        if any(k in cat for k in ["moviles", "smartphones"]):
            return True, "INCLUDE:category_mobile"

    if capacity and "gb" in normalize_text(capacity):
        return True, "INCLUDE:capacity_has_gb"

    # Último recurso: si el nombre contiene RAM/ROM => móvil
    ram, rom = extract_ram_rom_from_name(name)
    if ram and rom:
        return True, "INCLUDE:name_has_ram_rom"

    return False, "EXCLUDE:no_mobile_category_and_no_capacity"


def print_required_logs(
    nombre_5g: str,
    nombre: str,
    memoria: str,
    capacidad: str,
    version: str,
    fuente: str,
    importado_de: str,
    precio_actual: Optional[int],
    precio_original: Optional[int],
    codigo_de_descuento: str,
    imagen_producto: str,
    enlace_de_compra_importado: str,
    url_oferta_sin_acortar: str,
    url_importada_sin_afiliado: str,
    url_sin_acortar_con_mi_afiliado: str,
    url_oferta: str,
    enviado_desde: str,
    fecha: str,
) -> None:
    def fmt_eur(v: Optional[int]) -> str:
        return f"{v}€" if v is not None else "N/A"

    # Detectado: es lo que almacenamos en ACF nombre_5g
    print(f"Detectado {nombre_5g}")

    # --- LOGS ACF (en este orden) ---
    print(f"1) Nombre Importado (nombre_5g): {nombre_5g}")
    print(f"2) Nombre (nombre): {nombre}")
    print(f"3) Memoria (memoria): {memoria}")
    print(f"4) Capacidad (capacidad): {capacidad}")
    print(f"5) Versión (version): {version}")
    print(f"6) Fuente (fuente): {fuente}")
    print(f"7) Importado de (importado_de): {importado_de}")
    print(f"8) Precio actual (precio_actual): {fmt_eur(precio_actual)}")
    print(f"9) Precio original (precio_originl): {fmt_eur(precio_original)}")
    print(f"10) Código de descuento (codigo_de_descuento): {codigo_de_descuento}")
    print(f"11) Version (version): {version}")
    print(f"12) URL Imagen (imagen_producto): {imagen_producto}")
    print(f"13) Enlace Importado (enlace_de_compra_importado): {enlace_de_compra_importado}")
    print(f"14) Enlace Expandido (url_oferta_sin_acortar): {url_oferta_sin_acortar}")
    print(f"15) URL importada sin afiliado (url_importada_sin_afiliado): {url_importada_sin_afiliado}")
    print(f"16) URL sin acortar con mi afiliado (url_sin_acortar_con_mi_afiliado): {url_sin_acortar_con_mi_afiliado}")
    print(f"17) URL acortada con mi afiliado (url_oferta): {url_oferta}")
    print(f"18) Enviado desde (enviado_desde): {enviado_desde}")
    print(f"19) Fecha (fecha): {fecha}")

    print("20) Encolado para comparar con base de datos...")
    print("-" * 60)


def scrape_dryrun(
    max_products: int,
    sleep_seconds: float,
    timeout: int,
    include_details: bool,
    write_jsonl_path: Optional[str],
    affiliate_query: str,
    do_isgd: bool,
    status: str,
) -> None:
    sess = make_session()
    list_html = fetch_html(sess, LIST_URL, timeout=timeout)
    candidates = extract_listing_candidates(list_html)

    if max_products > 0:
        candidates = candidates[:max_products]

    jsonl_file = open(write_jsonl_path, "w", encoding="utf-8") if write_jsonl_path else None

    try:
        for offer in candidates:
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

            if include_details:
                detail_html = fetch_html(sess, offer.url, timeout=timeout)

                fields = parse_detail_fields(detail_html)

                # Preferir SIEMPRE los campos de ficha (sobrescriben listado)
                if fields.get("name"):
                    offer.name = str(fields["name"])
                if fields.get("ref"):
                    offer.ref = str(fields["ref"])
                if fields.get("brand"):
                    offer.brand = str(fields["brand"])
                if fields.get("category_path"):
                    offer.category_path = str(fields["category_path"])
                if fields.get("image_large"):
                    offer.image_large = str(fields["image_large"])
                if fields.get("price_eur") is not None:
                    offer.price_eur = float(fields["price_eur"])  # type: ignore
                if fields.get("pvr_eur") is not None:
                    offer.pvr_eur = float(fields["pvr_eur"])  # type: ignore
                if fields.get("product_id") is not None:
                    try:
                        offer.product_id = int(fields["product_id"])  # type: ignore
                    except Exception:
                        pass

            # RAM/ROM: primero del nombre (8GB/256GB), si no, por URL (slugs)
            raw_name = offer.name
            ram, rom = extract_ram_rom_from_name(raw_name, offer.url)

            # Guardar capacity para clasificación/filtro (ram/rom)
            if ram and rom:
                offer.capacity = f"{ram}/{rom}"

            # 1) Nombre_5G + Nombre (limpio)
            nombre_5g, nombre_limpio = build_nombre_fields(raw_name)

            # 3) Excluir Oukitel
            if re.match(r"^oukitel\b", normalize_text(nombre_5g)):
                continue

            # Clasificación (móvil / excluir tablets)
            is_mobile, reason = classify_offer(nombre_5g, offer.category_path, offer.capacity)
            if not is_mobile:
                continue

            ver = compute_version(nombre_limpio)
            fuente = offer.source or FUENTE_POWERPLANET

            # 2) Precios sin decimales (TRUNCADOS)
            precio_actual_int = truncate_price(offer.price_eur)
            precio_original_int = truncate_price(offer.pvr_eur)

            cup = CUPON_DEFAULT

            img_src = (offer.image_large or offer.image_small or "").strip()

            importado_de = IMPORTADO_DE_POWERPLANET
            fecha = today_ddmmyyyy()

            enlace_de_compra_importado = offer.url
            url_importada_sin_afiliado = offer.url
            url_sin_acortar_con_mi_afiliado = build_affiliate_url(offer.url, affiliate_query)
            url_oferta_sin_acortar = url_sin_acortar_con_mi_afiliado

            url_oferta = url_oferta_sin_acortar
            if do_isgd:
                url_oferta = shorten_isgd(sess, url_oferta_sin_acortar)

            enviado_desde = ENVIO_POWERPLANET

            print_required_logs(
                nombre_5g=nombre_5g,
                nombre=nombre_limpio,
                memoria=ram,
                capacidad=rom,
                version=ver,
                fuente=fuente,
                importado_de=importado_de,
                precio_actual=precio_actual_int,
                precio_original=precio_original_int,
                codigo_de_descuento=cup,
                imagen_producto=img_src,
                enlace_de_compra_importado=enlace_de_compra_importado,
                url_oferta_sin_acortar=url_oferta_sin_acortar,
                url_importada_sin_afiliado=url_importada_sin_afiliado,
                url_sin_acortar_con_mi_afiliado=url_sin_acortar_con_mi_afiliado,
                url_oferta=url_oferta,
                enviado_desde=enviado_desde,
                fecha=fecha,
            )

            if jsonl_file:
                payload = asdict(offer)
                payload["_reason"] = reason
                payload["_affiliate_query"] = affiliate_query

                # --- ACF (campos finales a asignar en Woo) ---
                payload["acf"] = {
                    "nombre_5g": nombre_5g,
                    "nombre": nombre_limpio,
                    "memoria": ram,
                    "capacidad": rom,
                    "version": ver,
                    "fuente": fuente,
                    "importado_de": importado_de,
                    "precio_actual": precio_actual_int,
                    "precio_originl": precio_original_int,
                    "codigo_de_descuento": cup,
                    "imagen_producto": img_src,
                    "enlace_de_compra_importado": enlace_de_compra_importado,
                    "url_oferta_sin_acortar": url_oferta_sin_acortar,
                    "url_importada_sin_afiliado": url_importada_sin_afiliado,
                    "url_sin_acortar_con_mi_afiliado": url_sin_acortar_con_mi_afiliado,
                    "url_oferta": url_oferta,
                    "enviado_desde": enviado_desde,
                    "fecha": fecha,
                }

                # Campos ACF / normalizados
                payload["_acf_nombre"] = nombre_limpio
                payload["_acf_nombre_5g"] = nombre_5g
                payload["_nombre_limpio"] = nombre_limpio  # alias compat

                payload["_ram"] = ram
                payload["_rom"] = rom
                payload["_version"] = ver

                payload["_acf_precio_actual"] = precio_actual_int
                payload["_acf_precio_original"] = precio_original_int

                payload["_url_oferta_isgd"] = url_oferta
                payload["_status"] = status

                jsonl_file.write(json.dumps(payload, ensure_ascii=False) + "\n")

    finally:
        if jsonl_file:
            jsonl_file.close()


def main() -> None:
    ap = argparse.ArgumentParser(
        description="PowerPlanetOnline - Móviles más vendidos (DRY-RUN SOLO LOGS + formato requerido)"
    )
    ap.add_argument("--max-products", type=int, default=0, help="0 = sin límite")
    ap.add_argument("--sleep", type=float, default=0.7, help="segundos entre requests")
    ap.add_argument("--timeout", type=int, default=25, help="timeout por request (seg)")
    ap.add_argument("--no-details", action="store_true", help="no entra en fichas (menos datos, peor precisión)")
    ap.add_argument("--jsonl", default="", help="ruta para guardar JSONL (opcional). Ej: logs/powerplanet.jsonl")
    ap.add_argument(
        "--affiliate-query",
        default="",
        help="querystring para afiliado, ej: 'utm_source=ofertasdemoviles&utm_medium=referral'",
    )
    ap.add_argument("--no-isgd", action="store_true", help="no acortar url_oferta con is.gd (recomendado: NO usar este flag)")

    # Compatibilidad con tu runner (aunque este script sea dry-run)
    ap.add_argument(
        "--status",
        default="publish",
        choices=["publish", "draft", "pending", "private"],
        help="Compatibilidad CLI. En este dry-run no crea productos, solo se guarda en JSONL.",
    )

    args = ap.parse_args()

    scrape_dryrun(
        max_products=args.max_products,
        sleep_seconds=args.sleep,
        timeout=args.timeout,
        include_details=(not args.no_details),
        write_jsonl_path=(args.jsonl.strip() or None),
        affiliate_query=args.affiliate_query.strip(),
        do_isgd=(not args.no_isgd),
        status=args.status,
    )


if __name__ == "__main__":
    main()
