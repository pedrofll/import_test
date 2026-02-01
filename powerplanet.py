#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import html
import json
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
    capacity: Optional[str] = None  # ej "8GB/256GB"

    category_path: Optional[str] = None
    image_large: Optional[str] = None
    product_id: Optional[int] = None

    scraped_at: str = ""


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def normalize_text(s: str) -> str:
    s = s.strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s


def smart_title_token(token: str) -> str:
    """
    - Primera letra en mayúscula.
    - Si mezcla letras/números (14t, 5g), letras en mayúscula => 14T, 5G.
    """
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


def format_price(v: Optional[float]) -> str:
    if v is None:
        return "N/A"
    return f"{v:.2f}€"


def extract_ram_rom_from_name(name: str) -> Tuple[str, str]:
    """
    Extrae RAM/ROM desde el nombre del producto:
      - '8GB/256GB' (o con espacios, guiones, '+', '|')
    Devuelve ('8GB','256GB') o ('','') si no detecta.
    """
    if not name:
        return "", ""

    n = name.replace("\xa0", " ")
    m = re.search(r"(\d+)\s*(GB|TB)\s*[/\+\-\|]\s*(\d+)\s*(GB|TB)", n, flags=re.IGNORECASE)
    if m:
        ram = f"{m.group(1)}{m.group(2).upper()}"
        rom = f"{m.group(3)}{m.group(4).upper()}"
        return ram, rom
    return "", ""


def strip_variant_from_name(name: str) -> str:
    """
    Quita del nombre:
      - el bloque '8GB/256GB' (cualquier separador común)
      - y un color final típico (Negro, Azul, etc.)
    """
    if not name:
        return name

    s = re.sub(r"\s+", " ", name.strip())

    # Quitar RAM/ROM
    s = re.sub(
        r"\s*\b\d+\s*(?:GB|TB)\s*[/\+\-\|]\s*\d+\s*(?:GB|TB)\b\s*",
        " ",
        s,
        flags=re.IGNORECASE,
    )
    s = re.sub(r"\s+", " ", s).strip()

    # Quitar color final
    colors = {
        "negro", "blanco", "azul", "rojo", "verde", "amarillo", "morado", "violeta",
        "gris", "plata", "dorado", "oro", "rosa", "naranja", "cian", "turquesa",
        "beige", "crema", "grafito", "lavanda", "marfil", "champan", "neblina",
        "midnight", "starlight", "titanio", "titanium",
    }
    parts = s.split(" ")
    if parts and normalize_text(parts[-1]) in colors:
        s = " ".join(parts[:-1]).strip()

    return re.sub(r"\s+", " ", s).strip()


def compute_version(clean_name: str) -> str:
    # PowerPlanet es tienda España: Global (salvo iPhone => IOS)
    n = normalize_text(clean_name)
    if "iphone" in n:
        return "IOS"
    return "Global"


def build_affiliate_url(url: str, affiliate_query: str) -> str:
    """
    Añade parámetros de afiliado (string tipo 'utm_source=x&utm_campaign=y').
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
                a for a in anchors
                if a["href"].startswith("/es/")
                and "moviles-mas-vendidos" not in a["href"]
                and len(a.get_text(" ", strip=True)) >= 6
            ]
            if prod_anchors:
                chosen = max(prod_anchors, key=lambda a: len(a.get_text(" ", strip=True)))
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
    if ip:
        content = ip.get("content")
        if content:
            price = safe_float(content)

    ib = soup.select_one(".data-all-prices .product-basePrice .integerPrice")
    if ib:
        content = ib.get("content")
        if content:
            pvr = safe_float(content)

    return price, pvr


def parse_detail_fields(detail_html: str) -> Dict[str, Optional[object]]:
    """
    IMPORTANTE: usamos retailPrice/basePrice (con IVA), NO alternative* (sin IVA).
    """
    soup = BeautifulSoup(detail_html, "html.parser")
    out: Dict[str, Optional[object]] = {}

    data = parse_product_data_json(soup)
    if data:
        out["product_id"] = data.get("id")
        out["ref"] = data.get("sku")
        out["name"] = data.get("name")
        out["brand"] = data.get("brandName")
        out["category_path"] = data.get("mainCategoryName")

        defn = data.get("definition") or {}

        # ✅ Con IVA (lo que ves en web): retailPrice / basePrice
        price = safe_float(defn.get("retailPrice"))
        if price is None:
            price = safe_float(defn.get("price")) or safe_float(defn.get("productRetailPrice"))

        pvr = safe_float(defn.get("basePrice"))
        if pvr is None:
            pvr = safe_float(defn.get("productBasePrice"))

        # Si falla, fallback DOM (sin usar alternative*)
        if price is None or pvr is None:
            dom_price, dom_pvr = parse_prices_from_dom(soup)
            price = price if price is not None else dom_price
            pvr = pvr if pvr is not None else dom_pvr

        out["price_eur"] = price
        out["pvr_eur"] = pvr

    # Imagen principal
    img = soup.select_one("img#main-image") or soup.select_one("img.mainImageTag")
    if img:
        out["image_large"] = (img.get("data-original") or img.get("src") or "").strip() or None

    # Fallback nombre
    if not out.get("name"):
        h1 = soup.select_one("h1.real-title, h1.h1, h1")
        if h1:
            out["name"] = h1.get_text(" ", strip=True)

    # Fallback precios si data-product no parseó
    if out.get("price_eur") is None or out.get("pvr_eur") is None:
        dom_price, dom_pvr = parse_prices_from_dom(soup)
        out["price_eur"] = out.get("price_eur") if out.get("price_eur") is not None else dom_price
        out["pvr_eur"] = out.get("pvr_eur") if out.get("pvr_eur") is not None else dom_pvr

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

    if cat and any(k in cat for k in ["tablet", "wearable", "smartwatch", "smartband"]):
        return False, "EXCLUDE:category_tablet_or_wearable"

    if capacity and "gb" in normalize_text(capacity):
        return True, "INCLUDE:capacity_has_gb"

    ram, rom = extract_ram_rom_from_name(name)
    if ram and rom:
        return True, "INCLUDE:name_has_ram_rom"

    return False, "EXCLUDE:no_mobile_category_and_no_capacity"


def print_required_logs(
    nombre: str,
    ram: str,
    rom: str,
    ver: str,
    fuente: str,
    p_act: str,
    p_reg: str,
    cup: str,
    img_src: str,
    url_imp: str,
    url_exp: str,
    url_importada_sin_afiliado: str,
    url_sin_acortar_con_mi_afiliado: str,
    url_oferta: str,
    enviado_desde: str,
) -> None:
    print(f"Detectado {nombre}")
    print(f"1) Nombre: {nombre}")
    print(f"2) Memoria: {ram}")
    print(f"3) Capacidad: {rom}")
    print(f"4) Versión: {ver}")
    print(f"5) Fuente: {fuente}")
    print(f"6) Precio actual: {p_act}")
    print(f"7) Precio original: {p_reg}")
    print(f"8) Código de descuento: {cup}")
    print(f"9) Version: {ver}")
    print(f"10) URL Imagen: {img_src}")
    print(f"11) Enlace Importado: {url_imp}")
    print(f"12) Enlace Expandido: {url_exp}")
    print(f"13) URL importada sin afiliado: {url_importada_sin_afiliado}")
    print(f"14) URL sin acortar con mi afiliado: {url_sin_acortar_con_mi_afiliado}")
    print(f"15) URL acortada con mi afiliado: {url_oferta}")
    print(f"16) Enviado desde: {enviado_desde}")
    print(f"17) Encolado para comparar con base de datos...")
    print("-" * 60)


def scrape_dryrun(
    max_products: int,
    sleep_seconds: float,
    timeout: int,
    include_details: bool,
    write_jsonl_path: Optional[str],
    affiliate_query: str,
    do_isgd: bool,
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

            raw_name = offer.name
            ram, rom = extract_ram_rom_from_name(raw_name)
            if ram and rom:
                offer.capacity = f"{ram}/{rom}"

            is_mobile, _reason = classify_offer(offer.name, offer.category_path, offer.capacity)
            if not is_mobile:
                continue

            nombre_limpio = format_product_title(strip_variant_from_name(raw_name))
            ver = compute_version(nombre_limpio)
            fuente = FUENTE_POWERPLANET

            p_act = format_price(offer.price_eur)
            p_reg = format_price(offer.pvr_eur)

            cup = CUPON_DEFAULT
            img_src = (offer.image_large or "").strip()

            url_imp = offer.url
            url_exp = offer.url

            url_importada_sin_afiliado = offer.url
            url_sin_acortar_con_mi_afiliado = build_affiliate_url(offer.url, affiliate_query)

            url_oferta = url_sin_acortar_con_mi_afiliado
            if do_isgd:
                url_oferta = shorten_isgd(sess, url_sin_acortar_con_mi_afiliado)

            enviado_desde = ENVIO_POWERPLANET

            print_required_logs(
                nombre=nombre_limpio,
                ram=ram,
                rom=rom,
                ver=ver,
                fuente=fuente,
                p_act=p_act,
                p_reg=p_reg,
                cup=cup,
                img_src=img_src,
                url_imp=url_imp,
                url_exp=url_exp,
                url_importada_sin_afiliado=url_importada_sin_afiliado,
                url_sin_acortar_con_mi_afiliado=url_sin_acortar_con_mi_afiliado,
                url_oferta=url_oferta,
                enviado_desde=enviado_desde,
            )

            if jsonl_file:
                payload = asdict(offer)
                payload["_affiliate_query"] = affiliate_query
                payload["_nombre_limpio"] = nombre_limpio
                payload["_ram"] = ram
                payload["_rom"] = rom
                payload["_version"] = ver
                payload["_url_oferta_isgd"] = url_oferta
                jsonl_file.write(json.dumps(payload, ensure_ascii=False) + "\n")

    finally:
        if jsonl_file:
            jsonl_file.close()


def main() -> None:
    ap = argparse.ArgumentParser(description="PowerPlanetOnline - Móviles más vendidos (DRY-RUN SOLO LOGS)")
    ap.add_argument("--max-products", type=int, default=0, help="0 = sin límite")
    ap.add_argument("--sleep", type=float, default=0.7, help="segundos entre requests")
    ap.add_argument("--timeout", type=int, default=25, help="timeout por request (seg)")
    ap.add_argument("--no-details", action="store_true", help="no entra en fichas (menos datos, peor precisión)")
    ap.add_argument("--jsonl", default="", help="ruta para guardar JSONL (opcional)")
    ap.add_argument(
        "--affiliate-query",
        default="",
        help="querystring para afiliado, ej: 'utm_source=ofertasdemoviles&utm_medium=referral'",
    )
    ap.add_argument("--no-isgd", action="store_true", help="no acortar url_oferta con is.gd")
    args = ap.parse_args()

    scrape_dryrun(
        max_products=args.max_products,
        sleep_seconds=args.sleep,
        timeout=args.timeout,
        include_details=(not args.no_details),
        write_jsonl_path=(args.jsonl.strip() or None),
        affiliate_query=args.affiliate_query.strip(),
        do_isgd=(not args.no_isgd),
    )


if __name__ == "__main__":
    main()
