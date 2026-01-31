#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
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


def normalize_text(s: str) -> str:
    s = s.strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s


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


def format_price(v: Optional[float]) -> str:
    if v is None:
        return "N/A"
    return f"{v:.2f}€"


def split_ram_rom(capacity: Optional[str]) -> Tuple[str, str]:
    """
    Devuelve (ram, rom) como strings (ej: '12GB', '256GB').
    Si solo hay una cifra (ej: '256GB'), se asume ROM.
    """
    if not capacity:
        return "", ""

    c = capacity.replace("\xa0", " ").strip()
    c_norm = normalize_text(c)

    # patrones tipo 12GB/256GB, 12gb + 256gb, 12GB-256GB
    m = re.search(r"(\d+)\s*(gb|tb)\s*[/\+\-\|]\s*(\d+)\s*(gb|tb)", c_norm, flags=re.IGNORECASE)
    if m:
        ram = f"{m.group(1)}{m.group(2).upper()}"
        rom = f"{m.group(3)}{m.group(4).upper()}"
        return ram, rom

    # solo una cifra: 256GB / 1TB etc -> ROM
    m2 = re.search(r"\b(\d+)\s*(gb|tb)\b", c_norm, flags=re.IGNORECASE)
    if m2:
        return "", f"{m2.group(1)}{m2.group(2).upper()}"

    return "", ""


def guess_color_from_name(name: str) -> str:
    """
    Fallback por si no viene color en ficha.
    Detecta colores típicos al final del nombre.
    """
    n = normalize_text(name)
    colors = [
        "negro", "blanco", "azul", "rojo", "verde", "amarillo", "morado", "violeta",
        "gris", "plata", "dorado", "oro", "rosa", "naranja", "cian", "turquesa",
        "beige", "crema", "grafito", "lavanda", "marfil", "champan", "champa",
        "neblina", "midnight", "starlight", "titanio", "titanium"
    ]
    for c in colors:
        if n.endswith(" " + c) or n.endswith("-" + c):
            return c
    return ""


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
        chosen_text = ""
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
                a_best = max(prod_anchors, key=lambda a: len(a.get_text(" ", strip=True)))
                chosen = a_best
                chosen_text = a_best.get_text(" ", strip=True)
                chosen_container = container
                break

            container = container.parent

        if not chosen or not chosen_container:
            continue

        url = urljoin(BASE_URL, chosen["href"])
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
            source="powerplanetonline",
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


def parse_tracking_line(html: str, product_url: str) -> Optional[Dict[str, object]]:
    idx = html.find(product_url)
    if idx == -1:
        return None

    window = html[idx: idx + 4000]
    window = re.sub(r"<[^>]+>", " ", window)
    window = window.replace("&nbsp;", " ")
    window = re.sub(r"\s+", " ", window).strip()

    m_s = re.search(
        r"https?://www\.powerplanetonline\.com/cdnassets/[^\s\"']+_s\.jpg",
        window,
        flags=re.IGNORECASE,
    )
    if not m_s:
        return None

    line = window[: m_s.end()]
    tokens = line.split(" ")
    if len(tokens) < 10:
        return None

    try:
        idx_l = next(i for i, t in enumerate(tokens) if "cdnassets" in t.lower() and t.lower().endswith("_l.jpg"))
    except StopIteration:
        return None

    idx_s = None
    for i in range(len(tokens) - 1, -1, -1):
        if tokens[i].lower().endswith("_s.jpg") and "cdnassets" in tokens[i].lower():
            idx_s = i
            break
    if idx_s is None:
        return None

    try:
        product_id = int(tokens[1])
    except Exception:
        product_id = None

    name = " ".join(tokens[2:idx_l]).strip()
    img_l = tokens[idx_l]

    price = None
    try:
        price = float(tokens[idx_l + 1])
    except Exception:
        pass

    brand = tokens[idx_s - 3] if idx_s >= 4 else None
    category_path = " ".join(tokens[idx_l + 2: idx_s - 3]).strip() if (idx_s - 3 > idx_l + 2) else None

    return {
        "product_id": product_id,
        "name": name or None,
        "image_large": img_l,
        "image_small": tokens[idx_s],
        "price_float": price,
        "category_path": category_path,
        "brand": brand,
    }


def parse_detail_fields(detail_html: str) -> Dict[str, Optional[object]]:
    soup = BeautifulSoup(detail_html, "html.parser")
    text = soup.get_text("\n", strip=True).replace("\xa0", " ")
    out: Dict[str, Optional[object]] = {}

    m_ref = re.search(r"-REF:\s*([A-Z0-9\-\/]+)", text, flags=re.IGNORECASE)
    if m_ref:
        out["ref"] = m_ref.group(1).strip()

    m_cap = re.search(r"Capacidad:\s*([^\n]+)", text, flags=re.IGNORECASE)
    if m_cap:
        cap = m_cap.group(1).strip()
        cap = cap.split("Selecciona:")[0].strip()
        out["capacity"] = cap

    m_col = re.search(r"Selecciona:\s*([^\n]+)", text, flags=re.IGNORECASE)
    if m_col:
        col = m_col.group(1).strip()
        col = col.split("Guardar")[0].strip()
        out["color"] = col

    m_pvr = re.search(r"Precio recomendado:\s*([0-9\.\,]+)\s*€", text, flags=re.IGNORECASE)
    if m_pvr:
        out["pvr_eur"] = parse_eur_amount(m_pvr.group(1) + "€")

    out["price_eur"] = None
    anchor = None
    m_anchor = re.search(r"Precio recomendado:", text, flags=re.IGNORECASE)
    if m_anchor:
        anchor = text[max(0, m_anchor.start() - 250): m_anchor.start() + 250]
    if anchor:
        euros = [parse_eur_amount(x) for x in re.findall(r"\d[\d\.\,]*\s*€", anchor)]
        euros = [e for e in euros if e is not None]
        if euros:
            out["price_eur"] = min(euros)

    out["discount_pct"] = parse_pct(text)
    out["reviews_count"] = parse_int_from(text, r"\((\d+)\s*opiniones\)")
    out["rating"] = parse_float_from(text, r"Valoraci[oó]n global\s*([0-9]+(?:[\.,][0-9]+)?)")

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
    # --- LOGS DETALLADOS SOLICITADOS ---
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
    # -----------------------------------


def scrape_dryrun(
    max_products: int,
    sleep_seconds: float,
    timeout: int,
    include_details: bool,
    write_jsonl_path: Optional[str],
    affiliate_query: str,
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

                tracking = parse_tracking_line(detail_html, offer.url)
                if tracking:
                    offer.product_id = tracking.get("product_id") or offer.product_id
                    offer.category_path = tracking.get("category_path") or offer.category_path
                    offer.image_large = tracking.get("image_large") or offer.image_large
                    offer.image_small = tracking.get("image_small") or offer.image_small
                    offer.brand = tracking.get("brand") or offer.brand
                    if tracking.get("name"):
                        offer.name = str(tracking["name"])
                    if offer.price_eur is None and tracking.get("price_float") is not None:
                        try:
                            offer.price_eur = float(tracking["price_float"])
                        except Exception:
                            pass

                fields = parse_detail_fields(detail_html)
                for k, v in fields.items():
                    if v is None:
                        continue
                    if k == "pvr_eur":
                        if offer.pvr_eur is None:
                            offer.pvr_eur = v  # type: ignore
                    elif k == "price_eur":
                        if offer.price_eur is None:
                            offer.price_eur = v  # type: ignore
                    elif hasattr(offer, k):
                        setattr(offer, k, v)

            # Clasificación
            is_mobile, reason = classify_offer(offer.name, offer.category_path, offer.capacity)
            decision = "IMPORT" if is_mobile else "SKIP"

            # Mapeo a tus variables de log
            nombre = offer.name
            ram, rom = split_ram_rom(offer.capacity)
            ver = (offer.color or guess_color_from_name(offer.name) or "").strip()
            fuente = offer.source

            p_act = format_price(offer.price_eur)
            p_reg = format_price(offer.pvr_eur)

            cup = ""  # PowerPlanet normalmente no trae cupón en el listado/ficha

            img_src = (offer.image_large or offer.image_small or "").strip()

            url_imp = offer.url
            url_exp = offer.url  # aquí no hay acortadores, así que coincide

            url_importada_sin_afiliado = offer.url
            url_sin_acortar_con_mi_afiliado = build_affiliate_url(offer.url, affiliate_query)
            url_oferta = url_sin_acortar_con_mi_afiliado  # sin acortador (por seguridad)

            # Incluye estado/motivo aquí sin romper tu bloque
            enviado_desde = f"powerplanetonline::{decision}::{reason}"

            print_required_logs(
                nombre=nombre,
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
                payload["_decision"] = decision
                payload["_reason"] = reason
                payload["_affiliate_query"] = affiliate_query
                jsonl_file.write(json.dumps(payload, ensure_ascii=False) + "\n")

    finally:
        if jsonl_file:
            jsonl_file.close()


def main() -> None:
    ap = argparse.ArgumentParser(description="PowerPlanetOnline - Móviles más vendidos (DRY-RUN SOLO LOGS + formato requerido)")
    ap.add_argument("--max-products", type=int, default=0, help="0 = sin límite")
    ap.add_argument("--sleep", type=float, default=0.7, help="segundos entre requests")
    ap.add_argument("--timeout", type=int, default=25, help="timeout por request (seg)")
    ap.add_argument("--no-details", action="store_true", help="no entra en fichas (menos datos, peor filtro)")
    ap.add_argument("--jsonl", default="", help="ruta para guardar JSONL (opcional). Ej: logs/powerplanet.jsonl")
    ap.add_argument(
        "--affiliate-query",
        default="",
        help="querystring para afiliado, ej: 'utm_source=ofertasdemoviles&utm_medium=referral'",
    )
    args = ap.parse_args()

    scrape_dryrun(
        max_products=args.max_products,
        sleep_seconds=args.sleep,
        timeout=args.timeout,
        include_details=(not args.no_details),
        write_jsonl_path=(args.jsonl.strip() or None),
        affiliate_query=args.affiliate_query.strip(),
    )


if __name__ == "__main__":
    main()
