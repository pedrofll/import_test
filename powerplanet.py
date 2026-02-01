#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import html as ihtml
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


@dataclass
class Offer:
    source: str
    name: str
    url: str
    price_eur: Optional[float] = None
    pvr_eur: Optional[float] = None
    image_large: Optional[str] = None
    scraped_at: Optional[str] = None
    category_path: Optional[str] = None


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def normalize_text(s: str) -> str:
    if s is None:
        return ""
    s = s.replace("\xa0", " ")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = re.sub(r"\s+", " ", s).strip()
    return s


def make_session() -> requests.Session:
    sess = requests.Session()
    sess.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        }
    )
    return sess


def fetch_html(sess: requests.Session, url: str, timeout: int = 25) -> str:
    # IMPORTANTE: en este scraper SOLO se llama a LIST_URL (PowerPlanet)
    r = sess.get(url, timeout=timeout)
    r.raise_for_status()
    return r.text


def parse_eur_amount(s: str) -> Optional[float]:
    if not s:
        return None
    s = s.replace("\xa0", " ").strip()
    m = re.search(r"(\d[\d\.\,]*)\s*€", s)
    if not m:
        return None
    num = m.group(1).replace(".", "").replace(",", ".")
    try:
        return float(num)
    except ValueError:
        return None


def format_price_int(v: Optional[float]) -> str:
    if v is None:
        return "N/A"
    try:
        return f"{int(v)}€"
    except Exception:
        return "N/A"


def int_str(v: Optional[float]) -> str:
    if v is None:
        return ""
    try:
        return str(int(v))
    except Exception:
        return ""


def infer_iphone_ram(name: str) -> str:
    n = normalize_text(name)
    for key, ram in IPHONE_RAM_MAP:
        if key in n:
            return ram
    return ""


def extract_ram_rom_from_name(name: str) -> Tuple[str, str]:
    """
    Detecta '8GB/256GB' (admite separadores comunes).
    """
    if not name:
        return "", ""
    n = name.replace("\xa0", " ")
    n = re.sub(r"\s+", " ", n).strip()
    m = re.search(
        r"\b(\d+)\s*(GB|TB)\s*[/\+\-\|]\s*(\d+)\s*(GB|TB)\b",
        n,
        flags=re.IGNORECASE,
    )
    if not m:
        return "", ""
    ram = f"{m.group(1)}{m.group(2).upper()}"
    rom = f"{m.group(3)}{m.group(4).upper()}"
    return ram, rom


def extract_storage_only(name: str) -> str:
    """
    Extrae una única capacidad (ROM) desde el nombre: '256GB' / '1TB'.
    Ignora GB < 32 (para no confundir RAM).
    """
    if not name:
        return ""
    caps = re.findall(r"\b(\d+)\s*(TB|GB)\b", name, flags=re.IGNORECASE)
    if not caps:
        return ""
    parsed = []
    for v, u in caps:
        try:
            parsed.append((int(v), u.upper()))
        except Exception:
            continue
    if not parsed:
        return ""
    val, unit = max(parsed, key=lambda x: ((x[1] == "TB"), x[0]))
    if unit == "GB" and val < 32:
        return ""
    return f"{val}{unit}"


def strip_variant_from_name(name: str) -> str:
    """
    Limpieza fuerte:
      - Quita RAM/ROM "8GB/256GB"
      - Quita sufijos "- Versión Internacional"
      - Quita color final (incluye Obsidiana)
    """
    if not name:
        return ""
    s = re.sub(r"\s+", " ", name.strip())

    # Quitar sufijos de versión tipo "- Versión Internacional"
    s = re.sub(r"\s*-\s*versi[oó]n\s+internacional\s*$", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s*-\s*version\s+internacional\s*$", "", s, flags=re.IGNORECASE)

    # Quitar RAM/ROM
    s = re.sub(
        r"\s*\b\d+\s*(?:GB|TB)\s*[/\+\-\|]\s*\d+\s*(?:GB|TB)\b\s*",
        " ",
        s,
        flags=re.IGNORECASE,
    )
    s = re.sub(r"\s+", " ", s).strip()

    # Quitar color final (última palabra)
    colors = {
        "negro", "blanco", "azul", "rojo", "verde", "amarillo", "morado", "violeta",
        "gris", "plata", "dorado", "oro", "rosa", "naranja", "cian", "turquesa",
        "beige", "crema", "grafito", "lavanda", "marfil", "champan", "neblina",
        "midnight", "starlight", "titanio", "titanium", "obsidiana", "violet", "purple",
    }

    parts = s.split(" ")
    if parts and normalize_text(parts[-1]) in colors:
        s = " ".join(parts[:-1]).strip()

    return re.sub(r"\s+", " ", s).strip()


def truncate_after_network(name: str) -> str:
    """Trunca el nombre al encontrar '4G' o '5G' y elimina todo lo posterior."""
    if not name:
        return ""
    s = re.sub(r"\s+", " ", name).strip()
    m = re.search(r"\b(?:4G|5G)\b", s, flags=re.IGNORECASE)
    if not m:
        return s
    return s[: m.start()].strip()


def format_product_title(name: str) -> str:
    """
    Primera letra de cada palabra en mayúscula, pero tokens alfanuméricos en mayúsculas (5G, 14T...).
    """
    if not name:
        return ""
    words = re.split(r"\s+", name.strip())
    out = []
    for w in words:
        if not w:
            continue
        # Mantener separadores internos
        if re.search(r"\d", w) and re.search(r"[a-zA-Z]", w):
            out.append(re.sub(r"[a-zA-Z]+", lambda m: m.group(0).upper(), w))
        else:
            out.append(w[:1].upper() + w[1:].lower())
    return " ".join(out).strip()


def compute_version(clean_name: str) -> str:
    n = normalize_text(clean_name)
    if "iphone" in n:
        return "IOS"
    return "Versión Global"


def strip_query(url: str) -> str:
    pu = urlparse(url)
    return urlunparse((pu.scheme, pu.netloc, pu.path, "", "", ""))


def build_affiliate_url(url: str, affiliate_query: str) -> str:
    """
    Añade parámetros de afiliado (utm...) a la URL base.
    Si affiliate_query está vacío => sin cambios.
    """
    if not affiliate_query:
        return url
    pu = urlparse(url)
    base_q = dict(parse_qsl(pu.query, keep_blank_values=True))
    extra = dict(parse_qsl(affiliate_query.lstrip("?"), keep_blank_values=True))
    base_q.update(extra)
    new_q = urlencode(base_q, doseq=True)
    return urlunparse((pu.scheme, pu.netloc, pu.path, pu.params, new_q, pu.fragment))


def shorten_isgd(url: str, timeout: int = 25) -> str:
    try:
        r = requests.get("https://is.gd/create.php", params={"format": "simple", "url": url}, timeout=timeout)
        if r.status_code == 200 and r.text.strip().startswith("http"):
            return r.text.strip()
    except Exception:
        pass
    return url


def is_refurb_or_openbox(name: str) -> bool:
    n = normalize_text(name)
    bad = [
        "desprecintado", "desprecintada", "desprecintados", "desprecintadas",
        "reacondicionado", "reacondicionada", "reacondicionados", "reacondicionadas",
        "refurbished", "open box", "openbox",
    ]
    return any(b in n for b in bad)


def classify_offer(name: str, category_path: Optional[str], capacity: Optional[str]) -> Tuple[bool, str]:
    n = normalize_text(name)
    cat = normalize_text(category_path) if category_path else ""

    if is_refurb_or_openbox(name):
        return False, "EXCLUDE:refurb_or_openbox"

    if " ipad" in f" {n} ":
        return False, "EXCLUDE:name_contains_ipad"
    if " tab" in f" {n} " or "tablet" in n:
        return False, "EXCLUDE:name_contains_tab/tablet"
    if "smartwatch" in n or "smartband" in n or "reloj" in n:
        return False, "EXCLUDE:name_contains_watch/band"
    if cat and any(k in cat for k in ["tablet", "wearable", "smartwatch", "smartband"]):
        return False, "EXCLUDE:category_tablet_or_wearable"

    # iPhone: aceptar aunque no venga RAM si hay ROM
    if "iphone" in n and capacity and any(x in normalize_text(capacity) for x in ["gb", "tb"]):
        return True, "INCLUDE:iphone_with_capacity"

    # resto: exigir capacidad (ROM) o RAM/ROM
    if capacity and any(x in normalize_text(capacity) for x in ["gb", "tb"]):
        return True, "INCLUDE:capacity_has_gb"

    ram, rom = extract_ram_rom_from_name(name)
    if ram and rom:
        return True, "INCLUDE:name_has_ram_rom"

    return False, "EXCLUDE:no_capacity"


def extract_listing_candidates(list_html: str) -> List[Offer]:
    """
    Escaneo robusto del listado:
      - NO depende de 'PVR nodes'
      - Dedup por URL
      - Saca nombre/precios/imagen del mismo card (subárbol)
    """
    soup = BeautifulSoup(list_html, "html.parser")
    offers: Dict[str, Offer] = {}

    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href.startswith("/es/"):
            continue
        if "moviles-mas-vendidos" in href:
            continue

        url = urljoin(BASE_URL, href.split("#")[0])
        # nombre desde texto o alt
        title = a.get_text(" ", strip=True)
        if len(title) < 6:
            img_in_a = a.find("img")
            if img_in_a and img_in_a.get("alt"):
                title = img_in_a.get("alt").strip()
        if len(title) < 6:
            continue

        # buscar el contenedor más cercano que tenga precios
        best_card = None
        best_text = None

        cur = a
        for _ in range(10):
            cur = cur.parent
            if cur is None:
                break
            txt = cur.get_text(" ", strip=True).replace("\xa0", " ")
            if "€" in txt:
                # evitar coger body/html (demasiado grande)
                if len(txt) > 2500:
                    continue
                if best_text is None or len(txt) < len(best_text):
                    best_card = cur
                    best_text = txt

        if not best_card or not best_text:
            continue

        # extraer precios del card
        euro_strs = re.findall(r"\d[\d\.\,]*\s*€", best_text)
        vals = [parse_eur_amount(x) for x in euro_strs]
        vals = [v for v in vals if v is not None]
        if not vals:
            continue

        price = min(vals)
        pvr = max(vals) if len(vals) >= 2 else price

        # imagen
        img = best_card.find("img")
        img_src = ""
        if img:
            img_src = (img.get("data-original") or img.get("data-src") or img.get("src") or "").strip()
        img_src = urljoin(BASE_URL, img_src) if img_src else ""

        if url not in offers:
            offers[url] = Offer(
                source=FUENTE_POWERPLANET,
                name=title,
                url=url,
                price_eur=price,
                pvr_eur=pvr,
                image_large=img_src or None,
                scraped_at=now_iso(),
            )

    return list(offers.values())


def print_required_logs(acf: dict, nombre: str) -> None:
    print(f"Detectado {nombre}")
    print(f"1) Nombre: {nombre}")
    print(f"2) Memoria (memoria): {acf.get('memoria','')}")
    print(f"3) Capacidad (capacidad): {acf.get('capacidad','')}")
    print(f"4) Versión (version): {acf.get('version','')}")
    print(f"5) Fuente (fuente): {acf.get('fuente','')}")
    print(f"6) Precio actual (precio_actual): {acf.get('precio_actual','')}€" if acf.get("precio_actual") else "6) Precio actual (precio_actual): ")
    print(f"7) Precio original (precio_original): {acf.get('precio_original','')}€" if acf.get("precio_original") else "7) Precio original (precio_original): ")
    print(f"8) Código de descuento (codigo_de_descuento): {acf.get('codigo_de_descuento','')}")
    print(f"9) URL Imagen (imagen_producto): {acf.get('imagen_producto','')}")
    print(f"10) Enlace Importado (enlace_de_compra_importado): {acf.get('enlace_de_compra_importado','')}")
    print(f"11) Enlace Expandido (url_oferta_sin_acortar): {acf.get('url_oferta_sin_acortar','')}")
    print(f"12) URL importada sin afiliado (url_importada_sin_afiliado): {acf.get('url_importada_sin_afiliado','')}")
    print(f"13) URL sin acortar con mi afiliado (url_sin_acortar_con_mi_afiliado): {acf.get('url_sin_acortar_con_mi_afiliado','')}")
    print(f"14) URL acortada con mi afiliado (url_oferta): {acf.get('url_oferta','')}")
    print(f"15) Enviado desde (enviado_desde): {acf.get('enviado_desde','')}")
    print(f"16) Encolado para comparar con base de datos...")
    print("-" * 60)


def scrape_dryrun(
    max_products: int,
    sleep_seconds: float,
    timeout: int,
    include_details: bool,  # se ignora a propósito (no se navega fuera del listado)
    write_jsonl_path: Optional[str],
    affiliate_query: str,
    do_isgd: bool,
) -> None:
    sess = make_session()

    # ✅ ÚNICA descarga a PowerPlanet (la página del listado)
    list_html = fetch_html(sess, LIST_URL, timeout=timeout)
    candidates = extract_listing_candidates(list_html)

    if max_products > 0:
        candidates = candidates[:max_products]

    out_f = open(write_jsonl_path, "a", encoding="utf-8") if write_jsonl_path else None

    try:
        for offer in candidates:
            raw_name = ihtml.unescape(offer.name or "").strip()
            if not raw_name:
                continue

            # filtros
            if is_refurb_or_openbox(raw_name):
                continue

            # RAM/ROM
            ram, rom = extract_ram_rom_from_name(raw_name)
            if not rom:
                rom = extract_storage_only(raw_name)
            if (not ram) and ("iphone" in normalize_text(raw_name)):
                ram = infer_iphone_ram(raw_name)

            ok, reason = classify_offer(raw_name, offer.category_path, rom)
            if not ok:
                continue

            clean_name = format_product_title(truncate_after_network(strip_variant_from_name(raw_name)))
            ver = compute_version(clean_name)

            url_oferta_sin_acortar = offer.url
            url_importada_sin_afiliado = strip_query(url_oferta_sin_acortar)
            url_sin_acortar_con_mi_afiliado = build_affiliate_url(url_importada_sin_afiliado, affiliate_query)
            url_oferta = shorten_isgd(url_sin_acortar_con_mi_afiliado, timeout=timeout) if do_isgd else url_sin_acortar_con_mi_afiliado

            acf = {
                "memoria": ram,
                "capacidad": rom,
                "version": ver,
                "fuente": FUENTE_POWERPLANET,
                "precio_actual": int_str(offer.price_eur),
                "precio_original": int_str(offer.pvr_eur if offer.pvr_eur is not None else offer.price_eur),
                "codigo_de_descuento": CUPON_DEFAULT,
                "imagen_producto": offer.image_large or "",
                "enlace_de_compra_importado": offer.url,
                "url_oferta_sin_acortar": url_oferta_sin_acortar,
                "url_importada_sin_afiliado": url_importada_sin_afiliado,
                "url_sin_acortar_con_mi_afiliado": url_sin_acortar_con_mi_afiliado,
                "url_oferta": url_oferta,
                "enviado_desde": ENVIO_POWERPLANET,
            }

            print_required_logs(acf, clean_name)

            if out_f:
                payload = asdict(offer)
                payload["nombre"] = clean_name
                payload["reason"] = reason
                payload.update(acf)          # claves ACF a nivel raíz
                payload["acf"] = acf         # y también anidadas
                out_f.write(json.dumps(payload, ensure_ascii=False) + "\n")
                out_f.flush()

            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

    finally:
        if out_f:
            out_f.close()


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-products", type=int, default=0)
    ap.add_argument("--sleep", type=float, default=0.7)
    ap.add_argument("--timeout", type=int, default=25)
    ap.add_argument("--no-details", action="store_true", help="compatibilidad (ignorado)")
    ap.add_argument("--jsonl", default=None)
    ap.add_argument("--affiliate-query", default="", help="ej: utm_source=x&utm_campaign=y")
    ap.add_argument("--no-isgd", action="store_true")
    ap.add_argument("--status", default="", help="compatibilidad (no se usa)")
    return ap


def main() -> None:
    args = build_parser().parse_args()
    include_details = False  # ✅ FORZADO: no navegar a fichas ni otras rutas PowerPlanet
    do_isgd = not args.no_isgd

    scrape_dryrun(
        max_products=args.max_products,
        sleep_seconds=args.sleep,
        timeout=args.timeout,
        include_details=include_details,
        write_jsonl_path=args.jsonl,
        affiliate_query=args.affiliate_query,
        do_isgd=do_isgd,
    )


if __name__ == "__main__":
    main()
