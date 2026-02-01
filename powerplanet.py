#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PowerPlanet scraper (LISTING-ONLY, NO-IMPORT) – pensado para depurar extracción y ACF.

✅ Solo lee 1 página: https://www.powerplanetonline.com/es/moviles-mas-vendidos
✅ NO crea / actualiza productos en WordPress (solo logs + JSONL opcional)
✅ Loggea TODAS las variables ACF que se guardarían por producto
✅ Precios sin decimales (€/int)

Uso típico (GitHub Actions / local):
  python powerplanet.py --max-products 0 --sleep 0.7 --timeout 25 --status publish --jsonl powerplanet.jsonl

Nota: --status / credenciales WP se aceptan por compatibilidad, pero se ignoran (no hay import).
"""
from __future__ import annotations

import argparse
import json
import random
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


# ------------------------- CONFIG -------------------------
BASE_URL = "https://www.powerplanetonline.com"
LIST_URL = "https://www.powerplanetonline.com/es/moviles-mas-vendidos"
SOURCE_NAME = "powerplanetonline"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
}

# ACF keys que SIEMPRE se logean (aunque estén vacíos)
ACF_KEYS_ORDER = [
    "fuente",
    "version",
    "memoria",
    "capacidad",
    "precio_actual",
    "precio_original",
    "codigo_de_descuento",
    "enlace_de_compra_importado",
    "pagina_de_compra",
    "url_imagen",
    "importado_de",
    "url_oferta",
    "url_oferta_sin_acortar",
    "url_importada_sin_afiliado",
    "url_sin_acortar_con_mi_afiliado",
    "enviado_desde",
    "enviado_desde_tg",
]

# Heurística simple (PowerPlanet es tienda española)
DEFAULT_SHIP_FROM = "España"

# iPhone RAM mapping (si no aparece RAM explícita en el título)
IPHONE_RAM_MAP = {
    "iphone 17 pro max": "12GB",
    "iphone 17 pro": "12GB",
    "iphone 17": "8GB",
    "iphone 16 pro max": "8GB",
    "iphone 16 pro": "8GB",
    "iphone 16 plus": "8GB",
    "iphone 16": "8GB",
    "iphone 16e": "8GB",
    "iphone 15 pro max": "8GB",
    "iphone 15 pro": "8GB",
    "iphone 15 plus": "6GB",
    "iphone 15": "6GB",
    "iphone 14 pro max": "6GB",
    "iphone 14 pro": "6GB",
    "iphone 14 plus": "6GB",
    "iphone 14": "6GB",
    "iphone 13 pro max": "6GB",
    "iphone 13 pro": "6GB",
    "iphone 13": "4GB",
    "iphone 13 mini": "4GB",
    "iphone 12 pro max": "6GB",
    "iphone 12 pro": "6GB",
    "iphone 12": "4GB",
    "iphone 12 mini": "4GB",
    "iphone 11 pro max": "4GB",
    "iphone 11 pro": "4GB",
    "iphone 11": "4GB",
}


# ------------------------- DATA -------------------------
@dataclass
class Offer:
    url: str
    name_raw: str
    image_url: Optional[str] = None
    price_eur: Optional[int] = None
    pvr_eur: Optional[int] = None  # precio recomendado (original)
    ship_from: str = DEFAULT_SHIP_FROM
    version: str = "Global"
    ram: Optional[str] = None
    rom: Optional[str] = None


# ------------------------- HELPERS -------------------------
def safe_get(d: Dict[str, Any], key: str, default=None):
    return d.get(key, default) if isinstance(d, dict) else default


def normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def to_float_money(s: str) -> Optional[float]:
    if s is None:
        return None
    s = str(s)
    s = s.replace("\xa0", " ")
    s = s.replace("€", "").strip()
    # "1.234,56" -> "1234.56"
    s = s.replace(".", "").replace(",", ".")
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    if not m:
        return None
    try:
        return float(m.group(1))
    except Exception:
        return None


def to_int_eur(s: str) -> Optional[int]:
    f = to_float_money(s)
    if f is None:
        return None
    # sin decimales: truncado
    if f < 0:
        return None
    return int(f)


def format_price_int_eur(p: Optional[int]) -> str:
    return "" if p is None else f"{int(p)}€"


def strip_variant_and_trailing_slashes(name: str) -> str:
    s = normalize_ws(name)
    # Quitar sufijos tipo " /", " / Color", etc.
    s = re.sub(r"\s*/\s*$", "", s)
    s = re.sub(r"\s*/\s+.*$", "", s)
    # Quitar sufijos " - ..." (variante)
    s = re.sub(r"\s+-\s+.*$", "", s)
    return normalize_ws(s)


def extract_ram_rom_from_title(title: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Devuelve (ram, rom, base_title_sin_specs)
    Soporta: "8GB/128GB", "8GB 128GB", "8GB+128GB", "4B128GB", etc.
    """
    t = normalize_ws(title)

    # Caso típico: 8GB/128GB
    m = re.search(r"\b(?P<ram>\d{1,2})\s*GB\s*[/+xX]\s*(?P<rom>\d{2,4})\s*GB\b", t, re.I)
    if m:
        base = normalize_ws(t[: m.start()].strip(" -/|,"))
        return f"{int(m.group('ram'))}GB", f"{int(m.group('rom'))}GB", base

    # Caso: 8GB 128GB (sin separador)
    m = re.search(r"\b(?P<ram>\d{1,2})\s*GB\b.*?\b(?P<rom>\d{2,4})\s*GB\b", t, re.I)
    if m:
        base = normalize_ws(t[: m.start()].strip(" -/|,"))
        return f"{int(m.group('ram'))}GB", f"{int(m.group('rom'))}GB", base

    # Caso: 4B128GB (muy común en slugs)
    m = re.search(r"\b(?P<ram>\d{1,2})\s*B\s*(?P<rom>\d{2,4})\s*GB\b", t, re.I)
    if m:
        base = normalize_ws(t[: m.start()].strip(" -/|,"))
        return f"{int(m.group('ram'))}GB", f"{int(m.group('rom'))}GB", base

    return None, None, strip_variant_and_trailing_slashes(t)


def detect_iphone_ram(title: str) -> Optional[str]:
    low = normalize_ws(title).lower()
    for k, v in IPHONE_RAM_MAP.items():
        if k in low:
            return v
    return None


def extract_iphone_rom(title: str) -> Optional[str]:
    # iPhone suele venir como "256GB"
    m = re.search(r"\b(?P<rom>\d{2,4})\s*GB\b", title, re.I)
    if not m:
        return None
    return f"{int(m.group('rom'))}GB"


def is_phone_like(title: str) -> bool:
    # En esta página debería ser solo móviles, pero filtramos por seguridad
    t = title.lower()
    return ("gb" in t) or ("iphone" in t) or ("galaxy" in t) or ("pixel" in t) or ("xiaomi" in t)


def build_acf_payload(offer: Offer) -> Dict[str, str]:
    """
    Devuelve TODAS las claves ACF que queremos guardar (string values para WordPress/ACF).
    Aquí solo debug (no import).
    """
    precio_actual = "" if offer.price_eur is None else str(int(offer.price_eur))
    precio_original = "" if offer.pvr_eur is None else str(int(offer.pvr_eur))

    # En PowerPlanet no estamos aplicando afiliado (no hay params en el txt); lo dejamos igual.
    url_importada_sin_afiliado = offer.url
    url_sin_acortar_con_mi_afiliado = offer.url
    url_oferta_sin_acortar = url_sin_acortar_con_mi_afiliado

    # url_oferta: si tu pipeline genera un shortlink, aquí puedes inyectarlo; por ahora vacío.
    url_oferta = ""

    # Enlace de compra importado: usamos la URL del producto
    enlace_de_compra_importado = offer.url

    # Enviado desde tg (si tienes lógica en otro scraper): aquí, derivado simple
    enviado_desde_tg = "Desde España" if (offer.ship_from or "").lower() == "españa" else ""

    # Código de descuento: se desconoce desde listado -> vacío
    codigo_de_descuento = ""

    acf: Dict[str, str] = {
        "fuente": SOURCE_NAME,
        "version": offer.version or "",
        "memoria": offer.ram or "",
        "capacidad": offer.rom or "",
        "precio_actual": precio_actual,
        "precio_original": precio_original,
        "codigo_de_descuento": codigo_de_descuento,
        "enlace_de_compra_importado": enlace_de_compra_importado,
        "pagina_de_compra": enlace_de_compra_importado,
        "url_imagen": offer.image_url or "",
        "importado_de": "https://www.powerplanetonline.com/",
        "url_oferta": url_oferta,
        "url_oferta_sin_acortar": url_oferta_sin_acortar,
        "url_importada_sin_afiliado": url_importada_sin_afiliado,
        "url_sin_acortar_con_mi_afiliado": url_sin_acortar_con_mi_afiliado,
        "enviado_desde": offer.ship_from or "",
        "enviado_desde_tg": enviado_desde_tg,
    }

    # Garantiza TODAS las claves, aunque vacías
    for k in ACF_KEYS_ORDER:
        acf.setdefault(k, "")

    return acf


# ------------------------- PARSING LISTING -------------------------
def http_get(url: str, timeout: int) -> str:
    r = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.text


def best_image_from_container(container) -> Optional[str]:
    if not container:
        return None
    img = container.find("img")
    if not img:
        return None
    for attr in ("data-src", "data-original", "src", "data-lazy", "data-srcset", "srcset"):
        v = img.get(attr)
        if not v:
            continue
        # srcset: coger el primer src
        v = str(v).strip()
        if " " in v and ("srcset" in attr):
            v = v.split(",")[0].split()[0]
        # normaliza relativa
        if v.startswith("//"):
            v = "https:" + v
        if v.startswith("/"):
            v = urljoin(BASE_URL, v)
        return v
    return None


def parse_prices_from_container_text(txt: str) -> Tuple[Optional[int], Optional[int]]:
    """
    Devuelve (price_actual, pvr_original) como int euros.
    En listing suele venir "PVR 579,00€ 398,00€" (pvr, actual).
    Si no hay PVR, intenta extraer 1 o 2 precios.
    """
    t = normalize_ws(txt)

    m = re.search(r"\bPVR\b\s*([0-9\.,]+)\s*€\s*([0-9\.,]+)\s*€", t, re.I)
    if m:
        pvr = to_int_eur(m.group(1))
        cur = to_int_eur(m.group(2))
        return cur, pvr

    # fallback: 2 precios cualquiera (evita coger precios de productos relacionados si el container es grande)
    prices = re.findall(r"([0-9]{1,5}(?:[.,][0-9]{2})?)\s*€", t)
    if not prices:
        return None, None
    if len(prices) == 1:
        cur = to_int_eur(prices[0])
        return cur, None
    # Heurística: si hay dos, asumimos [pvr, cur] o [cur, pvr] según texto "recomendado"
    cur = to_int_eur(prices[-1])
    pvr = to_int_eur(prices[-2])
    return cur, pvr


def find_product_container(a_tag) -> Optional[Any]:
    """
    Sube por padres hasta encontrar un contenedor que tenga pinta de tarjeta de producto.
    Criterio: que contenga "€" y/o "PVR".
    """
    node = a_tag
    for _ in range(10):
        if not node or not getattr(node, "parent", None):
            break
        node = node.parent
        try:
            txt = node.get_text(" ", strip=True)
        except Exception:
            continue
        if "€" in txt or "PVR" in txt:
            return node
    return None


def extract_offers_from_listing(html: str) -> List[Offer]:
    soup = BeautifulSoup(html, "html.parser")

    # Los nombres de producto suelen estar en h2/h3 con enlace.
    anchors = soup.select("h2 a[href], h3 a[href], a[href]")
    offers: List[Offer] = []
    seen_urls: set[str] = set()

    for a in anchors:
        href = a.get("href") or ""
        if not href.startswith("/es/"):
            continue
        if href == "/es/moviles-mas-vendidos" or "moviles-mas-vendidos" in href:
            continue

        title = normalize_ws(a.get_text(" ", strip=True))
        if not title:
            continue
        if not is_phone_like(title):
            continue

        full_url = urljoin(BASE_URL, href)
        if full_url in seen_urls:
            continue

        container = find_product_container(a)
        if not container:
            continue

        container_txt = container.get_text(" ", strip=True)
        if "€" not in container_txt and "PVR" not in container_txt:
            continue

        price, pvr = parse_prices_from_container_text(container_txt)
        # Si no hay precio, probablemente es un link de menú / ruido
        if price is None and pvr is None:
            continue

        img_url = best_image_from_container(container)

        offers.append(
            Offer(
                url=full_url,
                name_raw=title,
                image_url=img_url,
                price_eur=price,
                pvr_eur=pvr,
                ship_from=DEFAULT_SHIP_FROM,
                version="Global",
            )
        )
        seen_urls.add(full_url)

    return offers


# ------------------------- MAIN FLOW -------------------------
def enrich_offer_specs(offer: Offer) -> None:
    raw = strip_variant_and_trailing_slashes(offer.name_raw)

    # iPhone: RAM por mapping si no viene en título
    ram, rom, base = extract_ram_rom_from_title(raw)
    if ram is None and "iphone" in raw.lower():
        ram = detect_iphone_ram(raw)
    if rom is None and "iphone" in raw.lower():
        rom = extract_iphone_rom(raw)

    offer.ram = ram
    offer.rom = rom

    # Base name para mostrar (sin RAM/ROM ni variante)
    base_name = base
    if "iphone" in raw.lower() and offer.rom:
        # Si el título trae "... 256GB ..." quitamos desde la ROM
        mrom = re.search(r"\b\d{2,4}\s*GB\b", raw, re.I)
        if mrom:
            base_name = normalize_ws(raw[: mrom.start()].strip(" -/|,"))

    # Limpieza adicional: quita doble espacio y caracteres residuales
    base_name = normalize_ws(base_name).strip(" -/|,")

    offer.name_raw = base_name  # sustituimos por nombre limpio para logs


def write_jsonl(path: str, obj: Dict[str, Any]) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def run(args: argparse.Namespace) -> int:
    print(f"📌 PowerPlanet: Escaneando SOLO: {LIST_URL}")

    html = http_get(LIST_URL, timeout=args.timeout)
    offers = extract_offers_from_listing(html)

    # Si el parsing ha sido demasiado laxo/estricto, evita confusiones
    print(f"📌 PowerPlanet: Productos detectados = {len(offers)}")

    if not offers:
        print("❌ No se han detectado productos. Revisa selectores/estructura HTML.")
        # no fallamos duro: devolvemos 0 para que puedas ver logs en CI
        return 0

    # Limitar productos si aplica
    if args.max_products and args.max_products > 0:
        offers = offers[: args.max_products]

    for idx, offer in enumerate(offers, start=1):
        enrich_offer_specs(offer)

        # Log principal (similar a tu formato)
        print(f"Detectado {offer.name_raw}")
        print(f"1) Nombre: {offer.name_raw}")
        print(f"2) Memoria: {offer.ram or ''}")
        print(f"3) Capacidad: {offer.rom or ''}")
        print(f"4) Versión: {offer.version}")
        print(f"5) Fuente: {SOURCE_NAME}")
        print(f"6) Precio actual: {format_price_int_eur(offer.price_eur)}")
        print(f"7) Precio original: {format_price_int_eur(offer.pvr_eur)}")
        print(f"8) Código de descuento: ")
        print(f"9) Version: {offer.version}")
        print(f"10) URL Imagen: {offer.image_url or ''}")
        print(f"11) Enlace Importado: {offer.url}")
        print(f"12) Enlace Expandido: {offer.url}")
        print(f"13) URL importada sin afiliado: {offer.url}")
        print(f"14) URL sin acortar con mi afiliado: {offer.url}")
        print(f"15) URL acortada con mi afiliado: ")
        print(f"16) Enviado desde: {offer.ship_from}")
        print(f"17) URL post acortada: ")
        print(f"18) Encolado para comparar con base de datos...")
        print("-" * 60)

        acf = build_acf_payload(offer)

        # Log ACF (todas las claves)
        print("ACF (valores que se guardarían):")
        for k in ACF_KEYS_ORDER:
            print(f" - {k}: {acf.get(k, '')}")
        print("-" * 60)

        if args.jsonl:
            write_jsonl(
                args.jsonl,
                {
                    "source": SOURCE_NAME,
                    "url": offer.url,
                    "name": offer.name_raw,
                    "image_url": offer.image_url,
                    "price_eur": offer.price_eur,
                    "pvr_eur": offer.pvr_eur,
                    "ram": offer.ram,
                    "rom": offer.rom,
                    "version": offer.version,
                    "ship_from": offer.ship_from,
                    "acf": acf,
                },
            )

        if args.sleep:
            time.sleep(args.sleep + random.random() * 0.2)

    print("✅ Dry-run terminado (NO se ha importado nada).")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="PowerPlanet (listing-only) scraper – NO IMPORT")
    # Compat: credenciales/flags de otros scrapers (se aceptan, se ignoran)
    p.add_argument("--wp-url", default=None)
    p.add_argument("--wc-key", default=None)
    p.add_argument("--wc-secret", default=None)
    p.add_argument("--wp-user", default=None)
    p.add_argument("--wp-app-pass", default=None)

    p.add_argument("--max-products", type=int, default=0, help="0 = sin límite")
    p.add_argument("--sleep", type=float, default=0.0)
    p.add_argument("--timeout", type=int, default=25)

    # Compat con tu pipeline
    p.add_argument("--dry-run", action="store_true", help="Compat (siempre dry-run)")
    p.add_argument(
        "--status",
        choices=["publish", "draft", "pending", "private"],
        default="publish",
        help="Compat (no se usa en dry-run)",
    )
    p.add_argument("--force-delete-obsoletes", action="store_true", help="Compat (no se usa)")
    p.add_argument("--affiliate-query", default="", help="Compat (no se usa)")

    # Salida
    p.add_argument("--jsonl", default="", help="Ruta JSONL de salida (opcional)")
    # Flag explícito por si tu workflow lo añade
    p.add_argument("--no-import", action="store_true", help="No importa (siempre activo)")
    return p


def main() -> None:
    args = build_parser().parse_args()
    # Normaliza jsonl: si viene vacío, None
    args.jsonl = (args.jsonl or "").strip() or ""
    raise SystemExit(run(args))


if __name__ == "__main__":
    main()
