#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PowerPlanetOnline - Móviles más vendidos (DRY-RUN SOLO LOGS)

- NO navega a fichas: solo descarga https://www.powerplanetonline.com/es/moviles-mas-vendidos y parsea el HTML.
- Genera logs y (opcional) JSONL con los campos ACF que luego consume tu importador.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
import urllib.parse
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

LIST_URL = "https://www.powerplanetonline.com/es/moviles-mas-vendidos"

# --------------------------
# RAM iPhone (fallback)
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

REFURB_KEYWORDS = [
    "reacondicionado",
    "desprecintado",
    "segunda mano",
    "usado",
    "exposicion",
    "renovado",
    "refurb",
    "refurbished",
    "estado excelente",
    "estado muy bueno",
    "estado bueno",
]

# Si aparece un tamaño en pulgadas en el título, lo tratamos como tablet/accesorio (no móvil)
INCHES_RE = re.compile(r"\b\d+(?:[\.,]\d+)?\s*(?:\"|''|pulgadas)\b", flags=re.IGNORECASE)

COLOR_WORDS = {
    "negro", "blanco", "azul", "rojo", "verde", "gris", "plata", "dorado", "oro",
    "marron", "marrón", "rosa", "morado", "violeta", "amarillo", "beige", "cobre",
    "titanio", "grafito",
}

COLOR_TRAIL_WORDS = {"oscuro", "claro", "neblina", "drill"}

FINANCE_HINTS = ("mes", "/mes", "cuota", "financi", "al mes")

# --------------------------
# Helpers
# --------------------------
def normalize_text(s: str) -> str:
    if not s:
        return ""
    s = s.replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip().lower()


def title_case_keep_alnum(s: str) -> str:
    """
    - Primera letra de cada palabra en mayúscula
    - Mantiene tokens alfanuméricos con letras en mayúscula: 16E, 14T, 5G, etc.
    """
    if not s:
        return ""
    s = " ".join(w for w in s.strip().split() if w)
    s = s.title()

    def fix_token(tok: str) -> str:
        if re.search(r"\d+[a-zA-Z]+", tok):
            return re.sub(r"(\d+)([a-zA-Z]+)", lambda m: m.group(1) + m.group(2).upper(), tok)
        return tok

    return " ".join(fix_token(t) for t in s.split())


def is_iphone(name: str) -> bool:
    n = normalize_text(name)
    return "iphone" in n


def infer_iphone_ram(name: str) -> str:
    n = normalize_text(name)
    for key, ram in IPHONE_RAM_MAP:
        if key in n:
            return ram
    return ""


def is_refurb_or_openbox(name: str, url: str = "") -> bool:
    n = normalize_text(name)
    u = normalize_text(url)
    for k in REFURB_KEYWORDS:
        if k in n or k in u:
            return True
    return False


def is_tablet_like(name: str, url: str = "") -> bool:
    n = normalize_text(name)
    u = normalize_text(url)
    # Palabras clave claras
    if " ipad" in f" {n} " or " ipad" in f" {u} ":
        return True
    if " tab" in f" {n} " or " tab" in f" {u} " or "tablet" in n or "tablet" in u:
        return True
    # Tamaño en pulgadas suele aparecer en tablets
    if INCHES_RE.search(name) or INCHES_RE.search(url):
        return True
    # Caso típico PowerPlanet tablets: "Wifi + 4G"
    if "wifi" in n and ("+" in name or " + " in n):
        return True
    return False


def cut_after_4g_5g(name: str) -> str:
    """
    Elimina '4G'/'5G' y todo lo que venga a continuación (incluido el propio token).
    """
    if not name:
        return ""
    m = re.search(r"\b(?:4g|5g)\b", name, flags=re.IGNORECASE)
    if not m:
        return name.strip()
    return name[: m.start()].strip()


def remove_storage_tokens(text: str) -> str:
    """
    Elimina tokens tipo 128GB, 1TB, 8GB/256GB (solo las partes con unidad).
    """
    if not text:
        return ""
    t = text
    t = re.sub(r"\b\d+\s*(?:GB|TB)\b", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def strip_trailing_colors(text: str) -> str:
    """
    Quita color final (1 palabra) y color compuesto (2 palabras): 'Azul Oscuro', 'Verde Drill', etc.
    """
    if not text:
        return ""
    words = text.strip().split()
    if not words:
        return ""

    # 2 palabras: ... <color> <trail>
    if len(words) >= 2:
        w1 = normalize_text(words[-2])
        w2 = normalize_text(words[-1])
        if w1 in COLOR_WORDS and w2 in COLOR_TRAIL_WORDS:
            words = words[:-2]

    # 1 palabra: ... <color>
    if words:
        w = normalize_text(words[-1])
        if w in COLOR_WORDS:
            words = words[:-1]

    return " ".join(words).strip()


def build_clean_name(raw_name: str, url: str) -> str:
    """
    Nombre final para WP:
    - Sin 4G/5G y sin lo que venga después
    - Sin GB/TB (capacidad/memoria)
    - Sin colores al final
    - iPhone: prefijo Apple
    """
    base = raw_name or ""
    base = base.replace("–", "-").replace("—", "-").strip()

    base = cut_after_4g_5g(base)
    base = remove_storage_tokens(base)
    base = strip_trailing_colors(base)
    base = re.sub(r"\s+", " ", base).strip()

    base = re.sub(r"\s*-\s*(version\s+internacional|internacional|global)\s*$", "", base, flags=re.IGNORECASE).strip()

    base = title_case_keep_alnum(base)

    if is_iphone(base) or is_iphone(url):
        if not base.lower().startswith("apple "):
            base = f"Apple {base}"
        base = title_case_keep_alnum(base)

    return base.strip()


def parse_eur_amount(s: str) -> Optional[float]:
    """
    Convierte '1.059,99€' / '1059.99 €' / '1059€' -> float
    """
    if not s:
        return None
    s = s.replace("\xa0", " ").strip()
    m = re.search(r"(\d[\d\.\,]*)\s*€", s)
    if not m:
        return None
    num = m.group(1)
    if "," in num and "." in num:
        num = num.replace(".", "").replace(",", ".")
    else:
        num = num.replace(".", "").replace(",", ".")
    try:
        return float(num)
    except Exception:
        return None


def _amounts_with_context(text: str) -> List[float]:
    """
    Extrae importes € evitando financiación tipo '0€/mes' y valores ridículos.
    """
    if not text:
        return []
    t = text.replace("\xa0", " ")
    out: List[float] = []
    for m in re.finditer(r"\d[\d\.\,]*\s*€", t):
        amt = parse_eur_amount(m.group(0))
        if amt is None:
            continue
        ctx = t[max(0, m.start() - 20): min(len(t), m.end() + 25)].lower()
        if any(h in ctx for h in FINANCE_HINTS):
            continue
        if amt <= 5:
            continue
        out.append(amt)
    return out


def _amounts_from_price_attrs(node) -> List[float]:
    out: List[float] = []
    for el in node.find_all(True):
        for k, v in (el.attrs or {}).items():
            ks = str(k).lower()
            if not any(x in ks for x in ("price", "precio", "pvp", "pvr")):
                continue
            if isinstance(v, (list, tuple)):
                continue
            vs = str(v)
            m = re.search(r"\d[\d\.\,]*", vs)
            if not m:
                continue
            raw = m.group(0)
            raw = raw.replace(".", "").replace(",", ".")
            try:
                val = float(raw)
            except Exception:
                continue
            if val > 5:
                out.append(val)
    return out


def extract_prices_from_node(node) -> Tuple[Optional[int], Optional[int]]:
    """
    Devuelve (precio_actual_int, precio_original_int) o (None,None) si no hay precio fiable.
    """
    if not node:
        return None, None

    old_vals: List[float] = []
    for el in node.find_all(["del", "s", "strike"]):
        old_vals.extend(_amounts_with_context(el.get_text(" ", strip=True)))

    cur_vals: List[float] = []
    for el in node.find_all(True):
        cls = " ".join(el.get("class", []))
        key = f"{cls} {(el.get('id') or '')}".lower()
        if any(k in key for k in ("price", "precio", "pvp", "pvr", "oferta", "special", "sale")):
            cur_vals.extend(_amounts_with_context(el.get_text(" ", strip=True)))

    attr_vals = _amounts_from_price_attrs(node)
    all_text_vals = _amounts_with_context(node.get_text(" ", strip=True))

    candidates_cur = cur_vals or all_text_vals or attr_vals
    candidates_old = old_vals or all_text_vals or attr_vals

    if not candidates_cur:
        return None, None

    price_cur = min(candidates_cur)
    price_old = max(candidates_old) if candidates_old else price_cur
    if price_old < price_cur:
        price_old = price_cur

    return int(price_cur), int(price_old)


def extract_memory_capacity(name: str, url: str = "") -> Tuple[str, str]:
    """
    Devuelve (RAM, ROM) como '8GB','256GB' o ('','') si no detecta ambos.
    """
    raw = (name or "")
    n = normalize_text(raw)
    u = normalize_text(url)

    # 1) RAM/ROM explícito con separador en nombre
    m = re.search(r"(\d+)\s*(gb|tb)\s*[/\+\-\|]\s*(\d+)\s*(gb|tb)", raw, flags=re.IGNORECASE)
    if m:
        ram = f"{m.group(1)}{m.group(2).upper()}"
        rom = f"{m.group(3)}{m.group(4).upper()}"
        return ram, rom

    # 2) Extraer todos los <num><unit> del nombre
    pairs = re.findall(r"(\d+)\s*(GB|TB)\b", raw, flags=re.IGNORECASE)
    ram_c: List[Tuple[int, str]] = []
    rom_c: List[Tuple[int, str]] = []
    for num_s, unit in pairs:
        try:
            num = int(num_s)
        except Exception:
            continue
        unit_u = unit.upper()
        if unit_u == "TB":
            rom_c.append((num, f"{num}{unit_u}"))
        else:
            if num <= 24:
                ram_c.append((num, f"{num}{unit_u}"))
            if num >= 32:
                rom_c.append((num, f"{num}{unit_u}"))

    # 3) URL: -8gb-256gb- y similares
    if not ram_c or not rom_c:
        url_pairs = re.findall(r"(\d+)\s*(gb|tb)", u, flags=re.IGNORECASE)
        for num_s, unit in url_pairs:
            try:
                num = int(num_s)
            except Exception:
                continue
            unit_u = unit.upper()
            if unit_u == "TB":
                rom_c.append((num, f"{num}{unit_u}"))
            else:
                if num <= 24:
                    ram_c.append((num, f"{num}{unit_u}"))
                if num >= 32:
                    rom_c.append((num, f"{num}{unit_u}"))

        # 4) URL: 4b128gb
        m2 = re.search(r"(\d+)b(\d+)(gb|tb)", u, flags=re.IGNORECASE)
        if m2:
            try:
                r = int(m2.group(1))
                c = int(m2.group(2))
                ram_c.append((r, f"{r}GB"))
                rom_c.append((c, f"{c}{m2.group(3).upper()}"))
            except Exception:
                pass

    ram = ""
    rom = ""
    if ram_c:
        ram = sorted(ram_c, key=lambda x: x[0])[0][1]
    if rom_c:
        rom = sorted(rom_c, key=lambda x: x[0], reverse=True)[0][1]

    # iPhone: RAM por mapa si falta (pero ROM debe existir)
    if (not ram) and ("iphone" in n or "iphone" in u):
        ram = infer_iphone_ram(raw) or infer_iphone_ram(u)

    return ram, rom


def classify_offer(raw_name: str, url: str = "", category_path: Optional[str] = None, ram: str = "", rom: str = "") -> Tuple[bool, str]:
    if is_refurb_or_openbox(raw_name, url):
        return False, "EXCLUDE:condition_not_new"
    if is_tablet_like(raw_name, url):
        return False, "EXCLUDE:tablet_like"

    cat = normalize_text(category_path) if category_path else ""
    if cat and any(k in cat for k in ["tablet", "wearable", "smartwatch", "smartband"]):
        return False, "EXCLUDE:category_tablet_or_wearable"

    if ram and rom:
        return True, "INCLUDE:has_ram_and_rom"

    if ("iphone" in normalize_text(raw_name) or "iphone" in normalize_text(url)) and rom and infer_iphone_ram(raw_name):
        return True, "INCLUDE:iphone_ram_inferred"

    return False, "EXCLUDE:missing_ram_or_rom"


def add_affiliate_query(url: str, affiliate_query: str) -> str:
    if not affiliate_query:
        return url
    parsed = urllib.parse.urlsplit(url)
    q = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    add = urllib.parse.parse_qsl(affiliate_query, keep_blank_values=True)
    q_dict = dict(q)
    for k, v in add:
        q_dict[k] = v
    new_query = urllib.parse.urlencode(list(q_dict.items()))
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, new_query, parsed.fragment))


def isgd_shorten(url: str, timeout: int = 20) -> str:
    try:
        r = requests.get("https://is.gd/create.php", params={"format": "simple", "url": url}, timeout=timeout)
        if r.status_code == 200:
            s = r.text.strip()
            if s.startswith("http"):
                return s
    except Exception:
        pass
    return url


@dataclass
class Offer:
    name: str
    url: str
    img_url: str
    category_path: str
    price_eur: Optional[int] = None
    pvr_eur: Optional[int] = None
    version: str = "Versión Global"
    source: str = "powerplanetonline"
    sent_from: str = "España"

    codigo_de_descuento: str = "OFERTA PROMO"
    enlace_de_compra_importado: str = ""
    url_oferta_sin_acortar: str = ""
    url_importada_sin_afiliado: str = ""
    url_sin_acortar_con_mi_afiliado: str = ""
    url_oferta: str = ""


def extract_listing_candidates(html: str) -> List[Offer]:
    soup = BeautifulSoup(html, "html.parser")

    anchors = soup.find_all("a", href=True)
    urls: List[Tuple[str, object]] = []

    for a in anchors:
        href = a.get("href", "").strip()
        if not href:
            continue
        if href.startswith("//"):
            href = "https:" + href
        if href.startswith("/"):
            href = urllib.parse.urljoin(LIST_URL, href)

        p = urllib.parse.urlsplit(href)
        if p.netloc and "powerplanetonline.com" not in p.netloc:
            continue
        if not p.path.startswith("/es/"):
            continue

        if any(x in href.lower() for x in ["#", "/carrito", "/checkout", "/login", "/account", "/mi-cuenta"]):
            continue

        if not re.match(r"^/es/[a-z0-9\-\_]+/?$", p.path, flags=re.IGNORECASE):
            continue
        if not re.search(r"\d", p.path):
            continue

        urls.append((href, a))

    seen: set = set()
    offers: List[Offer] = []

    for href, a in urls:
        if href in seen:
            continue
        seen.add(href)

        best_node = None
        best_len = 10**9
        node = a

        for _ in range(10):
            if not node or not hasattr(node, "get_text"):
                break
            txt = node.get_text(" ", strip=True)
            if not txt:
                node = node.parent
                continue

            vals = _amounts_with_context(txt)
            if vals:
                l = len(txt)
                if 40 <= l <= best_len:
                    best_node = node
                    best_len = l

            node = node.parent

        card = best_node or a

        title = a.get_text(" ", strip=True) or ""
        if not title or len(title) < 6:
            cand = card.get_text(" ", strip=True)
            if cand:
                title = cand.split("€")[0].strip()
        title = re.sub(r"\s+", " ", title).strip()

        img_url = ""
        img = card.find("img")
        if img:
            img_url = (
                img.get("data-original")
                or img.get("data-src")
                or img.get("data-lazy")
                or img.get("src")
                or ""
            ).strip()
            if img_url.startswith("//"):
                img_url = "https:" + img_url
            if img_url.startswith("/"):
                img_url = urllib.parse.urljoin(LIST_URL, img_url)

        price_eur, pvr_eur = extract_prices_from_node(card)
        if price_eur is None:
            continue
        if pvr_eur is None:
            pvr_eur = price_eur

        offers.append(
            Offer(
                name=title,
                url=href,
                img_url=img_url,
                category_path="Móviles > Más vendidos",
                price_eur=price_eur,
                pvr_eur=pvr_eur,
            )
        )

    return offers


def build_acf_payload(offer: Offer, ram: str, rom: str) -> Dict[str, str]:
    return {
        "nombre": offer.name,
        "memoria": ram,
        "capacidad": rom,
        "version": offer.version,
        "fuente": offer.source,
        "precio_actual": f"{offer.price_eur}€" if offer.price_eur is not None else "0€",
        "precio_original": f"{offer.pvr_eur}€" if offer.pvr_eur is not None else "0€",
        "codigo_de_descuento": offer.codigo_de_descuento,
        "imagen_producto": offer.img_url,
        "enlace_de_compra_importado": offer.enlace_de_compra_importado,
        "url_oferta_sin_acortar": offer.url_oferta_sin_acortar,
        "url_importada_sin_afiliado": offer.url_importada_sin_afiliado,
        "url_sin_acortar_con_mi_afiliado": offer.url_sin_acortar_con_mi_afiliado,
        "url_oferta": offer.url_oferta,
        "enviado_desde": offer.sent_from,
    }


def print_required_logs(offer: Offer, ram: str, rom: str) -> None:
    print("------------------------------------------------------------")
    print(f"Detectado {offer.name}")
    print(f"1) Nombre: {offer.name}")
    print(f"2) Memoria (memoria): {ram}")
    print(f"3) Capacidad (capacidad): {rom}")
    print(f"4) Versión (version): {offer.version}")
    print(f"5) Fuente (fuente): {offer.source}")
    print(f"6) Precio actual (precio_actual): {offer.price_eur}€" if offer.price_eur is not None else "6) Precio actual (precio_actual): 0€")
    print(f"7) Precio original (precio_original): {offer.pvr_eur}€" if offer.pvr_eur is not None else "7) Precio original (precio_original): 0€")
    print(f"8) Código de descuento (codigo_de_descuento): {offer.codigo_de_descuento}")
    print(f"9) URL Imagen (imagen_producto): {offer.img_url}")
    print(f"10) Enlace Importado (enlace_de_compra_importado): {offer.enlace_de_compra_importado}")
    print(f"11) Enlace Expandido (url_oferta_sin_acortar): {offer.url_oferta_sin_acortar}")
    print(f"12) URL importada sin afiliado (url_importada_sin_afiliado): {offer.url_importada_sin_afiliado}")
    print(f"13) URL sin acortar con mi afiliado (url_sin_acortar_con_mi_afiliado): {offer.url_sin_acortar_con_mi_afiliado}")
    print(f"14) URL acortada con mi afiliado (url_oferta): {offer.url_oferta}")
    print(f"15) Enviado desde (enviado_desde): {offer.sent_from}")
    print("16) Encolado para comparar con base de datos...")


def scrape_dryrun(
    max_products: int = 0,
    sleep_seconds: float = 0.7,
    timeout: int = 25,
    include_details: bool = False,
    write_jsonl_path: Optional[str] = None,
    affiliate_query: str = "",
    do_isgd: bool = True,
) -> None:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; ofertasdemoviles-bot/1.0; +https://ofertasdemoviles.com/)",
        "Accept-Language": "es-ES,es;q=0.9",
    }

    jsonl_file = None
    if write_jsonl_path:
        jsonl_file = open(write_jsonl_path, "w", encoding="utf-8")

    try:
        r = requests.get(LIST_URL, headers=headers, timeout=timeout)
        r.raise_for_status()
        html = r.text

        offers = extract_listing_candidates(html)

        count = 0
        for offer in offers:
            if sleep_seconds:
                time.sleep(sleep_seconds)

            if is_refurb_or_openbox(offer.name, offer.url):
                continue
            if is_tablet_like(offer.name, offer.url):
                continue

            ram, rom = extract_memory_capacity(offer.name, offer.url)
            ok, _reason = classify_offer(offer.name, offer.url, offer.category_path, ram, rom)
            if not ok:
                continue

            offer.name = build_clean_name(offer.name, offer.url)
            if not offer.name:
                continue

            if is_iphone(offer.name) or is_iphone(offer.url):
                offer.version = "IOS"

            offer.enlace_de_compra_importado = offer.url
            offer.url_oferta_sin_acortar = offer.url
            offer.url_importada_sin_afiliado = offer.url

            offer.url_sin_acortar_con_mi_afiliado = add_affiliate_query(offer.url, affiliate_query)
            offer.url_oferta = offer.url_sin_acortar_con_mi_afiliado
            if do_isgd:
                offer.url_oferta = isgd_shorten(offer.url_sin_acortar_con_mi_afiliado, timeout=timeout)

            print_required_logs(offer, ram, rom)

            if jsonl_file:
                jsonl_file.write(json.dumps(build_acf_payload(offer, ram, rom), ensure_ascii=False) + "\n")

            count += 1
            if max_products and count >= max_products:
                break

    except KeyboardInterrupt:
        print("\n[ABORT] Cancelado por el usuario.", file=sys.stderr)
    finally:
        if jsonl_file:
            jsonl_file.close()


def main() -> None:
    ap = argparse.ArgumentParser(description="PowerPlanetOnline - Móviles más vendidos (DRY-RUN SOLO LOGS)")
    ap.add_argument("--max-products", type=int, default=0, help="0 = sin límite")
    ap.add_argument("--sleep", type=float, default=0.7, help="segundos entre requests")
    ap.add_argument("--timeout", type=int, default=25, help="timeout por request (seg)")
    ap.add_argument("--status", default="publish", help="compat: ignored (wp/cli)")
    ap.add_argument("--no-details", action="store_true", help="compat: ignored (no entra en fichas)")
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
