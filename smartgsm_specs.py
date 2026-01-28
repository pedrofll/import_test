#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""smartgsm_specs.py

Importa la sección "Ficha técnica" desde Smart-GSM y la guarda en la
DESCRIPCIÓN de las subcategorías de WooCommerce (product_cat).

Base Smart-GSM:
    https://www.smart-gsm.com/moviles

Ficha de un dispositivo:
    https://www.smart-gsm.com/moviles/<slug>

ENV:
    WP_URL                -> https://ofertasdemoviles.com
    WP_KEY                -> Woo consumer key
    WP_SECRET             -> Woo consumer secret
    SMARTGSM_OVERWRITE    -> 1 para sobrescribir descripción existente, 0 para no tocar si ya hay contenido
    SMARTGSM_SLEEP        -> pausa entre requests (segundos), ej 0.8
"""

from __future__ import annotations

import os
import re
import sys
import time
import json
import unicodedata
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Iterable

import requests
from requests.auth import HTTPBasicAuth

from bs4 import BeautifulSoup  # type: ignore


# ---------------------------------------------------------------------
# Config / constantes
# ---------------------------------------------------------------------

SMARTGSM_BASE = "https://www.smart-gsm.com/moviles"

DEFAULT_SLEEP = 0.8

# Nunca importar estos labels (por seguridad, aunque aparezcan)
BANNED_LABEL_PREFIXES = (
    "precio",
    "price",
)

# Regex precompiladas
_slug_re_spaces = re.compile(r"\s+")
_slug_re_non_alnum = re.compile(r"[^a-z0-9]+")
_slug_re_multi_dash = re.compile(r"-{2,}")

# Algunos modelos/brands tienen peculiaridades en Smart-GSM
REALME_PARENT_SLUGS = {"realme"}
REALME_SMARTGSM_PREFIX = "oppo-"  # Smart-GSM los lista como "oppo-realme-..."

# Nubia en Smart-GSM suele ir con prefijo "zte-nubia-..."
NUBIA_PARENT_SLUGS = {"nubia"}
NUBIA_SMARTGSM_PREFIX = "zte-"

# ---------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------


def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    v = v.strip().lower()
    return v in ("1", "true", "yes", "y", "on")


def env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if not v:
        return default
    try:
        return float(v)
    except Exception:
        return default


def strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))


def normalize_slug(s: str) -> str:
    """
    Normaliza para comparar slugs/nombres:
    - minus
    - sin acentos
    - no alfanum -> '-'
    - colapsa guiones
    - trim '-'
    """
    s = (s or "").strip().lower()
    s = strip_accents(s)
    s = _slug_re_spaces.sub("-", s)
    s = _slug_re_non_alnum.sub("-", s)
    s = _slug_re_multi_dash.sub("-", s)
    return s.strip("-")


def slugify_name_for_smartgsm(name: str) -> str:
    """
    Genera un slug aproximado desde el nombre humano.
    - convierte '+' en 'plus'
    - elimina paréntesis y caracteres raros
    """
    s = (name or "").strip().lower()
    s = strip_accents(s)
    s = s.replace("+", " plus ")
    s = re.sub(r"[()]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return normalize_slug(s)


def clean_html_text(s: str) -> str:
    """Limpia texto para evitar que quede pegado o con espacios raros."""
    s = s.replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


# ---------------------------------------------------------------------
# Woo / REST
# ---------------------------------------------------------------------


@dataclass
class WooConfig:
    url: str
    key: str
    secret: str


def get_woo_config_from_env() -> WooConfig:
    wp_url = os.getenv("WP_URL", "").rstrip("/")
    wp_key = os.getenv("WP_KEY", "")
    wp_secret = os.getenv("WP_SECRET", "")
    if not (wp_url and wp_key and wp_secret):
        raise SystemExit("❌ Faltan env vars: WP_URL / WP_KEY / WP_SECRET")
    return WooConfig(url=wp_url, key=wp_key, secret=wp_secret)


def woo_get_all_product_cats(cfg: WooConfig, per_page: int = 100) -> List[dict]:
    """
    Obtiene TODAS las categorías de producto (product_cat) de Woo.
    """
    out: List[dict] = []
    page = 1
    while True:
        r = requests.get(
            f"{cfg.url}/wp-json/wc/v3/products/categories",
            params={"per_page": per_page, "page": page, "hide_empty": "false"},
            auth=HTTPBasicAuth(cfg.key, cfg.secret),
            timeout=60,
        )
        if r.status_code != 200:
            raise RuntimeError(f"Woo GET categories failed: {r.status_code} {r.text[:300]}")
        items = r.json()
        if not items:
            break
        out.extend(items)
        page += 1
    return out


def woo_update_cat_description(cfg: WooConfig, cat_id: int, html_description: str) -> bool:
    """
    Actualiza la descripción HTML de una categoría (product_cat).
    """
    r = requests.put(
        f"{cfg.url}/wp-json/wc/v3/products/categories/{cat_id}",
        json={"description": html_description},
        auth=HTTPBasicAuth(cfg.key, cfg.secret),
        timeout=60,
    )
    return r.status_code in (200, 201)


# ---------------------------------------------------------------------
# Smart-GSM scraping
# ---------------------------------------------------------------------


def fetch_smartgsm_page(url: str, timeout: int = 60) -> Optional[str]:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; smartgsm_specs/1.0; +https://example.com/)",
        "Accept": "text/html,application/xhtml+xml",
    }
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        if r.status_code != 200:
            return None
        return r.text
    except Exception:
        return None


def extract_ficha_tecnica(html_text: str) -> Dict[str, str]:
    """Extrae la sección 'Ficha técnica' y devuelve dict label->value.

    Smart-GSM no siempre usa exactamente el mismo HTML en todas las fichas:
    - A veces el título es 'Ficha tecnica' (sin tilde).
    - A veces la tabla no lleva clase 'table' (o cambia).
    - Algunas filas usan <th> en lugar de <td>.

    Por eso hacemos una búsqueda más tolerante.
    """
    soup = BeautifulSoup(html_text, "html.parser")

    # 1) Intento: encontrar un heading (h2/h3) que contenga "ficha" y "tecnica"/"técnica"
    h2 = None
    for tag in soup.find_all(["h2", "h3"]):
        t = tag.get_text(" ", strip=True).lower()
        if "ficha" in t and ("técnica" in t or "tecnica" in t):
            h2 = tag
            break

    table = None
    if h2 is not None:
        nxt = h2.find_next("table")
        if nxt is not None:
            table = nxt

    # 2) Fallbacks: tabla con clases típicas, luego cualquier tabla
    if table is None:
        table = soup.find("table", class_=re.compile(r"(table|striped|spec|ficha)", re.I))
    if table is None:
        table = soup.find("table")

    if table is None:
        return {}

    specs: Dict[str, str] = {}

    for tr in table.find_all("tr"):
        # Casos habituales: dos <td> (o <th>/<td>)
        cells = tr.find_all(["td", "th"], recursive=True)
        if len(cells) < 2:
            continue

        # Heurística: primera celda = label, segunda celda = value
        c0, c1 = cells[0], cells[1]

        strong = c0.find("strong")
        if strong:
            label = strong.get_text(" ", strip=True)
        else:
            label = c0.get_text(" ", strip=True)

        value = c1.get_text(" ", strip=True)

        if not label or not value:
            continue

        label_clean = re.sub(r"\s+", " ", label).strip()
        value_clean = re.sub(r"\s+", " ", value).strip()

        # No importar precio
        l_low = label_clean.lower()
        if any(l_low.startswith(pfx) for pfx in BANNED_LABEL_PREFIXES):
            continue

        specs[label_clean] = value_clean

    return specs


def specs_to_html_table(specs: Dict[str, str]) -> str:
    """
    Convierte el dict specs en una tabla HTML limpia para meter en la descripción.
    """
    rows = []
    for k, v in specs.items():
        rows.append(
            f"<tr><td><strong>{k}</strong></td><td>{v}</td></tr>"
        )
    body = "\n".join(rows)
    return (
        "<h2>Ficha técnica</h2>"
        "<table class='table table-striped'>"
        "<tbody>"
        f"{body}"
        "</tbody>"
        "</table>"
    )


def fix_special_hyphens(slug: str) -> List[str]:
    """
    Genera variantes de slug para casos especiales:
    - quita sufijos -5g / -4g
    - añade variantes con -5g / -4g
    - Samsung Z Flip/Fold suele ir con guion entre Flip/Fold y número: z-flip-6
    - S25 Ultra sin "galaxy" a veces viene como "samsung-galaxy-s25-ultra"
    - Realme: añade prefijo oppo-realme-...
    - Nubia: añade prefijo zte-
    """
    slug = (slug or "").strip("-")
    if not slug:
        return []

    out = []

    base = slug
    out.append(base)

    # Variantes 5G/4G
    if base.endswith("-5g"):
        out.append(base[:-3])
    if base.endswith("-4g"):
        out.append(base[:-3])

    out.append(base + "-5g")
    out.append(base + "-4g")

    # Samsung Z Flip/Fold: "z-flip6" -> "z-flip-6"
    m = re.search(r"(samsung-galaxy-z-(flip|fold))(\d+)(.*)$", base)
    if m:
        prefix = m.group(1)
        num = m.group(3)
        rest = m.group(4) or ""
        out.append(f"{prefix}-{num}{rest}")
        out.append(f"{prefix}-{num}{rest}-5g")
        out.append(f"{prefix}-{num}{rest}-4g")

    # Samsung S25 Ultra: "samsung-s25-ultra" -> "samsung-galaxy-s25-ultra"
    if base.startswith("samsung-s") and "-ultra" in base and "galaxy" not in base:
        out.append("samsung-galaxy-" + base[len("samsung-"):])

    # Realme: en Smart-GSM suele ser "oppo-realme-..."
    if base.startswith("realme-") or base.startswith("oppo-realme-"):
        if base.startswith("realme-"):
            out.append(REALME_SMARTGSM_PREFIX + base)  # oppo-realme-...
        # además, a veces añaden guion extra en GT8 -> gt-8
        out.extend(split_gt_numbers(base))
        out.extend(split_gt_numbers(REALME_SMARTGSM_PREFIX + base.replace("oppo-realme-", "")))

    # Nubia: prefijo zte-
    if base.startswith("nubia-") or base.startswith("zte-nubia-"):
        if base.startswith("nubia-"):
            out.append(NUBIA_SMARTGSM_PREFIX + base)  # zte-nubia-...
        out.extend(split_redmagic(base))
        if base.startswith("nubia-"):
            out.extend(split_redmagic(NUBIA_SMARTGSM_PREFIX + base))

    # Unicidad preservando orden
    seen = set()
    uniq = []
    for s in out:
        s2 = normalize_slug(s)
        if not s2:
            continue
        if s2 not in seen:
            seen.add(s2)
            uniq.append(s2)
    return uniq


def split_gt_numbers(slug: str) -> List[str]:
    """
    Para Realme GT8/GT8 Pro etc: Smart-GSM suele usar gt-8 / gt-8-pro
    """
    slug = normalize_slug(slug)
    out = []
    # gt8 -> gt-8
    out.append(re.sub(r"\bgt(\d+)\b", r"gt-\1", slug))
    # gt7t -> gt-7t (normalmente ya viene con guion, pero por si acaso)
    out.append(re.sub(r"\bgt(\d+)([a-z])\b", r"gt-\1\2", slug))
    return [normalize_slug(x) for x in out if x]


def split_redmagic(slug: str) -> List[str]:
    """
    Para Nubia Redmagic: a veces lo escriben como "red-magic"
    """
    slug = normalize_slug(slug)
    out = []
    out.append(slug.replace("redmagic", "red-magic"))
    out.append(slug.replace("red-magic", "redmagic"))
    return [normalize_slug(x) for x in out if x]


def candidate_slugs(cat_slug: str, cat_name: str, parent_slug: str) -> List[str]:
    """
    Construye slugs candidatos para buscar la ficha en Smart-GSM.
    """
    base = normalize_slug(cat_slug)
    name_slug = slugify_name_for_smartgsm(cat_name)

    cands = []
    if base:
        cands.append(base)
    if name_slug and name_slug != base:
        cands.append(name_slug)

    expanded: List[str] = []
    for s in cands:
        expanded.extend(fix_special_hyphens(s))

    # POCO en Smart-GSM va como xiaomi-poco-...
    # Si la categoría padre es "poco", probamos también con "xiaomi-" delante
    out: List[str] = []
    for s in expanded:
        out.append(s)
        if parent_slug == "poco" and not s.startswith("xiaomi-"):
            out.append("xiaomi-" + s)

    # Redmi/K en Smart-GSM a veces también con "xiaomi-" delante
    for s in list(out):
        if parent_slug in ("redmi", "xiaomi") and not s.startswith("xiaomi-") and ("redmi" in s or s.startswith("k")):
            out.append("xiaomi-" + s)

    # Unicidad preservando orden
    seen = set()
    uniq = []
    for s in out:
        s2 = normalize_slug(s)
        if s2 and s2 not in seen:
            seen.add(s2)
            uniq.append(s2)
    return uniq


def find_first_existing_smartgsm_url(slugs: List[str], sleep_s: float) -> Tuple[Optional[str], Optional[str]]:
    """
    Devuelve (url, slug) del primer slug que exista (HTTP 200).
    """
    for s in slugs:
        url = f"{SMARTGSM_BASE}/{s}"
        html = fetch_smartgsm_page(url)
        if html:
            return url, html
        time.sleep(sleep_s)
    return None, None


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------


def main() -> int:
    overwrite = env_bool("SMARTGSM_OVERWRITE", False)
    sleep_s = env_float("SMARTGSM_SLEEP", DEFAULT_SLEEP)

    print("=" * 60)
    print(f"📡 SMART-GSM → Woo (Subcategorías) ({time.strftime('%Y-%m-%d %H:%M:%S')})")
    print("=" * 60)
    print(f"Overwrite descripción existente: {overwrite}")
    print(f"Pausa entre requests: {sleep_s}s")
    print(f"Base Smart-GSM: {SMARTGSM_BASE}")
    print("=" * 60)

    cfg = get_woo_config_from_env()
    cats = woo_get_all_product_cats(cfg)
    print(f"📦 Subcategorías detectadas: {len(cats)}")

    # index por id para encontrar parent
    by_id = {c["id"]: c for c in cats}

    updated = []
    not_found = []
    ignored = []
    woo_errors = []

    for c in cats:
        cat_id = int(c["id"])
        name = c.get("name", "") or ""
        slug = c.get("slug", "") or ""
        parent_id = int(c.get("parent") or 0)
        parent = by_id.get(parent_id) if parent_id else None
        parent_slug = (parent.get("slug") if parent else "") or ""
        parent_name = (parent.get("name") if parent else "") or ""

        # ignorar "marca -> marca" (subcat == parent)
        if normalize_slug(name) == normalize_slug(parent_name) or normalize_slug(slug) == normalize_slug(parent_slug):
            ignored.append((name, cat_id, "subcategoría == marca"))
            continue

        # ignorar tablets (según tu lógica previa)
        if "pad" in normalize_slug(name) and "xiaomi" in normalize_slug(name):
            ignored.append((name, cat_id, "tablet"))
            continue

        desc = (c.get("description") or "").strip()
        if desc and not overwrite:
            # ya tiene descripción
            continue

        print("-" * 60)
        print(f"📁 Subcategoría: {name} (ID: {cat_id})")
        print(f"   slug: {slug} | parent_slug: {parent_slug}")

        slugs = candidate_slugs(slug, name, parent_slug)
        url, html = find_first_existing_smartgsm_url(slugs, sleep_s)

        if not url or not html:
            print(f"   ❌ NO ENCONTRADA ficha en Smart-GSM con slugs: {slugs}")
            not_found.append((name, cat_id, slug))
            continue

        print(f"   ✅ Ficha encontrada: {url}")

        specs = extract_ficha_tecnica(html)
        print(f"   🔎 Campos extraídos: {len(specs)}")

        if not specs:
            print("   ⚠️  Sin specs (no se actualiza)")
            continue

        # Mostrar algunas líneas (sin precio)
        for k, v in list(specs.items())[:12]:
            print(f"      - {k}: {v}")

        html_table = specs_to_html_table(specs)

        ok = woo_update_cat_description(cfg, cat_id, html_table)
        if ok:
            print("   💾 DESCRIPCIÓN actualizada en Woo ✅")
            updated.append((name, cat_id, len(specs)))
        else:
            print("   ❌ Error actualizando en Woo")
            woo_errors.append((name, cat_id))

        time.sleep(sleep_s)

    # resumen
    print("=" * 60)
    print(f"📋 RESUMEN DE EJECUCIÓN ({time.strftime('%Y-%m-%d %H:%M:%S')})")
    print("=" * 60)
    print(f"a) SUBCATEGORÍAS ACTUALIZADAS: {len(updated)}")
    for n, cid, k in updated:
        print(f"- {n} (ID: {cid}): {k} campos")

    print(f"b) SUBCATEGORÍAS NO ENCONTRADAS EN SMART-GSM: {len(not_found)}")
    for n, cid, sl in not_found:
        print(f"- {n} (ID: {cid}) slug='{sl}'")

    print(f"c) SUBCATEGORÍAS IGNORADAS: {len(ignored)}")
    for n, cid, why in ignored:
        print(f"- {n} (ID: {cid}): {why}")

    print(f"d) ERRORES ACTUALIZANDO EN WOO: {len(woo_errors)}")
    for n, cid in woo_errors:
        print(f"- {n} (ID: {cid})")

    print("=" * 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
