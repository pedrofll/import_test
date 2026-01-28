#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SMART-GSM → Woo (Subcategorías)
- Lee subcategorías de WooCommerce (product_cat) y para cada una busca su ficha en Smart-GSM.
- Extrae "Ficha técnica" y actualiza la DESCRIPCIÓN de la categoría (term description).
- Soporta overwrite (SMARTGSM_OVERWRITE=1) y sleep entre requests (SMARTGSM_SLEEP=0.8).

Cambios clave implementados en esta versión:
- POCO: soporta prefijo "xiaomi-" cuando aplica (ya estaba) y fuerza 5G/4G sin crear "-4g-5g".
- Realme: los modelos están bajo "oppo-" en Smart-GSM (ya estaba) + corrige GT8 -> GT-8.
- OPPO Reno: corrige patrón reno12 -> reno-12 (ya estaba).
- Samsung:
  - Corrige Z Flip/Fold: flip6 -> flip-6, fold7 -> fold-7.
  - Corrige slugs tipo samsung-s25-ultra -> samsung-galaxy-s25-ultra.
- HTML de ficha: usa <ul><li> en vez de tabla para evitar que algunos themes “aplanan” <tr>/<td>.
- No importa "Precio" (precio/price) desde la ficha técnica.
"""

from __future__ import annotations

import html
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup


# ------------------------------ Config --------------------------------------

SMARTGSM_BASE = "https://www.smart-gsm.com/moviles"

WP_URL = os.getenv("WP_URL", "").rstrip("/")
WP_KEY = os.getenv("WP_KEY", "")
WP_SECRET = os.getenv("WP_SECRET", "")

SMARTGSM_OVERWRITE = os.getenv("SMARTGSM_OVERWRITE", "0").strip() == "1"
SMARTGSM_SLEEP = float(os.getenv("SMARTGSM_SLEEP", "0.8"))

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

TIMEOUT = 20

# “Ficha técnica” labels que NO queremos importar
BANNED_LABEL_PREFIXES = (
    "precio",
    "price",
)

# Algunos términos que indican que NO queremos procesar (ej: tablets)
IGNORE_IF_NAME_CONTAINS = (
    "tablet",
)

# Cuando una subcategoría es igual a su marca, suele ser una categoría "marca"
IGNORE_IF_SUBCATEGORY_EQUALS_PARENT = True


# ------------------------------ HTTP utils ----------------------------------

def wp_session() -> requests.Session:
    s = requests.Session()
    s.auth = (WP_KEY, WP_SECRET)
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def smartgsm_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def safe_get(sess: requests.Session, url: str, **kwargs) -> requests.Response:
    kwargs.setdefault("timeout", TIMEOUT)
    r = sess.get(url, **kwargs)
    r.raise_for_status()
    return r


def safe_post(sess: requests.Session, url: str, **kwargs) -> requests.Response:
    kwargs.setdefault("timeout", TIMEOUT)
    r = sess.post(url, **kwargs)
    r.raise_for_status()
    return r


# ------------------------------ WP (Woo) API --------------------------------

@dataclass
class CategoryTerm:
    id: int
    name: str
    slug: str
    parent: int
    description: str


def wp_get_all_categories(sess: requests.Session) -> List[CategoryTerm]:
    """Lee todas las categorías product_cat (incluye subcategorías)."""
    terms: List[CategoryTerm] = []
    page = 1
    per_page = 100

    while True:
        url = f"{WP_URL}/wp-json/wc/v3/products/categories"
        params = {"per_page": per_page, "page": page, "hide_empty": False}
        r = safe_get(sess, url, params=params)
        data = r.json()
        if not data:
            break
        for t in data:
            terms.append(
                CategoryTerm(
                    id=int(t["id"]),
                    name=t.get("name", "") or "",
                    slug=t.get("slug", "") or "",
                    parent=int(t.get("parent", 0) or 0),
                    description=t.get("description", "") or "",
                )
            )
        page += 1

    return terms


def wp_get_category(sess: requests.Session, term_id: int) -> CategoryTerm:
    url = f"{WP_URL}/wp-json/wc/v3/products/categories/{term_id}"
    r = safe_get(sess, url)
    t = r.json()
    return CategoryTerm(
        id=int(t["id"]),
        name=t.get("name", "") or "",
        slug=t.get("slug", "") or "",
        parent=int(t.get("parent", 0) or 0),
        description=t.get("description", "") or "",
    )


def wp_update_category_description(sess: requests.Session, term_id: int, description: str) -> None:
    url = f"{WP_URL}/wp-json/wc/v3/products/categories/{term_id}"
    payload = {"description": description}
    safe_post(sess, url, json=payload)


# ------------------------------ HTML / parsing --------------------------------

def normalize_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def extract_ficha_tecnica(html_content: str) -> Dict[str, str]:
    """Extrae pares clave/valor desde el bloque de 'Ficha técnica'."""
    soup = BeautifulSoup(html_content, "html.parser")

    # Localiza el H2 "Ficha técnica"
    h2 = None
    for tag in soup.find_all(["h1", "h2", "h3"]):
        if normalize_text(tag.get_text(" ", strip=True)).lower() == "ficha técnica":
            h2 = tag
            break
    if not h2:
        return {}

    # Busca la tabla siguiente
    table = h2.find_next("table")
    if not table:
        return {}

    specs: Dict[str, str] = {}

    for tr in table.find_all("tr"):
        tds = tr.find_all(["td", "th"])
        if len(tds) < 2:
            continue

        label = normalize_text(tds[0].get_text(" ", strip=True))
        value = normalize_text(tds[1].get_text(" ", strip=True))

        if not label or not value:
            continue

        l_low = label.lower()
        if any(l_low.startswith(b) for b in BANNED_LABEL_PREFIXES):
            continue

        specs[label] = value

    return specs


def build_specs_html_table(specs: Dict[str, str]) -> str:
    """Genera HTML estable y legible en Woo.

    Nota: algunos themes/sanitizers de WP terminan "aplanando" tablas (tr/td)
    en descripciones de taxonomías. Para evitar que se vea todo concatenado,
    renderizamos la ficha como lista <ul><li>, que suele sobrevivir mejor.
    """
    if not specs:
        return ""

    items = []
    for k, v in specs.items():
        k_esc = html.escape(k)
        v_esc = html.escape(v)
        items.append(f"<li><strong>{k_esc}</strong>: {v_esc}</li>")

    items_html = "\n".join(items)

    return (
        "<div class=\"smartgsm-specs\">\n"
        "<h2>Ficha técnica</h2>\n"
        "<ul>\n"
        f"{items_html}\n"
        "</ul>\n"
        "</div>\n"
    )

# ------------------------------ Slug utils ---------------------------------

def normalize_slug(slug: str) -> str:
    s = (slug or "").strip().lower()
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"[^a-z0-9\-]+", "", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s


def strip_network_suffix(slug: str) -> List[str]:
    """Devuelve [slug, slug_sin_-5g/-4g] si aplica."""
    s = normalize_slug(slug)
    if s.endswith("-5g"):
        return [s, s[:-3]]
    if s.endswith("-4g"):
        return [s, s[:-3]]
    return [s]


def add_network_suffixes(slug: str) -> List[str]:
    """Añade variantes -5g/-4g si no tiene ya sufijo."""
    s = normalize_slug(slug)
    if s.endswith("-5g") or s.endswith("-4g"):
        return [s]
    return [s, f"{s}-5g", f"{s}-4g"]


def fix_oppo_reno_hyphen(slug: str) -> List[str]:
    """Corrige patrón de Smart-GSM para OPPO Reno (reno12 -> reno-12)."""
    s = normalize_slug(slug)
    out = [s]

    # oppo-reno12-fs -> oppo-reno-12-fs
    out.append(re.sub(r"(oppo-reno)(\d+)(?=-|$)", r"\1-\2", s))

    # dedupe
    seen = set()
    res: List[str] = []
    for x in out:
        x = normalize_slug(x)
        if x and x not in seen:
            seen.add(x)
            res.append(x)
    return res


def base_without_network(slug: str) -> str:
    """Devuelve el slug sin sufijo final -5g / -4g (si lo tuviera)."""
    return re.sub(r"-(5g|4g)$", "", slug.strip().lower())


def fix_special_hyphens(slug: str) -> List[str]:
    """Genera variantes de slug corrigiendo patrones habituales de Smart-GSM.

    Casos cubiertos (ejemplos):
    - samsung-galaxy-z-flip6  -> samsung-galaxy-z-flip-6
    - samsung-galaxy-z-fold7  -> samsung-galaxy-z-fold-7
    - oppo-realme-gt8         -> oppo-realme-gt-8
    - realme-gt8-pro          -> realme-gt-8-pro
    """
    s = normalize_slug(slug)
    if not s:
        return []

    out = [s]

    # Samsung Z Flip/Fold: insertar guión entre flip/fold y el número
    out.append(re.sub(r"(z-(?:flip|fold))(\d+)(?=-|$)", r"\1-\2", s))

    # Realme GTx: insertar guión entre gt y el número (con o sin prefijo oppo-)
    out.append(re.sub(r"((?:oppo-)?realme-gt)(\d+)(?=-|$)", r"\1-\2", s))

    # Dedupe preservando orden
    seen = set()
    res: List[str] = []
    for x in out:
        x = normalize_slug(x)
        if x and x not in seen:
            seen.add(x)
            res.append(x)
    return res


def candidate_slugs(term_slug: str, parent_slug: str, term_name: str) -> List[str]:
    """Genera lista de slugs candidatos para Smart-GSM."""
    base = normalize_slug(term_slug)
    parent = normalize_slug(parent_slug)
    name = normalize_text(term_name).lower()

    slugs: List[str] = []

    # 1) Base + variantes sin -5g/-4g
    for s in strip_network_suffix(base):
        slugs.append(s)

    # 1b) Correcciones de guiones típicas (flip6 -> flip-6, gt8 -> gt-8, etc.)
    tmp: List[str] = []
    for s in slugs:
        tmp.extend(fix_special_hyphens(s))
    slugs = tmp

    # 2) Fix OPPO Reno (reno12 -> reno-12)
    tmp2: List[str] = []
    for s in slugs:
        tmp2.extend(fix_oppo_reno_hyphen(s))
    slugs = tmp2

    # 3) Prefijos especiales
    prefixed: List[str] = []
    if parent == "samsung":
        for s in slugs:
            if s.startswith("samsung-") and not s.startswith("samsung-galaxy-"):
                prefixed.append(f"samsung-galaxy-{s[len('samsung-'):]}")
    if parent == "poco":
        # Smart-GSM suele listar POCO como "xiaomi-poco-..."
        for s in slugs:
            if not s.startswith("xiaomi-"):
                prefixed.append(f"xiaomi-{s}")
    if parent == "redmi":
        for s in slugs:
            if not s.startswith("xiaomi-"):
                prefixed.append(f"xiaomi-{s}")
    if parent == "xiaomi":
        # algunos están como xiaomi-...
        for s in slugs:
            if not s.startswith("xiaomi-"):
                prefixed.append(f"xiaomi-{s}")
    if parent == "realme":
        # Smart-GSM lista realme bajo "oppo-"
        for s in slugs:
            if not s.startswith("oppo-"):
                prefixed.append(f"oppo-{s}")

    slugs.extend(prefixed)

    # 3b) Aplicar de nuevo correcciones de guiones sobre variantes nuevas
    tmp2: List[str] = []
    for s in slugs:
        tmp2.extend(fix_special_hyphens(s))
    slugs = tmp2

    # 4) Añadir sufijos de red
    with_net: List[str] = []
    for s in slugs:
        with_net.extend(add_network_suffixes(s))

    # 5) Eliminar duplicados preservando orden
    seen = set()
    uniq: List[str] = []
    for s in with_net:
        s = normalize_slug(s)
        if s and s not in seen:
            seen.add(s)
            uniq.append(s)

    # 6) Ajustes por nombre (si el nombre contiene 5g/4g, probar primero esa variante)
    # Evitamos generar cosas tipo "-4g-5g" usando base_without_network()
    if " 5g" in name or name.endswith("5g"):
        forced: List[str] = []
        for s in uniq:
            if s.endswith("-5g"):
                forced.append(s)
            else:
                forced.append(f"{base_without_network(s)}-5g")
        # añade el resto al final
        for s in uniq:
            if s not in forced:
                forced.append(s)
        uniq = forced

    if " 4g" in name or name.endswith("4g"):
        forced = []
        for s in uniq:
            if s.endswith("-4g"):
                forced.append(s)
            else:
                forced.append(f"{base_without_network(s)}-4g")
        for s in uniq:
            if s not in forced:
                forced.append(s)
        uniq = forced

    return uniq


# ------------------------------ Smart-GSM fetch ------------------------------

def smartgsm_url_for_slug(slug: str) -> str:
    return f"{SMARTGSM_BASE}/{slug}"


def smartgsm_fetch_specs(sess: requests.Session, slug: str) -> Tuple[Optional[str], Dict[str, str]]:
    """Devuelve (url_encontrada, specs) o (None, {})."""
    url = smartgsm_url_for_slug(slug)
    try:
        r = safe_get(sess, url)
    except Exception:
        return None, {}

    specs = extract_ficha_tecnica(r.text)
    if not specs:
        # Si no hay ficha técnica, para nuestro caso lo consideramos "no válida"
        return None, {}

    return url, specs


# ------------------------------ Main logic ----------------------------------

def should_ignore(term: CategoryTerm, parent: Optional[CategoryTerm]) -> bool:
    name_low = (term.name or "").strip().lower()
    if any(tok in name_low for tok in IGNORE_IF_NAME_CONTAINS):
        return True

    if IGNORE_IF_SUBCATEGORY_EQUALS_PARENT and parent:
        if normalize_text(term.name).lower() == normalize_text(parent.name).lower():
            return True

    return False


def main() -> int:
    if not WP_URL or not WP_KEY or not WP_SECRET:
        print("❌ Faltan variables WP_URL / WP_KEY / WP_SECRET", file=sys.stderr)
        return 2

    wp = wp_session()
    sg = smartgsm_session()

    print("============================================================")
    print(f"📡 SMART-GSM → Woo (Subcategorías) ({time.strftime('%Y-%m-%d %H:%M:%S')})")
    print("============================================================")
    print(f"Overwrite descripción existente: {SMARTGSM_OVERWRITE}")
    print(f"Pausa entre requests: {SMARTGSM_SLEEP}s")
    print(f"Base Smart-GSM: {SMARTGSM_BASE}")
    print("============================================================")

    all_terms = wp_get_all_categories(wp)
    term_by_id = {t.id: t for t in all_terms}

    # Subcategorías: parent != 0
    subs = [t for t in all_terms if t.parent != 0]

    print(f"📦 Subcategorías detectadas: {len(subs)}")

    updated: List[Tuple[str, int, int]] = []
    not_found: List[Tuple[str, int, str]] = []
    ignored: List[Tuple[str, int, str]] = []
    errors: List[Tuple[str, int, str]] = []

    for term in subs:
        parent = term_by_id.get(term.parent)

        if should_ignore(term, parent):
            reason = "subcategoría == marca" if parent and normalize_text(term.name).lower() == normalize_text(parent.name).lower() else "filtro"
            ignored.append((term.name, term.id, reason))
            continue

        parent_slug = parent.slug if parent else ""
        print("------------------------------------------------------------")
        print(f"📁 Subcategoría: {term.name} (ID: {term.id})")
        print(f"   slug: {term.slug} | parent_slug: {parent_slug}")

        # Si no overwrite y ya hay descripción, saltar
        if (not SMARTGSM_OVERWRITE) and normalize_text(term.description):
            print("   ⏭️  Saltada: ya tiene descripción (overwrite=0)")
            continue

        # Slugs candidatos
        cands = candidate_slugs(term.slug, parent_slug, term.name)

        found_url = None
        found_specs: Dict[str, str] = {}

        for s in cands:
            url, specs = smartgsm_fetch_specs(sg, s)
            if url and specs:
                found_url = url
                found_specs = specs
                break

            time.sleep(SMARTGSM_SLEEP)

        if not found_url:
            print(f"   ❌ NO ENCONTRADA ficha en Smart-GSM con slugs: {cands[:6]}{'...' if len(cands) > 6 else ''}")
            not_found.append((term.name, term.id, term.slug))
            continue

        print(f"   ✅ Ficha encontrada: {found_url}")
        print(f"   🔎 Campos extraídos: {len(found_specs)}")
        for k, v in list(found_specs.items())[:6]:
            print(f"      - {k}: {v}")

        new_html = build_specs_html_table(found_specs)

        try:
            wp_update_category_description(wp, term.id, new_html)
            print("   💾 DESCRIPCIÓN actualizada en Woo ✅")
            updated.append((term.name, term.id, len(found_specs)))
        except Exception as e:
            msg = str(e)
            print(f"   ❌ Error actualizando en Woo: {msg}")
            errors.append((term.name, term.id, msg))

        time.sleep(SMARTGSM_SLEEP)

    print("============================================================")
    print(f"📋 RESUMEN DE EJECUCIÓN ({time.strftime('%Y-%m-%d %H:%M:%S')})")
    print("============================================================")
    print(f"a) SUBCATEGORÍAS ACTUALIZADAS: {len(updated)}")
    for name, tid, nfields in updated:
        print(f"- {name} (ID: {tid}): {nfields} campos")

    print(f"b) SUBCATEGORÍAS NO ENCONTRADAS EN SMART-GSM: {len(not_found)}")
    for name, tid, slug in not_found[:200]:
        print(f"- {name} (ID: {tid}) slug='{slug}'")
    if len(not_found) > 200:
        print(f"... ({len(not_found) - 200} más)")

    print(f"c) SUBCATEGORÍAS IGNORADAS: {len(ignored)}")
    for name, tid, reason in ignored:
        print(f"- {name} (ID: {tid}): {reason}")

    print(f"d) ERRORES ACTUALIZANDO EN WOO: {len(errors)}")
    for name, tid, msg in errors:
        print(f"- {name} (ID: {tid}): {msg}")

    print("============================================================")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
