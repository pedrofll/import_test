#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SMART-GSM → Woo (Subcategorías)

- Recorre subcategorías (product_cat) en WooCommerce, encuentra la ficha en Smart-GSM
  y actualiza la descripción con una ficha técnica (texto limpio).
- Soporta heurísticas de slugs: 5G/4G, prefijos (xiaomi-, oppo-realme-, zte-...), guiones especiales, etc.

ENV:
  WP_URL, WP_KEY, WP_SECRET
  SMARTGSM_OVERWRITE: 1/0 (default 0)
  SMARTGSM_SLEEP: segundos (default 0.8)
"""

from __future__ import annotations

import os
import re
import sys
import time
import html
import json
from typing import Dict, List, Optional, Tuple

import requests


# -----------------------------
# Regex / helpers globales
# -----------------------------

_slug_re_non_alnum = re.compile(r"[^a-z0-9]+", re.I)
_slug_re_multi_dash = re.compile(r"-{2,}")
_re_5g4g_end = re.compile(r"-(5g|4g)$", re.I)


# -----------------------------
# Config / constants
# -----------------------------

SMARTGSM_BASE = "https://www.smart-gsm.com/moviles"
USER_AGENT = "Mozilla/5.0 (compatible; smartgsm_specs/1.0; +https://example.com)"

DEFAULT_SLEEP = 0.8


# Campos que NO deben importarse nunca aunque aparezcan en Smart-GSM
DISALLOWED_FIELDS = {
    "Precio",  # Nunca importar
}


# -----------------------------
# WP REST helpers
# -----------------------------

def env(name: str, default: Optional[str] = None) -> str:
    v = os.environ.get(name)
    if v is None:
        if default is None:
            raise RuntimeError(f"Falta variable de entorno: {name}")
        return default
    return v


def wp_request(method: str, url: str, auth: Tuple[str, str], **kwargs) -> requests.Response:
    headers = kwargs.pop("headers", {})
    headers["User-Agent"] = USER_AGENT
    return requests.request(method, url, auth=auth, headers=headers, timeout=60, **kwargs)


def wp_get_all_categories(wp_url: str, auth: Tuple[str, str]) -> List[dict]:
    out = []
    page = 1
    per_page = 100
    while True:
        r = wp_request(
            "GET",
            f"{wp_url}/wp-json/wc/v3/products/categories",
            auth,
            params={"per_page": per_page, "page": page, "hide_empty": False},
        )
        r.raise_for_status()
        data = r.json()
        if not data:
            break
        out.extend(data)
        if len(data) < per_page:
            break
        page += 1
    return out


def wp_update_category_description(wp_url: str, auth: Tuple[str, str], cat_id: int, description_html: str) -> None:
    r = wp_request(
        "PUT",
        f"{wp_url}/wp-json/wc/v3/products/categories/{cat_id}",
        auth,
        json={"description": description_html},
    )
    r.raise_for_status()


# -----------------------------
# Slug normalization / variants
# -----------------------------

def normalize_slug(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("ñ", "n")
    s = s.replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u").replace("ü", "u")
    s = _slug_re_non_alnum.sub("-", s)
    s = _slug_re_multi_dash.sub("-", s)
    return s.strip("-")


def base_without_network(slug: str) -> str:
    slug = (slug or "").strip("-").lower()
    slug = _re_5g4g_end.sub("", slug)
    return slug.strip("-")


def strip_network_suffix(slug: str) -> List[str]:
    """
    Devuelve lista con:
    - el slug tal cual (si existe)
    - y el slug sin sufijo -5g/-4g (si aplica)
    """
    out = []
    slug = (slug or "").strip("-").lower()
    if slug:
        out.append(slug)
    base = base_without_network(slug)
    if base and base != slug:
        out.append(base)
    # Dedup manteniendo orden
    seen = set()
    res = []
    for s in out:
        if s not in seen:
            seen.add(s)
            res.append(s)
    return res


def add_network_suffixes(slug: str, prefer: str = "") -> List[str]:
    """
    Genera variantes del slug con -5g/-4g.
    prefer: '', '5g' o '4g' para priorizar.
    """
    slug = (slug or "").strip("-")
    if not slug:
        return []

    if slug.endswith("-5g") or slug.endswith("-4g"):
        return [slug]

    if prefer == "5g":
        return [f"{slug}-5g", slug, f"{slug}-4g"]
    if prefer == "4g":
        return [f"{slug}-4g", slug, f"{slug}-5g"]
    return [slug, f"{slug}-5g", f"{slug}-4g"]


def add_network_suffixes_many(slugs: List[str], term_name: str) -> List[str]:
    """Genera variantes con sufijos -5g/-4g (sin duplicados) y con un orden razonable.

    - Si el nombre del término contiene '5G' o '4G', se prioriza ese sufijo.
    - Se incluye siempre el slug base (sin sufijo) porque Smart-GSM a veces no añade el sufijo.
    """
    name_lc = (term_name or "").lower()
    prefer_5g = "5g" in name_lc
    prefer_4g = "4g" in name_lc

    out: List[str] = []
    seen = set()

    def _add(v: str) -> None:
        if v and v not in seen:
            seen.add(v)
            out.append(v)

    for s in slugs:
        if not s:
            continue

        # Por seguridad: si por algún bug llega una lista, la aplanamos.
        if isinstance(s, list):
            for item in s:
                if isinstance(item, str):
                    _add(item)
            continue

        if s.endswith("-5g") or s.endswith("-4g"):
            _add(s)
            continue

        base = s
        if prefer_5g and not prefer_4g:
            _add(f"{base}-5g")
            _add(base)
            _add(f"{base}-4g")
        elif prefer_4g and not prefer_5g:
            _add(f"{base}-4g")
            _add(base)
            _add(f"{base}-5g")
        else:
            _add(base)
            _add(f"{base}-5g")
            _add(f"{base}-4g")

    return out


def fix_special_hyphens(slug: str) -> List[str]:
    """
    Genera variantes de slug arreglando casos típicos donde Smart-GSM usa guiones diferentes:
    - "magic7" -> "magic-7"
    - "flip6" -> "flip-6", "fold7" -> "fold-7"
    - "gt8" -> "gt-8"
    - "redmagic" -> "red-magic"
    - "reno12" -> "reno-12", etc.
    """
    slug = (slug or "").strip("-")
    if not slug:
        return []

    out = [slug]

    # Separar letras+dígitos (genérico): flip6 -> flip-6, fold7 -> fold-7, gt8 -> gt-8, reno13 -> reno-13, etc.
    gen = re.sub(r"([a-z])(\d)", r"\1-\2", slug, flags=re.I)
    if gen != slug:
        out.append(gen)

    # Casos específicos
    specs = [
        (r"(magic)(\d)", r"\1-\2"),
        (r"(flip)(\d)", r"\1-\2"),
        (r"(fold)(\d)", r"\1-\2"),
        (r"(gt)(\d)", r"\1-\2"),
        (r"(reno)(\d)", r"\1-\2"),
        (r"(redmagic)", r"red-magic"),
        (r"(red-magic)(\d)", r"\1-\2"),
        (r"(s)(\d{2})", r"\1\2"),  # por si entra "s25" (no lo cambia)
    ]
    for pat, rep in specs:
        v = re.sub(pat, rep, slug, flags=re.I)
        if v != slug:
            out.append(v)

        v2 = re.sub(pat, rep, gen, flags=re.I)
        if v2 != gen:
            out.append(v2)

    # Dedup manteniendo orden
    seen = set()
    res = []
    for s in out:
        if s not in seen:
            seen.add(s)
            res.append(s)
    return res


def plus_to_plus_word(slug: str) -> str:
    # Convierte pro+ / plus en slug: si ya contiene plus lo deja.
    # Si detecta "-pro" y el name contiene "+", se maneja fuera.
    return slug


# -----------------------------
# Smart-GSM scraping
# -----------------------------

def fetch_smartgsm_page(url: str) -> Optional[str]:
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=45)
        if r.status_code != 200:
            return None
        return r.text
    except Exception:
        return None


def clean_text(s: str) -> str:
    s = html.unescape(s or "")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def extract_specs_from_html(page_html: str) -> Dict[str, str]:
    """
    Extrae la ficha técnica como dict campo->valor.
    Nota: evita capturar Precio y limpia HTML malformado.
    """
    if not page_html:
        return {}

    # Intentar localizar bloque de specs (muy variable)
    # Estrategia: buscar pares <strong>Campo</strong>Valor o tablas con th/td.
    specs: Dict[str, str] = {}

    # 1) Caso inline: <strong>Pantalla</strong>6.7", ...
    #    Captura secuencia strong + texto hasta siguiente strong
    strong_pat = re.compile(r"<strong>\s*([^<]+?)\s*</strong>\s*([^<]+?)(?=<strong>|</p>|</div>|$)", re.I | re.S)
    for m in strong_pat.finditer(page_html):
        k = clean_text(re.sub(r"<.*?>", "", m.group(1)))
        v = clean_text(re.sub(r"<.*?>", "", m.group(2)))
        if not k or not v:
            continue
        if k in DISALLOWED_FIELDS:
            continue
        # Filtra valores absurdos muy largos
        if len(v) > 200:
            continue
        specs[k] = v

    # 2) Tablas (fallback)
    row_pat = re.compile(r"<tr[^>]*>\s*<t[hd][^>]*>\s*([^<]+?)\s*</t[hd]>\s*<t[hd][^>]*>\s*([^<]+?)\s*</t[hd]>\s*</tr>", re.I | re.S)
    for m in row_pat.finditer(page_html):
        k = clean_text(re.sub(r"<.*?>", "", m.group(1)))
        v = clean_text(re.sub(r"<.*?>", "", m.group(2)))
        if not k or not v:
            continue
        if k in DISALLOWED_FIELDS:
            continue
        if len(v) > 200:
            continue
        specs.setdefault(k, v)

    # Limpieza extra: si por algún motivo entra "Precio" como parte de otra captura
    specs.pop("Precio", None)
    return specs


def specs_to_description_html(specs: Dict[str, str]) -> str:
    """
    Render de la ficha técnica en HTML limpio (sin mezclar <strong> pegados).
    """
    if not specs:
        return ""

    # Orden preferido (si existe)
    preferred = [
        "Pantalla",
        "Procesador",
        "Memoria RAM",
        "Almacenamiento",
        "Expansión",
        "Cámara",
        "Batería",
        "OS",
        "Actualizaciones OS",
        "Soporte",
        "Perfil",
        "Peso",
    ]

    # Mantener campos preferidos primero y luego el resto
    keys = []
    for k in preferred:
        if k in specs:
            keys.append(k)
    for k in specs.keys():
        if k not in keys:
            keys.append(k)

    lines = ["<p><strong>Ficha técnica</strong></p>", "<ul>"]
    for k in keys:
        if k in DISALLOWED_FIELDS:
            continue
        v = specs.get(k, "")
        if not v:
            continue
        lines.append(f"<li><strong>{html.escape(k)}:</strong> {html.escape(v)}</li>")
    lines.append("</ul>")
    return "\n".join(lines)


# -----------------------------
# Candidate slug logic
# -----------------------------

def candidate_slugs(slug: str, term_name: str, parent_slug: str) -> List[str]:
    """
    Devuelve una lista ordenada de slugs candidatos para construir URL de Smart-GSM.
    """
    slug = (slug or "").strip().lower()
    parent_slug = (parent_slug or "").strip().lower()
    name_lc = (term_name or "").strip().lower()

    # Base: slug original y sin red
    bases = strip_network_suffix(slug)

    # Si el slug termina en "-{marca}" (ej: redmi-note-15-pro-redmi), quitar ese sufijo
    # Esto ayuda con categorías mal formadas.
    cleaned_bases = []
    for b in bases:
        if parent_slug and b.endswith(f"-{parent_slug}"):
            cleaned_bases.append(b[: -(len(parent_slug) + 1)])
        else:
            cleaned_bases.append(b)
    bases = cleaned_bases

    slugs: List[str] = []
    for b in bases:
        slugs.extend(strip_network_suffix(b))

    # Variantes por guiones especiales (magic7 -> magic-7, gt8 -> gt-8, flip6 -> flip-6, redmagic -> red-magic...)
    expanded: List[str] = []
    for s in slugs:
        expanded.extend(fix_special_hyphens(s))

    # Dedup manteniendo orden
    seen = set()
    slugs2 = []
    for s in expanded:
        if s not in seen:
            seen.add(s)
            slugs2.append(s)
    slugs = slugs2

    prefixed: List[str] = []

    # Reglas por marca / parent_slug:
    # - Realme aparece como "oppo-realme-..."
    if parent_slug == "realme":
        for s in slugs:
            prefixed.append(f"oppo-{s}")

    # - POCO y Redmi suelen estar como "xiaomi-poco-..." / "xiaomi-redmi-..."
    if parent_slug in {"poco", "redmi"}:
        for s in slugs:
            prefixed.append(f"xiaomi-{s}")

    # - Nubia: suele ir como "zte-nubia-..." (y a veces también "zte-...")
    if parent_slug == "nubia":
        for s in slugs:
            prefixed.append(f"zte-{s}")
            prefixed.append(f"zte-nubia-{s}")

    # - Samsung: algunos vienen con "samsung-galaxy-..." aunque en Woo sea "samsung-s25..."
    if parent_slug == "samsung":
        for s in slugs:
            if s.startswith("samsung-") and not s.startswith("samsung-galaxy-"):
                prefixed.append("samsung-galaxy-" + s[len("samsung-") :])

    # Reno: a veces Smart-GSM usa "reno-12" (con guion)
    # Esto ya lo cubre fix_special_hyphens, pero mantenemos por seguridad.
    # (No hace daño si ya existe.)
    for s in slugs:
        if "reno" in s:
            prefixed.extend(fix_special_hyphens(s))

    # Caso "+" en nombre: Pro+ => pro-plus
    # Ej: "Redmi Note 15 Pro+" => "...-pro-plus..."
    # Solo aplicarlo cuando se detecta '+' en el nombre (o "pro+")
    if "+" in term_name or "pro+" in name_lc:
        plus_variants: List[str] = []
        for s in slugs + prefixed:
            if s.endswith("-pro"):
                plus_variants.append(s + "-plus")
            # Si ya contiene "-pro-" podemos insertar plus después de pro
            s2 = re.sub(r"-pro($|-)", r"-pro-plus\1", s)
            if s2 != s:
                plus_variants.append(s2)
        for v in plus_variants:
            prefixed.append(v)

    # Excepción: POCO F8 Pro (slug raro "f8-pro"), Smart-GSM parece haberlo publicado sin "pro"
    if parent_slug == "poco" and slug == "f8-pro":
        prefixed.append("xiaomi-poco-f8-5g")
        prefixed.append("xiaomi-poco-f8")

    # Si el nombre contiene 5G/4G, priorizar esa variante
    prefer = ""
    if "5g" in name_lc:
        prefer = "5g"
    elif "4g" in name_lc:
        prefer = "4g"

    all_slugs = add_network_suffixes_many(slugs + prefixed, term_name)

    # Si prefer está definido, reordenar para que los que acaban en prefer vayan primero
    if prefer:
        preferred_first = []
        rest = []
        for s in all_slugs:
            if s.endswith(f"-{prefer}"):
                preferred_first.append(s)
            else:
                rest.append(s)
        all_slugs = preferred_first + rest

    # Dedup final manteniendo orden
    seen = set()
    res = []
    for s in all_slugs:
        if s not in seen:
            seen.add(s)
            res.append(s)
    return res


def build_smartgsm_url(slug: str) -> str:
    return f"{SMARTGSM_BASE}/{slug}"


def find_first_working_url(candidates: List[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Devuelve (url, html) para el primer candidato que exista.
    """
    for s in candidates:
        url = build_smartgsm_url(s)
        html_txt = fetch_smartgsm_page(url)
        if html_txt:
            return url, html_txt
    return None, None


# -----------------------------
# Main
# -----------------------------

def main() -> int:
    wp_url = env("WP_URL")
    wp_key = env("WP_KEY")
    wp_secret = env("WP_SECRET")

    overwrite = env("SMARTGSM_OVERWRITE", "0").strip() == "1"
    sleep_s = float(env("SMARTGSM_SLEEP", str(DEFAULT_SLEEP)))

    auth = (wp_key, wp_secret)

    print("============================================================")
    print(f"📡 SMART-GSM → Woo (Subcategorías) ({time.strftime('%Y-%m-%d %H:%M:%S')})")
    print("============================================================")
    print(f"Overwrite descripción existente: {overwrite}")
    print(f"Pausa entre requests: {sleep_s}s")
    print(f"Base Smart-GSM: {SMARTGSM_BASE}")
    print("============================================================")

    cats = wp_get_all_categories(wp_url, auth)

    # Mapa id->cat
    cats_by_id = {c["id"]: c for c in cats}
    cats_by_slug = {}
    for c in cats:
        cats_by_slug.setdefault(c.get("slug", ""), c)

    # Detectar subcategorías: level 2 (tienen parent)
    subs = [c for c in cats if int(c.get("parent") or 0) != 0]

    print(f"📦 Subcategorías detectadas: {len(subs)}")
    print("------------------------------------------------------------")

    updated = []
    not_found = []
    ignored = []
    errors = []

    for c in subs:
        cat_id = int(c["id"])
        name = c.get("name") or ""
        slug = (c.get("slug") or "").strip()
        parent_id = int(c.get("parent") or 0)
        parent = cats_by_id.get(parent_id) or {}
        parent_slug = (parent.get("slug") or "").strip().lower()
        parent_name = (parent.get("name") or "").strip()

        # Ignorar si "subcategoría == marca"
        if normalize_slug(name) == normalize_slug(parent_name) or normalize_slug(slug) == normalize_slug(parent_slug):
            ignored.append((name, cat_id, "subcategoría == marca"))
            continue

        # Ignorar tablets
        if "tablet" in name.lower() or "pad" in name.lower():
            ignored.append((name, cat_id, "tablet"))
            continue

        existing_desc = (c.get("description") or "").strip()
        if existing_desc and not overwrite:
            print(f"📁 Subcategoría: {name} (ID: {cat_id})")
            print(f"   slug: {slug} | parent_slug: {parent_slug}")
            print("   ⏭️  Saltada: ya tiene descripción (overwrite=0)")
            continue

        print(f"📁 Subcategoría: {name} (ID: {cat_id})")
        print(f"   slug: {slug} | parent_slug: {parent_slug}")

        cands = candidate_slugs(slug, name, parent_slug)
        url, page = find_first_working_url(cands)

        if not url:
            print(f"   ❌ NO ENCONTRADA ficha en Smart-GSM con slugs: {cands}")
            not_found.append((name, cat_id, slug))
            print("------------------------------------------------------------")
            continue

        print(f"   ✅ Ficha encontrada: {url}")

        specs = extract_specs_from_html(page)
        # Filtrar precio si entrara por alguna razón
        specs.pop("Precio", None)

        print(f"   🔎 Campos extraídos: {len(specs)}")
        for k, v in list(specs.items())[:6]:
            print(f"      - {k}: {v}")

        desc_html = specs_to_description_html(specs)
        if not desc_html:
            print("   ⚠️  Sin specs (no se actualiza)")
            print("------------------------------------------------------------")
            continue

        try:
            wp_update_category_description(wp_url, auth, cat_id, desc_html)
            updated.append((name, cat_id, len(specs)))
            print("   💾 DESCRIPCIÓN actualizada en Woo ✅")
        except Exception as e:
            errors.append((name, cat_id, str(e)))
            print(f"   ❌ ERROR actualizando en Woo: {e}")

        print("------------------------------------------------------------")
        time.sleep(sleep_s)

    # Resumen
    print("============================================================")
    print(f"📋 RESUMEN DE EJECUCIÓN ({time.strftime('%Y-%m-%d %H:%M:%S')})")
    print("============================================================")
    print(f"a) SUBCATEGORÍAS ACTUALIZADAS: {len(updated)}")
    for n, cid, cnt in updated:
        print(f"- {n} (ID: {cid}): {cnt} campos")

    print(f"b) SUBCATEGORÍAS NO ENCONTRADAS EN SMART-GSM: {len(not_found)}")
    for n, cid, s in not_found:
        print(f"- {n} (ID: {cid}) slug='{s}'")

    print(f"c) SUBCATEGORÍAS IGNORADAS: {len(ignored)}")
    for n, cid, why in ignored:
        print(f"- {n} (ID: {cid}): {why}")

    print(f"d) ERRORES ACTUALIZANDO EN WOO: {len(errors)}")
    for n, cid, err in errors:
        print(f"- {n} (ID: {cid}): {err}")

    print("============================================================")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
