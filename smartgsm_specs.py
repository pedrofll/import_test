#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""smartgsm_specs.py

Sincroniza la "Ficha técnica" de Smart-GSM dentro de la DESCRIPCIÓN de
las subcategorías (product_cat hijos) en WooCommerce.

- Recorre todas las subcategorías (términos con parent != 0)
- Construye el slug de Smart-GSM y descarga la tabla de "Ficha técnica"
- Convierte la ficha en HTML (lista) y la guarda en la descripción del término

ENV requeridas:
  WP_URL      -> https://tudominio.com
  WP_KEY      -> consumer_key
  WP_SECRET   -> consumer_secret

ENV opcionales:
  SMARTGSM_OVERWRITE -> 1/0 (default 0) si 1, sobreescribe descripciones existentes
  SMARTGSM_SLEEP     -> pausa entre requests a Smart-GSM (default 0.8)
  SMARTGSM_TIMEOUT   -> timeout requests (default 25)

Notas:
- Filtra tablets (TAB, IPAD, PAD, TABLET) y no toca esas subcategorías.
- Nunca guarda "Precio" (o variantes) si aparece en la ficha de Smart-GSM.
- Si no encuentra la ficha con el slug, prueba variantes:
    * quitar sufijo -5g / -4g
    * prefijos para marcas "internas" (poco/redmi -> xiaomi)
    * slug regenerado desde el nombre (normalizado)

"""

from __future__ import annotations

import os
import re
import sys
import time
import json
import unicodedata
import html
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import requests

try:
    from bs4 import BeautifulSoup
except Exception as e:
    print("❌ Falta dependencia: bs4 (BeautifulSoup). Instala con: pip install beautifulsoup4")
    raise


SMARTGSM_BASE = "https://www.smart-gsm.com/moviles"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
}

# Palabras que implican que NO es un móvil
TABLET_WORD_RE = re.compile(r"\b(tab|ipad|tablet|pad)\b", re.IGNORECASE)

# Campos que NO queremos importar jamás
# (lo hacemos por contains porque Smart-GSM puede usar variantes tipo "Precio" / "Precio aprox" / etc.)
SKIP_KEY_RE = re.compile(r"\b(precio|precios|price|prices)\b", re.IGNORECASE)

# Prefijos de Smart-GSM cuando nuestra subcategoría es de una marca "interna"
# Ejemplo real: POCO M7 Pro 5G -> https://www.smart-gsm.com/moviles/xiaomi-poco-m7-pro-5g
PREFIX_MAP = {
    "poco": "xiaomi",
    "redmi": "xiaomi",
    # En Smart-GSM muchos Realme salen como "Oppo Realme" y usan prefijo "oppo-"
    # Ej: https://www.smart-gsm.com/moviles/oppo-realme-gt-7t
    "realme": "oppo",
}


def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if not v:
        return default
    try:
        return float(v)
    except Exception:
        return default


def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if not v:
        return default
    try:
        return int(v)
    except Exception:
        return default


def now_fmt() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))


def smart_slugify(name: str) -> str:
    """Convierte 'Xiaomi 15T Pro' -> 'xiaomi-15t-pro' (manteniendo 5g/4g)."""
    s = strip_accents(name).lower().strip()
    # normaliza símbolos raros
    s = s.replace("+", " plus ")
    s = re.sub(r"[’'`´]", "", s)
    # cambia cualquier no-alfanum por guion
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s


def candidates_from_slug(slug: str) -> List[str]:
    """Genera variantes por -5g / -4g al final."""
    out = [slug]
    if slug.endswith("-5g"):
        out.append(slug[:-3])
    if slug.endswith("-4g"):
        out.append(slug[:-3])
    # casos como ...-5g-xxx (a veces viene redundante), solo intentamos quitar el último token
    # pero SIN destrozar otros slugs; lo dejamos simple.
    # remove trailing '-5g' / '-4g' ya cubierto.
    # unique
    uniq = []
    for s in out:
        if s and s not in uniq:
            uniq.append(s)
    return uniq


def is_tablet_term(term_name: str) -> bool:
    return bool(TABLET_WORD_RE.search(term_name or ""))


@dataclass
class WPTerm:
    id: int
    name: str
    slug: str
    parent: int
    description: str


class WooClient:
    def __init__(self, base_url: str, ck: str, cs: str, timeout: int = 25):
        self.base_url = base_url.rstrip("/")
        self.ck = ck
        self.cs = cs
        self.timeout = timeout

    def _url(self, path: str) -> str:
        return f"{self.base_url}/wp-json/wc/v3{path}"

    def get_all_categories(self) -> List[WPTerm]:
        terms: List[WPTerm] = []
        page = 1
        per_page = 100
        while True:
            url = self._url("/products/categories")
            params = {
                "consumer_key": self.ck,
                "consumer_secret": self.cs,
                "per_page": per_page,
                "page": page,
                "hide_empty": False,
            }
            r = requests.get(url, params=params, timeout=self.timeout)
            if r.status_code != 200:
                raise RuntimeError(f"Woo GET categories HTTP {r.status_code}: {r.text[:300]}")
            data = r.json()
            if not data:
                break
            for t in data:
                terms.append(
                    WPTerm(
                        id=int(t.get("id")),
                        name=str(t.get("name") or ""),
                        slug=str(t.get("slug") or ""),
                        parent=int(t.get("parent") or 0),
                        description=str(t.get("description") or ""),
                    )
                )
            if len(data) < per_page:
                break
            page += 1
        return terms

    def update_category_description(self, term_id: int, html: str) -> None:
        url = self._url(f"/products/categories/{term_id}")
        params = {
            "consumer_key": self.ck,
            "consumer_secret": self.cs,
        }
        payload = {"description": html}
        r = requests.put(url, params=params, json=payload, timeout=self.timeout)
        if r.status_code not in (200, 201):
            raise RuntimeError(f"Woo PUT category {term_id} HTTP {r.status_code}: {r.text[:400]}")


def fetch_smartgsm_html(slug: str, timeout: int = 25) -> Optional[str]:
    url = f"{SMARTGSM_BASE}/{slug}"
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    if r.status_code != 200:
        return None
    # Heurística rápida para evitar páginas irrelevantes
    if "Ficha técnica" not in r.text:
        return None
    return r.text


def parse_ficha_tecnica(html: str) -> Dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")

    # busca el H2 exacto "Ficha técnica"
    h2 = None
    for node in soup.find_all(["h2", "h3"]):
        txt = " ".join(node.get_text(" ", strip=True).split())
        if txt.lower() == "ficha técnica" or txt.lower() == "ficha tecnica":
            h2 = node
            break

    if not h2:
        return {}

    # La tabla suele estar dentro del mismo bloque/row; buscamos la primera tabla tras el h2
    table = h2.find_next("table")
    if not table:
        return {}

    specs: Dict[str, str] = {}
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue

        # key: normalmente en el primer td hay <strong>Clave</strong>
        key = tds[0].get_text(" ", strip=True)
        key = re.sub(r"\s+", " ", key).strip()
        # elimina iconos/ruidos: nos quedamos con el texto del strong si existe
        strong = tds[0].find("strong")
        if strong:
            key2 = strong.get_text(" ", strip=True)
            if key2:
                key = key2

        val = tds[1].get_text(" ", strip=True)
        val = re.sub(r"\s+", " ", val).strip()

        if not key or not val:
            continue
        if SKIP_KEY_RE.search(key):
            continue

        specs[key] = val

    return specs


def build_specs_html(specs: Dict[str, str]) -> str:
    """Genera HTML robusto (evita que quede "pegado" en themes/plugins que limpian listas/tablas).

    Usamos <br> para que, incluso si el tema hace sanitizado agresivo,
    el contenido siga legible.
    """
    lines: List[str] = []
    lines.append('<div class="smartgsm-specs">')
    lines.append("<h2>Ficha técnica</h2>")
    lines.append("<p>")
    first = True
    for k, v in specs.items():
        # Escape HTML (seguro y consistente)
        k_esc = html.escape(k, quote=False)
        v_esc = html.escape(v, quote=False)
        if not first:
            lines.append("<br>")
        first = False
        lines.append(f"<strong>{k_esc}</strong>: {v_esc}")
    lines.append("</p>")
    lines.append("</div>")
    return "\n".join(lines)


def build_slug_candidates(term: WPTerm, parent_slug: str, parent_name: str) -> List[str]:
    """Genera lista de slugs a intentar en Smart-GSM."""
    candidates: List[str] = []

    # 1) el slug existente (y variantes -5g/-4g)
    for s in candidates_from_slug(term.slug):
        candidates.append(s)

    # 2) slug regenerado desde el nombre (por si el slug del término está raro: f7-pro, motorola-motorola, etc.)
    name_slug = smart_slugify(term.name)
    for s in candidates_from_slug(name_slug):
        candidates.append(s)

    # 3) Prefijos de Smart-GSM para ciertas marcas
    #    - Si el parent es poco/redmi/realme, Smart-GSM puede usar prefijos distintos.
    #    - Además, si el slug o el nombre empiezan por poco/redmi/realme, aplicamos el prefijo
    #      aunque el árbol de categorías esté "raro".
    p = (parent_slug or "").strip().lower()

    # señales para aplicar prefijos aunque el parent no sea exacto
    name_l = (term.name or "").strip().lower()
    base_slugs = [term.slug, name_slug]

    def should_apply(prefix_key: str) -> bool:
        if p == prefix_key:
            return True
        if name_l.startswith(prefix_key + " "):
            return True
        if any((bs or "").lower().startswith(prefix_key + "-") for bs in base_slugs):
            return True
        return False

    for k, pref in PREFIX_MAP.items():
        if not should_apply(k):
            continue
        for base in base_slugs:
            for s in candidates_from_slug(base):
                # evita doble prefijo tipo xiaomi-xiaomi-...
                if s.startswith(pref + "-"):
                    candidates.append(s)
                else:
                    candidates.append(f"{pref}-{s}")

    # 4) Heurística: si el nombre empieza por una marca conocida distinta del parent
    #    (ej: si el parent es 'poco' y el nombre es 'POCO ...', ya lo cubrimos con prefijo xiaomi)
    #    (ej: subcat 'Vivo IQOO ...' a veces parent 'iqoo' o 'vivo')
    if p == "iqoo":
        for base in [term.slug, name_slug]:
            for s in candidates_from_slug(base):
                candidates.append(f"vivo-{s}")

    # unique manteniendo orden
    uniq: List[str] = []
    for s in candidates:
        s = (s or "").strip()
        if not s:
            continue
        if s not in uniq:
            uniq.append(s)

    return uniq


def main() -> int:
    wp_url = os.getenv("WP_URL", "").strip()
    wp_key = os.getenv("WP_KEY", "").strip()
    wp_secret = os.getenv("WP_SECRET", "").strip()

    if not wp_url or not wp_key or not wp_secret:
        print("❌ Faltan variables de entorno WP_URL / WP_KEY / WP_SECRET")
        return 2

    overwrite = env_bool("SMARTGSM_OVERWRITE", False)
    sleep_s = env_float("SMARTGSM_SLEEP", 0.8)
    timeout_s = env_int("SMARTGSM_TIMEOUT", 25)

    woo = WooClient(wp_url, wp_key, wp_secret, timeout=timeout_s)

    print("=" * 60)
    print(f"📡 SMART-GSM → Woo (Subcategorías) ({now_fmt()})")
    print("=" * 60)
    print(f"Overwrite descripción existente: {overwrite}")
    print(f"Pausa entre requests: {sleep_s}s")
    print(f"Base Smart-GSM: {SMARTGSM_BASE}")
    print("=" * 60)

    terms = woo.get_all_categories()
    by_id = {t.id: t for t in terms}

    subcats = [t for t in terms if t.parent != 0]
    print(f"📦 Subcategorías detectadas: {len(subcats)}")
    print("-" * 60)

    updated: List[Tuple[str, int, int]] = []  # (name, id, campos)
    not_found: List[Tuple[str, int, str]] = []  # (name, id, slug)
    ignored: List[Tuple[str, int, str]] = []  # (name, id, reason)
    errors: List[Tuple[str, int, str]] = []

    for term in subcats:
        parent = by_id.get(term.parent)
        parent_slug = parent.slug if parent else ""
        parent_name = parent.name if parent else ""

        # Filtro tablets
        if is_tablet_term(term.name):
            ignored.append((term.name, term.id, "tablet"))
            continue

        # Si el término es básicamente la marca (ej: Motorola (slug motorola-motorola)), lo ignoramos
        if parent and smart_slugify(term.name) == smart_slugify(parent.name):
            ignored.append((term.name, term.id, "subcategoría == marca"))
            continue

        # Si no overwrite y ya tiene descripción, ignorar
        if (not overwrite) and term.description and term.description.strip():
            ignored.append((term.name, term.id, "ya tiene descripción"))
            continue

        print(f"📁 Subcategoría: {term.name} (ID: {term.id})")
        print(f"   slug: {term.slug} | parent_slug: {parent_slug}")

        slug_candidates = build_slug_candidates(term, parent_slug, parent_name)

        found_slug = None
        found_specs: Dict[str, str] = {}
        for cand in slug_candidates:
            html_txt = fetch_smartgsm_html(cand, timeout=timeout_s)
            if not html_txt:
                continue
            specs = parse_ficha_tecnica(html_txt)
            # defensivo: si parsea pero no hay nada útil, sigue probando
            if specs:
                found_slug = cand
                found_specs = specs
                break

        if not found_slug:
            print(f"   ❌ NO ENCONTRADA ficha en Smart-GSM con slugs: {slug_candidates}")
            not_found.append((term.name, term.id, term.slug))
            print("-" * 60)
            continue

        html_desc = build_specs_html(found_specs)

        print(f"   ✅ Ficha encontrada: {SMARTGSM_BASE}/{found_slug}")
        print(f"   🔎 Campos extraídos: {len(found_specs)}")
        # muestra solo algunos campos (sin saturar logs)
        for i, (k, v) in enumerate(found_specs.items()):
            if i >= 6:
                break
            print(f"      - {k}: {v}")

        try:
            woo.update_category_description(term.id, html_desc)
            print("   💾 DESCRIPCIÓN actualizada en Woo ✅")
            updated.append((term.name, term.id, len(found_specs)))
        except Exception as e:
            msg = str(e)
            print(f"   ❌ Error actualizando en Woo: {msg}")
            errors.append((term.name, term.id, msg))

        print("-" * 60)
        time.sleep(max(0.0, sleep_s))

    # RESUMEN
    print("=" * 60)
    print(f"📋 RESUMEN DE EJECUCIÓN ({now_fmt()})")
    print("=" * 60)

    print(f"a) SUBCATEGORÍAS ACTUALIZADAS: {len(updated)}")
    for name, tid, nfields in updated:
        print(f"- {name} (ID: {tid}): {nfields} campos")

    print(f"b) SUBCATEGORÍAS NO ENCONTRADAS EN SMART-GSM: {len(not_found)}")
    for name, tid, slug in not_found:
        print(f"- {name} (ID: {tid}) slug='{slug}'")

    print(f"c) SUBCATEGORÍAS IGNORADAS: {len(ignored)}")
    for name, tid, reason in ignored:
        print(f"- {name} (ID: {tid}): {reason}")

    print(f"d) ERRORES ACTUALIZANDO EN WOO: {len(errors)}")
    for name, tid, msg in errors:
        print(f"- {name} (ID: {tid}): {msg}")

    print("=" * 60)

    # exit code: 0 si no hay errores
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
