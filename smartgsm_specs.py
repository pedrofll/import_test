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
    SMARTGSM_OVERWRITE    -> 1/0 (default 0). Si 0, sólo escribe si la descripción está vacía.
    SMARTGSM_SLEEP        -> segundos de pausa entre requests a Smart-GSM (default 0.8)

Notas:
- No importamos tablets: si el nombre de la subcategoría contiene TAB o IPAD => IGNORADA.
- No queremos que se “cuele” el precio (Smart-GSM a veces lo muestra): se ignora cualquier
  fila cuyo label sea "Precio" (o empiece por "Precio").
- Mejoras de matching de slugs:
    * Si no encuentra, prueba quitando sufijos -5g/-4g.
    * Si no encuentra, prueba añadiendo -5g y/o -4g.
    * POCO/Redmi: a veces Smart-GSM los publica como "xiaomi-poco-..." / "xiaomi-redmi-...".
    * Realme: Smart-GSM los publica bajo OPPO: "oppo-realme-...".
    * OPPO Reno: Smart-GSM usa "oppo-reno-12-..." (con guión entre reno y el número).

"""

from __future__ import annotations

import html
import os
import re
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup


# ------------------------------- Config ------------------------------------

SMARTGSM_BASE = "https://www.smart-gsm.com/moviles"
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)

# Filtrado tablets
TABLET_TOKENS = {"TAB", "IPAD"}

# Labels que NO queremos importar jamás
BANNED_LABEL_PREFIXES = (
    "precio",
    "price",
)

# Para el caso "subcategoría == marca" (p.ej. Xiaomi > Xiaomi)
IGNORE_IF_EQUAL_PARENT = True


def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return float(str(v).strip())
    except Exception:
        return default


OVERWRITE = env_bool("SMARTGSM_OVERWRITE", default=False)
SLEEP_SECONDS = env_float("SMARTGSM_SLEEP", default=0.8)

WP_URL = os.getenv("WP_URL", "").strip().rstrip("/")
WP_KEY = os.getenv("WP_KEY", "").strip()
WP_SECRET = os.getenv("WP_SECRET", "").strip()

if not WP_URL or not WP_KEY or not WP_SECRET:
    print("❌ Faltan variables de entorno WP_URL / WP_KEY / WP_SECRET")
    sys.exit(1)


# ------------------------------- Woo API -----------------------------------

class Woo:
    def __init__(self, base_url: str, key: str, secret: str, timeout: int = 40):
        self.base_url = base_url.rstrip("/")
        self.key = key
        self.secret = secret
        self.timeout = timeout

    def _url(self, path: str) -> str:
        return f"{self.base_url}/wp-json/wc/v3{path}"

    def get(self, path: str, params: Optional[dict] = None) -> requests.Response:
        params = params or {}
        params.update({"consumer_key": self.key, "consumer_secret": self.secret})
        return requests.get(
            self._url(path),
            params=params,
            timeout=self.timeout,
            headers={"User-Agent": USER_AGENT},
        )

    def put(self, path: str, json_payload: dict) -> requests.Response:
        params = {"consumer_key": self.key, "consumer_secret": self.secret}
        return requests.put(
            self._url(path),
            params=params,
            json=json_payload,
            timeout=self.timeout,
            headers={"User-Agent": USER_AGENT},
        )


def woocommerce_get_all_categories(woo: Woo) -> List[dict]:
    """Devuelve TODAS las categorías de productos de Woo (incluyendo vacías)."""
    all_items: List[dict] = []
    page = 1
    per_page = 100

    while True:
        r = woo.get("/products/categories", params={"per_page": per_page, "page": page, "hide_empty": False})
        if r.status_code != 200:
            raise RuntimeError(f"Woo GET categories error {r.status_code}: {r.text[:200]}")
        batch = r.json()
        if not batch:
            break
        all_items.extend(batch)
        if len(batch) < per_page:
            break
        page += 1

    return all_items


def woocommerce_update_category_description(woo: Woo, term_id: int, new_desc_html: str) -> None:
    r = woo.put(f"/products/categories/{term_id}", json_payload={"description": new_desc_html})
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Woo PUT term {term_id} error {r.status_code}: {r.text[:250]}")


# ----------------------------- Smart-GSM -----------------------------------

def http_get(url: str) -> Optional[str]:
    try:
        r = requests.get(
            url,
            headers={"User-Agent": USER_AGENT},
            timeout=40,
        )
        if r.status_code != 200:
            return None
        return r.text
    except Exception:
        return None


def extract_ficha_tecnica(html_text: str) -> Dict[str, str]:
    """Extrae la tabla de 'Ficha técnica' y devuelve dict label->value."""
    soup = BeautifulSoup(html_text, "html.parser")

    # Normalmente está en un <h2>Ficha técnica</h2> seguido de una tabla
    # pero para robustez buscamos cualquier <h2> que contenga "Ficha técnica".
    h2 = None
    for tag in soup.find_all(["h2", "h3"]):
        t = tag.get_text(" ", strip=True).lower()
        if "ficha" in t and "técnica" in t:
            h2 = tag
            break

    table = None
    if h2 is not None:
        # buscar tabla cercana
        nxt = h2.find_next("table")
        if nxt is not None:
            table = nxt

    if table is None:
        # fallback: primera tabla con "table-striped" o "table"
        table = soup.find("table", class_=re.compile(r"table"))

    if table is None:
        return {}

    specs: Dict[str, str] = {}
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue

        # Label: prioriza <strong>
        strong = tds[0].find("strong")
        if strong:
            label = strong.get_text(" ", strip=True)
        else:
            label = tds[0].get_text(" ", strip=True)

        value = tds[1].get_text(" ", strip=True)

        if not label or not value:
            continue

        # Limpieza
        label_clean = re.sub(r"\s+", " ", label).strip()
        value_clean = re.sub(r"\s+", " ", value).strip()

        # No importar precio
        l_low = label_clean.lower()
        if any(l_low.startswith(pfx) for pfx in BANNED_LABEL_PREFIXES):
            continue

        specs[label_clean] = value_clean

    return specs


def build_specs_html_table(specs: Dict[str, str]) -> str:
    """Genera HTML estable y legible en Woo.

    En descripciones de taxonomías, algunos filtros de WP/themes pueden
    aplanar listas (<ul>/<li>) y acabar concatenando el texto. Un bloque
    <p> con <br> suele renderizarse de forma consistente.
    """
    if not specs:
        return ""

    lines = []
    for k, v in specs.items():
        k_esc = html.escape(k)
        v_esc = html.escape(v)
        lines.append(f"<strong>{k_esc}</strong>: {v_esc}")
    body = '<br/>'.join(lines)
    return (
        '<div class="smartgsm-specs">'
        '<h2>Ficha técnica</h2>'
        f'<p>{body}</p>'
        '</div>'
    )

def normalize_slug(s: str) -> str:
    s = s.strip().lower()
    s = s.replace("_", "-")
    s = _slug_re_non_alnum.sub("-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s


def strip_network_suffix(slug: str) -> List[str]:
    """Devuelve [slug, slug_sin_-5g/-4g] si aplica."""
    out = [slug]
    if slug.endswith("-5g"):
        out.append(slug[:-3])
    if slug.endswith("-4g"):
        out.append(slug[:-3])
    # dedupe manteniendo orden
    seen = set()
    res = []
    for x in out:
        if x and x not in seen:
            seen.add(x)
            res.append(x)
    return res


def add_network_suffixes(slug: str) -> List[str]:
    """Si no tiene -5g/-4g, añade variantes con esos sufijos."""
    if slug.endswith("-5g") or slug.endswith("-4g"):
        return [slug]
    return [slug, f"{slug}-5g", f"{slug}-4g"]


def fix_oppo_reno_hyphen(slug: str) -> List[str]:
    """Smart-GSM usa 'oppo-reno-12-...' en vez de 'oppo-reno12-...'."""
    out = [slug]
    m = re.match(r"^(oppo-)reno(\d)(.+)$", slug)
    if m:
        out.append(f"{m.group(1)}reno-{m.group(2)}{m.group(3)}")

    # Caso sin prefijo oppo en el slug, pero el nombre indica reno
    m2 = re.match(r"^reno(\d)(.+)$", slug)
    if m2:
        out.append(f"reno-{m2.group(1)}{m2.group(2)}")

    # dedupe
    seen = set()
    res = []
    for x in out:
        if x not in seen:
            seen.add(x)
            res.append(x)
    return res

def base_without_network(slug: str) -> str:
    """Devuelve el slug sin sufijo final -5g / -4g (si lo tuviera)."""
    return re.sub(r"-(5g|4g)$", "", slug.strip().lower())


def fix_special_hyphens(slug: str) -> List[str]:
    """Genera variantes de slug corrigiendo patrones habituales de Smart-GSM.

    Casos cubiertos (ejemplos):
      - Samsung Galaxy Z Flip6 -> samsung-galaxy-z-flip-6
      - Samsung Galaxy Z Fold7 -> samsung-galaxy-z-fold-7
      - Honor Magic7 Lite -> honor-magic-7-lite
      - Nubia Redmagic -> nubia-red-magic-...
      - Oppo Reno12 -> oppo-reno-12
      - GT8 -> gt-8

    Devuelve la lista (sin duplicados, mismo orden de descubrimiento).
    """

    slug = (slug or "").strip("-")
    res: List[str] = []

    def add(s: str):
        s = (s or "").strip("-")
        if s and s not in res:
            res.append(s)

    add(slug)

    # 1) Insertar guiones entre letras y números cuando vienen pegados
    #    Ej: flip6 -> flip-6, fold7 -> fold-7, gt8 -> gt-8
    add(re.sub(r"([a-zA-Z])([0-9])", r"\1-\2", slug))

    # 2) Casos tipo "reno12" -> "reno-12" (y también "reno13" ...)
    add(re.sub(r"(reno)([0-9])", r"\1-\2", slug, flags=re.I))

    # 3) Honor Magic7 -> Honor Magic-7
    #    (slug normalizado suele empezar por honor-magic...)
    add(re.sub(r"(honor-magic)([0-9]+)", r"\1-\2", slug, flags=re.I))

    # 4) Nubia Redmagic -> red-magic
    add(re.sub(r"redmagic", "red-magic", slug, flags=re.I))

    # 5) Quitar dobles guiones por si acaso
    cleaned: List[str] = []
    for s in res:
        s2 = re.sub(r"-+", "-", s)
        if s2 and s2 not in cleaned:
            cleaned.append(s2)

    return cleaned


def candidate_slugs(term_slug: str, term_name: str, parent_slug: str) -> List[str]:
    """Genera una lista ordenada de slugs (sin dominio) para buscar en Smart-GSM.

    Objetivo: maximizar hit-rate sin hardcodear demasiadas excepciones.
    """
    parent = normalize_slug(parent_slug)
    name_lc = (term_name or '').lower()

    base = normalize_slug(term_slug)
    bases = [base]

    # A veces Woo genera slugs con el parent añadido al final (ej: ...-redmi)
    suffix = f"-{parent}"
    if parent and base.endswith(suffix) and base != parent:
        bases.append(base[: -len(suffix)])

    # Si el nombre contiene '+', Smart-GSM suele usar 'plus'
    if '+' in name_lc or ' pro+' in name_lc or 'pro +' in name_lc:
        extra = []
        for b in bases:
            if 'plus' in b:
                continue
            # caso típico: ...-pro  -> ...-pro-plus
            b2 = re.sub(r"-pro(?=-|$)", "-pro-plus", b)
            if b2 != b:
                extra.append(b2)
            else:
                # fallback: añadir -plus al final
                extra.append(b + '-plus')
        bases.extend(extra)

    # 1) base sin variantes de red
    slugs: List[str] = []
    for b in bases:
        slugs.append(strip_network_suffix(b))

    # 2) variantes de hyphenation (flip6 -> flip-6, gt8 -> gt-8, honor-magic7 -> honor-magic-7, redmagic -> red-magic, ...)
    expanded: List[str] = []
    for s in slugs:
        expanded.append(s)
        expanded.extend(fix_special_hyphens(s))
    slugs = expanded

    # 3) prefijos de fabricante que Smart-GSM usa diferente
    prefixed: List[str] = []

    # a) Xiaomi antepone 'xiaomi-' para Poco/Redmi (y muchos POCO realmente son 'xiaomi-poco-...')
    if parent in {'poco', 'redmi'}:
        for s in slugs:
            if not s.startswith('xiaomi-'):
                prefixed.append(f"xiaomi-{s}")

        # POCO: algunos slugs vienen sin 'poco-' (ej: f8-pro), y Smart-GSM usa xiaomi-poco-...
        if parent == 'poco':
            for s in slugs:
                s0 = s[6:] if s.startswith('xiaomi-') else s
                if not s0.startswith('poco-'):
                    prefixed.append(f"xiaomi-poco-{s0}")

            # Excepción conocida: "Poco F8 Pro" está como xiaomi-poco-f8-5g (se han olvidado del 'pro')
            if 'f8-pro' in bases or term_slug.strip().lower() == 'f8-pro' or 'poco f8 pro' in term_name.lower():
                prefixed.extend(['xiaomi-poco-f8-5g', 'xiaomi-poco-f8'])

    # b) Realme aparece en Smart-GSM bajo "oppo-..."
    if parent == 'realme':
        for s in slugs:
            if not s.startswith('oppo-'):
                prefixed.append(f"oppo-{s}")

    # c) Nubia aparece bajo "zte-nubia-..."
    if parent == 'nubia':
        for s in slugs:
            if not s.startswith('zte-'):
                prefixed.append(f"zte-{s}")

    # d) Samsung: a veces falta 'galaxy' (ej: samsung-s25-ultra -> samsung-galaxy-s25-ultra)
    if parent == 'samsung':
        for s in slugs:
            if s.startswith('samsung-') and not s.startswith('samsung-galaxy-'):
                prefixed.append('samsung-galaxy-' + s[len('samsung-'):])

    # e) Vivo IQOO: se mezcla iqoo/iQOO
    if parent == 'vivo':
        for s in slugs:
            if 'iqoo' in s and not s.startswith('vivo-iqoo'):
                prefixed.append('vivo-' + s)

    # Re-expand por si los prefijos introducen nuevos patrones
    for s in list(prefixed):
        prefixed.extend(fix_special_hyphens(s))

    # 4) añadir sufijos de red
    all_slugs = add_network_suffixes(slugs + prefixed, term_name)

    # 5) dedupe preservando orden
    seen = set()
    res: List[str] = []
    for s in all_slugs:
        if not s or s in seen:
            continue
        seen.add(s)
        res.append(s)
    return res

def build_smartgsm_url(slug: str) -> str:
    return f"{SMARTGSM_BASE}/{slug}"


def fetch_specs_for_candidates(slugs: List[str]) -> Tuple[Optional[str], Dict[str, str], List[str]]:
    """Intenta varios slugs hasta que encuentra ficha válida.

    Returns:
        (url_encontrada, specs_dict, slugs_probados)
    """
    tried: List[str] = []
    for s in slugs:
        tried.append(s)
        url = build_smartgsm_url(s)
        html_text = http_get(url)
        if not html_text:
            time.sleep(SLEEP_SECONDS)
            continue

        specs = extract_ficha_tecnica(html_text)
        # Consideramos válido si hay al menos 4 campos (evita falsas coincidencias)
        if len(specs) >= 4:
            return url, specs, tried

        time.sleep(SLEEP_SECONDS)

    return None, {}, tried


# ----------------------------- Main logic ----------------------------------


def is_tablet(name: str) -> bool:
    n = name.upper()
    return any(tok in n for tok in TABLET_TOKENS)


def build_parent_map(categories: List[dict]) -> Dict[int, dict]:
    return {int(c["id"]): c for c in categories}


def main() -> int:
    woo = Woo(WP_URL, WP_KEY, WP_SECRET)

    hoy_fmt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print("============================================================")
    print(f"📡 SMART-GSM → Woo (Subcategorías) ({hoy_fmt})")
    print("============================================================")
    print(f"Overwrite descripción existente: {OVERWRITE}")
    print(f"Pausa entre requests: {SLEEP_SECONDS}s")
    print(f"Base Smart-GSM: {SMARTGSM_BASE}")
    print("============================================================")

    categories = woocommerce_get_all_categories(woo)
    parent_map = build_parent_map(categories)

    # Subcategorías = categorías con parent != 0
    subcats = [c for c in categories if int(c.get("parent") or 0) != 0]
    print(f"📦 Subcategorías detectadas: {len(subcats)}")

    summary_actualizadas: List[dict] = []
    summary_no_encontradas: List[dict] = []
    summary_ignoradas: List[dict] = []
    summary_errores: List[dict] = []

    for term in subcats:
        term_id = int(term["id"])
        name = (term.get("name") or "").strip()
        slug = (term.get("slug") or "").strip()
        parent_id = int(term.get("parent") or 0)
        parent_term = parent_map.get(parent_id, {})
        parent_slug = (parent_term.get("slug") or "").strip()
        parent_name = (parent_term.get("name") or "").strip()

        # Ignorar tablets
        if is_tablet(name):
            summary_ignoradas.append({"nombre": name, "id": term_id, "motivo": "tablet"})
            continue

        # Ignorar subcategorías que son igual a la marca (p.ej. Xiaomi > Xiaomi)
        if IGNORE_IF_EQUAL_PARENT:
            if normalize_slug(name) == normalize_slug(parent_name) or normalize_slug(slug) == normalize_slug(parent_slug):
                summary_ignoradas.append({"nombre": name, "id": term_id, "motivo": "subcategoría == marca"})
                continue

        current_desc = (term.get("description") or "").strip()
        if current_desc and not OVERWRITE:
            # ya hay descripción, no sobreescribimos
            continue

        print("------------------------------------------------------------")
        print(f"📁 Subcategoría: {name} (ID: {term_id})")
        print(f"   slug: {slug} | parent_slug: {parent_slug}")

        cands = candidate_slugs(slug, name, parent_slug)

        url, specs, tried = fetch_specs_for_candidates(cands)

        if not url:
            print(f"   ❌ NO ENCONTRADA ficha en Smart-GSM con slugs: {tried[:8]}{' ...' if len(tried) > 8 else ''}")
            summary_no_encontradas.append({"nombre": name, "id": term_id, "slug": slug, "slugs_probados": tried})
            continue

        print(f"   ✅ Ficha encontrada: {url}")
        print(f"   🔎 Campos extraídos: {len(specs)}")

        # Log de algunos campos (sin saturar)
        shown = 0
        for k, v in specs.items():
            if shown >= 10:
                break
            print(f"      - {k}: {v}")
            shown += 1

        new_html = build_specs_html_table(specs)
        if not new_html:
            print("   ⚠️ No se generó HTML (sin datos).")
            summary_no_encontradas.append({"nombre": name, "id": term_id, "slug": slug, "slugs_probados": tried})
            continue

        try:
            woocommerce_update_category_description(woo, term_id, new_html)
            print("   💾 DESCRIPCIÓN actualizada en Woo ✅")
            summary_actualizadas.append({"nombre": name, "id": term_id, "campos": len(specs), "url": url})
        except Exception as e:
            print(f"   ❌ ERROR actualizando en Woo: {e}")
            summary_errores.append({"nombre": name, "id": term_id, "error": str(e)})

        time.sleep(SLEEP_SECONDS)

    # Resumen
    hoy_fmt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("\n============================================================")
    print(f"📋 RESUMEN DE EJECUCIÓN ({hoy_fmt})")
    print("============================================================")

    print(f"a) SUBCATEGORÍAS ACTUALIZADAS: {len(summary_actualizadas)}")
    for item in summary_actualizadas[:300]:
        print(f"- {item['nombre']} (ID: {item['id']}): {item['campos']} campos")

    print(f"b) SUBCATEGORÍAS NO ENCONTRADAS EN SMART-GSM: {len(summary_no_encontradas)}")
    for item in summary_no_encontradas[:300]:
        print(f"- {item['nombre']} (ID: {item['id']}) slug='{item['slug']}'")

    print(f"c) SUBCATEGORÍAS IGNORADAS: {len(summary_ignoradas)}")
    for item in summary_ignoradas[:300]:
        print(f"- {item['nombre']} (ID: {item['id']}): {item['motivo']}")

    print(f"d) ERRORES ACTUALIZANDO EN WOO: {len(summary_errores)}")
    for item in summary_errores[:50]:
        print(f"- {item['nombre']} (ID: {item['id']}): {item['error']}")

    print("============================================================")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
