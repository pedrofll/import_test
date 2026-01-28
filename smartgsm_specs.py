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
    """Genera HTML estable y legible en Woo."""
    if not specs:
        return ""

    # Escapar valores por seguridad (labels también)
    rows = []
    for k, v in specs.items():
        k_esc = html.escape(k)
        v_esc = html.escape(v)
        rows.append(
            f"<tr><td class=\"text-nowrap\"><strong>{k_esc}</strong></td><td>{v_esc}</td></tr>"
        )

    rows_html = "\n".join(rows)

    return (
        "<h2>Ficha técnica</h2>\n"
        "<table class=\"table table-striped smartgsm-specs\">\n"
        "<tbody>\n"
        f"{rows_html}\n"
        "</tbody>\n"
        "</table>\n"
    )


# ------------------------------ Slug utils ---------------------------------

_slug_re_non_alnum = re.compile(r"[^a-z0-9\-]+")


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




def fix_honor_magic_number_hyphen(slug: str) -> List[str]:
    # Smart-GSM suele usar '...magic-7-...' en lugar de '...magic7-...'
    out = [slug]
    if 'magic' in slug:
        out.append(re.sub(r"(?<!-)magic(\d)", r"magic-\1", slug))
    return unique_list(out)


def fix_samsung_slug_variants(slug: str) -> List[str]:
    out = [slug]

    # Algunos slugs internos usan 'samsung-s25-ultra' pero Smart-GSM usa 'samsung-galaxy-s25-ultra'
    if slug.startswith('samsung-s'):
        out.append(slug.replace('samsung-s', 'samsung-galaxy-s', 1))

    # Z Flip / Z Fold: Smart-GSM usa '...z-flip-6' en lugar de '...z-flip6'
    out.append(re.sub(r"(samsung-galaxy-z-(?:flip|fold))(\d)", r"\1-\2", slug))

    # Aplica también al variante galaxy-s si se generó
    if out[-1].startswith('samsung-galaxy-s'):
        out.append(re.sub(r"(samsung-galaxy-z-(?:flip|fold))(\d)", r"\1-\2", out[-1]))

    return unique_list(out)


def fix_realme_gt_number_hyphen(slug: str) -> List[str]:
    # Smart-GSM usa '...realme-gt-8...' en lugar de '...realme-gt8...'
    out = [slug]
    out.append(re.sub(r"(realme-gt)(\d)", r"\1-\2", slug))
    out.append(re.sub(r"(oppo-realme-gt)(\d)", r"\1-\2", slug))
    return unique_list(out)
def candidate_slugs(term_slug: str, term_name: str, parent_slug: str) -> List[str]:
    base = normalize_slug(term_slug)
    parent = normalize_slug(parent_slug)
    name = normalize_slug(term_name)

    slugs: List[str] = []

    # 1) base sin sufijos de red
    for s in strip_network_suffix(base):
        slugs.append(s)

    # 2) caso OPPO Reno (Smart-GSM usa 'reno-12', no 'reno12')
    #    Nota: fix_oppo_reno_hyphen trabaja con un slug (str). Aquí lo aplicamos a toda la lista.
    tmp: List[str] = []
    for s in slugs:
        tmp.extend(fix_oppo_reno_hyphen(s))
    slugs = unique_list(tmp)

    # 3) normalizaciones por marca/modelo
    if parent == 'honor' or any(s.startswith('honor-magic') for s in slugs):
        tmp: List[str] = []
        for s in slugs:
            tmp.extend(fix_honor_magic_number_hyphen(s))
        slugs = unique_list(tmp)

    if parent == 'samsung' or any('samsung-galaxy-z-' in s for s in slugs) or any(s.startswith('samsung-s') for s in slugs):
        tmp = []
        for s in slugs:
            tmp.extend(fix_samsung_slug_variants(s))
        slugs = unique_list(tmp)

    if parent == 'realme' or any('realme-gt' in s for s in slugs) or any('oppo-realme-gt' in s for s in slugs):
        tmp = []
        for s in slugs:
            tmp.extend(fix_realme_gt_number_hyphen(s))
        slugs = unique_list(tmp)

    # 4) prefijos que Smart-GSM usa en algunas marcas
    prefixed: List[str] = []
    if parent in {'poco', 'redmi'}:
        prefixed.extend([f"xiaomi-{s}" for s in slugs])
    if parent == 'realme':
        prefixed.extend([f"oppo-{s}" for s in slugs])
    if parent == 'nubia':
        # Smart-GSM lista Nubia bajo ZTE
        prefixed.extend([f"zte-{s}" for s in slugs])

    slugs.extend(prefixed)

    # Reaplicar algunas normalizaciones sobre variantes prefijadas
    if parent == 'realme':
        tmp = []
        for s in slugs:
            tmp.extend(fix_realme_gt_number_hyphen(s))
        slugs = unique_list(tmp)

    # 5) añadir sufijos de red
    with_net = []
    for s in slugs:
        with_net.extend(add_network_suffixes(s))
    slugs = unique_list(with_net)

    # 6) Samsung FE suele llevar -5g
    if parent == 'samsung' and 'fe' in base:
        slugs.append(f"{strip_network_suffix(base)[0]}-5g")

    # 7) Si el nombre ya contiene 4G/5G, intentar ambas variantes
    if re.search(r"\b5g\b", term_name, re.IGNORECASE):
        slugs.append(f"{strip_network_suffix(base)[0]}-5g")
    if re.search(r"\b4g\b", term_name, re.IGNORECASE):
        slugs.append(f"{strip_network_suffix(base)[0]}-4g")

    # 8) Excepción: POCO F* Pro donde Smart-GSM a veces omite 'pro' en la URL
    if parent == 'poco':
        m = re.match(r"^f(\d+)-pro$", base)
        if base == 'f8-pro' or m:
            num = '8' if base == 'f8-pro' else m.group(1)
            slugs.extend([
                f"xiaomi-poco-f{num}",
                f"xiaomi-poco-f{num}-5g",
                f"xiaomi-poco-f{num}-4g",
            ])

    # 9) dedupe final
    res = unique_list(slugs)
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
