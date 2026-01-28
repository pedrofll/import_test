#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
smartgsm_specs.py
-----------------
Importa automáticamente la "Ficha técnica" desde:
  https://www.smart-gsm.com/moviles/<slug>

Y la guarda como DESCRIPCIÓN en tus subcategorías (product_cat) de WooCommerce.

✅ Recorre TODAS tus subcategorías (categorías con parent != 0)
✅ Usa el SLUG REAL de WooCommerce para construir el slug de Smart-GSM
✅ Parseo robusto de la tabla "Ficha técnica" (table.table-striped)
✅ Importa TODAS las filas de la ficha (no solo 4)
✅ Logs + resumen final

Requisitos:
  pip install requests beautifulsoup4 unidecode woocommerce

Variables de entorno (igual que tus scrapers):
  WP_URL
  WP_KEY
  WP_SECRET

Opcional:
  SMARTGSM_OVERWRITE=1  -> sobreescribe descripciones existentes (por defecto: SI)
  SMARTGSM_SLEEP=0.8    -> pausa entre requests a Smart-GSM
"""

import os
import re
import time
from datetime import datetime
from typing import List, Tuple, Optional

import requests
from bs4 import BeautifulSoup
from unidecode import unidecode
from woocommerce import API


SMARTGSM_BASE = "https://www.smart-gsm.com/moviles"

# =========================
# CONFIG
# =========================
OVERWRITE_EXISTING_DESCRIPTION = os.environ.get("SMARTGSM_OVERWRITE", "1").strip() not in ("0", "false", "False")
SLEEP_BETWEEN_REQUESTS_SEC = float(os.environ.get("SMARTGSM_SLEEP", "0.8").strip() or "0.8")

HTTP_RETRIES = 3
HTTP_RETRY_SLEEP_SEC = 2.0

# Evitar tablets
EXCLUDE_WORDS = {"tab", "ipad", "pad"}

# Normalización de labels (opcional). Si no está, se deja tal cual (Title Case)
KEY_MAP = {
    "pantalla": "Pantalla",
    "procesador": "Procesador",
    "memoria ram": "Memoria RAM",
    "almacenamiento": "Almacenamiento",
    "expansion": "Expansión",
    "expansión": "Expansión",
    "cámara": "Cámara",
    "camara": "Cámara",
    "batería": "Batería",
    "bateria": "Batería",
    "os": "OS",
    "perfil": "Perfil",
    "peso": "Peso",
}

# =========================
# WooCommerce API
# =========================
wcapi = API(
    url=os.environ["WP_URL"],
    consumer_key=os.environ["WP_KEY"],
    consumer_secret=os.environ["WP_SECRET"],
    version="wc/v3",
    timeout=60
)

# =========================
# Summaries
# =========================
summary_actualizadas = []
summary_ignoradas = []
summary_no_encontradas = []
summary_error = []


# =========================
# Helpers
# =========================
def normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def is_tablet_term(name: str, slug: str) -> bool:
    n = unidecode(name or "").lower()
    s = unidecode(slug or "").lower()
    for w in EXCLUDE_WORDS:
        if w in n or w in s:
            return True
    return False


def request_with_retries(session: requests.Session, url: str) -> Optional[requests.Response]:
    last_err = None
    for i in range(1, HTTP_RETRIES + 1):
        try:
            r = session.get(url, timeout=25)
            return r
        except Exception as e:
            last_err = e
            if i < HTTP_RETRIES:
                time.sleep(HTTP_RETRY_SLEEP_SEC)
    print(f"❌ ERROR HTTP GET {url} -> {last_err}", flush=True)
    return None


def normalize_label(label: str) -> str:
    raw = normalize_spaces(label)
    if not raw:
        return ""
    k = unidecode(raw).lower()
    k = k.replace(":", "").strip()
    return KEY_MAP.get(k, raw[:1].upper() + raw[1:])


def fetch_smartgsm_specs_ordered(url: str, session: requests.Session) -> List[Tuple[str, str]]:
    """
    Devuelve lista ordenada [(label, value), ...] según la tabla "Ficha técnica".
    """
    r = request_with_retries(session, url)
    if not r:
        return []
    if r.status_code != 200:
        return []

    soup = BeautifulSoup(r.text, "html.parser")

    # Buscar el H2 "Ficha técnica" y luego su tabla
    h2s = soup.find_all(["h2", "h3"])
    ficha_anchor = None
    for h in h2s:
        t = normalize_spaces(h.get_text(" ", strip=True)).lower()
        if "ficha" in t and "técnica" in t:
            ficha_anchor = h
            break

    table = None
    if ficha_anchor:
        # La tabla suele venir cerca debajo del h2
        table = ficha_anchor.find_next("table", class_=re.compile(r"table-striped", re.I))

    # fallback por si el HTML cambia
    if not table:
        table = soup.select_one("table.table.table-striped") or soup.select_one("table.table-striped")

    if not table:
        return []

    rows = table.select("tbody tr") or table.select("tr")
    out: List[Tuple[str, str]] = []

    for tr in rows:
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue

        left = tds[0]
        right = tds[1]

        # label suele estar en <strong>
        strong = left.find("strong")
        label = strong.get_text(" ", strip=True) if strong else left.get_text(" ", strip=True)
        value = right.get_text(" ", strip=True)

        label = normalize_label(label)
        value = normalize_spaces(value)

        if label and value:
            out.append((label, value))

    return out


def build_description(specs: List[Tuple[str, str]]) -> str:
    """
    Formato texto simple (para campo descripción del término):
      Pantalla: ...
      Procesador: ...
      ...
    """
    lines = [f"{k}: {v}" for k, v in specs]
    return "\n".join(lines).strip()


def wc_get_all_categories() -> List[dict]:
    cats = []
    page = 1
    while True:
        res = wcapi.get("products/categories", params={"per_page": 100, "page": page}).json()
        if not res or isinstance(res, dict) and res.get("message"):
            break
        cats.extend(res)
        if len(res) < 100:
            break
        page += 1
    return cats


def wc_update_category_description(cat_id: int, new_desc: str) -> bool:
    # Reintentos suaves (igual que haces en productos)
    for attempt in range(1, 6):
        try:
            r = wcapi.put(f"products/categories/{cat_id}", {"description": new_desc})
            if r.status_code in (200, 201):
                return True
        except Exception:
            pass
        time.sleep(1.5 * attempt)
    return False


def build_candidate_slugs(term_slug: str, parent_slug: str) -> List[str]:
    """
    Principal: term_slug (tu caso real: ya es xiaomi-15t-pro, etc.)
    Fallbacks por si algún término no lleva la marca en slug.
    """
    term_slug = (term_slug or "").strip().lower()
    parent_slug = (parent_slug or "").strip().lower()

    cands = []
    if term_slug:
        cands.append(term_slug)

    # Si por cualquier motivo un término no incluye la marca (p.ej. "15t-pro"),
    # probamos parent-term
    if parent_slug and term_slug and not term_slug.startswith(parent_slug + "-"):
        cands.append(f"{parent_slug}-{term_slug}")

    # Evitar xiaomi-xiaomi-...
    if parent_slug and term_slug.startswith(parent_slug + "-" + parent_slug + "-"):
        cands.append(term_slug.replace(parent_slug + "-" + parent_slug + "-", parent_slug + "-", 1))

    # Dedup
    out = []
    seen = set()
    for c in cands:
        if c and c not in seen:
            out.append(c)
            seen.add(c)
    return out


# =========================
# Main
# =========================
def main():
    hoy_fmt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n============================================================")
    print(f"📡 SMART-GSM → Woo (Subcategorías) ({hoy_fmt})")
    print(f"============================================================")
    print(f"Overwrite descripción existente: {OVERWRITE_EXISTING_DESCRIPTION}")
    print(f"Pausa entre requests: {SLEEP_BETWEEN_REQUESTS_SEC}s")
    print(f"Base Smart-GSM: {SMARTGSM_BASE}")
    print(f"============================================================\n")

    cats = wc_get_all_categories()

    # Index por id para sacar slug del padre
    by_id = {c.get("id"): c for c in cats if isinstance(c, dict)}

    # Solo subcategorías (parent != 0)
    subs = [c for c in cats if isinstance(c, dict) and int(c.get("parent") or 0) != 0]

    print(f"📦 Subcategorías detectadas: {len(subs)}\n")

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    })

    for term in subs:
        term_id = int(term.get("id"))
        term_name = term.get("name") or ""
        term_slug = (term.get("slug") or "").strip().lower()
        term_desc = (term.get("description") or "").strip()

        parent_id = int(term.get("parent") or 0)
        parent_slug = (by_id.get(parent_id) or {}).get("slug") or ""

        print("-" * 60)
        print(f"📁 Subcategoría: {term_name} (ID: {term_id})")
        print(f"   slug: {term_slug} | parent_slug: {parent_slug}")

        # Tablets fuera
        if is_tablet_term(term_name, term_slug):
            print("   ⏭️  IGNORADA (parece tablet: TAB/IPAD/PAD).")
            summary_ignoradas.append({"nombre": term_name, "id": term_id, "razon": "tablet"})
            continue

        # Si no quieres pisar lo que metes a mano
        if (not OVERWRITE_EXISTING_DESCRIPTION) and term_desc:
            print("   ⏭️  IGNORADA (ya tiene descripción y overwrite=0).")
            summary_ignoradas.append({"nombre": term_name, "id": term_id, "razon": "ya tiene descripción"})
            continue

        candidates = build_candidate_slugs(term_slug, parent_slug)

        found_specs = []
        used_url = ""

        for cand in candidates:
            url = f"{SMARTGSM_BASE}/{cand}"
            specs = fetch_smartgsm_specs_ordered(url, session)
            if specs:
                found_specs = specs
                used_url = url
                break

        if not found_specs:
            print(f"   ❌ NO ENCONTRADA ficha en Smart-GSM con slugs: {candidates}")
            summary_no_encontradas.append({"nombre": term_name, "id": term_id, "slug": term_slug})
            time.sleep(SLEEP_BETWEEN_REQUESTS_SEC)
            continue

        desc = build_description(found_specs)

        # Log resumen de lo extraído
        print(f"   ✅ Ficha encontrada: {used_url}")
        print(f"   🔎 Campos extraídos: {len(found_specs)}")
        # muestra 6 primeras líneas
        preview = desc.splitlines()[:6]
        for ln in preview:
            print(f"      - {ln}")

        ok = wc_update_category_description(term_id, desc)
        if ok:
            summary_actualizadas.append({"nombre": term_name, "id": term_id, "url": used_url, "campos": len(found_specs)})
            print("   💾 DESCRIPCIÓN actualizada en Woo ✅")
        else:
            summary_error.append({"nombre": term_name, "id": term_id, "url": used_url})
            print("   ❌ ERROR actualizando descripción en Woo")

        time.sleep(SLEEP_BETWEEN_REQUESTS_SEC)

    # =========================
    # Resumen final
    # =========================
    hoy_fmt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n============================================================")
    print(f"📋 RESUMEN DE EJECUCIÓN ({hoy_fmt})")
    print(f"============================================================")
    print(f"a) SUBCATEGORÍAS ACTUALIZADAS: {len(summary_actualizadas)}")
    for it in summary_actualizadas:
        print(f"- {it['nombre']} (ID: {it['id']}): {it['campos']} campos")

    print(f"\nb) SUBCATEGORÍAS NO ENCONTRADAS EN SMART-GSM: {len(summary_no_encontradas)}")
    for it in summary_no_encontradas:
        print(f"- {it['nombre']} (ID: {it['id']}) slug='{it.get('slug','')}'")

    print(f"\nc) SUBCATEGORÍAS IGNORADAS: {len(summary_ignoradas)}")
    for it in summary_ignoradas:
        print(f"- {it['nombre']} (ID: {it['id']}): {it.get('razon','')}")

    print(f"\nd) ERRORES ACTUALIZANDO EN WOO: {len(summary_error)}")
    for it in summary_error:
        print(f"- {it['nombre']} (ID: {it['id']})")

    print(f"============================================================\n")


if __name__ == "__main__":
    main()
