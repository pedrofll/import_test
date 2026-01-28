#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
smartgsm_specs.py
-----------------
Actualiza la DESCRIPCIÓN de las subcategorías (product_cat hijas) en WooCommerce
importando la "Ficha técnica" desde https://www.smart-gsm.com/moviles/<slug>

- Recorre todas las categorías de Woo (taxonomía product_cat).
- Solo procesa SUBCATEGORÍAS (parent != 0).
- Si detecta tablets (TAB / IPAD) -> ignora.
- Si no encuentra la ficha con el slug normal, prueba variantes:
    - quitar "-5g" o "-4g" al FINAL
    - quitar "-5g" / "-4g" justo antes del sufijo de marca (p.ej. "-5g-xiaomi")
- Extrae todas las filas de la tabla "Ficha técnica" y genera HTML.
- Overwrite controlado por env SMARTGSM_OVERWRITE (1/0).
- Pausa entre requests controlada por env SMARTGSM_SLEEP (default 0.8).

ENV requeridas:
  WP_URL     -> https://tudominio.com
  WP_KEY     -> Woo REST consumer key
  WP_SECRET  -> Woo REST consumer secret

Opcionales:
  SMARTGSM_OVERWRITE=1
  SMARTGSM_SLEEP=0.8
"""

import os
import re
import sys
import time
import html
from datetime import datetime
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


SMARTGSM_BASE = "https://www.smart-gsm.com/moviles/"
UA = "Mozilla/5.0 (compatible; ofertasdemoviles-smartgsm/1.0; +https://ofertasdemoviles.com)"


# ----------------------------
# Helpers
# ----------------------------
def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name, "")
    if v == "":
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def env_float(name: str, default: float) -> float:
    v = os.getenv(name, "")
    if not v:
        return default
    try:
        return float(v)
    except Exception:
        return default


def is_tablet_term(name: str, slug: str) -> bool:
    s = f"{name} {slug}".lower()
    # reglas del proyecto: si contiene TAB o IPAD -> tablet -> NO
    return (" tab" in s) or ("ipad" in s) or s.startswith("tab") or ("-tab" in s)


def slugify(text: str) -> str:
    """
    Slugify simple (sin dependencias extra).
    """
    t = text.strip().lower()

    # normalizaciones típicas
    t = t.replace("&", " and ")
    t = t.replace("+", " plus ")
    t = t.replace(" pro+", " pro plus ")
    t = t.replace("  ", " ")

    # quitar paréntesis
    t = re.sub(r"[()]", " ", t)

    # reemplazar separadores
    t = re.sub(r"[\s_/]+", "-", t)

    # quitar caracteres no alfanuméricos salvo guión
    t = re.sub(r"[^a-z0-9\-]+", "", t)

    # compactar guiones
    t = re.sub(r"-{2,}", "-", t).strip("-")
    return t


def unique_keep_order(items):
    seen = set()
    out = []
    for x in items:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out


def smartgsm_slug_variants(slug: str, parent_slug: str | None) -> list[str]:
    """
    Genera variantes para intentar encontrar la ficha en Smart-GSM.

    Ejemplos:
      oppo-find-x3-neo-5g        -> oppo-find-x3-neo
      motorola-edge-50-neo-5g    -> motorola-edge-50-neo
      xiaomi-14t-5g              -> xiaomi-14t
      xiaomi-redmi-note-14-pro-5g-xiaomi -> xiaomi-redmi-note-14-pro-xiaomi
    """
    base = slug.strip().lower()
    variants = [base]

    # 1) quitar -5g o -4g al final
    variants.append(re.sub(r"-(5g|4g)$", "", base))

    # 2) quitar -5g / -4g antes del sufijo final (p.ej. "-5g-xiaomi")
    variants.append(re.sub(r"-(5g|4g)(?=-[a-z0-9]+$)", "", base))

    # 3) si conocemos el parent_slug, quitar -5g/-4g justo antes del parent_slug
    if parent_slug:
        ps = parent_slug.strip().lower()
        variants.append(re.sub(rf"-(5g|4g)-({re.escape(ps)})$", r"-\2", base))

    return unique_keep_order([v for v in variants if v and v != base or v == base])


def build_specs_html(title: str, specs: list[tuple[str, str]]) -> str:
    """
    Construye HTML limpio para usar como descripción en la subcategoría.
    """
    safe_title = html.escape(title)

    rows = []
    for k, v in specs:
        kk = html.escape(k)
        vv = html.escape(v)
        rows.append(
            f"<tr>"
            f"<td class='text-nowrap'><strong>{kk}</strong></td>"
            f"<td>{vv}</td>"
            f"</tr>"
        )

    table = (
        "<div class='row'>"
        "<div class='col-xs-12'>"
        "<h2>Ficha técnica</h2>"
        "<table class='table table-striped'>"
        "<tbody>"
        f"{''.join(rows)}"
        "</tbody>"
        "</table>"
        "</div>"
        "</div>"
    )

    # Si quieres añadir un pequeño encabezado/intro, lo dejamos mínimo:
    return f"{table}"


def extract_specs_from_smartgsm(html_text: str) -> list[tuple[str, str]]:
    """
    Extrae pares (campo, valor) de la sección 'Ficha técnica'.
    """
    soup = BeautifulSoup(html_text, "html.parser")

    # buscamos el h2 "Ficha técnica" y su tabla siguiente
    h2 = None
    for candidate in soup.find_all(["h1", "h2", "h3"]):
        if candidate.get_text(" ", strip=True).lower() == "ficha técnica":
            h2 = candidate
            break

    if not h2:
        return []

    table = h2.find_next("table")
    if not table:
        return []

    specs = []
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue

        # label: suele venir dentro de <strong> en la primera celda
        label = tds[0].get_text(" ", strip=True)
        value = tds[1].get_text(" ", strip=True)

        # limpieza extra
        label = re.sub(r"\s+", " ", label).strip()
        value = re.sub(r"\s+", " ", value).strip()

        if label and value:
            specs.append((label, value))

    # mantener orden y sin duplicados exactos
    out = []
    seen = set()
    for k, v in specs:
        key = (k.lower(), v.lower())
        if key not in seen:
            seen.add(key)
            out.append((k, v))
    return out


# ----------------------------
# Woo client
# ----------------------------
class WooClient:
    def __init__(self, base_url: str, ck: str, cs: str):
        self.base_url = base_url.rstrip("/")
        self.ck = ck
        self.cs = cs
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": UA})

    def _url(self, path: str) -> str:
        return urljoin(self.base_url + "/", path.lstrip("/"))

    def get_all_categories(self) -> list[dict]:
        """
        GET /wp-json/wc/v3/products/categories?per_page=100&page=N
        """
        out = []
        page = 1
        while True:
            url = self._url("/wp-json/wc/v3/products/categories")
            params = {"per_page": 100, "page": page, "consumer_key": self.ck, "consumer_secret": self.cs}
            r = self.session.get(url, params=params, timeout=30)
            if r.status_code != 200:
                raise RuntimeError(f"Error listando categorías (page={page}): {r.status_code} {r.text[:200]}")
            data = r.json()
            if not data:
                break
            out.extend(data)
            if len(data) < 100:
                break
            page += 1
        return out

    def update_category_description(self, cat_id: int, description_html: str) -> bool:
        url = self._url(f"/wp-json/wc/v3/products/categories/{cat_id}")
        payload = {"description": description_html}
        params = {"consumer_key": self.ck, "consumer_secret": self.cs}
        r = self.session.put(url, params=params, json=payload, timeout=30)
        return r.status_code in (200, 201)


# ----------------------------
# Main
# ----------------------------
def main():
    wp_url = os.getenv("WP_URL", "").strip()
    wp_key = os.getenv("WP_KEY", "").strip()
    wp_secret = os.getenv("WP_SECRET", "").strip()

    if not wp_url or not wp_key or not wp_secret:
        print("❌ Faltan variables de entorno: WP_URL, WP_KEY, WP_SECRET")
        sys.exit(1)

    overwrite = env_bool("SMARTGSM_OVERWRITE", default=False)
    sleep_s = env_float("SMARTGSM_SLEEP", default=0.8)

    woo = WooClient(wp_url, wp_key, wp_secret)

    hoy_fmt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"============================================================")
    print(f"📡 SMART-GSM → Woo (Subcategorías) ({hoy_fmt})")
    print(f"============================================================")
    print(f"Overwrite descripción existente: {overwrite}")
    print(f"Pausa entre requests: {sleep_s}s")
    print(f"Base Smart-GSM: {SMARTGSM_BASE}")
    print(f"============================================================")

    # summary
    updated = []      # [{name,id,count}]
    not_found = []    # [{name,id,slug}]
    ignored = []      # [{name,id,reason}]
    errors = []       # [{name,id,error}]

    # fetch categories
    try:
        cats = woo.get_all_categories()
    except Exception as e:
        print(f"❌ Error obteniendo categorías Woo: {e}")
        sys.exit(1)

    print(f"📦 Subcategorías detectadas: {len([c for c in cats if int(c.get('parent', 0)) != 0])}")

    # map id -> slug for parent lookup
    id_to_slug = {int(c["id"]): (c.get("slug") or "").strip().lower() for c in cats if "id" in c}

    # iterate
    for c in cats:
        try:
            cid = int(c.get("id", 0))
            name = (c.get("name") or "").strip()
            slug = (c.get("slug") or "").strip().lower()
            parent_id = int(c.get("parent", 0))
            desc = (c.get("description") or "").strip()

            # solo subcategorías
            if parent_id == 0:
                continue

            parent_slug = id_to_slug.get(parent_id, "").strip().lower() or None

            print("------------------------------------------------------------")
            print(f"📁 Subcategoría: {name} (ID: {cid})")
            print(f"   slug: {slug} | parent_slug: {parent_slug or ''}")

            # tablets -> ignorar
            if is_tablet_term(name, slug):
                print("   ⛔ IGNORADO (tablet)")
                ignored.append({"nombre": name, "id": cid, "razon": "tablet"})
                continue

            # si no overwrite y ya hay descripción, ignorar
            if (not overwrite) and desc:
                print("   ⏭️  IGNORADO (ya tiene descripción y overwrite=0)")
                ignored.append({"nombre": name, "id": cid, "razon": "ya tiene descripción"})
                continue

            # candidatos: slug actual + slugify(name) (por si hay slugs raros)
            candidates = []
            candidates.extend(smartgsm_slug_variants(slug, parent_slug))
            candidates.extend(smartgsm_slug_variants(slugify(name), parent_slug))
            candidates = unique_keep_order(candidates)

            found_url = None
            found_specs = None

            for cand in candidates:
                url = urljoin(SMARTGSM_BASE, cand)
                try:
                    r = requests.get(url, headers={"User-Agent": UA}, timeout=30)
                except Exception:
                    continue

                if r.status_code != 200:
                    time.sleep(sleep_s)
                    continue

                specs = extract_specs_from_smartgsm(r.text)
                if specs:
                    found_url = url
                    found_specs = specs
                    break

                time.sleep(sleep_s)

            if not found_url or not found_specs:
                print(f"   ❌ NO ENCONTRADA ficha en Smart-GSM con slugs: {candidates[:6]}{'...' if len(candidates) > 6 else ''}")
                not_found.append({"nombre": name, "id": cid, "slug": slug})
                continue

            print(f"   ✅ Ficha encontrada: {found_url}")
            print(f"   🔎 Campos extraídos: {len(found_specs)}")
            for k, v in found_specs[:6]:
                print(f"      - {k}: {v}")
            if len(found_specs) > 6:
                print(f"      ...")

            new_html = build_specs_html(name, found_specs)

            ok = woo.update_category_description(cid, new_html)
            if ok:
                print("   💾 DESCRIPCIÓN actualizada en Woo ✅")
                updated.append({"nombre": name, "id": cid, "campos": len(found_specs)})
            else:
                print("   ❌ Error actualizando en Woo")
                errors.append({"nombre": name, "id": cid, "error": "PUT Woo failed"})

            time.sleep(sleep_s)

        except Exception as e:
            print(f"   ❌ ERROR: {e}")
            errors.append({"nombre": c.get("name", "??"), "id": c.get("id", "??"), "error": str(e)})

    # resumen final
    hoy_fmt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("\n============================================================")
    print(f"📋 RESUMEN DE EJECUCIÓN ({hoy_fmt})")
    print("============================================================")

    print(f"a) SUBCATEGORÍAS ACTUALIZADAS: {len(updated)}")
    for item in updated:
        print(f"- {item['nombre']} (ID: {item['id']}): {item['campos']} campos")

    print(f"b) SUBCATEGORÍAS NO ENCONTRADAS EN SMART-GSM: {len(not_found)}")
    for item in not_found:
        print(f"- {item['nombre']} (ID: {item['id']}) slug='{item['slug']}'")

    print(f"c) SUBCATEGORÍAS IGNORADAS: {len(ignored)}")
    for item in ignored:
        print(f"- {item['nombre']} (ID: {item['id']}): {item['razon']}")

    print(f"d) ERRORES ACTUALIZANDO EN WOO: {len(errors)}")
    for item in errors:
        print(f"- {item['nombre']} (ID: {item['id']}): {item['error']}")

    print("============================================================")


if __name__ == "__main__":
    main()
