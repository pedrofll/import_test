#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PREVIEW El Corte Inglés (SIN CREAR PRODUCTOS)
- Scrapea PLP
- Extrae: nombre, memoria, capacidad, precios (si aparecen), imagen, URL limpia + URL con afiliado
- Logs estilo ODM (ACF)
- Reintentos + backoff (10 intentos, 15s) para evitar timeouts
"""

import os
import re
import time
import html
from datetime import datetime
from urllib.parse import urljoin, urlsplit, urlunsplit

import requests
from bs4 import BeautifulSoup


# =========================
# CONFIG
# =========================
SCRAPER_VERSION = "ECI_PREVIEW_v1.1_retry_headers_timeout"
BASE = "https://www.elcorteingles.es"

# URL principal (la que tú pasaste)
PLP_URL_PRIMARY = "https://www.elcorteingles.es/limite-48-horas/electronica/moviles-y-smartphones/"

# Fallbacks (por si el primary está especialmente pesado)
PLP_URL_FALLBACKS = [
    "https://www.elcorteingles.es/electronica/moviles-y-smartphones/",
    # antiguos patrones indexados; a veces responden más “ligero”
    "https://www.elcorteingles.es/limite-48-horas/electronica/moviles-y-smartphones/2/",
]

# Puedes sobreescribir por ENV si quieres probar otra
PLP_URL = os.getenv("ECI_PLP_URL", PLP_URL_PRIMARY)

REQUEST_PAUSE = float(os.getenv("REQUEST_PAUSE", "0.8"))

# Reintentos estilo ODM
MAX_FETCH_ATTEMPTS = int(os.getenv("ECI_MAX_FETCH_ATTEMPTS", "10"))
RETRY_SLEEP_SECONDS = int(os.getenv("ECI_RETRY_SLEEP_SECONDS", "15"))

# Timeout: (connect, read)
CONNECT_TIMEOUT = float(os.getenv("ECI_CONNECT_TIMEOUT", "12"))
READ_TIMEOUT = float(os.getenv("ECI_READ_TIMEOUT", "120"))

# Afiliado (tu runner lo tiene como secret AFF_ELCORTEINGLES)
AFF_ECI = (
    os.getenv("AFF_ELCORTEINGLES")
    or os.getenv("AFF_ELCORTE_INGLES")
    or os.getenv("AFF_ECI")
    or ""
).strip()

TIENDAS_ESPANA = ["pccomponentes", "aliexpress plaza", "aliexpress", "mediamarkt", "amazon", "fnac", "phone house", "powerplanet", "xiaomi store", "el corte ingles"]

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
})


# =========================
# HELPERS (formato ODM)
# =========================
def now_fmt():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def clean_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def strip_query(url: str) -> str:
    """Quita querystring y fragment."""
    if not url:
        return url
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


def with_affiliate(url_clean: str, aff: str) -> str:
    """Añade afiliado (aff normalmente empieza por '?')."""
    if not url_clean:
        return url_clean
    if not aff:
        return url_clean
    # si el aff no empieza por ?, lo añadimos bien
    if not aff.startswith("?"):
        aff = "?" + aff.lstrip("&?")
    return url_clean + aff


def image_to_600(img_url: str) -> str:
    """Intenta forzar 600x600 en URLs tipo ...?impolicy=Resize&width=640&height=640"""
    if not img_url:
        return img_url
    # reemplazos típicos
    img_url = img_url.replace("width=640", "width=600").replace("height=640", "height=600")
    img_url = img_url.replace("width=1200", "width=600").replace("height=1200", "height=600")
    return img_url


def normalize_token(tok: str) -> str:
    """
    - Primera letra mayúscula por palabra
    - Si hay mezcla letras+números: letras en mayúsculas (ej: g85 -> G85, 5g -> 5G)
    """
    tok = tok.strip()
    if not tok:
        return tok

    # conserva separadores tipo "/"
    if re.fullmatch(r"[+/]", tok):
        return tok

    # tokens con dígitos: upper a letras
    if re.search(r"\d", tok):
        out = []
        for ch in tok:
            if ch.isalpha():
                out.append(ch.upper())
            else:
                out.append(ch)
        return "".join(out)

    # tokens sin dígitos: capitaliza normal
    return tok[:1].upper() + tok[1:].lower()


def titlecase_keep_format(s: str) -> str:
    s = clean_spaces(s)
    # separa por espacios, pero respeta símbolos + y /
    parts = re.split(r"(\s+)", s)
    out = []
    for p in parts:
        if p.isspace():
            out.append(p)
        else:
            # descompone "Note-14" en subpartes manteniendo guiones
            sub = re.split(r"(-)", p)
            out_sub = []
            for ss in sub:
                if ss == "-":
                    out_sub.append(ss)
                else:
                    out_sub.append(normalize_token(ss))
            out.append("".join(out_sub))
    return "".join(out).strip()


def parse_ram_storage(title: str):
    """
    Busca patrones:
    - "8GB + 256GB"
    - "12 GB + 512 GB"
    - también TB (ej: 1TB)
    Devuelve ("8 GB", "256 GB") o (None, None)
    """
    t = title or ""
    m = re.search(r"(\d+)\s*(GB|TB)\s*\+\s*(\d+)\s*(GB|TB)", t, flags=re.I)
    if not m:
        return None, None

    ram_n, ram_u, sto_n, sto_u = m.group(1), m.group(2).upper(), m.group(3), m.group(4).upper()
    memoria = f"{ram_n} {ram_u}"
    capacidad = f"{sto_n} {sto_u}"
    return memoria, capacidad


def clean_base_name(raw_title: str) -> str:
    """
    De "Xiaomi Redmi Note 14 8GB + 256GB móvil libre"
    -> "Xiaomi Redmi Note 14"
    """
    t = html.unescape(raw_title or "")
    t = re.sub(r"\b(\d+)\s*(GB|TB)\s*\+\s*(\d+)\s*(GB|TB)\b", "", t, flags=re.I)
    t = re.sub(r"\bm[oó]vil\s+libre\b", "", t, flags=re.I)
    t = re.sub(r"\bsmartphone\b", "", t, flags=re.I)
    t = re.sub(r"\btelefon[oó]\s+m[oó]vil\b", "", t, flags=re.I)
    t = clean_spaces(t)
    return titlecase_keep_format(t)


def is_tablet_or_invalid(title: str) -> bool:
    t = (title or "").upper()
    if "TAB" in t or "IPAD" in t:
        return True
    return False


def parse_prices_from_card_text(card_text: str):
    """
    Extrae precios tipo:
    - 229,90 €
    - 1.599,90 €
    Devuelve (precio_actual, precio_original) como strings numéricas sin símbolo.
    """
    if not card_text:
        return None, None

    euros = re.findall(r"(\d{1,3}(?:\.\d{3})*(?:,\d{2})?)\s*€", card_text)
    # Limpia duplicados manteniendo orden
    seen = set()
    vals = []
    for e in euros:
        if e not in seen:
            seen.add(e)
            vals.append(e)

    if not vals:
        return None, None

    # Si hay 2 o más, normalmente actual < original
    def euro_to_float(x: str) -> float:
        return float(x.replace(".", "").replace(",", "."))

    floats = [(v, euro_to_float(v)) for v in vals]
    floats_sorted = sorted(floats, key=lambda x: x[1])

    if len(floats_sorted) == 1:
        return floats_sorted[0][0], None

    precio_actual = floats_sorted[0][0]
    precio_original = floats_sorted[-1][0]
    # Si por lo que sea original == actual, lo dejamos como None
    if precio_original == precio_actual:
        precio_original = None
    return precio_actual, precio_original


def fetch_html(url: str) -> str:
    """
    Fetch robusto con reintentos (10) y pausa 15s.
    """
    last_err = None
    for attempt in range(1, MAX_FETCH_ATTEMPTS + 1):
        try:
            r = SESSION.get(url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT), allow_redirects=True)
            r.raise_for_status()
            return r.text
        except Exception as e:
            last_err = e
            print(f"⚠️  Error fetch (intento {attempt}/{MAX_FETCH_ATTEMPTS}) -> {type(e).__name__}: {e}")
            if attempt < MAX_FETCH_ATTEMPTS:
                time.sleep(RETRY_SLEEP_SECONDS)
    raise last_err


def scrape_plp(plp_url: str):
    html_txt = fetch_html(plp_url)
    soup = BeautifulSoup(html_txt, "html.parser")

    # ECI usa <li class="products_list-item"> <article class="product_preview ...">
    cards = soup.select("li.products_list-item article.product_preview")
    if not cards:
        # fallback: a veces no está el li pero sí el article
        cards = soup.select("article.product_preview")

    items = []
    for art in cards:
        # id
        pid = art.get("id") or ""
        # title
        a = art.select_one("a.product_preview-title")
        title = ""
        href = ""
        if a:
            title = a.get("title") or a.get_text(" ", strip=True)
            href = a.get("href") or ""
        if not title:
            title = art.get("aria-label") or ""

        title = clean_spaces(title)

        # url detalle (también existe data-url en div.product_link)
        if not href:
            divlink = art.select_one("[data-url]")
            if divlink:
                href = divlink.get("data-url") or ""
        if href and href.startswith("/"):
            href = urljoin(BASE, href)
        href = clean_spaces(href)

        # imagen
        img = art.select_one("img.js_preview_image")
        img_url = ""
        if img:
            img_url = img.get("src") or ""
        if not img_url:
            # variante
            vimg = art.select_one("[data-variant-image-src]")
            if vimg:
                img_url = vimg.get("data-variant-image-src") or ""
        img_url = clean_spaces(img_url)
        img_url = image_to_600(img_url)

        # precios (si están en el HTML)
        card_text = clean_spaces(art.get_text(" ", strip=True))
        precio_actual, precio_original = parse_prices_from_card_text(card_text)

        items.append({
            "pid": pid,
            "raw_title": title,
            "href": href,
            "img": img_url,
            "precio_actual": precio_actual,
            "precio_original": precio_original,
        })

    return items


def main():
    print("============================================================")
    print(f"🔎 PREVIEW EL CORTE INGLÉS (SIN CREAR) ({now_fmt()})")
    print("============================================================")
    print(f"SCRAPER_VERSION: {SCRAPER_VERSION}")
    print(f"PLP: {PLP_URL}")
    print(f"Pausa entre requests: {REQUEST_PAUSE}s")
    print(f"Afiliado ECI configurado: {'SI' if bool(AFF_ECI) else 'NO'}")
    print("============================================================")

    urls_to_try = [PLP_URL] + [u for u in PLP_URL_FALLBACKS if u != PLP_URL]

    last_error = None
    items = None
    used_url = None

    for u in urls_to_try:
        try:
            print(f"\n🌐 Intentando PLP: {u}")
            items = scrape_plp(u)
            used_url = u
            if items:
                break
            print("⚠️  PLP descargada pero sin productos detectados.")
        except Exception as e:
            last_error = e
            print(f"❌ Fallo PLP {u}: {type(e).__name__}: {e}")

    if not items:
        print("\n============================================================")
        print("❌ No se pudo obtener ninguna PLP con productos.")
        if last_error:
            print(f"Último error: {type(last_error).__name__}: {last_error}")
        print("============================================================")
        return

    print(f"\n✅ PLP OK: {used_url}")
    print(f"📦 Productos detectados (brutos): {len(items)}")
    print("------------------------------------------------------------")

    ok = 0
    skipped_tablet = 0
    skipped_no_memcap = 0

    for idx, it in enumerate(items, 1):
        raw_title = it["raw_title"]
        if is_tablet_or_invalid(raw_title):
            skipped_tablet += 1
            continue

        memoria, capacidad = parse_ram_storage(raw_title)
        if not memoria or not capacidad:
            skipped_no_memcap += 1
            continue

        nombre = clean_base_name(raw_title)

        # Fuente / enviado / version
        fuente = "El Corte Inglés"
        enviado_desde = "España"

        is_iphone = bool(re.search(r"\biphone\b", raw_title, flags=re.I))
        if is_iphone:
            version = "IOS"
        else:
            version = "Versión Global"

        codigo_descuento = "OFERTA: PROMO."

        # URL limpia + afiliado
        url_importada_sin_afiliado = strip_query(it["href"])
        url_con_mi_afiliado = with_affiliate(url_importada_sin_afiliado, AFF_ECI)

        print(f"Detectado ({ok+1})")
        print(f"1) Nombre: {nombre}")
        print(f"2) Memoria: {memoria}")
        print(f"3) Capacidad: {capacidad}")
        print(f"4) Versión: {version}")
        print(f"5) Fuente: {fuente}")
        print(f"6) Precio actual: {it['precio_actual'] if it['precio_actual'] else 'N/D'}")
        print(f"7) Precio original: {it['precio_original'] if it['precio_original'] else 'N/D'}")
        print(f"8) Código de descuento: {codigo_descuento}")
        print(f"9) Enviado desde: {enviado_desde}")
        print(f"10) Importado_de: {used_url}")
        print(f"11) URL Imagen: {it['img'] if it['img'] else 'N/D'}")
        print(f"12) URL importada sin afiliado: {url_importada_sin_afiliado if url_importada_sin_afiliado else 'N/D'}")
        print(f"13) URL sin acortar con mi afiliado: {url_con_mi_afiliado if url_con_mi_afiliado else 'N/D'}")
        print("------------------------------------------------------------")

        ok += 1
        time.sleep(REQUEST_PAUSE)

    print("\n============================================================")
    print(f"📋 RESUMEN PREVIEW ({now_fmt()})")
    print("============================================================")
    print(f"a) Productos OK (con Memoria+Capacidad): {ok}")
    print(f"b) Ignorados (TAB/IPAD): {skipped_tablet}")
    print(f"c) Ignorados (sin Memoria/Capacidad): {skipped_no_memcap}")
    print(f"d) Total cards brutas en PLP: {len(items)}")
    print("============================================================")


if __name__ == "__main__":
    main()
