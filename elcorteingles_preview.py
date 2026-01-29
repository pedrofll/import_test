#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import json
import time
import random
from datetime import datetime
from urllib.parse import urljoin, urlparse, urlunparse, urlencode, parse_qsl

import requests
from requests.exceptions import ReadTimeout, ConnectTimeout, ConnectionError, RequestException
Reason = Exception

from bs4 import BeautifulSoup


# =========================
# CONFIG
# =========================
PLP_URL = "https://www.elcorteingles.es/limite-48-horas/electronica/moviles-y-smartphones/"
BASE_URL = "https://www.elcorteingles.es"

# ACF / negocio
FUENTE = "El Corte Inglés"
IMPORTADO_DE = "https://www.elcorteingles.es"
ENVIADO_DESDE = "España"
PAUSA_REQUESTS = float(os.getenv("PAUSA_REQUESTS", "0.8"))

# Afiliado ECI (env)
ID_AFILIADO_ELCORTEINGLES = os.getenv("ID_AFILIADO_ELCORTEINGLES", "").strip()

# DEBUG/preview
MAX_PRODUCTS = int(os.getenv("MAX_PRODUCTS", "0"))  # 0 = sin límite
TIMEOUT_CONNECT = float(os.getenv("TIMEOUT_CONNECT", "12"))  # seconds
TIMEOUT_READ = float(os.getenv("TIMEOUT_READ", "90"))        # seconds
MAX_RETRIES_HTTP = int(os.getenv("MAX_RETRIES_HTTP", "6"))   # reintentos GET

SESSION = requests.Session()

# Headers “realistas” (mejoran la respuesta en algunos sitios)
SESSION.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "DNT": "1",
        "Referer": "https://www.elcorteingles.es/",
    }
)

# =========================
# REGEX
# =========================
RE_RAM_ROM = re.compile(r"(\d{1,3})\s*GB\s*\+\s*(\d{2,4})\s*GB", re.IGNORECASE)
RE_TABLET = re.compile(r"\b(TAB|IPAD)\b", re.IGNORECASE)
RE_PRICE_NUM = re.compile(r"(\d+(?:[.,]\d+)?)")


# =========================
# HTTP robusto
# =========================
def backoff_sleep(attempt: int, base: float = 1.4, cap: float = 20.0):
    # exponencial con jitter
    t = min(cap, (base ** attempt)) + random.uniform(0.0, 0.8)
    time.sleep(t)


def http_get(url: str, max_retries: int = MAX_RETRIES_HTTP, allow_redirects: bool = True) -> requests.Response:
    """
    GET robusto:
    - timeouts connect/read separados
    - reintentos ante timeouts/errores y 429/5xx
    """
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            r = SESSION.get(
                url,
                timeout=(TIMEOUT_CONNECT, TIMEOUT_READ),
                allow_redirects=allow_redirects,
            )

            # Reintento si rate-limit o server error
            if r.status_code in (429, 500, 502, 503, 504):
                last_err = RuntimeError(f"HTTP {r.status_code} en {url}")
                backoff_sleep(attempt)
                continue

            # 403 a veces es antibot: reintentar con pausa
            if r.status_code == 403:
                last_err = RuntimeError(f"HTTP 403 (posible antibot) en {url}")
                backoff_sleep(attempt)
                continue

            r.raise_for_status()
            return r

        except (ReadTimeout, ConnectTimeout) as e:
            last_err = e
            backoff_sleep(attempt)
        except (ConnectionError, RequestException) as e:
            last_err = e
            backoff_sleep(attempt)

    raise RuntimeError(f"No se pudo descargar tras {max_retries} intentos: {url} | Último error: {last_err}")


def sleep_polite(base=PAUSA_REQUESTS):
    time.sleep(base + random.uniform(0.05, 0.25))


# =========================
# HELPERS
# =========================
def normaliza_600x600(url_img: str) -> str:
    if not url_img:
        return url_img
    parsed = urlparse(url_img)
    qs = dict(parse_qsl(parsed.query, keep_blank_values=True))
    qs["width"] = "600"
    qs["height"] = "600"
    qs.setdefault("impolicy", "Resize")
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, urlencode(qs, doseq=True), parsed.fragment))


def smart_title_case(s: str) -> str:
    s = re.sub(r"\s+", " ", (s or "").strip())

    def fix_token(tok: str) -> str:
        if not tok:
            return tok
        # tokens alfanuméricos: letras mayúsculas (g15 -> G15, 14t -> 14T, 5g -> 5G)
        if re.search(r"[A-Za-z]", tok) and re.search(r"\d", tok):
            return re.sub(r"[A-Za-z]", lambda m: m.group(0).upper(), tok)
        if tok.isupper():
            return tok
        return tok[:1].upper() + tok[1:].lower()

    out = []
    for w in s.split(" "):
        if w in {"+", "|"}:
            out.append(w)
        else:
            out.append(fix_token(w))
    return " ".join(out)


def extrae_ram_rom(titulo: str):
    t = (titulo or "").replace("GB+", "GB +")
    m = RE_RAM_ROM.search(t)
    if not m:
        return None, None
    ram = f"{int(m.group(1))} GB"
    rom = f"{int(m.group(2))} GB"
    return ram, rom


def es_movil_valido(titulo: str) -> bool:
    if RE_TABLET.search(titulo or ""):
        return False
    ram, rom = extrae_ram_rom(titulo or "")
    return bool(ram and rom)


def limpia_url_sin_query(url: str) -> str:
    p = urlparse(url)
    return urlunparse((p.scheme, p.netloc, p.path, p.params, "", ""))


def aplica_afiliado(url_sin_query: str, afiliado: str) -> str:
    afiliado = (afiliado or "").strip()
    if not afiliado:
        return url_sin_query
    if afiliado.startswith("?") or afiliado.startswith("&"):
        return url_sin_query + afiliado
    return url_sin_query + "?" + afiliado


def compute_version(nombre: str) -> str:
    if re.search(r"\biphone\b", nombre or "", re.IGNORECASE):
        return "IOS"
    return "Versión Global"


def isgd_shorten(long_url: str, retries: int = 5) -> str:
    api = "https://is.gd/create.php"
    last = None
    for i in range(1, retries + 1):
        try:
            r = SESSION.get(api, params={"format": "simple", "url": long_url}, timeout=(10, 30))
            r.raise_for_status()
            short = r.text.strip()
            if short.startswith("http"):
                return short
        except Exception as e:
            last = e
        time.sleep(1.2 * i)
    # fallback
    return long_url


# =========================
# SCRAPE
# =========================
def scrape_plp():
    r = http_get(PLP_URL)
    soup = BeautifulSoup(r.text, "html.parser")

    items = []
    for art in soup.select("li.products_list-item article.product_preview[id]"):
        pid = (art.get("id") or "").strip()

        a = art.select_one("h2 a.product_preview-title")
        if not a:
            continue

        titulo_raw = a.get_text(" ", strip=True)
        href = a.get("href", "")
        url_producto = urljoin(BASE_URL, href)

        # Imagen
        img = art.select_one("img.js_preview_image")
        img_url = img.get("src") if img else ""
        if not img_url:
            img2 = art.select_one("[data-variant-image-src]")
            img_url = img2.get("data-variant-image-src") if img2 else ""

        # Fallback PID desde href
        if not pid:
            m = re.search(r"/electronica/(A\d+)", href)
            pid = m.group(1) if m else ""

        if not es_movil_valido(titulo_raw):
            continue

        ram, rom = extrae_ram_rom(titulo_raw)
        titulo = smart_title_case(titulo_raw)

        sku = f"eci-{pid.lower()}-{ram.split()[0]}-{rom.split()[0]}" if pid and ram and rom else ""

        items.append(
            {
                "pid": pid,
                "sku": sku,
                "titulo_raw": titulo_raw,
                "titulo": titulo,
                "ram": ram,
                "rom": rom,
                "url_producto": url_producto,
                "img": normaliza_600x600(img_url),
            }
        )

        if MAX_PRODUCTS and len(items) >= MAX_PRODUCTS:
            break

    return items


def scrape_pdp_prices(url_producto: str):
    """
    Devuelve (precio_actual, precio_original, moneda)
    - precio_actual: JSON-LD offers.price
    - precio_original: heurística simple; si no, None
    """
    r = http_get(url_producto)
    soup = BeautifulSoup(r.text, "html.parser")

    precio_actual = None
    moneda = None

    for sc in soup.select('script[type="application/ld+json"]'):
        txt = sc.get_text(strip=True)
        if not txt:
            continue
        try:
            data = json.loads(txt)
        except Exception:
            continue

        objs = data if isinstance(data, list) else [data]
        for obj in objs:
            if not isinstance(obj, dict):
                continue
            offers = obj.get("offers")
            if isinstance(offers, dict) and offers.get("price") is not None:
                precio_actual = str(offers.get("price"))
                moneda = str(offers.get("priceCurrency") or "EUR")
                break
            if isinstance(offers, list):
                for off in offers:
                    if isinstance(off, dict) and off.get("price") is not None:
                        precio_actual = str(off.get("price"))
                        moneda = str(off.get("priceCurrency") or "EUR")
                        break
            if precio_actual:
                break
        if precio_actual:
            break

    # Precio original (heurística ligera)
    precio_original = None
    texto = soup.get_text(" ", strip=True).lower()
    if "precio anterior" in texto or "antes" in texto:
        mblock = re.search(r"(precio anterior.{0,160})", texto)
        if mblock:
            mnum = RE_PRICE_NUM.search(mblock.group(1))
            if mnum:
                precio_original = mnum.group(1).replace(",", ".")

    def norm(p):
        if p is None:
            return None
        try:
            f = float(str(p).replace(",", "."))
            s = f"{f:.2f}".rstrip("0").rstrip(".")
            return s
        except Exception:
            return str(p).strip()

    return norm(precio_actual), norm(precio_original), (moneda or "EUR")


# =========================
# LOGS (preview)
# =========================
def log_producto(it, precio_actual, precio_original, url_acortada, url_sin_query, url_con_afiliado, version, codigo_descuento):
    print(f"Detectado {it['titulo']}")
    print(f"1) Nombre: {it['titulo']}")
    print(f"2) Memoria: {it['ram']}")
    print(f"3) Capacidad: {it['rom']}")
    print(f"4) Versión: {version}")
    print(f"5) Fuente: {FUENTE}")
    print(f"6) Precio actual: {precio_actual if precio_actual is not None else 'SIN PRECIO'}")
    print(f"7) Precio original: {precio_original if precio_original is not None else (precio_actual if precio_actual is not None else 'SIN PRECIO')}")
    print(f"8) Código de descuento: {codigo_descuento}")
    print(f"9) URL Imagen: {it['img']}")
    print(f"10) Enlace Importado: {it['url_producto']}")
    print(f"11) Enlace Expandido: {it['url_producto']}")
    print(f"12) URL importada sin afiliado: {url_sin_query}")
    print(f"13) URL sin acortar con mi afiliado: {url_con_afiliado}")
    print(f"14) URL acortada con mi afiliado: {url_acortada}")
    print(f"15) Enviado desde: {ENVIADO_DESDE}")
    print(f"16) Importado de: {IMPORTADO_DE}")
    print(f"17) SKU (solo control interno): {it['sku']}")
    print("-" * 60)


def main():
    print("============================================================")
    print(f"🔎 PREVIEW EL CORTE INGLÉS (SIN CREAR) ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
    print("============================================================")
    print(f"PLP: {PLP_URL}")
    print(f"Pausa entre requests: {PAUSA_REQUESTS}s")
    print(f"Timeout connect/read: {TIMEOUT_CONNECT}s / {TIMEOUT_READ}s")
    print(f"Reintentos HTTP: {MAX_RETRIES_HTTP}")
    print(f"Afiliado ECI configurado: {'SI' if bool(ID_AFILIADO_ELCORTEINGLES) else 'NO'}")
    print(f"MAX_PRODUCTS: {MAX_PRODUCTS if MAX_PRODUCTS else 'SIN LÍMITE'}")
    print("============================================================")

    summary_creados = []
    summary_eliminados = []
    summary_actualizados = []
    summary_ignorados = []

    # PLP
    try:
        items = scrape_plp()
    except Exception as e:
        print(f"❌ ERROR al descargar/parsear PLP: {e}")
        # Resumen vacío para que el workflow no “reviente” sin logs
        items = []

    print(f"📦 Productos móviles detectados (con RAM+ROM): {len(items)}")
    print("------------------------------------------------------------")

    detectados = 0
    sin_precio = 0

    for it in items:
        sleep_polite()

        try:
            precio_actual, precio_original, moneda = scrape_pdp_prices(it["url_producto"])
        except Exception as e:
            # seguimos con logs aunque la PDP falle
            precio_actual, precio_original, moneda = None, None, "EUR"
            print(f"⚠️ PDP falló ({it['pid']}): {e}")

        version = compute_version(it["titulo"])
        codigo_descuento = "OFERTA: PROMO."

        url_sin_query = limpia_url_sin_query(it["url_producto"])
        url_con_afiliado = aplica_afiliado(url_sin_query, ID_AFILIADO_ELCORTEINGLES)
        url_acortada = isgd_shorten(url_con_afiliado)

        detectados += 1
        if precio_actual is None:
            sin_precio += 1

        if precio_actual is not None and precio_original is None:
            precio_original = precio_actual

        log_producto(
            it=it,
            precio_actual=precio_actual,
            precio_original=precio_original,
            url_acortada=url_acortada,
            url_sin_query=url_sin_query,
            url_con_afiliado=url_con_afiliado,
            version=version,
            codigo_descuento=codigo_descuento,
        )

        # Preview: todo ignorado
        summary_ignorados.append({"nombre": it["titulo"], "id": it["pid"] or "N/A"})

    hoy_fmt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("\n============================================================")
    print(f"📋 RESUMEN DE EJECUCIÓN ({hoy_fmt})")
    print("============================================================")
    print(f"\nDetectados en origen: {detectados}")
    print(f"Sin precio en PDP (JSON-LD): {sin_precio}")

    print(f"\na) ARTICULOS CREADOS: {len(summary_creados)}")
    for item in summary_creados:
        print(f"- {item['nombre']} (ID: {item['id']})")

    print(f"\nb) ARTICULOS ELIMINADOS (OBSOLETOS): {len(summary_eliminados)}")
    for item in summary_eliminados:
        print(f"- {item['nombre']} (ID: {item['id']})")

    print(f"\nc) ARTICULOS ACTUALIZADOS: {len(summary_actualizados)}")
    for item in summary_actualizados:
        print(f"- {item['nombre']} (ID: {item['id']}): {', '.join(item['cambios'])}")

    print(f"\nd) ARTICULOS IGNORADOS (SIN CAMBIOS): {len(summary_ignorados)}")
    for item in summary_ignorados[:20]:
        print(f"- {item['nombre']} (ID: {item['id']})")
    if len(summary_ignorados) > 20:
        print(f"... ({len(summary_ignorados) - 20} más)")

    print("============================================================")


if __name__ == "__main__":
    main()
