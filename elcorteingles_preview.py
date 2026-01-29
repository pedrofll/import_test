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
from bs4 import BeautifulSoup


# =========================
# CONFIG
# =========================
PLP_URL = "https://www.elcorteingles.es/limite-48-horas/electronica/moviles-y-smartphones/"
BASE_URL = "https://www.elcorteingles.es"

# ACF / negocio
FUENTE = "El Corte Inglés"
IMPORTADO_DE = "https://www.elcorteingles.es"  # si tu ACF es select, añade esta opción
ENVIADO_DESDE = "España"
PAUSA_REQUESTS = float(os.getenv("PAUSA_REQUESTS", "0.8"))

# Afiliado ECI (ponlo en env: ID_AFILIADO_ELCORTEINGLES)
# Ejemplo: "utm_source=tuFuente&utm_medium=affiliate&id=XXXXX"
ID_AFILIADO_ELCORTEINGLES = os.getenv("ID_AFILIADO_ELCORTEINGLES", "").strip()

# Modo preview (no toca Woo)
PREVIEW_ONLY = True

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0 Safari/537.36"
    }
)

# =========================
# REGEX
# =========================
RE_RAM_ROM = re.compile(r"(\d{1,3})\s*GB\s*\+\s*(\d{2,4})\s*GB", re.IGNORECASE)  # "8GB + 256GB" / "12 GB + 512 GB"
RE_TABLET = re.compile(r"\b(TAB|IPAD)\b", re.IGNORECASE)
RE_PRICE_NUM = re.compile(r"(\d+(?:[.,]\d+)?)")


# =========================
# HELPERS
# =========================
def sleep_polite(base=PAUSA_REQUESTS):
    time.sleep(base + random.uniform(0.05, 0.25))


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
    """
    - Primera letra de cada palabra en mayúscula.
    - Tokens con números+letras: letras en mayúsculas (g15 -> G15, 14t -> 14T, 5g -> 5G).
    - Mantiene acrónimos ya en mayúscula.
    """
    s = re.sub(r"\s+", " ", (s or "").strip())

    def fix_token(tok: str) -> str:
        if not tok:
            return tok
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
    """
    Regla segura:
    - Excluye TAB/IPAD.
    - Exige RAM+ROM (si no hay, no es móvil para tu import).
    """
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
    for i in range(1, retries + 1):
        try:
            r = SESSION.get(api, params={"format": "simple", "url": long_url}, timeout=20)
            r.raise_for_status()
            short = r.text.strip()
            if short.startswith("http"):
                return short
        except Exception:
            pass
        time.sleep(1.5 * i)
    return long_url


# =========================
# SCRAPE
# =========================
def scrape_plp():
    r = SESSION.get(PLP_URL, timeout=30)
    r.raise_for_status()
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

    return items


def scrape_pdp_prices(url_producto: str):
    """
    Devuelve (precio_actual, precio_original) como strings sin símbolo.
    - precio_actual: preferente JSON-LD offers.price
    - precio_original: si no existe, None
    """
    r = SESSION.get(url_producto, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    # 1) JSON-LD
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

    # 2) Precio original (heurística suave):
    # Busca cualquier "precio anterior" / "antes" / etc. y extrae el número.
    precio_original = None
    texto = soup.get_text(" ", strip=True).lower()
    if "precio anterior" in texto or "antes" in texto:
        # Intento: buscar un patrón cercano a "precio anterior"
        mblock = re.search(r"(precio anterior.{0,120})", texto)
        if mblock:
            mnum = RE_PRICE_NUM.search(mblock.group(1))
            if mnum:
                precio_original = mnum.group(1).replace(",", ".")
    # Si no conseguimos, None

    # Normaliza a forma simple
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
    print(f"Afiliado ECI configurado: {'SI' if bool(ID_AFILIADO_ELCORTEINGLES) else 'NO'}")
    print("============================================================")

    summary_creados = []
    summary_eliminados = []
    summary_actualizados = []
    summary_ignorados = []

    items = scrape_plp()
    print(f"📦 Productos móviles detectados (con RAM+ROM): {len(items)}")
    print("------------------------------------------------------------")

    detectados = 0
    sin_precio = 0

    for it in items:
        sleep_polite()

        precio_actual, precio_original, moneda = scrape_pdp_prices(it["url_producto"])
        version = compute_version(it["titulo"])
        codigo_descuento = "OFERTA: PROMO."  # si no hay cupón

        url_sin_query = limpia_url_sin_query(it["url_producto"])
        url_con_afiliado = aplica_afiliado(url_sin_query, ID_AFILIADO_ELCORTEINGLES)
        url_acortada = isgd_shorten(url_con_afiliado)

        detectados += 1
        if precio_actual is None:
            sin_precio += 1

        # si no hay precio original, lo dejamos como actual en logs (tu práctica habitual)
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

        # En preview, siempre "ignorados" (no tocamos Woo)
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
