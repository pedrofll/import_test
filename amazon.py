
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Amazon (ES) scraper - MODO SOLO LOGS (sin publicar)

- Lee SOURCE_URL_AMAZON desde variables de entorno (GitHub Secrets).
- Extrae productos de resultados de búsqueda de Amazon.
- Normaliza:
  - nombre (antes de guión/coma; si no, sin el bloque RAM/ROM)
  - RAM / ROM (p.ej. 8+256GB, 16GB + 512GB, 12GB RAM 256GB, etc.)
  - iPhone: RAM por mapeo (IPHONE_RAM_MAP) y ROM por "de XXX GB/TB"
  - precio_actual y precio_original (si no hay tachado, *1.20)
  - cupón (si no hay, "OFERTA PROMO")
  - url limpia sin query (/dp/ASIN), afiliado (?tag=...), y acortado (is.gd)
- Identificador recomendado: ASIN + page_id (hash de la URL origen)
- NO crea / actualiza / elimina nada en WordPress: solo imprime logs.

Requisitos: selenium, requests (y navegador/driver disponibles en runner).
"""

import os
import re
import time
import hashlib
import urllib.parse
from dataclasses import dataclass
from typing import Optional, Tuple, List, Dict

import requests

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options


# -----------------------------
# Config
# -----------------------------

SOURCE_URL_AMAZON = os.environ.get("SOURCE_URL_AMAZON", "").strip()

# Tag de afiliado (solo uno para Amazon)
# Puedes definir en GitHub Secrets: AMAZON_TAG=tusofertasd0a-21 (sin "?tag=")
AMAZON_TAG = os.environ.get("AMAZON_TAG", "tusofertasd0a-21").strip()
ID_AFILIADO_AMAZON = f"tag={AMAZON_TAG}"  # para querystring

# Solo logs: no publicar
DRY_RUN = os.environ.get("DRY_RUN", "1").strip() not in ("0", "false", "False")

# Objetivo (número de productos a listar por logs)
OBJETIVO = int(os.environ.get("OBJETIVO", "72"))

# Scroll / espera
SCROLL_PAUSE = float(os.environ.get("SCROLL_PAUSE", "0.9"))
MAX_SCROLLS = int(os.environ.get("MAX_SCROLLS", "12"))

# Country
ENVIADO_DESDE = "España"
FUENTE = "Amazon"
VERSION_DEFAULT = "Versión Global"

# iPhone RAM map (lowercase match sobre nombre)
IPHONE_RAM_MAP = [
    ("iphone 17 pro max", "12GB"),
    ("iphone 17 pro", "12GB"),
    ("iphone 17 air", "12GB"),
    ("iphone air", "12GB"),
    ("iphone 17", "8GB"),
    ("iphone 16 pro max", "8GB"),
    ("iphone 16 pro", "8GB"),
    ("iphone 16 plus", "8GB"),
    ("iphone 16e", "8GB"),
    ("iphone 16", "8GB"),
    ("iphone 15 pro max", "8GB"),
    ("iphone 15 pro", "8GB"),
    ("iphone 15 plus", "6GB"),
    ("iphone 15", "6GB"),
    ("iphone 14 pro max", "6GB"),
    ("iphone 14 pro", "6GB"),
    ("iphone 14 plus", "6GB"),
    ("iphone 14", "6GB"),
    ("iphone 13 pro max", "6GB"),
    ("iphone 13 pro", "6GB"),
    ("iphone 13 mini", "4GB"),
    ("iphone 13", "4GB"),
    ("iphone 12 pro max", "6GB"),
    ("iphone 12 pro", "6GB"),
    ("iphone 12 mini", "4GB"),
    ("iphone 12", "4GB"),
]


# -----------------------------
# Utilidades
# -----------------------------

def page_id_from_url(url: str) -> str:
    return hashlib.md5(url.encode("utf-8")).hexdigest()[:10]

def mask_url(u: str) -> str:
    """En logs: muestra URL pero sin query sensible. Mantiene path."""
    try:
        parsed = urllib.parse.urlsplit(u)
        if not parsed.scheme:
            return u
        return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))
    except Exception:
        return u

def is_alphanum_token(word: str) -> bool:
    # ejemplos: 4G, 5G, 14T, GS, G5, A56, S21, etc.
    return bool(re.match(r"^(?=.*[A-Za-z])(?=.*\d)[A-Za-z0-9]+$", word))

def smart_titlecase(s: str) -> str:
    s = re.sub(r"\s+", " ", s).strip()
    words = s.split(" ")
    out = []
    for w in words:
        if is_alphanum_token(w):
            out.append(w.upper())
        elif w.isupper() and len(w) <= 5:
            # marcas cortas (OPPO, POCO, etc.) -> primera mayus, resto minus
            out.append(w[:1].upper() + w[1:].lower())
        else:
            out.append(w[:1].upper() + w[1:].lower() if w else w)
    return " ".join(out)

def parse_price_to_float(txt: str) -> Optional[float]:
    if not txt:
        return None
    t = txt.strip()
    # Ej: "1.099,00 €" o "799€"
    t = t.replace("€", "").replace("\xa0", " ").strip()
    t = t.replace(".", "").replace(" ", "")
    t = t.replace(",", ".")
    m = re.search(r"(\d+(?:\.\d+)?)", t)
    if not m:
        return None
    try:
        return float(m.group(1))
    except Exception:
        return None

def format_eur_int(x: float) -> str:
    # En tu ecosistema parece que trabajas con enteros en euros
    return f"{int(round(x))}€"

def compute_precio_original(precio_actual: float) -> float:
    # Precio original = actual * 1.20
    return round(precio_actual * 1.20)

def strip_query_keep_product(url: str) -> str:
    """
    De Amazon: conserva solo /dp/ASIN (o /gp/product/ASIN) y elimina query y /ref...
    Ej:
    https://www.amazon.es/.../dp/B0FX.../ref=...?... -> https://www.amazon.es/dp/B0FX...
    """
    if not url:
        return url
    try:
        u = urllib.parse.urljoin("https://www.amazon.es", url)
        parsed = urllib.parse.urlsplit(u)
        path = parsed.path

        # prefer /dp/ASIN
        m = re.search(r"/dp/([A-Z0-9]{10})", path)
        if m:
            asin = m.group(1)
            return f"https://{parsed.netloc}/dp/{asin}"

        m = re.search(r"/gp/product/([A-Z0-9]{10})", path)
        if m:
            asin = m.group(1)
            return f"https://{parsed.netloc}/gp/product/{asin}"

        # fallback: sin query
        return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))
    except Exception:
        return url

def add_amazon_affiliate(clean_url: str) -> str:
    if not clean_url:
        return clean_url
    parsed = urllib.parse.urlsplit(clean_url)
    q = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    q["tag"] = [AMAZON_TAG]
    new_query = urllib.parse.urlencode(q, doseq=True)
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, new_query, ""))

def acortar_isgd(long_url: str, timeout: int = 25) -> Optional[str]:
    try:
        r = requests.get("https://is.gd/create.php", params={"format": "simple", "url": long_url}, timeout=timeout)
        if r.status_code == 200 and r.text.strip().startswith("http"):
            return r.text.strip()
    except Exception:
        pass
    return None

def detectar_cupon(container_text: str) -> str:
    if not container_text:
        return "OFERTA PROMO"
    # Búsqueda simple: "cupón" / "Cupón"
    if re.search(r"\bcup[oó]n\b", container_text, re.IGNORECASE):
        # intenta capturar una linea con cupón
        for line in container_text.splitlines():
            if re.search(r"\bcup[oó]n\b", line, re.IGNORECASE):
                line = re.sub(r"\s+", " ", line).strip()
                return line[:80]
        return "Cupón disponible"
    return "OFERTA PROMO"

def detectar_iphone_ram(nombre: str) -> Optional[str]:
    nl = (nombre or "").lower().strip()
    for key, ram in IPHONE_RAM_MAP:
        if key in nl:
            return ram
    return None

def parse_ram_rom_from_title(title: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Devuelve ("8GB","256GB") o (None,None)
    Acepta:
      - 8+256GB
      - 8 + 256 GB
      - 16GB + 512GB
      - 12GB RAM 256GB
      - 12 GB RAM, 256 GB
    """
    if not title:
        return None, None
    t = title.replace("\xa0", " ")
    t_norm = re.sub(r"\s+", " ", t).strip()

    # 8+256GB / 8 + 256GB
    m = re.search(r"(\d{1,2})\s*\+\s*(\d{2,4})\s*GB\b", t_norm, re.IGNORECASE)
    if m:
        return f"{int(m.group(1))}GB", f"{int(m.group(2))}GB"

    # 16GB + 512GB
    m = re.search(r"(\d{1,2})\s*GB\s*\+\s*(\d{2,4})\s*GB\b", t_norm, re.IGNORECASE)
    if m:
        return f"{int(m.group(1))}GB", f"{int(m.group(2))}GB"

    # 12GB RAM 256GB (o variantes)
    m = re.search(r"(\d{1,2})\s*GB\s*RAM[^0-9]{0,10}(\d{2,4})\s*GB\b", t_norm, re.IGNORECASE)
    if m:
        return f"{int(m.group(1))}GB", f"{int(m.group(2))}GB"

    # 1TB para rom (ram en GB)
    m = re.search(r"(\d{1,2})\s*GB[^0-9]{0,15}(1)\s*TB\b", t_norm, re.IGNORECASE)
    if m:
        return f"{int(m.group(1))}GB", "1TB"

    return None, None

def parse_iphone_capacity(title: str) -> Optional[str]:
    if not title:
        return None
    t = title.replace("\xa0", " ")
    # "de 256 GB" o "de 1 TB"
    m = re.search(r"\bde\s*(\d{2,4})\s*GB\b", t, re.IGNORECASE)
    if m:
        return f"{int(m.group(1))}GB"
    m = re.search(r"\bde\s*(1)\s*TB\b", t, re.IGNORECASE)
    if m:
        return "1TB"
    # fallback: primer GB/TB "relevante"
    m = re.search(r"\b(\d{2,4})\s*GB\b", t, re.IGNORECASE)
    if m:
        return f"{int(m.group(1))}GB"
    m = re.search(r"\b(1)\s*TB\b", t, re.IGNORECASE)
    if m:
        return "1TB"
    return None

def build_nombre_from_title(title: str, ram: Optional[str], rom: Optional[str]) -> str:
    """
    Regla:
    - Si hay '-' o '–' o ',' → tomar lo anterior.
    - Si no, eliminar el bloque RAM/ROM (si existe) y limpiar.
    """
    if not title:
        return ""
    t = re.sub(r"\s+", " ", title).strip()

    # separadores
    for sep in [" - ", " – ", "-", "–", ","]:
        if sep in t:
            left = t.split(sep, 1)[0].strip()
            if left:
                return smart_titlecase(left)

    # sin separadores: elimina bloque 8+256GB etc
    t2 = t
    t2 = re.sub(r"\b\d{1,2}\s*\+\s*\d{2,4}\s*GB\b", "", t2, flags=re.IGNORECASE)
    t2 = re.sub(r"\b\d{1,2}\s*GB\s*\+\s*\d{2,4}\s*GB\b", "", t2, flags=re.IGNORECASE)
    t2 = re.sub(r"\b\d{1,2}\s*GB\s*RAM\b", "", t2, flags=re.IGNORECASE)
    t2 = re.sub(r"\b\d{2,4}\s*GB\b", "", t2, flags=re.IGNORECASE)
    t2 = re.sub(r"\b1\s*TB\b", "", t2, flags=re.IGNORECASE)
    t2 = re.sub(r"\s+", " ", t2).strip(" -–,")
    return smart_titlecase(t2.strip())

def categoria_from_nombre(nombre: str) -> str:
    # categoría = primera palabra (marca)
    if not nombre:
        return ""
    return nombre.split(" ", 1)[0]

@dataclass
class ItemAmazon:
    asin: str
    page_id: str
    nombre: str
    ram: str
    rom: str
    ver: str
    fuente: str
    precio_actual: str
    precio_original: str
    cup: str
    img_src: str
    url_imp: str
    url_exp: str
    url_importada_sin_afiliado: str
    url_sin_acortar_con_mi_afiliado: str
    url_oferta: str
    enviado_desde: str
    pagina_label: str


# -----------------------------
# Selenium
# -----------------------------

def build_driver() -> webdriver.Chrome:
    """Crea un Chrome headless intentando minimizar bloqueos (Amazon es sensible a bots)."""
    opts = Options()

    # Headless moderno (Chrome >= 109). En algunos entornos puede fallar; si fuera necesario,
    # se puede cambiar a "--headless" clásico.
    opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")

    # Idioma / región (ayuda a que el HTML coincida con selectores esperados)
    opts.add_argument("--lang=es-ES")

    # Anti-automation flags comunes
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)

    # User-Agent de escritorio “realista”
    user_agent = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
    opts.add_argument(f"--user-agent={user_agent}")

    # Preferencias (idioma)
    prefs = {"intl.accept_languages": "es-ES,es;q=0.9,en;q=0.8"}
    opts.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(options=opts)

    # Reducir huellas de automatización
    try:
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    except Exception:
        pass

    return driver

def scroll_to_bottom(driver: webdriver.Chrome) -> None:
    last_h = driver.execute_script("return document.body.scrollHeight")
    for _ in range(MAX_SCROLLS):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_PAUSE)
        new_h = driver.execute_script("return document.body.scrollHeight")
        if new_h == last_h:
            break
        last_h = new_h

def extraer_items(driver: webdriver.Chrome, page_url: str) -> List[webdriver.remote.webelement.WebElement]:
    # Amazon: resultados en div[data-component-type="s-search-result"]
    return driver.find_elements(By.CSS_SELECTOR, 'div[data-component-type="s-search-result"][data-asin]')


# -----------------------------
# Extracción Amazon
# -----------------------------

def extraer_producto_desde_item(el, page_url: str, pagina_label: str) -> Optional[ItemAmazon]:
    asin = (el.get_attribute("data-asin") or "").strip()
    if not asin or len(asin) != 10:
        return None

    # título
    title = ""
    try:
        title = el.find_element(By.CSS_SELECTOR, "h2 a span").text.strip()
    except Exception:
        return None

    # enlace (expandido)
    href = ""
    try:
        href = el.find_element(By.CSS_SELECTOR, "h2 a").get_attribute("href") or ""
    except Exception:
        href = ""
    url_exp = href.strip() if href else ""
    url_importada_sin_afiliado = strip_query_keep_product(url_exp)

    # ram/rom
    ram, rom = parse_ram_rom_from_title(title)

    # iPhone special-case (si no hay RAM por patrón)
    if ("iphone" in title.lower()) and (ram is None or rom is None):
        rom2 = parse_iphone_capacity(title)
        ram2 = detectar_iphone_ram(build_nombre_from_title(title, None, rom2))
        if rom is None:
            rom = rom2
        if ram is None:
            ram = ram2

    if not ram or not rom:
        # requisito: si no encuentras ambos, no importes (excepto iPhone map donde ram/rom deben existir)
        return None

    # nombre
    nombre = build_nombre_from_title(title, ram, rom)
    if not nombre:
        return None

    # imagen
    img_src = ""
    try:
        img_src = el.find_element(By.CSS_SELECTOR, "img.s-image").get_attribute("src") or ""
    except Exception:
        img_src = ""

    # precios
    p_act_f = None
    p_reg_f = None
    try:
        p_act_txt = el.find_element(By.CSS_SELECTOR, "span.a-price span.a-offscreen").text.strip()
        p_act_f = parse_price_to_float(p_act_txt)
    except Exception:
        p_act_f = None

    # tachado (precio anterior)
    try:
        p_reg_txt = el.find_element(By.CSS_SELECTOR, "span.a-text-price span.a-offscreen").text.strip()
        p_reg_f = parse_price_to_float(p_reg_txt)
    except Exception:
        p_reg_f = None

    if p_act_f is None:
        return None
    if p_reg_f is None:
        p_reg_f = compute_precio_original(p_act_f)

    precio_actual = format_eur_int(p_act_f)
    precio_original = format_eur_int(p_reg_f)

    # cupón
    cup = detectar_cupon(el.text or "")

    # afiliado + acortado
    url_sin_acortar_con_mi_afiliado = add_amazon_affiliate(url_importada_sin_afiliado)
    url_oferta = acortar_isgd(url_sin_acortar_con_mi_afiliado) or ""

    pid = page_id_from_url(page_url)

    return ItemAmazon(
        asin=asin,
        page_id=pid,
        nombre=nombre,
        ram=ram,
        rom=rom,
        ver=VERSION_DEFAULT,
        fuente=FUENTE,
        precio_actual=precio_actual,
        precio_original=precio_original,
        cup=cup,
        img_src=img_src,
        url_imp=mask_url(page_url),  # en Amazon no hay "importado" corto; mantenemos origen enmascarado
        url_exp=url_exp,
        url_importada_sin_afiliado=url_importada_sin_afiliado,
        url_sin_acortar_con_mi_afiliado=url_sin_acortar_con_mi_afiliado,
        url_oferta=url_oferta,
        enviado_desde=ENVIADO_DESDE,
        pagina_label=pagina_label
    )

def obtener_datos_remotos() -> List[ItemAmazon]:
    if not SOURCE_URL_AMAZON:
        raise SystemExit("❌ Falta SOURCE_URL_AMAZON en variables de entorno.")

    print("\n--- FASE 1: ESCANEANDO AMAZON ---", flush=True)
    print(f"URL: {SOURCE_URL_AMAZON}", flush=True)
    print("-" * 60, flush=True)

    driver = build_driver()
    try:
        driver.get(SOURCE_URL_AMAZON)
        time.sleep(2.5)
        scroll_to_bottom(driver)

        items = extraer_items(driver, SOURCE_URL_AMAZON)
        print(f"✅ Resultados detectados: {len(items)}", flush=True)

        out: List[ItemAmazon] = []
        seen_keys = set()

        for el in items:
            if len(out) >= OBJETIVO:
                break
            prod = extraer_producto_desde_item(el, SOURCE_URL_AMAZON, pagina_label="1")
            if not prod:
                continue
            key = f"{prod.asin}_{prod.page_id}"
            if key in seen_keys:
                continue
            seen_keys.add(key)
            out.append(prod)

        print(f"\n📊 RESUMEN EXTRACCIÓN:", flush=True)
        print(f"   Productos válidos encontrados: {len(out)}", flush=True)
        return out

    finally:
        try:
            driver.quit()
        except Exception:
            pass


# -----------------------------
# Logs (solo)
# -----------------------------

def log_producto(p: ItemAmazon) -> None:
    # NOTA: el usuario pidió ver la URL limpia completa de Amazon sin afiliado.
    # Para afiliado, imprimimos con "***" al final (como haces en otros scrapers).
    print("-" * 60, flush=True)
    print(f"Detectado {p.nombre}", flush=True)
    print(f"1) Nombre: {p.nombre}", flush=True)
    print(f"2) Memoria: {p.ram}", flush=True)
    print(f"3) Capacidad: {p.rom}", flush=True)
    print(f"4) Versión: {p.ver}", flush=True)
    print(f"5) Fuente: {p.fuente}", flush=True)
    print(f"6) Precio actual: {p.precio_actual}", flush=True)
    print(f"7) Precio original: {p.precio_original}", flush=True)
    print(f"8) Código de descuento: {p.cup}", flush=True)
    print(f"9) Version: {p.ver}", flush=True)
    print(f"10) URL Imagen: {p.img_src}", flush=True)
    print(f"11) Enlace Importado: {p.url_imp}", flush=True)
    print(f"12) Enlace Expandido: {p.url_exp}", flush=True)
    print(f"13) URL importada sin afiliado: {p.url_importada_sin_afiliado}", flush=True)
    print(f"14) URL sin acortar con mi afiliado: {p.url_sin_acortar_con_mi_afiliado}***", flush=True)
    print(f"15) URL acortada con mi afiliado: {p.url_oferta}", flush=True)
    print(f"16) Enviado desde: {p.enviado_desde}", flush=True)
    print(f"17) Encolado para comparar con base de datos...", flush=True)
    print("-" * 60, flush=True)

def main():
    remotos = obtener_datos_remotos()
    for p in remotos:
        log_producto(p)

    print("\n============================================================", flush=True)
    print("📋 RESUMEN (SOLO LOGS / SIN PUBLICAR)", flush=True)
    print("============================================================", flush=True)
    print(f"Productos logueados: {len(remotos)}", flush=True)
    print(f"DRY_RUN: {DRY_RUN}", flush=True)
    print(f"Page ID (origen): {page_id_from_url(SOURCE_URL_AMAZON)}", flush=True)
    print("============================================================\n", flush=True)

if __name__ == "__main__":
    main()
