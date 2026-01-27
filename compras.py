# scraper_compras.py
import os
import time
import requests
import urllib.parse
import re
import json
from datetime import datetime
from bs4 import BeautifulSoup
from woocommerce import API

# --- CONFIGURACIÓN WORDPRESS desde variables de entorno ---
wcapi = API(
    url=os.environ.get("WP_URL", ""),
    consumer_key=os.environ.get("WP_KEY", ""),
    consumer_secret=os.environ.get("WP_SECRET", ""),
    version="wc/v3",
    timeout=60
)

# --- ORIGEN Y AFILIADOS desde variables de entorno ---
# No hay literales en el código: todo se lee desde variables de entorno o secrets.

# ============================================================
#   CONFIGURACIÓN ROBUSTA (BASE_URL, URLS_PAGINAS, IMPORTACIÓN)
# ============================================================
# 1) Intentar leer lista completa desde COMPRAS_URLS (secreto opcional)
#    Formato: url1,url2,url3
compras_urls_raw = os.environ.get("COMPRAS_URLS", "").strip()

if compras_urls_raw:
    # Si COMPRAS_URLS existe, se usa directamente
    URLS_PAGINAS = [u.strip().rstrip("/") for u in compras_urls_raw.split(",") if u.strip()]

    # BASE_URL derivado automáticamente de la primera URL
    # Ejemplo: https://XXX.com/ofertas/xiaomi → https://XXX.com
    primera = URLS_PAGINAS[0]
    BASE_URL = primera.split("/ofertas")[0].rstrip("/")

else:
    # 2) Fallback: usar URL principal desde secreto SOURCE_URL_COMPRAS
    url_principal = os.environ.get("SOURCE_URL_COMPRAS", "").strip().rstrip("/")
    if not url_principal:
        raise SystemExit("ERROR: Falta SOURCE_URL_COMPRAS o COMPRAS_URLS en variables de entorno")

    BASE_URL = url_principal.split("/ofertas")[0].rstrip("/")
    base_ofertas = f"{BASE_URL}/ofertas"

    # Mantener la URL principal tal cual, pero asegurar que siempre se añaden las subpáginas solicitadas
    URLS_PAGINAS = [
        url_principal,
        f"{base_ofertas}/apple",
        f"{base_ofertas}/samsung",
        f"{base_ofertas}/xiaomi",
        f"{base_ofertas}/poco",
    ]

# 3) Identificador de importación (oculto)
#    Sirve para agrupar importaciones sin que lo vea el usuario; se usa en meta "importado_de".
ID_IMPORTACION = f"{BASE_URL}/ofertas/"


def _norm_import_id(v: str) -> str:
    """Normaliza el identificador de importación para evitar duplicados por / finales."""
    return (v or "").strip().rstrip("/")


ID_IMPORTACION_NORM = _norm_import_id(ID_IMPORTACION)
ID_AFILIADO_ALIEXPRESS = os.environ.get("AFF_ALIEXPRESS", "")
ID_AFILIADO_MEDIAMARKT = os.environ.get("AFF_MEDIAMARKT", "")
ID_AFILIADO_AMAZON = os.environ.get("AFF_AMAZON", "")
ID_AFILIADO_FNAC = os.environ.get("AFF_FNAC", "")

# Acumuladores globales
summary_creados = []
summary_eliminados = []
summary_ignorados = []
summary_actualizados = []


def limpiar_precio(texto):
    if not texto:
        return "0"
    return texto.replace("€", "").replace(".", "").replace(",", ".").strip()


def acortar_url(url):
    try:
        url_encoded = urllib.parse.quote(url, safe="")
        r = requests.get(f"https://is.gd/create.php?format=simple&url={url_encoded}", timeout=8)
        return r.text.strip() if r.status_code == 200 else url
    except:
        return url


def expandir_url(url: str) -> str:
    """
    Expande enlaces soportando:
      - redirects 3xx (requests allow_redirects)
      - wrappers (Tradedoubler url=, Tradetracker u=)
      - redirecciones HTML/JS (meta refresh, window.location)
    Devuelve la URL final (idealmente la URL real de la tienda).
    """
    if not url:
        return ""

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "es-ES,es;q=0.9",
    }

    s = requests.Session()
    current = url

    # Evitar bucles
    for _hop in range(8):
        try:
            r = s.get(current, allow_redirects=True, headers=headers, timeout=20)
        except Exception:
            return current

        final_url = getattr(r, "url", "") or current

        # 1) Deswrapper por querystring (cuando el destino viene embebido)
        p = urllib.parse.urlparse(final_url)
        host = (p.netloc or "").lower()
        qs = urllib.parse.parse_qs(p.query)

        if "tradedoubler" in host and "url" in qs:
            nxt = urllib.parse.unquote(qs["url"][0])
            if nxt and nxt != current:
                current = nxt
                continue

        if "tradetracker" in host and "u" in qs:
            nxt = urllib.parse.unquote(qs["u"][0])
            if nxt and nxt != current:
                current = nxt
                continue

        body = (r.text or "")

        # 2) meta refresh: content="0;url=..."
        m = re.search(r'http-equiv=["\']refresh["\'][^>]*content=["\'][^"\']*url=([^"\']+)["\']', body, re.I)
        if m:
            nxt = m.group(1).strip()
            nxt = urllib.parse.urljoin(final_url, nxt)
            if nxt and nxt != current:
                current = nxt
                continue

        # 3) JS: window.location / location.href / document.location
        m = re.search(r'(?:window\.location|location\.href|document\.location)\s*=\s*["\']([^"\']+)["\']', body, re.I)
        if m:
            nxt = m.group(1).strip()
            nxt = urllib.parse.urljoin(final_url, nxt)
            if nxt and nxt != current:
                current = nxt
                continue

        return final_url

    return current


def deswrap_url(url: str) -> str:
    """Quita wrappers de afiliación y devuelve la URL destino real si viene en querystring."""
    u = url or ""
    for _ in range(5):
        p = urllib.parse.urlparse(u)
        host = (p.netloc or "").lower()
        qs = urllib.parse.parse_qs(p.query)

        candidate = None
        # Tradedoubler: .../click?...&url=https%3A%2F%2F...
        if "tradedoubler" in host and "url" in qs:
            candidate = qs["url"][0]
        # Tradetracker: ...?u=https%3A%2F%2F...
        elif "tradetracker" in host and "u" in qs:
            candidate = qs["u"][0]

        if not candidate:
            break

        candidate = urllib.parse.unquote(candidate)
        if candidate == u:
            break
        u = candidate

    return u


# --- GESTIÓN DE CATEGORÍAS ---
def obtener_todas_las_categorias():
    categorias = []
    page = 1
    while True:
        try:
            res = wcapi.get("products/categories", params={"per_page": 100, "page": page}).json()
            if not res or "message" in res or len(res) == 0:
                break
            categorias.extend(res)
            page += 1
        except:
            break
    return categorias


def resolver_jerarquia(nombre_completo, cache_categorias):
    palabras = nombre_completo.split()
    nombre_padre = palabras[0]
    nombre_hijo = nombre_completo
    id_cat_padre = None
    id_cat_hijo = None
    foto_final = None

    for cat in cache_categorias:
        if cat["name"].lower() == nombre_padre.lower() and cat["parent"] == 0:
            id_cat_padre = cat["id"]
            break

    if not id_cat_padre:
        payload = {"name": nombre_padre, "parent": 0}
        r = wcapi.post("products/categories", payload)
        if r.status_code in [200, 201]:
            nueva = r.json()
            id_cat_padre = nueva["id"]
            cache_categorias.append(nueva)

    # Buscar hijo exacto dentro del padre
    for cat in cache_categorias:
        if cat["name"].lower() == nombre_hijo.lower() and cat["parent"] == id_cat_padre:
            id_cat_hijo = cat["id"]
            # intentar reutilizar imagen de la subcategoría si existe
            img = cat.get("image") or {}
            foto_final = img.get("src")
            break

    if not id_cat_hijo:
        payload = {"name": nombre_hijo, "parent": id_cat_padre}
        r = wcapi.post("products/categories", payload)
        if r.status_code in [200, 201]:
            nueva = r.json()
            id_cat_hijo = nueva["id"]
            cache_categorias.append(nueva)

    return id_cat_padre, id_cat_hijo, foto_final


def obtener_productos_existentes():
    productos = []
    page = 1
    while True:
        res = wcapi.get("products", params={"per_page": 100, "page": page, "status": "publish"}).json()
        if not res or "message" in res:
            break
        productos.extend(res)
        if len(res) < 100:
            break
        page += 1
    return productos


def meta_get(meta_list, key):
    for m in meta_list:
        if m.get("key") == key:
            return m.get("value")
    return ""


def producto_match(p_existente, p_nuevo):
    meta = p_existente.get("meta_data", [])
    nombre = p_existente.get("name", "").strip()

    memoria = meta_get(meta, "memoria")
    capacidad = meta_get(meta, "capacidad")
    precio_actual = meta_get(meta, "precio_actual")
    precio_original = meta_get(meta, "precio_original")
    fuente = meta_get(meta, "fuente")
    importado_de = _norm_import_id(meta_get(meta, "importado_de"))

    return (
        nombre.lower() == p_nuevo["nombre"].lower()
        and (memoria or "").strip().lower() == p_nuevo["ram"].strip().lower()
        and (capacidad or "").strip().lower() == p_nuevo["rom"].strip().lower()
        and str(precio_original).strip() == str(p_nuevo["p_reg"]).strip()
        and str(precio_actual).strip() == str(p_nuevo["p_act"]).strip()
        and (fuente or "").strip().lower() == p_nuevo["fuente"].strip().lower()
        and importado_de == ID_IMPORTACION_NORM
    )



def obtener_datos_remotos():
    """
    Scraper robusto para comprasmartphone.com/ofertas (+subsecciones).
    NO depende de clases CSS (que cambian con frecuencia). Se basa en patrones estables:
      - Link del producto: href contiene "/telefonos/"
      - Link de compra: texto comienza por "Cómpralo en"
      - RAM/ROM: patrón 12/256GB, 8/128GB, etc.
      - Precios: importes con "€" dentro del bloque
    """
    productos_por_clave = {}

    def fetch_html(url: str) -> str:
        # Reintentos y cabeceras “browser-like” para minimizar bloqueos
        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
            "Connection": "keep-alive",
        }
        last_exc = None
        for i in range(1, 4):
            try:
                resp = requests.get(url, timeout=40, headers=headers)
                status = resp.status_code
                text = resp.text or ""
                print(f"   ↳ HTTP {status} | bytes={len(text)}")
                # Si hay bloqueo típico, deja pista clara en logs
                low = text.lower()
                if status in (403, 429, 503) or "cloudflare" in low or "captcha" in low:
                    print("   ⚠️ Posible bloqueo (Cloudflare/captcha/rate-limit).")
                return text
            except Exception as e:
                last_exc = e
                print(f"   ⚠️ Error HTTP intento {i}/3: {e}")
                time.sleep(5)
        raise last_exc

    # Helper: extrae ram/rom desde texto del bloque (ej "16/128GB", "8GB/1TB")
    def extraer_ram_rom_de_bloque(texto: str):
        t = (texto or "").upper().replace(" ", "")
        # Caso principal: 16/128GB
        m = re.search(r"\b(\d{1,3})/(\d{2,4})GB\b", t)
        if m:
            return f"{m.group(1)} GB", f"{m.group(2)} GB"
        # Caso: 8GB/1TB
        m = re.search(r"\b(\d{1,3})GB/(\d{1,2})TB\b", t)
        if m:
            return f"{m.group(1)} GB", f"{m.group(2)} TB"
        return "", ""

    for idx, url in enumerate(URLS_PAGINAS, start=1):
        print(f"\nEscaneando listado ({idx}/{len(URLS_PAGINAS)}): {url}")
        try:
            html = fetch_html(url)
        except Exception as e:
            print(f"  ❌ Error leyendo {url}: {e}")
            continue

        soup = BeautifulSoup(html, "html.parser")
        # Fallback: si el HTML no trae el listado (render client-side), intentar extraer datos de __NEXT_DATA__
        def extraer_items_desde_next_data(html_text: str):
            items = []
            try:
                s2 = BeautifulSoup(html_text, "html.parser")
                script = s2.find("script", id="__NEXT_DATA__")
                if not script or not script.string:
                    return items
                data = json.loads(script.string)
            except Exception:
                return items

            name_keys = ["name", "title", "productName", "model", "nombre"]
            url_keys = ["url", "href", "link", "productUrl", "offerUrl", "buyUrl", "enlace", "targetUrl"]
            store_keys = ["store", "shop", "merchant", "fuente", "source", "tienda"]
            price_keys = ["price", "salePrice", "currentPrice", "precio", "precio_actual", "priceNow"]
            old_keys = ["oldPrice", "regularPrice", "precio_original", "precioAnterior", "priceOld", "wasPrice"]
            img_keys = ["image", "imageUrl", "img", "picture", "thumbnail", "foto"]

            def pick(d, keys):
                for k in keys:
                    if k in d and d[k]:
                        return d[k]
                return None

            def as_str(v):
                if isinstance(v, str):
                    return v
                if isinstance(v, (int, float)):
                    return str(v)
                if isinstance(v, dict):
                    for kk in ("href", "url", "link"):
                        if kk in v and isinstance(v[kk], str):
                            return v[kk]
                return ""

            def walk(o):
                if isinstance(o, dict):
                    n = pick(o, name_keys)
                    u = pick(o, url_keys)
                    p = pick(o, price_keys)
                    # Candidato “producto/oferta”
                    if n and u and p:
                        item = {
                            "nombre_raw": as_str(n),
                            "url": as_str(u),
                            "precio": as_str(p),
                            "precio_old": as_str(pick(o, old_keys) or ""),
                            "fuente": as_str(pick(o, store_keys) or ""),
                            "imagen": as_str(pick(o, img_keys) or ""),
                        }
                        items.append(item)
                    for v in o.values():
                        walk(v)
                elif isinstance(o, list):
                    for it in o:
                        walk(it)

            walk(data)

            # Dedup básico
            uniq = []
            seen = set()
            for it in items:
                key = (it.get("nombre_raw","")[:120], it.get("url","")[:200], it.get("precio",""))
                if key in seen:
                    continue
                seen.add(key)
                uniq.append(it)
            return uniq



        # 1) Detectar candidatos por “Cómpralo en …” (estable)
        buy_links = soup.find_all("a", string=re.compile(r"^\s*Cómpralo en\b", re.I))
        print(f"✅ Items detectados: {len(buy_links)}")

        # Si no hay anchors "Cómpralo en", probablemente el contenido se hidrata por JS.
        # Intentar extraer ofertas desde __NEXT_DATA__ (Next.js) y procesarlas.
        if len(buy_links) == 0:
            next_items = extraer_items_desde_next_data(html)
            print(f"ℹ️ Fallback __NEXT_DATA__: {len(next_items)} items candidatos")

            for it in next_items:
                try:
                    nombre_raw = it.get("nombre_raw", "")
                    nombre = normalizar_nombre(nombre_raw)

                    # Ignorar tablets
                    if "TAB" in nombre.upper() or "IPAD" in nombre.upper():
                        continue

                    ram, rom = extraer_ram_rom(nombre_raw)
                    if not ram or not rom:
                        # Reglas: sin memoria/capacidad -> ignorar
                        continue

                    fuente = (it.get("fuente") or "").strip() or "Tienda"
                    url_imp = (it.get("url") or "").strip()
                    if not url_imp:
                        continue

                    p_act = limpiar_precio(it.get("precio", ""))
                    p_reg = limpiar_precio(it.get("precio_old", "")) or p_act
                    if not p_act:
                        continue

                    # ✅ expandir SIEMPRE antes de guardar url_oferta_sin_acortar
                    url_oferta_sin_acortar = expandir_url(url_imp)
                    url_exp = url_oferta_sin_acortar
                    enlace_de_compra_importado = url_imp

                    # URL sin afiliado (base) según fuente
                    url_importada_sin_afiliado = url_oferta_sin_acortar
                    if fuente == "MediaMarkt":
                        url_importada_sin_afiliado = url_exp.split("?")[0]
                    elif fuente == "AliExpress Plaza":
                        url_importada_sin_afiliado = (
                            url_exp.split(".html")[0] + ".html"
                            if ".html" in url_exp else url_exp.split("?")[0]
                        )
                    elif fuente in ["PcComponentes", "Fnac", "Amazon", "Phone House"]:
                        url_importada_sin_afiliado = url_exp.split("?")[0]
                    else:
                        url_importada_sin_afiliado = url_exp.split("?")[0] if url_exp else url_exp

                    # aplicar tu afiliado
                    url_sin_acortar_con_mi_afiliado = url_importada_sin_afiliado

                    if fuente == "MediaMarkt" and AFF_MEDIAMARKT:
                        sep = "&" if "?" in url_sin_acortar_con_mi_afiliado else "?"
                        url_sin_acortar_con_mi_afiliado = url_sin_acortar_con_mi_afiliado + sep + AFF_MEDIAMARKT
                    elif fuente == "Amazon" and AFF_AMAZON:
                        sep = "&" if "?" in url_sin_acortar_con_mi_afiliado else "?"
                        url_sin_acortar_con_mi_afiliado = url_sin_acortar_con_mi_afiliado + sep + AFF_AMAZON
                    elif fuente == "Fnac" and AFF_FNAC:
                        sep = "&" if "?" in url_sin_acortar_con_mi_afiliado else "?"
                        url_sin_acortar_con_mi_afiliado = url_sin_acortar_con_mi_afiliado + sep + AFF_FNAC
                    elif fuente == "PcComponentes" and AFF_PCCOMPONENTES:
                        sep = "&" if "?" in url_sin_acortar_con_mi_afiliado else "?"
                        url_sin_acortar_con_mi_afiliado = url_sin_acortar_con_mi_afiliado + sep + AFF_PCCOMPONENTES
                    elif fuente in ["AliExpress", "AliExpress Plaza"] and AFF_ALIEXPRESS:
                        sep = "&" if "?" in url_sin_acortar_con_mi_afiliado else "?"
                        url_sin_acortar_con_mi_afiliado = url_sin_acortar_con_mi_afiliado + sep + AFF_ALIEXPRESS
                    elif fuente == "Phone House" and AFF_PHONEHOUSE:
                        sep = "&" if "?" in url_sin_acortar_con_mi_afiliado else "?"
                        url_sin_acortar_con_mi_afiliado = url_sin_acortar_con_mi_afiliado + sep + AFF_PHONEHOUSE

                    # acortar con is.gd
                    url_oferta = ""
                    try:
                        rshort = requests.get(
                            "https://is.gd/create.php",
                            params={"format": "simple", "url": url_sin_acortar_con_mi_afiliado},
                            timeout=15
                        )
                        if rshort.status_code == 200:
                            url_oferta = (rshort.text or "").strip()
                    except:
                        url_oferta = url_sin_acortar_con_mi_afiliado

                    cup = "OFERTA PROMO"
                    ver = detectar_version(nombre, fuente)
                    enviado_desde = detectar_enviado_desde(fuente)
                    enviado_desde_tg = bandera_enviado_desde(enviado_desde)
                    img_src = (it.get("imagen") or "").strip()

                    cat = nombre.split()[0] if nombre else "Móviles"
                    subcat = nombre
                    clave = (nombre.lower(), ram, rom, fuente.lower())

                    if clave not in productos_por_clave:
                        productos_por_clave[clave] = {
                            "nombre": nombre,
                            "p_act": p_act,
                            "p_reg": p_reg,
                            "ram": ram,
                            "rom": rom,
                            "ver": ver,
                            "fuente": fuente,
                            "cup": cup,
                            "url_exp": url_exp,
                            "url_oferta_sin_acortar": url_oferta_sin_acortar,
                            "url_imp": url_imp,
                            "enlace_de_compra_importado": enlace_de_compra_importado,
                            "url_importada_sin_afiliado": url_importada_sin_afiliado,
                            "url_sin_acortar_con_mi_afiliado": url_sin_acortar_con_mi_afiliado,
                            "url_oferta": url_oferta,
                            "imagen": img_src,
                            "enviado_desde": enviado_desde,
                            "enviado_desde_tg": enviado_desde_tg,
                            "cat": cat,
                            "subcat": subcat,
                            "paginas_origen": {url},
                        }

                    # log mínimo en fallback
                    print(f"✅ NEXT item: {nombre} | {fuente} | {p_act}€ | {ram}/{rom}")
                except Exception as e:
                    print(f"  ⚠️ Error procesando item NEXT: {e}")
                    continue

            # Saltamos al siguiente listado (ya procesado por NEXT)
            continue


        for buy in buy_links:
            try:
                fuente = buy.get_text(strip=True).replace("Cómpralo en", "").strip()
                url_imp = buy.get("href", "") or ""

                # Bloque contenedor (li / article / div)
                cont = buy.find_parent(["li", "article", "div"], recursive=True) or buy.parent
                bloque_txt = cont.get_text(" ", strip=True) if cont else buy.get_text(" ", strip=True)

                # 2) Nombre: primer link /telefonos/ dentro del bloque
                nombre = ""
                prod_link = None
                if cont:
                    prod_link = cont.find("a", href=re.compile(r"/telefonos/", re.I))
                if prod_link:
                    nombre = normalizar_nombre(prod_link.get_text(strip=True))

                if not nombre:
                    # Fallback: usar alt del primer img, si existe
                    img = cont.find("img") if cont else None
                    if img and img.get("alt"):
                        nombre = normalizar_nombre(img.get("alt"))

                if not nombre:
                    continue

                # Ignorar tablets (regla tuya)
                if "TAB" in nombre.upper() or "IPAD" in nombre.upper():
                    continue

                # 3) RAM/ROM (obligatorio para considerar “móvil”)
                ram, rom = extraer_ram_rom_de_bloque(bloque_txt)
                if not ram or not rom:
                    # Si no tiene memoria/capacidad, lo ignoramos
                    continue

                # 4) Precios: buscamos importes con €
                euros = re.findall(r"(\d[\d\.]*)\s*€", bloque_txt)
                p_act = limpiar_precio(euros[0]) if len(euros) >= 1 else ""
                p_reg = limpiar_precio(euros[1]) if len(euros) >= 2 else ""
                if not p_act:
                    continue
                if not p_reg:
                    p_reg = p_act

                # 5) Código cupón
                cup = "OFERTA PROMO"
                m = re.search(r"\bC[oó]digo\s+([A-Z0-9_-]+)\b", bloque_txt, re.I)
                if m:
                    cup = m.group(1).strip()

                # 6) Imagen
                img_src = ""
                img = cont.find("img") if cont else None
                if img:
                    img_src = img.get("src") or img.get("data-src") or ""

                # 7) Expandir enlace SIEMPRE antes de url_oferta_sin_acortar
                url_oferta_sin_acortar = expandir_url(url_imp)
                url_exp = url_oferta_sin_acortar
                enlace_de_compra_importado = url_imp

                # 8) URL sin afiliado (depende de la tienda real)
                url_importada_sin_afiliado = url_oferta_sin_acortar
                if fuente == "MediaMarkt":
                    url_importada_sin_afiliado = url_exp.split("?")[0]
                elif fuente == "AliExpress Plaza":
                    url_importada_sin_afiliado = (
                        url_exp.split(".html")[0] + ".html"
                        if ".html" in url_exp else url_exp.split("?")[0]
                    )
                elif fuente in ["PcComponentes", "Fnac", "Amazon", "Phone House"]:
                    url_importada_sin_afiliado = url_exp.split("?")[0]
                else:
                    url_importada_sin_afiliado = url_exp.split("?")[0] if url_exp else url_exp

                # 9) Aplicar tu afiliado
                url_sin_acortar_con_mi_afiliado = url_importada_sin_afiliado

                if fuente == "MediaMarkt" and AFF_MEDIAMARKT:
                    sep = "&" if "?" in url_sin_acortar_con_mi_afiliado else "?"
                    url_sin_acortar_con_mi_afiliado += sep + AFF_MEDIAMARKT
                elif fuente == "Amazon" and AFF_AMAZON:
                    sep = "&" if "?" in url_sin_acortar_con_mi_afiliado else "?"
                    url_sin_acortar_con_mi_afiliado += sep + AFF_AMAZON
                elif fuente == "Fnac" and AFF_FNAC:
                    sep = "&" if "?" in url_sin_acortar_con_mi_afiliado else "?"
                    url_sin_acortar_con_mi_afiliado += sep + AFF_FNAC
                elif fuente == "PcComponentes" and AFF_PCCOMPONENTES:
                    sep = "&" if "?" in url_sin_acortar_con_mi_afiliado else "?"
                    url_sin_acortar_con_mi_afiliado += sep + AFF_PCCOMPONENTES
                elif fuente in ["AliExpress", "AliExpress Plaza"] and AFF_ALIEXPRESS:
                    sep = "&" if "?" in url_sin_acortar_con_mi_afiliado else "?"
                    url_sin_acortar_con_mi_afiliado += sep + AFF_ALIEXPRESS
                elif fuente == "Phone House" and AFF_PHONEHOUSE:
                    sep = "&" if "?" in url_sin_acortar_con_mi_afiliado else "?"
                    url_sin_acortar_con_mi_afiliado += sep + AFF_PHONEHOUSE

                # 10) Acortar con is.gd
                url_oferta = ""
                try:
                    rshort = requests.get(
                        "https://is.gd/create.php",
                        params={"format": "simple", "url": url_sin_acortar_con_mi_afiliado},
                        timeout=15,
                    )
                    if rshort.status_code == 200:
                        url_oferta = (rshort.text or "").strip()
                except:
                    url_oferta = url_sin_acortar_con_mi_afiliado

                ver = detectar_version(nombre, fuente)
                enviado_desde = detectar_enviado_desde(fuente)
                enviado_desde_tg = bandera_enviado_desde(enviado_desde)

                cat = nombre.split()[0] if nombre else "Móviles"
                subcat = nombre

                # Importante: puede haber duplicados del mismo móvil y specs pero de distinta página/tienda
                clave = (nombre.lower(), ram, rom, fuente.lower())

                if clave not in productos_por_clave:
                    productos_por_clave[clave] = {
                        "nombre": nombre,
                        "p_act": p_act,
                        "p_reg": p_reg,
                        "ram": ram,
                        "rom": rom,
                        "ver": ver,
                        "fuente": fuente,
                        "cup": cup,
                        "url_exp": url_exp,
                        "url_oferta_sin_acortar": url_oferta_sin_acortar,
                        "url_imp": url_imp,
                        "enlace_de_compra_importado": enlace_de_compra_importado,
                        "url_importada_sin_afiliado": url_importada_sin_afiliado,
                        "url_sin_acortar_con_mi_afiliado": url_sin_acortar_con_mi_afiliado,
                        "url_oferta": url_oferta,
                        "imagen": img_src,
                        "enviado_desde": enviado_desde,
                        "enviado_desde_tg": enviado_desde_tg,
                        "cat": cat,
                        "subcat": subcat,
                        "paginas_origen": {url},
                    }
                else:
                    productos_por_clave[clave]["paginas_origen"].add(url)

            except Exception as e:
                print(f"  ⚠️ Error parseando item: {e}")
                continue

    return list(productos_por_clave.values())


def sincronizar(remotos):
    cache_categorias = obtener_todas_las_categorias()
    existentes = obtener_productos_existentes()

    # Conjunto de claves remotas para detectar obsoletos por importado_de + identidad (nombre+ram+rom+fuente)
    claves_remotas = set()
    for p in remotos:
        clave = f"{p['nombre']}|{p['ram']}|{p['rom']}|{p['fuente']}".lower()
        claves_remotas.add(clave)

    # 1) Crear/actualizar
    for p in remotos:
        # Buscar si ya existe un producto idéntico (según reglas del proyecto)
        encontrado_id = None
        encontrado = None

        for ex in existentes:
            meta = ex.get("meta_data", [])
            importado_de = _norm_import_id(meta_get(meta, "importado_de"))
            if importado_de != ID_IMPORTACION_NORM:
                continue

            # comparar por identidad mínima (nombre + memoria + capacidad + fuente)
            nombre = ex.get("name", "").strip()
            memoria = (meta_get(meta, "memoria") or "").strip()
            capacidad = (meta_get(meta, "capacidad") or "").strip()
            fuente = (meta_get(meta, "fuente") or "").strip()

            if (
                nombre.lower() == p["nombre"].lower()
                and memoria.lower() == p["ram"].lower()
                and capacidad.lower() == p["rom"].lower()
                and fuente.lower() == p["fuente"].lower()
            ):
                encontrado_id = ex["id"]
                encontrado = ex
                break

        # Si existe y es idéntico en TODOS los campos a comparar => ignorado
        if encontrado and producto_match(encontrado, p):
            summary_ignorados.append({"nombre": p["nombre"], "id": encontrado_id})
            continue

        # Resolver categorías
        id_cat_padre, id_cat_hijo, foto_subcat = resolver_jerarquia(p["nombre"], cache_categorias)

        # Foto: reutilizar subcat si existe, si no usar imagen del producto (si la trae)
        if foto_subcat:
            imagen_final = foto_subcat
        else:
            imagen_final = p["imagen"]

        # Payload producto
        data = {
            "name": p["nombre"],
            "type": "external",
            "status": "publish",
            "regular_price": str(p["p_reg"]),
            "sale_price": str(p["p_act"]),
            "external_url": p["url_oferta"],
            "button_text": "Comprar oferta",
            "categories": [{"id": id_cat_hijo}] if id_cat_hijo else [{"id": id_cat_padre}],
            "images": [{"src": imagen_final}] if imagen_final else [],
            "meta_data": [
                {"key": "importado_de", "value": ID_IMPORTACION_NORM},
                {"key": "memoria", "value": p["ram"]},
                {"key": "capacidad", "value": p["rom"]},
                {"key": "version", "value": p["ver"]},
                {"key": "fuente", "value": p["fuente"]},
                {"key": "imagen_producto", "value": p["imagen"]},
                {"key": "precio_actual", "value": str(p["p_act"])},
                {"key": "precio_original", "value": str(p["p_reg"])},
                {"key": "codigo_de_descuento", "value": p["cup"]},
                {"key": "enlace_de_compra_importado", "value": p.get("enlace_de_compra_importado", p.get("url_exp", ""))},
                {"key": "url_oferta_sin_acortar", "value": p.get("url_oferta_sin_acortar", p.get("url_exp", ""))},
                {"key": "url_importada_sin_afiliado", "value": p["url_importada_sin_afiliado"]},
                {"key": "url_sin_acortar_con_mi_afiliado", "value": p["url_sin_acortar_con_mi_afiliado"]},
                {"key": "url_oferta", "value": p["url_oferta"]},
                {"key": "enviado_desde", "value": p["enviado_desde"]},
                {"key": "enviado_desde_tg", "value": p["enviado_desde_tg"]},
                {"key": "fecha", "value": datetime.now().strftime("%d/%m/%Y")},
            ],
        }

        intentos = 0
        max_intentos = 10
        creado = False

        while intentos < max_intentos and not creado:
            intentos += 1
            try:
                if encontrado_id:
                    r = wcapi.put(f"products/{encontrado_id}", data)
                    if r.status_code in [200, 201]:
                        summary_actualizados.append({"nombre": p["nombre"], "id": encontrado_id, "cambios": ["precio/meta"]})
                        creado = True
                        break
                else:
                    r = wcapi.post("products", data)
                    if r.status_code in [200, 201]:
                        nuevo = r.json()
                        summary_creados.append({"nombre": p["nombre"], "id": nuevo["id"]})
                        creado = True
                        break
            except Exception:
                pass

            time.sleep(15)

        if not creado:
            print(f"❌ No se pudo crear/actualizar: {p['nombre']}")

    # 2) Eliminar obsoletos: SOLO los que son de esta importación y no están en remoto
    for ex in existentes:
        meta = ex.get("meta_data", [])
        importado_de = _norm_import_id(meta_get(meta, "importado_de"))
        if importado_de != ID_IMPORTACION_NORM:
            continue

        nombre = ex.get("name", "").strip()
        memoria = (meta_get(meta, "memoria") or "").strip()
        capacidad = (meta_get(meta, "capacidad") or "").strip()
        fuente = (meta_get(meta, "fuente") or "").strip()

        clave = f"{nombre}|{memoria}|{capacidad}|{fuente}".lower()
        if clave not in claves_remotas:
            try:
                wcapi.delete(f"products/{ex['id']}", params={"force": True})
                summary_eliminados.append({"nombre": nombre, "id": ex["id"]})
            except:
                pass

    # Resumen final
    hoy_fmt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n============================================================")
    print(f"📋 RESUMEN DE EJECUCIÓN ({hoy_fmt})")
    print(f"============================================================")
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
    for item in summary_ignorados:
        print(f"- {item['nombre']} (ID: {item['id']})")
    print(f"============================================================")


def main():
    remotos = obtener_datos_remotos()
    if remotos:
        sincronizar(remotos)
    else:
        print("No se han obtenido productos remotos.")


if __name__ == "__main__":
    main()
