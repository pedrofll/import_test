# scraper_compras.py
import os
import time
import requests
import urllib.parse
import json
import re
import base64
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
ID_AFILIADO_XIAOMI_STORE = os.environ.get("AFF_XIAOMI_STORE", "")

# Acumuladores globales
summary_creados = []
summary_eliminados = []
summary_ignorados = []
summary_actualizados = []

def limpiar_precio(texto):
    if not texto:
        return "0"
    return texto.replace("€", "").replace(".", "").replace(",", ".").strip()

def extraer_items_next_data(html: str):
    """Fallback para páginas Next.js: extrae items desde <script id="__NEXT_DATA__">.
    Devuelve una lista de dicts con claves similares a las usadas por el parser HTML.
    """
    try:
        soup = BeautifulSoup(html, "lxml")
        script = soup.find("script", id="__NEXT_DATA__")
        if not script or not script.string:
            return []
        data = json.loads(script.string)

        candidatos = []

        def walk(x):
            if isinstance(x, dict):
                keys = {str(k).lower() for k in x.keys()}
                has_name = ("name" in keys) or ("title" in keys)
                has_url = ("url" in keys) or ("href" in keys) or ("link" in keys)
                has_price = any(k in keys for k in ("price","precio","saleprice","currentprice","precio_actual"))
                if has_name and has_url and has_price:
                    candidatos.append(x)
                for v in x.values():
                    walk(v)
            elif isinstance(x, list):
                for v in x:
                    walk(v)

        walk(data)

        items = []
        for c in candidatos:
            nombre = c.get("name") or c.get("title") or ""
            url_imp = c.get("url") or c.get("href") or c.get("link") or ""
            fuente = c.get("store") or c.get("merchant") or c.get("fuente") or c.get("shop") or ""
            img = c.get("image") or c.get("imageUrl") or c.get("img") or ""
            p_act = c.get("salePrice") or c.get("currentPrice") or c.get("price") or c.get("precio_actual") or c.get("precio") or ""
            p_reg = c.get("oldPrice") or c.get("regularPrice") or c.get("precio_original") or ""
            ram = c.get("ram") or c.get("memoria") or ""
            rom = c.get("rom") or c.get("storage") or c.get("capacidad") or ""
            specs = c.get("specs") or c.get("description") or ""

            items.append({
                "raw_nombre": str(nombre),
                "img_src": str(img),
                "specs_text": str(specs),
                "ram": str(ram),
                "rom": str(rom),
                "p_act": str(p_act),
                "p_reg": str(p_reg),
                "url_imp": str(url_imp),
                "fuente": str(fuente),
            })
        return items
    except Exception:
        return []

def acortar_url(url):
    try:
        url_encoded = urllib.parse.quote(url, safe="")
        r = requests.get(f"https://is.gd/create.php?format=simple&url={url_encoded}", timeout=8)
        return r.text.strip() if r.status_code == 200 else url
    except:
        return url

def expandir_url(url: str) -> str:
    """
    Expande enlaces (redirects + acortadores + wrappers), con especial cuidado en:
      - Amazon: NO extraer del HTML (evita devolver sprites/recursos).
      - Tradedoubler PDT: pdt.tradedoubler.com/click?...url(ENCODED) -> extrae destino.
      - Awin: si aparece cread/pclick con 'ued=', se devuelve el destino real (tienda).
      - topesdg.link: intenta Location header (302) y, si no, extracción por HTML/JS.
    """
    if not url:
        return ""

    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
        "Accept-Language": "es-ES,es;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    SHORTENER_HOSTS = {
        "topesdg.link",
        "is.gd",
        "bit.ly",
        "t.co",
        "tinyurl.com",
        "cutt.ly",
        "rebrand.ly",
        "shorturl.at",
        "rb.gy",
        "amzn.to",
    }

    def _host(u: str) -> str:
        try:
            return (urllib.parse.urlparse(u).netloc or "").lower()
        except Exception:
            return ""

    def _is_amazon(h: str) -> bool:
        return ("amazon." in h) and (not h.startswith("images-") and "ssl-images-amazon" not in h)

    def _looks_like_resource(u: str) -> bool:
        ul = (u or "").lower()
        return ul.endswith((".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".css", ".js", ".ico"))

    def _extract_awin_destination(u: str) -> str:
        try:
            pu = urllib.parse.urlparse(u)
            host = (pu.netloc or "").lower()
            if "awin1.com" not in host:
                return ""
            qs = urllib.parse.parse_qs(pu.query)
            for k in ("ued", "url", "desturl", "destination"):
                if k in qs and qs[k]:
                    return urllib.parse.unquote(qs[k][0])
        except Exception:
            pass
        return ""

    def _unwrap_tradedoubler_pdt(u: str) -> str:
        # https://pdt.tradedoubler.com/click?a(3181447)p(270504)...url(ENCODED)
        try:
            if "pdt.tradedoubler.com/click" not in u:
                return u
            m = re.search(r"a\((\d+)\).*?p\((\d+)\).*?url\(([^)]+)\)", u)
            if not m:
                return u
            dest = urllib.parse.unquote(m.group(3))
            return dest if dest.startswith("http") else u
        except Exception:
            return u

    def _unwrap_query_wrappers(u: str) -> str:
        try:
            p = urllib.parse.urlparse(u)
            host = (p.netloc or "").lower()
            qs = urllib.parse.parse_qs(p.query)
            if "tradedoubler" in host and "url" in qs and qs["url"]:
                return urllib.parse.unquote(qs["url"][0])
            if "tradetracker" in host and "u" in qs and qs["u"]:
                return urllib.parse.unquote(qs["u"][0])
        except Exception:
            pass
        return u

    def _extract_from_html(base_url: str, html: str) -> str:
        # Awin embed
        m = re.search(r'https?://www\.awin1\.com/(?:cread|pclick)\.php\?[^"\']+', html, re.I)
        if m:
            aw = m.group(0).strip()
            dest = _extract_awin_destination(aw)
            if dest:
                return dest

        # canonical
        m = re.search(r'rel=["\']canonical["\'][^>]*href=["\']([^"\']+)["\']', html, re.I)
        if m:
            return urllib.parse.urljoin(base_url, m.group(1).strip())

        # og:url
        m = re.search(r'property=["\']og:url["\'][^>]*content=["\']([^"\']+)["\']', html, re.I)
        if m:
            return urllib.parse.urljoin(base_url, m.group(1).strip())

        # meta refresh
        m = re.search(r'http-equiv=["\']refresh["\'][^>]*content=["\'][^"\']*url=([^"\']+)["\']', html, re.I)
        if m:
            return urllib.parse.urljoin(base_url, m.group(1).strip())

        # JS redirects
        for pat in [
            r'(?:window\.location|location\.href|document\.location)\s*=\s*["\']([^"\']+)["\']',
            r'location\.replace\(\s*["\']([^"\']+)["\']\s*\)',
        ]:
            m = re.search(pat, html, re.I)
            if m:
                return urllib.parse.urljoin(base_url, m.group(1).strip())

        # atob('...') base64
        m = re.search(r'atob\(\s*["\']([^"\']+)["\']\s*\)', html, re.I)
        if m:
            try:
                b64 = m.group(1).strip()
                pad = '=' * (-len(b64) % 4)
                decoded = base64.b64decode((b64 + pad).encode('utf-8', 'ignore')).decode('utf-8', 'ignore')
                mm = re.search(r'https?://[^\s"\']+', decoded, re.I)
                if mm:
                    return mm.group(0).strip()
            except Exception:
                pass

        # decodeURIComponent('...')
        m = re.search(r'decodeURIComponent\(\s*["\']([^"\']+)["\']\s*\)', html, re.I)
        if m:
            try:
                dec = urllib.parse.unquote(m.group(1).strip())
                mm = re.search(r'https?://[^\s"\']+', dec, re.I)
                if mm:
                    return mm.group(0).strip()
            except Exception:
                pass

        # URLs escapadas (https:\/\/...)
        m = re.search(r'https?:\\/\\/[^\s"\']+', html, re.I)
        if m:
            return m.group(0).replace('\\/','/').replace('\\','').strip()

        # Protocolo relativo //...
        m = re.search(r'//[^\s"\']+', html, re.I)
        if m:
            return "https:" + m.group(0).strip()

        # fallback URL razonable
        for cand in re.findall(r'https?://[^\s"\']+', html):
            c = cand.strip()
            if _looks_like_resource(c):
                continue
            ch = _host(c)
            if "ssl-images-amazon" in ch or ch.startswith("images-"):
                continue
            return c

        return base_url

    s = requests.Session()
    current = _unwrap_tradedoubler_pdt(url)

    for _ in range(10):
        current = _unwrap_query_wrappers(current)
        h_cur = _host(current)

        # Preflight sin redirects (Location)
        if h_cur in SHORTENER_HOSTS:
            try:
                r0 = s.get(current, headers=headers, allow_redirects=False, timeout=20)
                loc = r0.headers.get("Location") or r0.headers.get("location")
                if loc:
                    current = urllib.parse.urljoin(current, loc)
                    aw = _extract_awin_destination(current)
                    if aw and not _looks_like_resource(aw):
                        return aw
                    continue
            except Exception:
                pass

        try:
            r = s.get(current, headers=headers, allow_redirects=True, timeout=25)
        except Exception:
            return current

        final_url = getattr(r, "url", "") or current

        # Awin destino directo
        aw = _extract_awin_destination(final_url)
        if aw and not _looks_like_resource(aw):
            return aw

        unwrapped = _unwrap_query_wrappers(final_url)
        if unwrapped != final_url:
            current = unwrapped
            continue

        h_final = _host(final_url)

        if _is_amazon(h_final) or _is_amazon(h_cur):
            return final_url

        content_type = (r.headers.get("Content-Type") or "").lower()
        if final_url == current and r.status_code == 200 and ("text/html" in content_type or "text/plain" in content_type or content_type == ""):
            if h_final in SHORTENER_HOSTS or any(x in h_final for x in ["tradedoubler", "tradetracker", "awin1.com"]):
                extracted = _extract_from_html(final_url, r.text or "")
                if extracted and extracted != current and not _looks_like_resource(extracted):
                    aw2 = _extract_awin_destination(extracted)
                    if aw2 and not _looks_like_resource(aw2):
                        return aw2
                    current = extracted
                    continue

        return final_url

    return current


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
        if cat['name'].lower() == nombre_padre.lower() and cat['parent'] == 0:
            id_cat_padre = cat['id']
            break
    if not id_cat_padre:
        res = wcapi.post("products/categories", {"name": nombre_padre}).json()
        id_cat_padre = res.get('id')
        cache_categorias.append(res)

    for cat in cache_categorias:
        if cat['name'].lower() == nombre_hijo.lower() and cat['parent'] == id_cat_padre:
            id_cat_hijo = cat['id']
            if cat.get('image') and cat['image'].get('src'):
                foto_final = cat['image']['src']
            break
    if not id_cat_hijo:
        res = wcapi.post("products/categories", {"name": nombre_hijo, "parent": id_cat_padre}).json()
        id_cat_hijo = res.get('id')
        cache_categorias.append(res)
    
    return id_cat_padre, id_cat_hijo, foto_final

# --- FASE 1: SCRAPING ---
def obtener_datos_remotos():
    print("--- FASE 1: ESCANEANDO COMPRAS SMARTPHONE ---")

    def _label_pagina(url: str) -> str:
        try:
            path = urllib.parse.urlparse(url).path.rstrip("/")
            if not path:
                return "root"
            # /ofertas, /ofertas/apple, /ofertas/samsung, ...
            if path.endswith("/ofertas"):
                return "ofertas"
            return path.split("/")[-1] or "ofertas"
        except Exception:
            return "ofertas"

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "es-ES,es;q=0.9",
    }

    # Deduplicamos por (nombre + ram + rom + fuente) para evitar dobles altas si el mismo producto aparece
    # en varias páginas (ofertas + marca, etc.). Para trazabilidad, acumulamos el/los orígenes en 'paginas_origen'.
    productos_por_clave = {}
    fuentes_6_principales = ["MediaMarkt", "AliExpress Plaza", "PcComponentes", "Fnac", "Amazon", "Phone House"]

    if not URLS_PAGINAS:
        print("ERROR: No hay URLs configuradas. Define SOURCE_URL_COMPRAS o COMPRAS_URLS.")
        return []

    for idx, url_listado in enumerate(URLS_PAGINAS, start=1):
        label = _label_pagina(url_listado)
        print("-" * 60)
        print(f"Escaneando listado ({idx}/{len(URLS_PAGINAS)}): {url_listado}")

        try:
            r = requests.get(url_listado, headers=headers, timeout=30)
            soup = BeautifulSoup(r.text, "lxml")
            items = soup.select("ul.grid li")
            print(f"✅ Items detectados: {len(items)}")

            # Fallback Next.js: si el HTML no trae <li> (hidrata por JS), sacamos datos de __NEXT_DATA__
            items_json = []
            if len(items) == 0:
                items_json = extraer_items_next_data(r.text)
                print(f"✅ Items detectados (__NEXT_DATA__): {len(items_json)}")

            for item in items:
                try:
                    link_el = item.select_one("a.text-white")
                    if not link_el:
                        continue

                    raw_nombre = link_el.get_text(strip=True)
                    nombre = ' '.join(w[:1].upper() + w[1:] for w in raw_nombre.split())

                    if any(k in nombre.upper() for k in ["TAB", "IPAD", "PAD"]):
                        continue

                    img = item.select_one("img")
                    img_src = img["src"] if img else ""
                    if "url=" in img_src:
                        parsed_img = urllib.parse.parse_qs(urllib.parse.urlparse(img_src).query)
                        if "url" in parsed_img:
                            img_src = parsed_img["url"][0]

                    specs_text = item.select_one("p.text-sm").get_text(strip=True) if item.select_one("p.text-sm") else ""
                    parts = specs_text.split("·")[0].replace("GB", "").split("/")
                    if len(parts) < 2:
                        continue

                    ram_part = parts[0].strip()
                    rom_part = parts[1].strip()
                    ram = ram_part if "TB" in ram_part else f"{ram_part} GB"
                    rom = rom_part if "TB" in rom_part else f"{rom_part} GB"

                    p_act = limpiar_precio(item.select_one("p.text-fluor-green").get_text(strip=True))
                    p_reg = limpiar_precio(item.select_one("span.line-through").get_text(strip=True)) if item.select_one("span.line-through") else p_act

                    btn = item.select_one("a.bg-fluor-green")
                    url_imp = btn["href"] if btn else ""
                    url_exp = expandir_url(url_imp)

                    fuente = btn.get_text(strip=True).replace("Cómpralo en", "").strip() if btn else "Tienda"
                    url_importada_sin_afiliado = url_exp

                    # Normalización de URL sin parámetros según fuente
                    if fuente == "MediaMarkt":
                        url_importada_sin_afiliado = url_exp.split("?")[0]
                    elif fuente == "AliExpress Plaza":
                        url_importada_sin_afiliado = (
                            url_exp.split(".html")[0] + ".html" if ".html" in url_exp else url_exp.split("?")[0]
                        )
                    elif fuente in ["PcComponentes", "Fnac", "Amazon", "Phone House"]:
                        url_importada_sin_afiliado = url_exp.split("?")[0]
                    elif fuente == "Xiaomi Store":
                        url_importada_sin_afiliado = url_exp.split("?")[0]
                    else:
                        url_importada_sin_afiliado = url_exp

                    # Construir URL con afiliado usando variables de entorno
                    if fuente == "MediaMarkt" and ID_AFILIADO_MEDIAMARKT:
                        url_sin_acortar_con_mi_afiliado = f"{url_importada_sin_afiliado}{ID_AFILIADO_MEDIAMARKT}"
                    elif fuente == "AliExpress Plaza" and ID_AFILIADO_ALIEXPRESS:
                        url_sin_acortar_con_mi_afiliado = f"{url_importada_sin_afiliado}{ID_AFILIADO_ALIEXPRESS}"
                    elif fuente == "Fnac" and ID_AFILIADO_FNAC:
                        url_sin_acortar_con_mi_afiliado = f"{url_importada_sin_afiliado}{ID_AFILIADO_FNAC}"
                    elif fuente == "Amazon" and ID_AFILIADO_AMAZON:
                        url_sin_acortar_con_mi_afiliado = f"{url_importada_sin_afiliado}{ID_AFILIADO_AMAZON}"
                    elif fuente == "Xiaomi Store" and ID_AFILIADO_XIAOMI_STORE:
                        url_sin_acortar_con_mi_afiliado = f"{url_importada_sin_afiliado}{ID_AFILIADO_XIAOMI_STORE}"
                    else:
                        url_sin_acortar_con_mi_afiliado = url_importada_sin_afiliado

                    url_oferta = acortar_url(url_sin_acortar_con_mi_afiliado)

                    # Enviado desde
                    tiendas_espana = [
                        "pccomponentes",
                        "aliexpress plaza",
                        "mediamarkt",
                        "amazon",
                        "fnac",
                        "phone house",
                        "powerplanet",
                    ]
                    enviado_desde = ""
                    if fuente.lower() in tiendas_espana or "Desde España" in item.get_text():
                        enviado_desde = "España"

                    enviado_desde_tg = ""
                    if enviado_desde == "España":
                        enviado_desde_tg = "🇪🇸 España"
                    elif enviado_desde == "Europa":
                        enviado_desde_tg = "🇪🇺 Europa"
                    elif enviado_desde == "China":
                        enviado_desde_tg = "🇨🇳 China"

                    if fuente not in fuentes_6_principales:
                        ver = "Global Version"
                    else:
                        ver = "Versión Global" if "Global" in item.get_text() or "Desde España" in item.get_text() else "N/A"

                    cup = (
                        item.select_one("button.border-fluor-green").get_text(strip=True).replace("Código", "").strip()
                        if item.select_one("button.border-fluor-green")
                        else "OFERTA PROMO"
                    )

                    # --- LOGS DETALLADOS SOLICITADOS ---
                    print(f"Detectado {nombre}")
                    print(f"1) Nombre: {nombre}")
                    print(f"2) Memoria: {ram}")
                    print(f"3) Capacidad: {rom}")
                    print(f"4) Versión: {ver}")
                    print(f"5) Fuente: {fuente}")
                    print(f"6) Precio actual: {p_act}")
                    print(f"7) Precio original: {p_reg}")
                    print(f"8) Código de descuento: {cup}")
                    print(f"9) Version: {ver}")
                    print(f"10) URL Imagen: {img_src}")
                    print(f"11) Enlace Importado: {url_imp}")
                    print(f"12) Enlace Expandido: {url_exp}")
                    print(f"13) URL importada sin afiliado: {url_importada_sin_afiliado}")
                    print(f"14) URL sin acortar con mi afiliado: {url_sin_acortar_con_mi_afiliado}")
                    print(f"15) URL acortada con mi afiliado: {url_oferta}")
                    print(f"16) Enviado desde: {enviado_desde}")
                    print(f"17) Encolado para comparar con base de datos...")
                    print("-" * 60)
                    # -----------------------------------

                    clave = f"{nombre}|{ram}|{rom}|{fuente}".lower()
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
                            "url_imp": url_imp,
                            "url_importada_sin_afiliado": url_importada_sin_afiliado,
                            "url_sin_acortar_con_mi_afiliado": url_sin_acortar_con_mi_afiliado,
                            "url_oferta": url_oferta,
                            "imagen": img_src,
                            "enviado_desde": enviado_desde,
                            "enviado_desde_tg": enviado_desde_tg,
                            "paginas_origen": {label},
                        }
                    else:
                        # Si ya existía, agregamos origen adicional para trazabilidad.
                        productos_por_clave[clave].setdefault("paginas_origen", set()).add(label)

                except Exception:
                    continue
            # Procesar items extraídos de __NEXT_DATA__ (cuando el HTML no trae el grid)
            for data in items_json:
                try:
                    raw_nombre = (data.get("raw_nombre") or "").strip()
                    if not raw_nombre:
                        continue
                    nombre = ' '.join(w[:1].upper() + w[1:] for w in raw_nombre.split())

                    if any(k in nombre.upper() for k in ["TAB", "IPAD", "PAD"]):
                        continue

                    img_src = (data.get("img_src") or "").strip()

                    specs_text = (data.get("specs_text") or "").strip()

                    # RAM/ROM: si vienen directas en JSON, usamos esas; si no, intentamos parsear como en HTML
                    ram_raw = (data.get("ram") or "").strip()
                    rom_raw = (data.get("rom") or "").strip()

                    if ram_raw and rom_raw:
                        ram_part = ram_raw.replace("GB", "").strip()
                        rom_part = rom_raw.replace("GB", "").strip()
                    else:
                        parts = specs_text.split("·")[0].replace("GB", "").split("/")
                        if len(parts) < 2:
                            continue
                        ram_part = parts[0].strip()
                        rom_part = parts[1].strip()

                    ram = ram_part if "TB" in ram_part.upper() else f"{ram_part} GB"
                    rom = rom_part if "TB" in rom_part.upper() else f"{rom_part} GB"

                    p_act = limpiar_precio(str(data.get("p_act") or ""))
                    p_reg = limpiar_precio(str(data.get("p_reg") or "")) or p_act
                    if not p_act:
                        continue

                    url_imp = (data.get("url_imp") or "").strip()
                    if not url_imp:
                        continue
                    url_exp = expandir_url(url_imp)

                    fuente = (data.get("fuente") or "").strip() or "Tienda"
                    url_importada_sin_afiliado = url_exp

                    # Normalización de URL sin parámetros según fuente
                    if fuente == "MediaMarkt":
                        url_importada_sin_afiliado = url_exp.split("?")[0]
                    elif fuente == "AliExpress Plaza":
                        url_importada_sin_afiliado = (
                            url_exp.split(".html")[0] + ".html" if ".html" in url_exp else url_exp.split("?")[0]
                        )
                    elif fuente in ["PcComponentes", "Fnac", "Amazon", "Phone House", "El Corte Inglés"]:
                        url_importada_sin_afiliado = url_exp.split("?")[0]
                    else:
                        url_importada_sin_afiliado = url_exp.split("?")[0] if url_exp else url_exp

                    cup = "OFERTA PROMO"
                    ver = detectar_version(nombre, fuente)
                    enviado_desde = detectar_enviado_desde(fuente)
                    enviado_desde_tg = bandera_enviado_desde(enviado_desde)

                    # Construir URL con afiliado usando variables de entorno (mismo comportamiento que el parser HTML)
                    if fuente == "MediaMarkt" and ID_AFILIADO_MEDIAMARKT:
                        url_sin_acortar_con_mi_afiliado = f"{url_importada_sin_afiliado}{ID_AFILIADO_MEDIAMARKT}"
                    elif fuente == "AliExpress Plaza" and ID_AFILIADO_ALIEXPRESS:
                        url_sin_acortar_con_mi_afiliado = f"{url_importada_sin_afiliado}{ID_AFILIADO_ALIEXPRESS}"
                    elif fuente == "Fnac" and ID_AFILIADO_FNAC:
                        url_sin_acortar_con_mi_afiliado = f"{url_importada_sin_afiliado}{ID_AFILIADO_FNAC}"
                    elif fuente == "Amazon" and ID_AFILIADO_AMAZON:
                        url_sin_acortar_con_mi_afiliado = f"{url_importada_sin_afiliado}{ID_AFILIADO_AMAZON}"
                    elif fuente == "Xiaomi Store" and ID_AFILIADO_XIAOMI_STORE:
                        url_sin_acortar_con_mi_afiliado = f"{url_importada_sin_afiliado}{ID_AFILIADO_XIAOMI_STORE}"
                    else:
                        url_sin_acortar_con_mi_afiliado = url_importada_sin_afiliado

                    url_oferta = acortar_url(url_sin_acortar_con_mi_afiliado)

                    clave = f"{nombre}|{ram}|{rom}|{fuente}".lower()
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
                            "url_imp": url_imp,
                            "url_importada_sin_afiliado": url_importada_sin_afiliado,
                            "url_sin_acortar_con_mi_afiliado": url_sin_acortar_con_mi_afiliado,
                            "url_oferta": url_oferta,
                            "imagen": img_src,
                            "enviado_desde": enviado_desde,
                            "enviado_desde_tg": enviado_desde_tg,
                        }
                except Exception:
                    continue


        except Exception as e:
            print(f"❌ ERROR escaneando listado '{url_listado}': {e}")
            continue

    productos_lista = []
    for p in productos_por_clave.values():
        paginas = p.get("paginas_origen")
        if isinstance(paginas, set):
            p["paginas_origen"] = ",".join(sorted(paginas))
        productos_lista.append(p)

    return productos_lista

# --- FASE 2: SINCRONIZACIÓN ---
def sincronizar(remotos):
    print("\n--- FASE 2: Sincronizando con WooCommerce ---")
    cache_categorias = obtener_todas_las_categorias()
    locales_wc = []
    page = 1
    while True:
        try:
            res = wcapi.get("products", params={"per_page": 100, "page": page, "status": "any"}).json()
            if not res or len(res) == 0:
                break
            locales_wc.extend(res)
            page += 1
        except:
            break

    propios_en_wc = []
    for p in locales_wc:
        meta = {m.get('key'): m.get('value') for m in (p.get('meta_data') or []) if isinstance(m, dict)}
        imp = _norm_import_id(str(meta.get('importado_de', '') or ''))
        if imp == ID_IMPORTACION_NORM:
            propios_en_wc.append(p)

    for local in propios_en_wc:
        meta = {m['key']: str(m['value']) for m in local.get('meta_data', [])}
        
        match_remoto = next((r for r in remotos if r['nombre'].lower() == local['name'].lower() and 
                             str(r['ram']).lower() == str(meta.get('memoria')).lower() and 
                             str(r['rom']).lower() == str(meta.get('capacidad')).lower() and 
                             str(r['fuente']).lower() == str(meta.get('fuente')).lower()), None)
        
        if match_remoto:
            cambios = []
            update_data = {"meta_data": []}
            
            try:
                if float(match_remoto['p_act']) != float(meta.get('precio_actual', 0)):
                    cambios.append(f"precio_actual ({meta.get('precio_actual')} -> {match_remoto['p_act']})")
                    update_data["sale_price"] = str(match_remoto['p_act'])
                    update_data["meta_data"].append({"key": "precio_actual", "value": str(match_remoto['p_act'])})
            except Exception:
                pass
            
            try:
                if float(match_remoto['p_reg']) != float(meta.get('precio_original', 0)):
                    cambios.append(f"precio_original ({meta.get('precio_original')} -> {match_remoto['p_reg']})")
                    update_data["regular_price"] = str(match_remoto['p_reg'])
                    update_data["meta_data"].append({"key": "precio_original", "value": str(match_remoto['p_reg'])})
            except Exception:
                pass

            if match_remoto['enviado_desde_tg'] != meta.get('enviado_desde_tg'):
                cambios.append(f"enviado_desde_tg ({meta.get('enviado_desde_tg')} -> {match_remoto['enviado_desde_tg']})")
                update_data["meta_data"].append({"key": "enviado_desde_tg", "value": match_remoto['enviado_desde_tg']})
            
            if cambios:
                wcapi.put(f"products/{local['id']}", update_data)
                summary_actualizados.append({"nombre": local['name'], "id": local['id'], "cambios": cambios})
                print(f"🔄 ACTUALIZADO -> {local['name']} (ID: {local['id']})")
            else:
                summary_ignorados.append({"nombre": local['name'], "id": local['id']})
            
            remotos.remove(match_remoto)
        else:
            wcapi.delete(f"products/{local['id']}", params={"force": True})
            summary_eliminados.append({"nombre": local['name'], "id": local['id']})
            print(f"🗑️ ELIMINADO -> {local['name']} (ID: {local['id']})")

    for p in remotos:
        id_cat_padre, id_cat_hijo, _ = resolver_jerarquia(p['nombre'], cache_categorias)
        data = {
            "name": p['nombre'], "type": "simple", "status": "publish", "regular_price": str(p['p_reg']), "sale_price": str(p['p_act']),
            "categories": [{"id": id_cat_padre}, {"id": id_cat_hijo}] if id_cat_hijo else [{"id": id_cat_padre}],
            "images": [{"src": p['imagen']}] if p['imagen'] else [],
            "meta_data": [
                {"key": "importado_de", "value": ID_IMPORTACION_NORM},
                {"key": "memoria", "value": p['ram']},
                {"key": "capacidad", "value": p['rom']},
                {"key": "version", "value": p['ver']},
                {"key": "fuente", "value": p['fuente']},
                {"key": "precio_actual", "value": str(p['p_act'])},
                {"key": "precio_original", "value": str(p['p_reg'])},
                {"key": "codigo_de_descuento", "value": p['cup']},
                {"key": "enlace_de_compra_importado", "value": p['url_imp']},
                {"key": "url_oferta_sin_acortar", "value": p['url_exp']},
                {"key": "url_importada_sin_afiliado", "value": p['url_importada_sin_afiliado']},
                {"key": "url_sin_acortar_con_mi_afiliado", "value": p['url_sin_acortar_con_mi_afiliado']},
                {"key": "url_oferta", "value": p['url_oferta']},
                {"key": "enviado_desde", "value": p['enviado_desde']},
                {"key": "enviado_desde_tg", "value": p['enviado_desde_tg']}
            ]
        }

        intentos = 0
        max_intentos = 10
        creado = False
        
        while intentos < max_intentos and not creado:
            intentos += 1
            print(f"    ⏳ Intentando crear {p['nombre']} (Intento {intentos}/{max_intentos})...", flush=True)
            try:
                res = wcapi.post("products", data)
                if res.status_code in [200, 201]:
                    prod_res = res.json()
                    new_id = prod_res['id']
                    product_url = prod_res.get('permalink')

                    # Acortar URL del post en la web propia si existe
                    url_post_acortada = acortar_url(product_url) if product_url else ""
                    if url_post_acortada:
                        wcapi.put(f"products/{new_id}", {
                            "meta_data": [{"key": "url_post_acortada", "value": url_post_acortada}]
                        })

                    summary_creados.append({"nombre": p['nombre'], "id": new_id})
                    print(f"✅ CREADO -> {p['nombre']} (ID: {new_id})")
                    creado = True
                else:
                    print(f"⚠️ Error {res.status_code} al crear {p['nombre']}. Reintentando...", flush=True)
            except Exception as e:
                print(f"❌ Excepción durante la creación. Reintentando...", flush=True)
            
            time.sleep(60)

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
