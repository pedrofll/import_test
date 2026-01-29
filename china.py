import requests
from bs4 import BeautifulSoup
from woocommerce import API
import os
import time
import re
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from urllib.parse import urlparse, parse_qs, unquote, quote
from collections import defaultdict

# --- CONFIGURACIÓN WORDPRESS ---
wcapi = API(
    url=os.environ.get("WP_URL", ""),
    consumer_key=os.environ.get("WP_KEY", ""),
    consumer_secret=os.environ.get("WP_SECRET", ""),
    version="wc/v3",
    timeout=60
)

# URL origen oculta en secret
URL_ORIGEN = os.environ.get("SOURCE_URL_CHINABAY", "")

# ✅ Identificador estable para inventario (no debe depender de URL_ORIGEN)
# Según tu spec: c) https://chinabay.es
IMPORTADO_DE_FIJO = "https://chinabay.es"

# --- PARÁMETROS DE AFILIADO (desde secrets) ---
ID_AFILIADO_ALIEXPRESS = os.environ.get("AFF_ALIEXPRESS", "")
ID_AFILIADO_TRADINGSENZHEN = os.environ.get("AFF_TRADINGSENZHEN", "")
ID_AFILIADO_MEDIAMARKT = os.environ.get("AFF_MEDIAMARKT", "")
ID_AFILIADO_DHGATE = os.environ.get("AFF_DHGATE", "")
ID_AFILIADO_AMAZON = os.environ.get("AFF_AMAZON", "")
ID_AFILIADO_PHONE_HOUSE = os.environ.get("AFF_PHONEHOUSE", "")
ID_AFILIADO_FNAC = os.environ.get("AFF_FNAC", "")
ID_AFILIADO_XIAOMI_STORE = os.environ.get("AFF_XIAOMI_STORE", "")
ID_AFILIADO_ELCORTEINGLES = os.environ.get("AFF_ELCORTEINGLES", "")

# --- LÓGICA "ENVIADO DESDE" (ACF) ---
TIENDAS_ESPANA = ["pccomponentes", "aliexpress plaza", "aliexpress", "mediamarkt", "amazon", "fnac", "phone house", "powerplanet"]
TIENDAS_CHINA = ["gshopper", "dhgate", "banggood"]

def _detectar_eu_warehouse_tradingshenzhen(candidate_urls):
    """Devuelve True si detecta 'EU Warehouse' para Tradingshenzhen; False si no; None si no pudo verificar."""
    if not candidate_urls:
        return None
    headers = {'User-Agent': 'Mozilla/5.0'}
    for u in candidate_urls:
        if not u:
            continue
        try:
            # 1) Si es página interna de Chinabay, buscamos el texto del botón que apunta a Tradingshenzhen
            if "chinabay.es" in u and "/wp-" not in u:
                r = requests.get(u, headers=headers, timeout=15)
                soup = BeautifulSoup(r.text, 'lxml')
                for a in soup.select("a.elementor-button-link"):
                    href = a.get("href", "") or ""
                    txt = a.get_text(" ", strip=True)
                    if "tradingshenzhen" in href.lower() or "tradingshenzhen" in txt.lower():
                        if "eu warehouse" in txt.lower():
                            return True
                # Si llegamos aquí, no se encontró el indicador
                return False

            # 2) Si es URL directa de Tradingshenzhen, buscamos el texto en la página (fallback)
            if "tradingshenzhen" in u.lower():
                r = requests.get(u, headers=headers, timeout=15)
                if "eu warehouse" in r.text.lower():
                    return True
                return False
        except Exception:
            continue
    return None

def calcular_enviado_desde(fuente, texto_item="", candidate_urls=None):
    """Calcula enviado_desde y enviado_desde_tg usando fuente y (opcionalmente) señales del texto/URLs."""
    fuente_lower = (fuente or "").strip().lower()
    texto_lower = (texto_item or "").lower()

    enviado_desde = ""
    if fuente_lower in TIENDAS_ESPANA or ("desde españa" in texto_lower):
        enviado_desde = "España"
    elif fuente_lower in TIENDAS_CHINA:
        enviado_desde = "China"
    elif fuente_lower == "tradingshenzhen":
        eu = _detectar_eu_warehouse_tradingshenzhen(candidate_urls or [])
        if eu is True:
            enviado_desde = "Europa"
        else:
            enviado_desde = "China"

    enviado_desde_tg = ""
    if enviado_desde == "España":
        enviado_desde_tg = "🇪🇸 España"
    elif enviado_desde == "Europa":
        enviado_desde_tg = "🇪🇺 Europa"
    elif enviado_desde == "China":
        enviado_desde_tg = "🇨🇳 China"

    return enviado_desde, enviado_desde_tg

# --- FUNCIONES AUXILIARES ---
def normalize_text(text):
    if not text:
        return ""
    text = str(text).strip()
    text = text.replace("🇨🇳", "").replace("🇫🇷", "").replace("🇪🇸", "").replace("🇮🇹", "").replace("🇩🇪", "").replace("🇪🇺", "")
    words = text.lower().split()
    capitalized_words = [w.capitalize() for w in words]
    text = " ".join(capitalized_words)
    text = re.sub(r'(\d)([a-z])', lambda m: m.group(1) + m.group(2).upper(), text)
    text = re.sub(r'\bgt\b', 'GT', text, flags=re.IGNORECASE)
    text = re.sub(r'\bfe\b', 'FE', text, flags=re.IGNORECASE)
    text = re.sub(r'\bse\b', 'SE', text, flags=re.IGNORECASE)
    text = re.sub(r'\bpro\+\b', 'Pro+', text, flags=re.IGNORECASE)
    text = re.sub(r'\bultra\b', 'Ultra', text, flags=re.IGNORECASE)
    text = text.replace("Gb", "GB").replace("Tb", "TB").replace("Cn", "CN")
    return text.strip()

def analizar_nombre_y_categoria(raw_brand_h2, raw_model_h3):
    full_raw = f"{raw_brand_h2} {raw_model_h3}".strip()
    nombre_limpio = normalize_text(full_raw)
    partes = nombre_limpio.split()
    if len(partes) > 1 and partes[0].lower() == partes[1].lower():
        partes.pop(0)
        nombre_limpio = " ".join(partes)
    primera_palabra = partes[0]
    marca = primera_palabra
    nombre_final = nombre_limpio
    es_vivo = False
    if primera_palabra.lower() == 'iqoo':
        es_vivo = True
    if re.match(r'^x\d+', primera_palabra.lower()):
        es_vivo = True
    if es_vivo:
        marca = "Vivo"
        if primera_palabra.lower() != 'vivo':
            nombre_final = f"Vivo {nombre_limpio}"
    if not es_vivo and primera_palabra.lower() == 'vivo':
        marca = "Vivo"
    return marca, nombre_final

def resolver_redireccion_http(url_corta):
    if not url_corta:
        return ""
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        r = requests.get(url_corta, allow_redirects=True, headers=headers, timeout=20, stream=True)
        return r.url
    except Exception:
        return url_corta

def procesar_afiliados_inteligente(url):
    if not url:
        return ""
    url_lower = url.lower()
    if "amazon" in url_lower or "amzn.to" in url_lower:
        if "?" in url:
            url = url.split("?")[0]
        return url
    if "dhgate.com" in url_lower:
        if '.html' in url:
            url_base = url.split('.html')[0] + '.html'
        else:
            url_base = url.split('?')[0]
        return f"{url_base}{ID_AFILIADO_DHGATE}"
    if 'mediamarkt.es' in url_lower:
        if '.html' in url:
            url_base = url.split('.html')[0] + '.html'
        else:
            url_base = url.split('?')[0]
        return f"{url_base}{ID_AFILIADO_MEDIAMARKT}"
    if 'tradetracker.net' in url:
        try:
            parsed = urlparse(url)
            qs = parse_qs(parsed.query)
            if 'u' in qs:
                url = unquote(qs['u'][0])
        except:
            pass
    if 'tradingshenzhen.com' in url:
        return url.split("?")[0] + ID_AFILIADO_TRADINGSENZHEN
    if 'aliexpress' in url_lower:
        match_id = re.search(r'(\d{10,})', url)
        if match_id:
            id_producto = match_id.group(1)
            url_base_limpia = f"https://es.aliexpress.com/item/{id_producto}.html"
            return f"{url_base_limpia}{ID_AFILIADO_ALIEXPRESS}"
        parsed = urlparse(url)
        url_base = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        return f"{url_base}{ID_AFILIADO_ALIEXPRESS}"
    return url

def analizar_pagina_interna(url_interna):
    fuente_detectada = "Chinabay"
    version_detectada = ""
    url_final = url_interna
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        r = requests.get(url_interna, headers=headers, timeout=15)
        soup = BeautifulSoup(r.text, 'lxml')
        candidato = ""
        botones = soup.select("a.elementor-button-link")
        for btn in botones:
            if "COMPRAR AQUI" in btn.get_text(strip=True).upper():
                candidato = btn.get('href', '')
                break
        if not candidato:
            for btn in botones:
                href = btn.get('href', '')
                if not href:
                    continue
                if "s.click.aliexpress.com" in href or "s.zbanx.com" in href:
                    candidato = href
                    break
                if any(x in href for x in ["chinabay.pro", "tradingshenzhen", "amazon", "amzn.to", "mediamarkt", "dhgate"]):
                    candidato = href
        if candidato:
            url_final = candidato
        else:
            fallback = soup.select_one("a[href*='s.click.aliexpress.com']") or soup.select_one("a[href*='s.zbanx.com']")
            if fallback:
                url_final = fallback['href']
        elementos_texto = soup.select("p.elementor-heading-title, h2, h3, div.elementor-widget-heading")
        for elem in elementos_texto:
            txt = elem.get_text(strip=True)
            txt_lower = txt.lower()
            if any(k in txt_lower for k in ["store", "shop", "oficial", "tradingshenzhen", "aliexpress", "amazon", "miravia", "eleczone", "mediamarkt", "dhgate"]):
                if len(txt) < 60 and "envio" not in txt_lower:
                    if "🇨🇳" in txt or "cn" in txt_lower.split():
                        version_detectada = "CN"
                    elif any(x in txt_lower for x in ["🇪🇸", "es", "🇪🇺", "global"]):
                        version_detectada = "Versión Global"
                    nombre_limpio = txt
                    for f in ["🇨🇳", "🇪🇸", "🇪🇺", "🇫🇷", "🇮🇹"]:
                        nombre_limpio = nombre_limpio.replace(f, "")
                    nombre_limpio = re.sub(r'\s(CN|ES|EU)$', '', nombre_limpio, flags=re.IGNORECASE)
                    fuente_detectada = normalize_text(nombre_limpio)
                    break
    except:
        pass
    return fuente_detectada, version_detectada, url_final

def acortar_url(url):
    if not url:
        return ""
    try:
        api_url = f"https://is.gd/create.php?format=simple&url={quote(url)}"
        r = requests.get(api_url, timeout=10)
        if r.status_code == 200 and "http" in r.text:
            return r.text.strip()
    except:
        pass
    return url

def formatear_precio_str(precio_float):
    if precio_float.is_integer():
        return str(int(precio_float))
    return str(precio_float)

def generar_url_puente(url_imagen_original):
    if not url_imagen_original:
        return ""
    try:
        headers = {'User-Agent': 'Mozilla/5.0', 'Referer': URL_ORIGEN or ''}
        r = requests.get(url_imagen_original, headers=headers, timeout=15)
        if r.status_code != 200:
            return ""
        catbox_url = "https://catbox.moe/user/api.php"
        files = {'fileToUpload': ('image.jpg', r.content, 'image/jpeg')}
        data = {'reqtype': 'fileupload', 'userhash': ''}
        post = requests.post(catbox_url, files=files, data=data, timeout=30)
        if post.status_code == 200 and "catbox.moe" in post.text:
            return post.text.strip()
    except:
        pass
    return ""

def normalize_memoria(text):
    if not text:
        return ""
    text = str(text).lower().strip()
    text = re.sub(r'(\d+)\s*gb', r'\1 GB', text)
    text = re.sub(r'(\d+)\s*tb', r'\1 TB', text)
    return text.upper()

def limpiar_precio(texto):
    if not texto:
        return 0.0
    texto = str(texto)
    limpio = texto.replace('€', '').replace('.', '').replace(',', '.').strip()
    try:
        return float(limpio)
    except:
        return 0.0

def limpiar_url_sin_afiliado(url, fuente):
    url_dest = url
    url_lower = url.lower()

    if "aliexpress.es" in url_lower:
        if "https%3A%2F%2Fes.aliexpress.com" in url:
            raw = url.split("https%3A%2F%2Fes.aliexpress.com")[1].split(".html")[0]
            url_dest = unquote("https%3A%2F%2Fes.aliexpress.com" + raw + ".html")
        else:
            url_dest = url.split(".html")[0] + ".html"
    elif "aliexpress.us" in url_lower or "aliexpress.com" in url_lower:
        url_dest = url.split(".html")[0] + ".html"

    elif fuente == "MediaMarkt":
        if "pdt.tradedoubler.com" in url_lower:
            if "https%3A%2F%2Fwww.mediamarkt.es" in url:
                raw = url.split("https%3A%2F%2Fwww.mediamarkt.es")[1].split(".html")[0]
                url_dest = unquote("https%3A%2F%2Fwww.mediamarkt.es" + raw + ".html")
        elif "https://www.mediamarkt.es/es" in url:
            url_dest = url.split("https://www.mediamarkt.es/es")[1].split(".html")[0]
            url_dest = "https://www.mediamarkt.es/es" + url_dest + ".html"

    elif fuente in ["PcComponentes", "Fnac", "Amazon", "Phone House", "DHGate", "Tradingshenzhen"]:
        url_dest = url.split("?")[0]

    return url_dest

def obtener_datos_finales(enlace_de_compra_importado, texto_item_original):
    if not enlace_de_compra_importado:
        return "", "Chinabay", ""
    fuente = "Chinabay"
    version = "Versión Global" if ("Europe" in texto_item_original or "Global" in texto_item_original) else ""
    url_procesar = enlace_de_compra_importado
    dominios_resolver = ["s.zbanx.com", "s.click.aliexpress.com", "chinabay.pro", "bit.ly", "is.gd"]
    if any(x in url_procesar for x in dominios_resolver):
        url_procesar = resolver_redireccion_http(url_procesar)
    if "chinabay.es" in url_procesar and "/wp-" not in url_procesar:
        fuente_int, version_int, url_int = analizar_pagina_interna(url_procesar)
        if fuente_int != "Chinabay":
            fuente = fuente_int
        if version_int:
            version = version_int
        url_procesar = url_int
        if any(x in url_procesar for x in dominios_resolver):
            url_procesar = resolver_redireccion_http(url_procesar)
    url_final = procesar_afiliados_inteligente(url_procesar)
    url_lower = url_final.lower()
    if "amazon" in url_lower or "amzn.to" in url_lower:
        fuente = "Amazon"
        version = "Versión Global"
    elif "mediamarkt" in url_lower:
        fuente = "MediaMarkt"
        version = "Versión Global"
    elif "dhgate" in url_lower:
        fuente = "DHGate"
        version = "Versión Global"
    elif fuente == "Chinabay":
        if "aliexpress" in url_lower:
            fuente = "AliExpress Plaza"
        elif "miravia" in url_lower:
            fuente = "Miravia"
        elif "tradingshenzhen" in url_lower:
            fuente = "Tradingshenzhen"
    return url_final, fuente, version

# --- GESTOR DE CATEGORÍAS E IMÁGENES ---
cache_categorias = []

def cargar_todas_las_categorias():
    print("📂 Cargando árbol de categorías existente (incluyendo imágenes)...", flush=True)
    global cache_categorias
    page = 1
    while True:
        try:
            res = wcapi.get("products/categories", params={"per_page": 100, "page": page}).json()
        except:
            break
        if not res:
            break
        cache_categorias.extend(res)
        page += 1
    print(f"   -> {len(cache_categorias)} categorías en memoria.", flush=True)

def buscar_categoria_local(nombre, parent_id=0):
    nombre_busqueda = normalize_text(nombre).lower()
    for cat in cache_categorias:
        if cat['name'].lower() == nombre_busqueda and cat['parent'] == parent_id:
            return cat
    return None

def gestionar_jerarquia_e_imagen(marca, nombre_completo_movil, url_imagen_scrap):
    global cache_categorias
    marca_bonita = normalize_text(marca)
    nombre_movil_bonito = normalize_text(nombre_completo_movil)

    # 1. Gestionar Padre (Nunca se borra)
    cat_padre = buscar_categoria_local(marca, parent_id=0)
    if cat_padre:
        id_padre = cat_padre['id']
    else:
        try:
            res = wcapi.post("products/categories", {"name": marca_bonita, "parent": 0}).json()
            id_padre = res.get('id')
            if id_padre:
                cache_categorias.append(res)
        except:
            return None, None, ""

    if not id_padre:
        return None, None, ""

    # 2. Gestionar Hijo/Subcategoría (Nunca se borra)
    cat_hijo = buscar_categoria_local(nombre_movil_bonito, parent_id=id_padre)
    imagen_final_url = ""

    if cat_hijo:
        id_hijo = cat_hijo['id']
        if cat_hijo.get('image') and cat_hijo['image'].get('src'):
            imagen_final_url = cat_hijo['image']['src']
            print(f"      [INFO] La subcategoría '{nombre_movil_bonito}' ya tiene imagen asignada.")
        else:
            exito_img = False
            for i in range(1, 6):
                print(f"      ⏳ Intento {i}/5 para subir imagen a subcategoría existente '{nombre_movil_bonito}'...")
                imagen_final_url = generar_url_puente(url_imagen_scrap)
                if imagen_final_url:
                    res_put = wcapi.put(f"products/categories/{id_hijo}", {"image": {"src": imagen_final_url}})
                    if res_put.status_code in [200, 201]:
                        print(f"      ✅ Imagen subida y vinculada con éxito en el intento {i}.")
                        exito_img = True
                        break
                if i < 5:
                    time.sleep(30)

            if not exito_img:
                print(f"      ❌ ERROR: No se pudo subir la imagen '{url_imagen_scrap}' tras 5 intentos.")
                imagen_final_url = url_imagen_scrap
    else:
        print(f"      [INFO] Creando nueva subcategoría '{nombre_movil_bonito}' debajo de {marca_bonita}...")
        exito_img = False
        img_temp = ""
        for i in range(1, 6):
            print(f"      ⏳ Intento {i}/5 para subir imagen para nueva subcategoría '{nombre_movil_bonito}'...")
            img_temp = generar_url_puente(url_imagen_scrap)
            if img_temp:
                print(f"      ✅ Imagen procesada con éxito para nueva categoría en intento {i}.")
                exito_img = True
                break
            if i < 5:
                time.sleep(30)

        imagen_final_url = img_temp if exito_img else url_imagen_scrap
        if not exito_img:
            print(f"      ❌ ERROR: Falló la subida de imagen para nueva subcategoría tras 5 intentos.")

        payload_cat = {
            "name": nombre_movil_bonito,
            "parent": id_padre,
            "image": {"src": imagen_final_url} if imagen_final_url else None
        }
        try:
            res = wcapi.post("products/categories", payload_cat).json()
            id_hijo = res.get('id')
            if id_hijo:
                cache_categorias.append(res)
                print(f"      [SUBCATEGORÍA CREADA] ID: {id_hijo}")
        except Exception as e:
            print(f"      ❌ Error crítico creando subcategoría: {e}")
            id_hijo = None

    return id_padre, id_hijo, imagen_final_url

# --- FASE 1: SCRAPING ---
def obtener_productos_remotos():
    print(f"--- FASE 1: Escaneando {URL_ORIGEN} ---", flush=True)
    if not URL_ORIGEN:
        print("ERROR: SOURCE_URL_CHINABAY no está configurada.", flush=True)
        return []
    headers = {'User-Agent': 'Mozilla/5.0'}
    productos_validos = []
    try:
        r = requests.get(URL_ORIGEN, headers=headers, timeout=20)
        soup = BeautifulSoup(r.text, 'lxml')
        items = soup.select("div.e-loop-item")
        print(f"🔍 Encontradas {len(items)} tarjetas. Procesando...", flush=True)
        for item in items:
            try:
                h2 = item.select_one("h2.elementor-heading-title")
                if not h2:
                    continue
                raw_brand = h2.get_text(strip=True)
                h3s = item.select("h3.elementor-heading-title")
                memoria_raw = ""
                precio_actual = 0.0
                raw_model = ""
                for h3 in h3s:
                    txt = h3.get_text(strip=True)
                    txt_lower = txt.lower()
                    if "€" in txt:
                        precio_actual = limpiar_precio(txt)
                    elif "gb" in txt_lower or "tb" in txt_lower:
                        memoria_raw = txt
                    else:
                        raw_model = txt
                if precio_actual <= 0 or not memoria_raw:
                    continue
                h4_precio = item.select_one("#precio_of h4")
                precio_regular = limpiar_precio(h4_precio.get_text(strip=True)) if h4_precio else precio_actual

                memoria_clean = re.sub(r'\s+', ' ', memoria_raw).strip()
                memoria = ""
                capacidad = ""
                if "·" in memoria_clean:
                    parts = memoria_clean.split("·")
                    memoria = normalize_memoria(parts[0])
                    capacidad = normalize_memoria(parts[1])
                elif "/" in memoria_clean:
                    parts = memoria_clean.split("/")
                    memoria = normalize_memoria(parts[0])
                    capacidad = normalize_memoria(parts[1])
                else:
                    capacidad = normalize_memoria(memoria_clean)

                marca_final, nombre_movil_final = analizar_nombre_y_categoria(raw_brand, raw_model)
                if "pad" in nombre_movil_final.lower():
                    continue

                btn = item.select_one("a.elementor-button-link")
                enlace_de_compra_importado = btn['href'] if btn else ""

                url_oferta_sin_acortar = enlace_de_compra_importado
                base_origen = URL_ORIGEN.rstrip('/') + '/'
                if enlace_de_compra_importado.startswith(base_origen):
                    _, _, url_compra_interna = analizar_pagina_interna(enlace_de_compra_importado)
                    url_oferta_sin_acortar = resolver_redireccion_http(url_compra_interna)
                else:
                    url_oferta_sin_acortar = resolver_redireccion_http(enlace_de_compra_importado)

                url_final, fuente_real, version_real = obtener_datos_finales(enlace_de_compra_importado, item.get_text())
                if fuente_real == "Amazon":
                    url_importada_sin_afiliado = url_oferta_sin_acortar.split('#')[0].split('?')[0]
                elif fuente_real.strip().lower() in ["phone house", "phonehouse"]:
                    url_importada_sin_afiliado = url_oferta_sin_acortar.split('#')[0].split('?')[0]
                else:
                    url_importada_sin_afiliado = limpiar_url_sin_afiliado(url_oferta_sin_acortar, fuente_real)

                if fuente_real == "MediaMarkt":
                    url_sin_acortar_con_mi_afiliado = f"{url_importada_sin_afiliado}{ID_AFILIADO_MEDIAMARKT}"
                elif fuente_real == "Amazon":
                    url_sin_acortar_con_mi_afiliado = f"{url_importada_sin_afiliado}{ID_AFILIADO_AMAZON}"
                elif fuente_real.strip().lower() in ["phone house", "phonehouse"]:
                    url_sin_acortar_con_mi_afiliado = f"{url_importada_sin_afiliado}{ID_AFILIADO_PHONE_HOUSE}"
                elif fuente_real in ["AliExpress Plaza", "Mi Store"] or url_importada_sin_afiliado.startswith(base_origen):
                    url_sin_acortar_con_mi_afiliado = f"{url_importada_sin_afiliado}{ID_AFILIADO_ALIEXPRESS}"
                elif fuente_real == "Tradingshenzhen":
                    url_sin_acortar_con_mi_afiliado = f"{url_importada_sin_afiliado}{ID_AFILIADO_TRADINGSENZHEN}"
                elif fuente_real == "DHGate":
                    url_sin_acortar_con_mi_afiliado = f"{url_importada_sin_afiliado}{ID_AFILIADO_DHGATE}"
                else:
                    url_sin_acortar_con_mi_afiliado = url_importada_sin_afiliado

                url_oferta = acortar_url(url_sin_acortar_con_mi_afiliado)

                img = item.select_one("div.elementor-widget-image img")
                img_src_original = img['src'] if img else ""
                img_src = img_src_original
                cupon = item.select_one("[data-coupon]")['data-coupon'] if item.select_one("[data-coupon]") else "OFERTA PROMO"

                enviado_desde, enviado_desde_tg = calcular_enviado_desde(
                    fuente_real,
                    texto_item=item.get_text(),
                    candidate_urls=[enlace_de_compra_importado, url_oferta_sin_acortar, url_importada_sin_afiliado]
                )

                # --- LOGS 1-15 ---
                print(f"\nDetectado {nombre_movil_final}", flush=True)
                print("1) Nombre: " + nombre_movil_final, flush=True)
                print("2) Memoria: " + memoria, flush=True)
                print("3) Capacidad: " + capacidad, flush=True)
                print(f"4) URL Imagen: {img_src}")
                print("5) Versión: " + version_real, flush=True)
                print("6) Fuente: " + fuente_real, flush=True)
                print(f"7) Precio actual: {formatear_precio_str(precio_actual)}", flush=True)
                print(f"8) Precio original: {formatear_precio_str(precio_regular)}", flush=True)
                print(f"9) Código de descuento: {cupon}", flush=True)
                print(f"10) Enlace Importado: {enlace_de_compra_importado}")
                print(f"11) Enlace Expandido: {url_oferta_sin_acortar}")
                print(f"12) URL importada sin afiliado: {url_importada_sin_afiliado}")
                print(f"13) URL sin acortar con mi afiliado: {url_sin_acortar_con_mi_afiliado}")
                print(f"14) URL acortada con mi afiliado: {url_oferta}")
                print(f"15) Enviado desde: {enviado_desde}")
                print(f"15) Encolado para comparar con base de datos...", flush=True)
                print("-" * 60, flush=True)

                item_data = {
                    "name": nombre_movil_final,
                    "marca": marca_final,
                    "price": precio_actual,
                    "regular_price": precio_regular,
                    "image_original": img_src,
                    "url_short": url_oferta,
                    "enlace_de_compra_importado": enlace_de_compra_importado,
                    "url_oferta_sin_acortar": url_oferta_sin_acortar,
                    "url_importada_sin_afiliado": url_importada_sin_afiliado,
                    "url_sin_acortar_con_mi_afiliado": url_sin_acortar_con_mi_afiliado,
                    "url_oferta": url_oferta,
                    "source": fuente_real,
                    "memoria": memoria,
                    "capacidad": capacidad,
                    "coupon": cupon,
                    "version": version_real,
                    "enviado_desde": enviado_desde,
                    "enviado_desde_tg": enviado_desde_tg
                }
                productos_validos.append(item_data)
            except Exception:
                continue
        return productos_validos
    except Exception:
        return []

# --- FASE 2: GESTIÓN DE PRODUCTOS ---
def sincronizar_productos(remotos):
    skip_inventory = False
    if not remotos:
        print("ALERTA: 0 productos encontrados en Chinabay.", flush=True)
        skip_inventory = True
        remotos = []
    else:
        print(f"\n--- FASE 2: PROCESANDO {len(remotos)} PRODUCTOS ---", flush=True)
        cargar_todas_las_categorias()
        for r in remotos:
            r['_procesado'] = False

    locales = []
    page = 1
    while True:
        try:
            res = wcapi.get("products", params={"per_page": 100, "page": page, "status": "publish"}).json()
            if not res or "message" in res: break
            for p in res:
                meta = {m['key']: m['value'] for m in p.get('meta_data', [])}

                # ✅ Transición (IMPORTANTE):
                # Históricamente 'importado_de' se guardaba con URL_ORIGEN (que puede cambiar).
                # Para poder eliminar obsoletos antiguos, consideramos Chinabay cualquier variante
                # que contenga 'chinabay' o que coincida con URL_ORIGEN, y normalizamos al valor fijo.
                importado_raw = str(meta.get('importado_de', '')).strip().lower()
                url_origen_raw = str(URL_ORIGEN or '').strip().lower()
                es_chinabay = (importado_raw == url_origen_raw) or ('chinabay' in importado_raw)

                if es_chinabay:
                    # Normaliza para que a partir de hoy el inventario sea estable
                    if meta.get('importado_de') != IMPORTADO_DE_FIJO:
                        try:
                            wcapi.put(
                                f"products/{p['id']}",
                                {"meta_data": [{"key": "importado_de", "value": IMPORTADO_DE_FIJO}]}
                            )
                            print(f"   🔁 [MIGRADO] importado_de -> {IMPORTADO_DE_FIJO} (ID: {p['id']})", flush=True)
                        except Exception as e:
                            print(f"   ⚠️ [MIGRADO] fallo actualizando importado_de (ID: {p['id']}): {e}", flush=True)
                    p_act_wp = limpiar_precio(meta.get('precio_actual', p.get('price', 0)))
                    p_reg_wp = limpiar_precio(meta.get('precio_original', p.get('regular_price', 0)))
                    
                    locales.append({
                        "id": p['id'],
                        "name": str(p['name']).strip(),
                        "price": p_act_wp,
                        "regular_price": p_reg_wp,
                        "fuente": meta.get('fuente', 'Desconocida'),
                        "memoria": meta.get('memoria', ''),
                        "capacidad": meta.get('capacidad', ''),
                        "enviado_desde": str(meta.get('enviado_desde', '')).strip(),
                        "enviado_desde_tg": str(meta.get('enviado_desde_tg', '')).strip(),
                        "enlace_de_compra_importado": str(meta.get('enlace_de_compra_importado', meta.get('enlace_importado', ''))).strip(),
                        "url_oferta_sin_acortar": str(meta.get('url_oferta_sin_acortar', meta.get('enlace_expandido', ''))).strip(),
                        "url_importada_sin_afiliado": str(meta.get('url_importada_sin_afiliado', '')).strip()
                    })
            if len(res) < 100:
                break
            page += 1
        except:
            break

    summary_creados = []
    summary_actualizados = []
    summary_existentes = []
    summary_eliminados = []

    if not skip_inventory:
        for local in locales:
            nombre_local = normalize_text(local['name']).strip().lower()
            fuente_local = normalize_text(local['fuente']).strip().lower()
            memoria_local = normalize_memoria(local['memoria'])
            capacidad_local = normalize_memoria(local['capacidad'])

            match = None
            for cand in remotos:
                if (
                    normalize_text(cand['name']).strip().lower() == nombre_local and
                    normalize_text(cand['source']).strip().lower() == fuente_local and
                    normalize_memoria(cand['memoria']) == memoria_local and
                    normalize_memoria(cand['capacidad']) == capacidad_local and
                    not cand['_procesado']
                ):
                    match = cand
                    break

            if match:
                match['_procesado'] = True
                cambios_log = []

                if abs(match['price'] - local['price']) > 0.01:
                    cambios_log.append(f"precio_actual ({formatear_precio_str(local['price'])} -> {formatear_precio_str(match['price'])})")
                if abs(match['regular_price'] - local['regular_price']) > 0.01:
                    cambios_log.append(f"precio_original ({formatear_precio_str(local['regular_price'])} -> {formatear_precio_str(match['regular_price'])})")

                if cambios_log:
                    sale_price_fmt = formatear_precio_str(match['price'])
                    regular_price_fmt = formatear_precio_str(match['regular_price'])

                    wcapi.put(f"products/{local['id']}", {
                        "sale_price": sale_price_fmt,
                        "regular_price": regular_price_fmt,
                        "meta_data": [
                            {"key": "precio_actual", "value": sale_price_fmt},
                            {"key": "precio_original", "value": regular_price_fmt}
                        ]
                    })
                    summary_actualizados.append({
                        "nombre": local['name'],
                        "id": local['id'],
                        "razon": ", ".join(cambios_log)
                    })
                else:
                    summary_existentes.append({"nombre": local['name'], "id": local['id']})
            else:
                wcapi.delete(f"products/{local['id']}", params={"force": True})
                summary_eliminados.append({"nombre": local['name'], "id": local['id']})

        for remoto in remotos:
            if not remoto['_procesado']:
                id_padre, id_hijo, img_final = gestionar_jerarquia_e_imagen(remoto['marca'], remoto['name'], remoto['image_original'])
                cats = [{"id": id_padre}] if id_padre else []
                if id_hijo:
                    cats.append({"id": id_hijo})

                sale_price_fmt = formatear_precio_str(remoto['price'])
                regular_price_fmt = formatear_precio_str(remoto['regular_price'])

                data = {
                    "name": remoto['name'],
                    "type": "simple",
                    "regular_price": regular_price_fmt,
                    "sale_price": sale_price_fmt,
                    "categories": cats,
                    "images": [{"src": img_final}] if img_final else [],
                    "meta_data": [
                        {"key": "importado_de", "value": IMPORTADO_DE_FIJO},
                        {"key": "source_url_origen", "value": URL_ORIGEN},
                        {"key": "url_oferta", "value": remoto['url_oferta']},
                        {"key": "enlace_importado", "value": remoto['enlace_de_compra_importado']},
                        {"key": "enlace_expandido", "value": remoto['url_oferta_sin_acortar']},
                        {"key": "enlace_de_compra_importado", "value": remoto['enlace_de_compra_importado']},
                        {"key": "url_oferta_sin_acortar", "value": remoto['url_oferta_sin_acortar']},
                        {"key": "url_importada_sin_afiliado", "value": remoto['url_importada_sin_afiliado']},
                        {"key": "url_sin_acortar_con_mi_afiliado", "value": remoto['url_sin_acortar_con_mi_afiliado']},
                        {"key": "fuente", "value": remoto['source']},
                        {"key": "memoria", "value": remoto['memoria']},
                        {"key": "capacidad", "value": remoto['capacidad']},
                        {"key": "precio_actual", "value": sale_price_fmt},
                        {"key": "precio_original", "value": regular_price_fmt},
                        {"key": "codigo_de_descuento", "value": remoto['coupon']},
                        {"key": "version", "value": remoto['version']},
                        {"key": "enviado_desde", "value": remoto['enviado_desde']},
                        {"key": "enviado_desde_tg", "value": remoto['enviado_desde_tg']}
                    ]
                }

                intentos = 0
                max_intentos = 10
                creado = False

                while intentos < max_intentos and not creado:
                    intentos += 1
                    print(f"    ⏳ Intentando crear producto {remoto['name']} (Intento {intentos}/{max_intentos})...", flush=True)
                    try:
                        res = wcapi.post("products", data)
                        if res.status_code == 201:
                            product_data = res.json()
                            new_id = product_data['id']
                            permalink = product_data.get('permalink')

                            url_post_acortada = acortar_url(permalink)
                            if url_post_acortada:
                                wcapi.put(f"products/{new_id}", {
                                    "meta_data": [{"key": "url_post_acortada", "value": url_post_acortada}]
                                })

                            summary_creados.append({"nombre": remoto['name'], "id": new_id, "post": url_post_acortada})
                            print(f"    ✅ PRODUCTO OK ID: {new_id} (Post: {url_post_acortada})", flush=True)
                            creado = True
                        else:
                            print(f"    ❌ ERROR WP ({res.status_code}): {res.text}", flush=True)
                    except Exception as e:
                        print(f"    ❌ EXCEPCIÓN: {e}", flush=True)

                    if not creado and intentos < max_intentos:
                        time.sleep(60)

                time.sleep(60)

    # --- FASE 2B: BACKFILL ENVÍO PARA PRODUCTOS YA EXISTENTES (importado_de = https://chinabay.es) ---
    envio_checked = 0
    envio_ok = 0
    envio_actualizados = 0
    envio_no_inferible = 0
    summary_envio_actualizados = []

    deleted_ids = set([x['id'] for x in summary_eliminados])

    for local in locales:
        if local.get('id') in deleted_ids:
            continue

        envio_checked += 1
        env_exist = (local.get('enviado_desde') or "").strip()
        env_tg_exist = (local.get('enviado_desde_tg') or "").strip()

        if env_exist and env_tg_exist:
            envio_ok += 1
            continue

        candidate_urls = [
            local.get('enlace_de_compra_importado', ''),
            local.get('url_oferta_sin_acortar', ''),
            local.get('url_importada_sin_afiliado', '')
        ]

        env_calc, env_tg_calc = calcular_enviado_desde(
            local.get('fuente', ''),
            texto_item="",
            candidate_urls=candidate_urls
        )

        updates = []
        filled = {}

        if (not env_exist) and env_calc:
            updates.append({"key": "enviado_desde", "value": env_calc})
            filled["enviado_desde"] = env_calc

        if (not env_tg_exist) and env_tg_calc:
            updates.append({"key": "enviado_desde_tg", "value": env_tg_calc})
            filled["enviado_desde_tg"] = env_tg_calc

        if updates:
            try:
                r_put = wcapi.put(f"products/{local['id']}", {"meta_data": updates})
                if r_put.status_code in [200, 201]:
                    envio_actualizados += 1
                    summary_envio_actualizados.append({
                        "nombre": local['name'],
                        "id": local['id'],
                        "campos": filled
                    })
                else:
                    envio_no_inferible += 1
            except Exception:
                envio_no_inferible += 1
        else:
            envio_no_inferible += 1

    print("\n" + "=" * 60)
    print("📋 RESUMEN DE GESTIÓN DE INVENTARIO CHINABAY")
    print("=" * 60)
    print(f"\n➕ CREADOS ({len(summary_creados)}):")
    for item in summary_creados:
        print(f"   - {item['nombre']} (ID: {item['id']}) [Post: {item.get('post', '')}]")
    print(f"\n🔄 ACTUALIZADOS ({len(summary_actualizados)}):")
    for item in summary_actualizados:
        print(f"   - {item['nombre']} (ID: {item['id']}): {item['razon']}")
    print(f"\n✅ EXISTENTES ({len(summary_existentes)}):")
    for item in summary_existentes:
        print(f"   - {item['nombre']} (ID: {item['id']})")
    print(f"\n❌ ELIMINADOS ({len(summary_eliminados)}):")
    for item in summary_eliminados:
        print(f"   - {item['nombre']} (ID: {item['id']})")

    print(f"\n🧩 ENVÍO (BACKFILL) - Revisados {envio_checked}:")
    print(f"   ✅ Ya completos: {envio_ok}")
    print(f"   ✍️ Actualizados: {len(summary_envio_actualizados)}")
    if summary_envio_actualizados:
        for item in summary_envio_actualizados:
            campos = ", ".join([f"{k}={v}" for k, v in item.get('campos', {}).items()])
            print(f"   - {item['nombre']} (ID: {item['id']}): {campos}")
    if envio_no_inferible:
        print(f"   ⚠️ Sin cambios / no inferible: {envio_no_inferible}")

def main():
    productos = obtener_productos_remotos()
    sincronizar_productos(productos)

if __name__ == "__main__":
    main()
