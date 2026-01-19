import os
import time
import re
import requests
import urllib.parse
from datetime import datetime
from bs4 import BeautifulSoup
from woocommerce import API

# ============================================================
#  SCRAPER PHONE HOUSE - VERSIÓN COMPLETA (SCROLL + URL MASK)
# ============================================================
#
# Objetivos:
#  - Extraer hasta 72 móviles del listado con scroll infinito
#  - Importar/actualizar en WooCommerce
#  - No mostrar URLs sensibles completas en logs (enmascara query/afiliado)
#
# Requisitos:
#  - WP_URL, WP_KEY, WP_SECRET (secrets)
#  - SOURCE_URL_PHONEHOUSE (opcional; por defecto el listado de smartphones)
#  - AFF_PHONEHOUSE (opcional; querystring o parámetros de afiliado)
#

# --- CONFIGURACIÓN ---
DEFAULT_START_URL = "https://www.phonehouse.es/moviles-y-telefonia/moviles/todos-los-smartphones.html"
START_URL = os.environ.get("SOURCE_URL_PHONEHOUSE") or DEFAULT_START_URL

FUENTE = "Phone House"
ID_IMPORTACION = "https://www.phonehouse.es"

# Afiliado: idealmente en secret/env. Permite:
#  - "?utm_source=..." (prefijado con '?')
#  - "utm_source=..." (sin '?', se añade)
AFF_RAW = os.environ.get("AFF_PHONEHOUSE", "").strip()
if AFF_RAW and not AFF_RAW.startswith("?") and not AFF_RAW.startswith("&"):
    AFF_RAW = "?" + AFF_RAW
ID_AFILIADO_PHONE_HOUSE = AFF_RAW

ENVIADO_DESDE = "España"
ENVIADO_DESDE_TG = "🇪🇸 España"
CODIGO_DESCUENTO = "OFERTA PROMO"

wcapi = API(
    url=os.environ["WP_URL"],
    consumer_key=os.environ["WP_KEY"],
    consumer_secret=os.environ["WP_SECRET"],
    version="wc/v3",
    timeout=60
)

summary_creados, summary_eliminados, summary_actualizados = [], [], []
summary_ignorados, summary_sin_stock_nuevos, summary_fallidos = [], [], []
summary_duplicados = []

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.phonehouse.es/",
}

# --------------------------
# UTILIDADES
# --------------------------
def mask_url(url: str) -> str:
    """Enmascara la URL para logs (no muestra querystring completa)."""
    try:
        u = urllib.parse.urlsplit(url)
        base = f"{u.scheme}://{u.netloc}{u.path}"
        return base + ("?***" if u.query else "")
    except Exception:
        return "***"

def acortar_url(url_larga: str) -> str:
    """Acorta con is.gd."""
    try:
        url_encoded = urllib.parse.quote(url_larga, safe="")
        r = requests.get(f"https://is.gd/create.php?format=simple&url={url_encoded}", timeout=10)
        return r.text.strip() if r.status_code == 200 else url_larga
    except Exception:
        return url_larga

def abs_url(base: str, href: str) -> str:
    """Convierte URL relativa a absoluta."""
    try:
        if href.startswith("//"):
            href = "https:" + href
        return urllib.parse.urljoin(base, href)
    except Exception:
        return href

def parse_eur_int(txt: str) -> int:
    """Convierte texto de precio a entero."""
    if not txt:
        return 0
    t = txt.replace("\xa0", " ").strip()
    m = re.search(r"(\d{1,5}(?:[.,]\d{1,2})?)", t)
    if not m:
        return 0
    num = m.group(1).replace(".", "").replace(",", ".")
    try:
        return int(float(num))
    except Exception:
        return 0

def normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

# --------------------------
# RAM iPhone
# --------------------------
IPHONE_RAM_MAP = [
    ("iphone 17 pro max", "12GB"),
    ("iphone 17 pro", "12GB"),
    ("iphone 17 air", "12GB"),
    ("iphone air", "12GB"),
    ("iphone 17", "8GB"),
    ("iphone 16 pro max", "8GB"),
    ("iphone 16 pro", "8GB"),
    ("iphone 16 plus", "8GB"),
    ("iphone 16", "8GB"),
    ("iphone 15 pro max", "8GB"),
    ("iphone 15 pro", "8GB"),
    ("iphone 15 plus", "6GB"),
    ("iphone 15", "6GB"),
    ("iphone 14 pro max", "6GB"),
    ("iphone 14 pro", "6GB"),
    ("iphone 14 plus", "6GB"),
    ("iphone 14", "6GB"),
    ("iphone se (3", "4GB"),
    ("iphone se 3", "4GB"),
    ("iphone 13 pro max", "6GB"),
    ("iphone 13 pro", "6GB"),
    ("iphone 13 mini", "4GB"),
    ("iphone 13", "4GB"),
    ("iphone 12 pro max", "6GB"),
    ("iphone 12 pro", "6GB"),
    ("iphone 12 mini", "4GB"),
    ("iphone 12", "4GB"),
    ("iphone se (2", "3GB"),
    ("iphone se 2", "3GB"),
    ("iphone 11 pro max", "4GB"),
    ("iphone 11 pro", "4GB"),
    ("iphone 11", "4GB"),
    ("iphone xs max", "4GB"),
    ("iphone xs", "4GB"),
    ("iphone xr", "3GB"),
    ("iphone x", "3GB"),
    ("iphone 8 plus", "3GB"),
    ("iphone 8", "2GB"),
    ("iphone 7 plus", "3GB"),
    ("iphone 7", "2GB"),
    ("iphone se (1", "2GB"),
    ("iphone se 1", "2GB"),
    ("iphone 6s plus", "2GB"),
    ("iphone 6s", "2GB"),
]

def ram_por_modelo_iphone(nombre: str):
    if not nombre:
        return None
    n = nombre.lower()
    if "iphone" not in n:
        return None
    for needle, ram in IPHONE_RAM_MAP:
        if needle in n:
            return ram
    return None

# --------------------------
# EXTRACCIÓN (memoria/capacidad)
# --------------------------
def extraer_nombre_memoria_capacidad(titulo: str):
    t = normalize_spaces(titulo)

    # Formatos combo (CAP+RAM o RAM+CAP) con separadores + o /
    m_combo = re.search(
        r"(?P<cap>\d{2,4})\s*(?P<unit>TB|GB)\s*[\+/]\s*(?P<ram>\d{1,2})\s*GB(?:\s*RAM)?\b"
        r"|(?P<ram2>\d{1,2})\s*GB(?:\s*RAM)?\s*[\+/]\s*(?P<cap2>\d{2,4})\s*(?P<unit2>TB|GB)\b",
        t,
        flags=re.I
    )
    if m_combo:
        if m_combo.group("cap") and m_combo.group("ram"):
            capacidad = f"{m_combo.group('cap')}{m_combo.group('unit').upper()}"
            memoria = f"{m_combo.group('ram')}GB"
        else:
            capacidad = f"{m_combo.group('cap2')}{m_combo.group('unit2').upper()}"
            memoria = f"{m_combo.group('ram2')}GB"

        nombre = t[:m_combo.start()].strip()
        return normalize_spaces(nombre), capacidad, memoria

    # Capacidad (almacenamiento) - restringimos para evitar falsos positivos (p.ej. datos)
    m_cap = re.search(r"\b(64|128|256|512|1024)\s*GB\b|\b(1|2)\s*TB\b", t, flags=re.I)
    capacidad = ""
    if m_cap:
        if m_cap.group(1):
            capacidad = f"{m_cap.group(1)}GB"
        else:
            capacidad = f"{m_cap.group(2)}TB"

    # RAM (memoria) con o sin literal "RAM"
    m_ram = re.search(r"\b(4|6|8|12|16)\s*GB(?:\s*RAM)?\b", t, flags=re.I)
    memoria = f"{m_ram.group(1)}GB" if m_ram else ""

    # Nombre: cortar por primera aparición
    cut_positions = []
    if m_cap:
        cut_positions.append(m_cap.start())
    if m_ram:
        cut_positions.append(m_ram.start())
    cut = min(cut_positions) if cut_positions else len(t)

    nombre = t[:cut].strip()
    return normalize_spaces(nombre), capacidad, memoria

# --------------------------
# CATEGORÍAS
# --------------------------
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
        except Exception:
            break
    return categorias

def resolver_jerarquia(nombre_completo, cache_categorias):
    palabras = (nombre_completo or "").split()
    nombre_padre = palabras[0] if palabras else "Otros"
    nombre_hijo = nombre_completo

    id_cat_padre = None
    id_cat_hijo = None

    for cat in cache_categorias:
        if cat.get("name", "").lower() == nombre_padre.lower() and cat.get("parent") == 0:
            id_cat_padre = cat.get("id")
            break
    if not id_cat_padre:
        res = wcapi.post("products/categories", {"name": nombre_padre}).json()
        id_cat_padre = res.get("id")
        cache_categorias.append(res)

    for cat in cache_categorias:
        if cat.get("name", "").lower() == nombre_hijo.lower() and cat.get("parent") == id_cat_padre:
            id_cat_hijo = cat.get("id")
            break
    if not id_cat_hijo:
        res = wcapi.post("products/categories", {"name": nombre_hijo, "parent": id_cat_padre}).json()
        id_cat_hijo = res.get("id")
        cache_categorias.append(res)

    return id_cat_padre, id_cat_hijo

def obtener_imagen_categoria(cache_categorias, cat_id):
    if not cat_id:
        return ""
    for c in cache_categorias:
        if c.get("id") == cat_id:
            img = c.get("image") or {}
            return img.get("src") or ""
    return ""

def actualizar_imagen_categoria(cache_categorias, cat_id, img_src):
    if not cat_id or not img_src:
        return False
    if obtener_imagen_categoria(cache_categorias, cat_id):
        return False
    try:
        res = wcapi.put(f"products/categories/{cat_id}", {"image": {"src": img_src}}).json()
        for i, c in enumerate(cache_categorias):
            if c.get("id") == cat_id:
                cache_categorias[i] = res
                break
        return True
    except Exception:
        return False

# --------------------------
# EXTRACCIÓN REMOTA (CON SCROLL)
# --------------------------
def obtener_html_con_scroll(start_url: str) -> str | None:
    """
    Intenta cargar la página con Selenium y hacer scroll hasta el final.
    Devuelve HTML si funciona; si no, None.
    """
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.common.exceptions import TimeoutException
    except Exception:
        return None

    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1400,900")

    driver = webdriver.Chrome(options=chrome_options)
    try:
        driver.set_page_load_timeout(40)
        driver.get(start_url)
        try:
            print(f"URL final (Selenium): {mask_url(driver.current_url)}", flush=True)
        except Exception:
            pass

        time.sleep(2)

        last_height = driver.execute_script("return document.body.scrollHeight")
        stable_rounds = 0
        max_rounds = 40  # evita bucles infinitos

        print("🧭 Haciendo scroll hasta el final...", flush=True)

        for _ in range(max_rounds):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1.5)
            new_height = driver.execute_script("return document.body.scrollHeight")

            if new_height == last_height:
                stable_rounds += 1
                # Algunos listados necesitan varios "ticks" para completar AJAX
                if stable_rounds >= 3:
                    break
            else:
                stable_rounds = 0
                last_height = new_height

        html = driver.page_source
        return html
    except TimeoutException:
        # Si PhoneHouse tarda demasiado o bloquea, caemos a requests
        return None
    except Exception:
        return None
    finally:
        try:
            driver.quit()
        except Exception:
            pass

def obtener_datos_remotos():
    total_productos = []
    hoy = datetime.now().strftime("%d/%m/%Y")

    print("================================================================================", flush=True)
    print("🤖 SCRAPER PHONE HOUSE - VERSIÓN COMPLETA", flush=True)
    print("================================================================================", flush=True)
    print(f"🔗 URL: {mask_url(START_URL)}", flush=True)
    print("🔄 Scroll AJAX: ACTIVADO", flush=True)
    print("🎯 Objetivo: 72 productos", flush=True)
    print("================================================================================", flush=True)

    print("\n--- FASE 1: ESCANEANDO PHONE HOUSE ---", flush=True)
    print(f"URL: {mask_url(START_URL)}", flush=True)

    # 1) Intentar Selenium + scroll
    html = obtener_html_con_scroll(START_URL)
    if html:
        soup = BeautifulSoup(html, "html.parser")
        print("✅ HTML obtenido con Selenium/scroll", flush=True)
    else:
        print("⚠️  Selenium no disponible o falló; usando requests (puede ver menos productos).", flush=True)
        r = requests.get(START_URL, headers=HEADERS, timeout=30)
        try:
            print(f"URL final (requests): {mask_url(r.url)}", flush=True)
        except Exception:
            pass
        soup = BeautifulSoup(r.text, "html.parser")

    # 2) Buscar enlaces de fichas de móviles (más estricto para evitar menús)
    print("\n🔍 Buscando TODOS los enlaces a productos...", flush=True)

    product_path_re = re.compile(r"^/movil/[^/]+/[^/?#]+\.html$", re.I)

    all_a = soup.find_all("a", href=True)
    raw_candidates = 0
    unique_links = {}

    for a in all_a:
        href = (a.get("href") or "").strip()
        if not href:
            continue

        # Normaliza href -> URL absoluta
        full_url = abs_url(START_URL, href)
        path = urllib.parse.urlsplit(full_url).path

        if not product_path_re.match(path):
            continue

        raw_candidates += 1

        # Deduplicación por URL sin query
        base_url = full_url.split("?")[0]
        unique_links[base_url] = a

    print(f"   🔗 Total candidatos ficha (/movil/.../*.html): {raw_candidates}", flush=True)
    print(f"   🔗 Enlaces únicos: {len(unique_links)}", flush=True)

    # 3) Procesar enlaces únicos
    productos_procesados = 0
    productos_ignorados_color = 0

    for idx, (url, link) in enumerate(unique_links.items(), 1):
        if productos_procesados >= 72:
            break
        try:
            # Extraer título
            nombre_element = link.find(["h2", "h3", "div", "span"], class_=re.compile(r"name|title|product", re.I))
            if nombre_element:
                titulo = normalize_spaces(nombre_element.get_text())
            else:
                titulo = normalize_spaces(link.get_text())

            if not titulo or len(titulo) < 5:
                continue

            titulo_limpio = titulo.replace("¡OFERTA!", "").replace("OFERTA", "").strip()

            nombre, capacidad, memoria = extraer_nombre_memoria_capacidad(titulo_limpio)

            # Para iPhone, derivar RAM si no aparece
            if "iphone" in (nombre or "").lower() and not memoria:
                memoria = ram_por_modelo_iphone(nombre) or ""

            # Reglas: necesitamos capacidad y (RAM o iPhone derivada)
            if not nombre or not capacidad:
                continue
            if ("iphone" not in nombre.lower()) and (not memoria):
                continue
            if ("iphone" in nombre.lower()) and (not memoria):
                continue

            # Buscar precios cerca del enlace
            precio_actual = 0
            precio_original = 0
            parent = link.parent
            for _ in range(4):
                if not parent:
                    break
                precio_elements = parent.find_all(["span", "div"], class_=re.compile(r"price|precio", re.I))
                for precio_el in precio_elements:
                    texto = normalize_spaces(precio_el.get_text())
                    if "€" in texto:
                        p = parse_eur_int(texto)
                        if p > 0:
                            if "tachado" in str(precio_el.get("class", "")) or precio_el.name in ["s", "del"]:
                                precio_original = p
                            else:
                                precio_actual = p
                if precio_actual > 0:
                    break
                parent = getattr(parent, "parent", None)

            # No inventamos precio: si no hay, descartamos (puede ajustarse a fetch de ficha si lo necesitas)
            if precio_actual == 0:
                continue
            if precio_original == 0:
                precio_original = precio_actual

            # Imagen
            img_url = ""
            img_element = link.find("img")
            if img_element:
                for attr in ["src", "data-src", "data-original", "data-lazy"]:
                    candidate = img_element.get(attr)
                    if candidate and "catalogo-blanco" not in candidate.lower():
                        img_url = abs_url(START_URL, candidate)
                        break

            version = "IOS" if "iphone" in nombre.lower() else "Global"

            key = (nombre.lower(), capacidad.upper(), (memoria or "").upper())

            # Dedupe por color/variantes: mismo (nombre, capacidad, RAM) se considera duplicado
            if any(p["dedupe_key"] == key for p in total_productos):
                summary_duplicados.append(f"{nombre} {capacidad} {memoria}".strip())
                productos_ignorados_color += 1
                continue

            total_productos.append({
                "nombre": nombre,
                "memoria": memoria,
                "capacidad": capacidad,
                "precio_actual": precio_actual,
                "precio_original": precio_original,
                "img": img_url,
                "url_imp": url,
                "enviado_desde": ENVIADO_DESDE,
                "enviado_desde_tg": ENVIADO_DESDE_TG,
                "fecha": hoy,
                "en_stock": True,
                "pagina": 1,
                "dedupe_key": key,
                "version": version,
                "fuente": FUENTE,
                "codigo_descuento": CODIGO_DESCUENTO
            })

            productos_procesados += 1
            if productos_procesados <= 10:
                print(f"   [{productos_procesados}] {nombre[:30]:30} | {precio_actual:4d}€ | {capacidad} | {memoria}", flush=True)

        except Exception:
            continue

    total_encontrados = len(total_productos) + len(summary_duplicados)

    print(f"\n📊 RESUMEN EXTRACCIÓN:", flush=True)
    print(f"   Productos únicos encontrados: {len(total_productos)}", flush=True)
    print(f"   Variantes de color ignoradas: {len(summary_duplicados)}", flush=True)
    print(f"   Total productos detectados: {total_encontrados}", flush=True)
    print(f"   Objetivo: 72 productos", flush=True)

    if total_encontrados < 72:
        print(f"   ⚠️  Faltan {72 - total_encontrados} productos por encontrar", flush=True)
        print(f"   💡 Si Selenium funciona y aún faltan, es probable que:", flush=True)
        print(f"      - Parte del catálogo se cargue por otra navegación/paginación", flush=True)
        print(f"      - O el listado use enlaces no presentes en el DOM (datos JSON)", flush=True)

    return total_productos

# --------------------------
# SINCRONIZACIÓN WP
# --------------------------
def sincronizar(remotos):
    print("\n--- FASE 2: SINCRONIZANDO ---", flush=True)
    cache_categorias = obtener_todas_las_categorias()

    # Cargar productos locales importados de PhoneHouse
    locales = []
    page = 1
    while True:
        res = wcapi.get("products", params={"per_page": 100, "page": page}).json()
        if not res or "message" in res:
            break
        for p in res:
            meta = {m["key"]: str(m.get("value", "")) for m in p.get("meta_data", [])}
            if "phonehouse.es" in meta.get("importado_de", "").lower():
                locales.append({"id": p["id"], "nombre": p.get("name", ""), "meta": meta})
        if len(res) < 100:
            break
        page += 1

    print(f"📦 Productos Phone House existentes en la web: {len(locales)}", flush=True)

    for r in remotos:
        try:
            # Preparar URLs
            url_importada_sin_afiliado = (r["url_imp"] or "").strip()
            url_con_afiliado = f"{url_importada_sin_afiliado}{ID_AFILIADO_PHONE_HOUSE}" if ID_AFILIADO_PHONE_HOUSE else url_importada_sin_afiliado
            url_oferta = acortar_url(url_con_afiliado)

            # Logs (enmascarados)
            print("-" * 60, flush=True)
            print(f"Detectado {r['nombre']}", flush=True)
            print(f"1) Nombre: {r['nombre']}", flush=True)
            print(f"2) Memoria: {r['memoria']}", flush=True)
            print(f"3) Capacidad: {r['capacidad']}", flush=True)
            print(f"4) Versión: {r.get('version','Global')}", flush=True)
            print(f"5) Fuente: {FUENTE}", flush=True)
            print(f"6) Precio actual: {r['precio_actual']}€", flush=True)
            print(f"7) Precio original: {r['precio_original']}€", flush=True)
            print(f"8) Código de descuento: {CODIGO_DESCUENTO}", flush=True)
            print(f"9) URL Imagen: {(r['img'][:80] + '...') if r.get('img') else '(vacía)'}", flush=True)
            print(f"11) Enlace Importado: {mask_url(url_importada_sin_afiliado)}", flush=True)
            print(f"14) URL con afiliado: {mask_url(url_con_afiliado)}", flush=True)
            print(f"15) URL acortada con afiliado: {mask_url(url_oferta)}", flush=True)
            print("-" * 60, flush=True)

            # Match por meta enlace_de_compra_importado
            url_r = url_importada_sin_afiliado.rstrip("/")
            match = next(
                (
                    l for l in locales
                    if l["meta"].get("enlace_de_compra_importado", "").strip().rstrip("/") == url_r
                ),
                None
            )

            # Categorías
            id_padre, id_hijo = resolver_jerarquia(r["nombre"], cache_categorias)

            # Imagen de subcategoría (si no existe, se intenta fijar con la del primer producto)
            img_subcat = obtener_imagen_categoria(cache_categorias, id_hijo)
            if not img_subcat and r.get("img"):
                actualizado = actualizar_imagen_categoria(cache_categorias, id_hijo, r["img"])
                img_subcat = obtener_imagen_categoria(cache_categorias, id_hijo) if actualizado else ""
            img_final_producto = img_subcat or r.get("img") or ""

            if match:
                # Actualizar precio si cambia
                p_acf = int(float(match["meta"].get("precio_actual", 0) or 0))
                if r["precio_actual"] != p_acf:
                    cambio_str = f"{p_acf}€ -> {r['precio_actual']}€"
                    print(f"   🔄 ACTUALIZANDO: {cambio_str}", flush=True)
                    wcapi.put(
                        f"products/{match['id']}",
                        {
                            "sale_price": str(r["precio_actual"]),
                            "regular_price": str(r["precio_original"]),
                            "meta_data": [
                                {"key": "precio_actual", "value": str(r["precio_actual"])},
                                {"key": "precio_original", "value": str(r["precio_original"])},
                                {"key": "enviado_desde_tg", "value": ENVIADO_DESDE_TG},
                                {"key": "url_oferta", "value": url_oferta},
                                {"key": "url_sin_acortar_con_mi_afiliado", "value": url_con_afiliado},
                                {"key": "imagen_producto", "value": r.get("img","")},
                                {"key": "version", "value": r.get("version","Global")},
                            ],
                        },
                    )
                    summary_actualizados.append({"nombre": r["nombre"], "id": match["id"], "cambio": cambio_str})
                else:
                    summary_ignorados.append({"nombre": r["nombre"], "id": match["id"]})
                    print("   ⏭️ IGNORADO: Ya está actualizado.", flush=True)

            else:
                print("   🆕 CREANDO PRODUCTO NUEVO...", flush=True)
                data = {
                    "name": r["nombre"],
                    "type": "simple",
                    "status": "publish",
                    "regular_price": str(r["precio_original"]),
                    "sale_price": str(r["precio_actual"]),
                    "categories": [{"id": id_padre}, {"id": id_hijo}] if id_hijo else ([{"id": id_padre}] if id_padre else []),
                    "images": [{"src": img_final_producto}] if img_final_producto else [],
                    "meta_data": [
                        {"key": "nombre_movil_final", "value": r["nombre"]},
                        {"key": "importado_de", "value": ID_IMPORTACION},
                        {"key": "fecha", "value": r["fecha"]},
                        {"key": "memoria", "value": r["memoria"]},
                        {"key": "capacidad", "value": r["capacidad"]},
                        {"key": "fuente", "value": FUENTE},
                        {"key": "precio_actual", "value": str(r["precio_actual"])},
                        {"key": "precio_original", "value": str(r["precio_original"])},
                        {"key": "codigo_de_descuento", "value": CODIGO_DESCUENTO},
                        {"key": "enviado_desde", "value": ENVIADO_DESDE},
                        {"key": "enviado_desde_tg", "value": ENVIADO_DESDE_TG},
                        {"key": "enlace_de_compra_importado", "value": url_importada_sin_afiliado},
                        {"key": "url_importada_sin_afiliado", "value": url_importada_sin_afiliado},
                        {"key": "url_sin_acortar_con_mi_afiliado", "value": url_con_afiliado},
                        {"key": "url_oferta", "value": url_oferta},
                        {"key": "imagen_producto", "value": r.get("img","")},
                        {"key": "version", "value": r.get("version","Global")},
                    ],
                }

                intentos = 0
                max_intentos = 10
                creado = False

                while intentos < max_intentos and not creado:
                    intentos += 1
                    try:
                        print(f"    ⏳ Intentando crear {r['nombre']} (Intento {intentos}/{max_intentos})...", flush=True)
                        res = wcapi.post("products", data)

                        if res.status_code in (200, 201):
                            creado = True
                            prod_res = res.json()
                            summary_creados.append({"nombre": r["nombre"], "id": prod_res.get("id")})

                            # Acortar permalink del post
                            url_short = acortar_url(prod_res.get("permalink", ""))
                            if url_short:
                                wcapi.put(
                                    f"products/{prod_res.get('id')}",
                                    {"meta_data": [{"key": "url_post_acortada", "value": url_short}]},
                                )

                            print(f"   ✅ CREADO -> ID: {prod_res.get('id')}", flush=True)
                        else:
                            body_preview = (res.text or "").replace("\n", " ")[:250]
                            print(f"   ⚠️  Error {res.status_code} al crear {r['nombre']}: {body_preview}", flush=True)

                    except Exception as e:
                        print(f"   ⚠️  Excepción al crear {r['nombre']}: {e}", flush=True)

                    if (not creado) and (intentos < max_intentos):
                        print("    ⏳ Esperando 15s antes del siguiente reintento...", flush=True)
                        time.sleep(15)

                if not creado:
                    print(f"   ❌ NO SE PUDO CREAR tras {max_intentos} intentos -> {r['nombre']}", flush=True)
                    summary_fallidos.append(r.get("nombre", "desconocido"))

        except Exception as e:
            print(f"   ❌ ERROR en {r.get('nombre')}: {e}", flush=True)
            summary_fallidos.append(r.get("nombre", "desconocido"))

    # Resumen ejecución
    total_procesados = (
        len(summary_creados) +
        len(summary_eliminados) +
        len(summary_actualizados) +
        len(summary_ignorados) +
        len(summary_sin_stock_nuevos) +
        len(summary_fallidos) +
        len(summary_duplicados)
    )

    print("\n" + "=" * 60, flush=True)
    print(f"📋 RESUMEN DE EJECUCIÓN ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})", flush=True)
    print("=" * 60, flush=True)
    print(f"📊 TOTAL PRODUCTOS PROCESADOS: {total_procesados} (Objetivo: 72)", flush=True)

    print("\n" + "=" * 60, flush=True)
    print(f"a) ARTÍCULOS CREADOS ({len(summary_creados)}):", flush=True)
    for item in summary_creados:
        print(f"- {item['nombre']} (ID: {item['id']})", flush=True)

    print("-" * 40, flush=True)
    print(f"c) ARTÍCULOS ACTUALIZADOS ({len(summary_actualizados)}):", flush=True)
    for item in summary_actualizados:
        print(f"- {item['nombre']} (ID: {item['id']}): {item['cambio']}", flush=True)

    print("-" * 40, flush=True)
    print(f"d) ARTÍCULOS IGNORADOS ({len(summary_ignorados)}):", flush=True)
    for item in summary_ignorados:
        print(f"- {item['nombre']} (ID: {item['id']})", flush=True)

    print("-" * 40, flush=True)
    print(f"f) VARIANTES DE COLOR IGNORADAS ({len(summary_duplicados)}):", flush=True)
    if summary_duplicados:
        for i, item in enumerate(summary_duplicados[:10], 1):
            print(f"   {i:2d}. {item}", flush=True)

    print("-" * 40, flush=True)
    print(f"g) FALLIDOS ({len(summary_fallidos)}):", flush=True)
    for item in summary_fallidos:
        print(f"- {item}", flush=True)

    print("=" * 60, flush=True)

if __name__ == "__main__":
    remotos = obtener_datos_remotos()
    if remotos:
        sincronizar(remotos)
