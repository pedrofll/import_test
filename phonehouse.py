import os
import time
import re
import requests
import urllib.parse
from datetime import datetime
from bs4 import BeautifulSoup
from woocommerce import API
from PIL import Image
import io
import hashlib
import json

# ============================================================
#  SCRAPER PHONE HOUSE - VERSIÓN COMPLETA SIN SELENIUM
# ============================================================

# --- CONFIGURACIÓN PRINCIPAL (OCULTA EN SECRETS) ---
START_URL = os.environ["PHONEHOUSE_URL"].strip()

FUENTE = "Phone House"

# Dominio base extraído automáticamente
ID_IMPORTACION = START_URL.split("/moviles")[0]

# Código de afiliado oculto en secret
ID_AFILIADO_PHONE_HOUSE = os.environ["AFF_PHONEHOUSE"]

ENVIADO_DESDE = "España"
ENVIADO_DESDE_TG = "🇪🇸 España"
CODIGO_DESCUENTO = "OFERTA PROMO"

# --- CONFIGURACIÓN REDIMENSIÓN IMÁGENES ---
REDIMENSIONAR_IMAGENES = True
TAMANO_IMAGEN = (600, 600)
CALIDAD_JPEG = 85
DIRECTORIO_IMAGENES = "imagenes_phonehouse_600x600"

# --- CONFIGURACIÓN WORDPRESS ---
wcapi = API(
    url=os.environ["WP_URL"],
    consumer_key=os.environ["WP_KEY"],
    consumer_secret=os.environ["WP_SECRET"],
    version="wc/v3",
    timeout=60
)

# --- VARIABLES GLOBALES ---
summary_creados, summary_eliminados, summary_actualizados = [], [], []
summary_ignorados, summary_sin_stock_nuevos, summary_fallidos = [], [], []
summary_duplicados = []
iphones_memoria = {}
logs = []
archivo_log = f"scraper_phonehouse_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.phonehouse.es/"
}

# --- SISTEMA DE LOGS ---
def registrar_log(mensaje, nivel="INFO", mostrar=True):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] [{nivel}] {mensaje}"
    logs.append(log_entry)

    with open(archivo_log, "a", encoding="utf-8") as f:
        f.write(log_entry + "\n")

    if mostrar:
        print(log_entry)

# --- DESCARGA Y REDIMENSIÓN DE IMÁGENES ---
def descargar_y_redimensionar_imagen(url_imagen, nombre_producto):
    if not REDIMENSIONAR_IMAGENES or not url_imagen or not url_imagen.startswith("http"):
        return url_imagen

    try:
        os.makedirs(DIRECTORIO_IMAGENES, exist_ok=True)

        nombre_seguro = re.sub(r"[^\w\-_]", "", nombre_producto[:50].lower().replace(" ", "_"))
        hash_url = hashlib.md5(url_imagen.encode()).hexdigest()[:8]
        nombre_archivo = f"{nombre_seguro}_{hash_url}.jpg"
        ruta_completa = os.path.join(DIRECTORIO_IMAGENES, nombre_archivo)

        if os.path.exists(ruta_completa):
            return ruta_completa

        r = requests.get(url_imagen, headers=HEADERS, timeout=10)
        r.raise_for_status()

        imagen = Image.open(io.BytesIO(r.content))

        if imagen.mode in ("RGBA", "LA", "P"):
            imagen = imagen.convert("RGB")

        imagen.thumbnail(TAMANO_IMAGEN, Image.Resampling.LANCZOS)

        imagen_cuadrada = Image.new("RGB", TAMANO_IMAGEN, (255, 255, 255))
        x_offset = (TAMANO_IMAGEN[0] - imagen.size[0]) // 2
        y_offset = (TAMANO_IMAGEN[1] - imagen.size[1]) // 2
        imagen_cuadrada.paste(imagen, (x_offset, y_offset))

        imagen_cuadrada.save(ruta_completa, "JPEG", quality=CALIDAD_JPEG, optimize=True)

        return ruta_completa

    except Exception as e:
        registrar_log(f"Error redimensionando imagen: {str(e)}", "WARNING")
        return url_imagen

# --- DESCARGA HTML SIN SELENIUM ---
def obtener_html_sin_selenium():
    print(">>> Descargando HTML sin Selenium...")
    registrar_log("Descargando HTML sin Selenium...", "INFO")
    try:
        r = requests.get(START_URL, headers=HEADERS, timeout=10)
        r.raise_for_status()
        registrar_log("HTML descargado correctamente", "SUCCESS")
        print(">>> HTML descargado correctamente")
        return r.text
    except Exception as e:
        print(">>> ERROR descargando HTML:", e)
        registrar_log(f"Error descargando HTML: {str(e)}", "ERROR")
        return None

# --- UTILIDADES ---
def acortar_url(url_larga):
    try:
        url_encoded = urllib.parse.quote(url_larga)
        r = requests.get(f"https://is.gd/create.php?format=simple&url={url_encoded}", timeout=10)
        return r.text.strip() if r.status_code == 200 else url_larga
    except:
        return url_larga

def abs_url(base, href):
    try:
        if href.startswith("//"):
            href = "https:" + href
        return urllib.parse.urljoin(base, href)
    except:
        return href

def parse_eur_int(txt):
    if not txt:
        return 0
    t = txt.replace("\xa0", " ").strip()
    m = re.search(r"(\d{1,5}(?:[.,]\d{1,2})?)", t)
    if not m:
        return 0
    num = m.group(1).replace(".", "").replace(",", ".")
    try:
        return int(float(num))
    except:
        return 0

def normalize_spaces(s):
    return re.sub(r"\s+", " ", (s or "")).strip()

# --- MAPA DE RAM PARA IPHONES ---
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
def ram_por_modelo_iphone(nombre):
    if not nombre:
        return None
    n = nombre.lower()
    if "iphone" not in n:
        return None
    for needle, ram in IPHONE_RAM_MAP:
        if needle in n:
            return ram
    return "8GB"


def registrar_iphone_memoria(nombre, memoria):
    clave = f"{nombre}_{memoria}"
    if clave in iphones_memoria:
        return False
    iphones_memoria[clave] = {
        "nombre": nombre,
        "memoria": memoria,
        "fecha": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    return True


def guardar_memoria_iphones():
    if iphones_memoria:
        archivo = f"memoria_iphones_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(archivo, "w", encoding="utf-8") as f:
            json.dump(iphones_memoria, f, ensure_ascii=False, indent=2)
        registrar_log(f"Memoria iPhones guardada: {archivo}", "INFO")


# --- EXTRACCIÓN DE INFORMACIÓN ---
def extraer_nombre_memoria_capacidad(titulo):
    t = normalize_spaces(titulo)

    m_combo = re.search(r"(\d+)\s*(TB|GB)\s*\+\s*(\d+)\s*GB", t, flags=re.I)
    if m_combo:
        capacidad = f"{m_combo.group(1)}{m_combo.group(2).upper()}"
        memoria = f"{m_combo.group(3)}GB"
        nombre = t[:m_combo.start()].strip()
        return normalize_spaces(nombre), capacidad, memoria

    m_cap = re.search(r"(\d+)\s*(TB|GB)", t, flags=re.I)
    capacidad = f"{m_cap.group(1)}{m_cap.group(2).upper()}" if m_cap else ""

    m_mem = re.search(r"(\d+)\s*GB\s*RAM", t, flags=re.I)
    memoria = f"{m_mem.group(1)}GB" if m_mem else ""

    if m_cap:
        nombre = t[:m_cap.start()].strip()
    else:
        nombre = t

    return normalize_spaces(nombre), capacidad, (memoria or "")


# --- GESTIÓN DE CATEGORÍAS ---
def obtener_todas_las_categorias():
    categorias = []
    page = 1
    while True:
        try:
            res = wcapi.get("products/categories", params={"per_page": 100, "page": page}).json()
            if not res or "message" in res:
                break
            categorias.extend(res)
            page += 1
        except:
            break
    return categorias


def resolver_jerarquia(nombre_completo, cache_categorias, img_categoria=None):
    palabras = (nombre_completo or "").split()
    nombre_padre = palabras[0] if palabras else "Smartphones"
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
        registrar_log(f"Categoría creada: {nombre_padre} (ID: {id_cat_padre})", "INFO")

    for cat in cache_categorias:
        if cat.get("name", "").lower() == nombre_hijo.lower() and cat.get("parent") == id_cat_padre:
            id_cat_hijo = cat.get("id")
            break

    if not id_cat_hijo:
        payload = {"name": nombre_hijo, "parent": id_cat_padre}
        if img_categoria:
            payload["image"] = {"src": img_categoria}
        res = wcapi.post("products/categories", payload).json()
        id_cat_hijo = res.get("id")
        cache_categorias.append(res)
        registrar_log(f"Subcategoría creada: {nombre_hijo} (ID: {id_cat_hijo})", "INFO")

    return id_cat_padre, id_cat_hijo


# --- EXTRACCIÓN DE PRODUCTOS ---
def obtener_datos_remotos():
    total_productos = []

    registrar_log("=" * 70, "INFO")
    registrar_log("INICIANDO EXTRACCIÓN DE PRODUCTOS", "INFO")
    registrar_log(f"URL: {START_URL}", "INFO")
    registrar_log("=" * 70, "INFO")

    html = obtener_html_sin_selenium()
    if not html:
        registrar_log("No se pudo obtener HTML", "ERROR")
        return []

    soup = BeautifulSoup(html, "html.parser")

    all_links = soup.find_all("a", href=True)
    registrar_log(f"Enlaces totales encontrados: {len(all_links)}", "INFO")
    print(f">>> Enlaces totales encontrados: {len(all_links)}")

    productos_procesados = 0
    urls_procesadas = set()

    for link in all_links:
        if productos_procesados >= 72:
            break

        try:
            href = link.get("href", "")
            if not href:
                continue

            # Solo móviles
            if not href.lower().startswith("/movil/"):
                continue

            # Excluir NO-móviles por URL
            if any(x in href.lower() for x in [
                "comparador", "tarifa", "fibra", "internet", "seguro",
                "repar", "smartwatch", "wear", "auricular", "tablet", "sim"
            ]):
                continue

            url_completa = abs_url(START_URL, href)
            if url_completa in urls_procesadas:
                continue
            urls_procesadas.add(url_completa)

            titulo = link.get_text(strip=True)
            titulo = re.sub(r"^¡?oferta!?[\s\-:]*", "", titulo, flags=re.IGNORECASE).strip()

            # Si no hay título, intentar desde ficha
            if not titulo or len(titulo) < 5:
                try:
                    detalle = requests.get(url_completa, headers=HEADERS, timeout=10)
                    detalle_soup = BeautifulSoup(detalle.text, "html.parser")
                    h1 = detalle_soup.find("h1")
                    if h1:
                        titulo = h1.get_text(strip=True)
                except Exception as e:
                    registrar_log(f"Error obteniendo título desde ficha: {e}", "WARNING")

            if not titulo or len(titulo) < 5:
                continue

            # Excluir NO-móviles por título
            if any(x in titulo.lower() for x in [
                "comparador", "tarifa", "fibra", "internet", "seguro",
                "repar", "smartwatch", "wear", "auricular", "tablet", "sim"
            ]):
                continue

            # Filtrar accesorios
            if any(x in titulo.lower() for x in ["funda", "cargador", "protector", "accesorio"]):
                continue

            nombre, capacidad, memoria = extraer_nombre_memoria_capacidad(titulo)

            nombre_normalizado = re.sub(
                r"\b(negro|black|azul|blue|verde|green|rojo|red|blanco|white|morado|purple|rosa|pink)\b",
                "",
                nombre,
                flags=re.IGNORECASE
            ).strip()

            es_iphone = "iphone" in nombre_normalizado.lower()
            if es_iphone and not memoria:
                memoria = ram_por_modelo_iphone(nombre_normalizado)

            # Regla: si no es iPhone y no tiene memoria ni capacidad → descartar
            if not es_iphone and (not memoria and not capacidad):
                continue

            if not capacidad:
                capacidad = "128GB"

            clave_unica = f"{nombre_normalizado}_{capacidad}"
            if any(p.get("clave_unica") == clave_unica for p in total_productos):
                summary_duplicados.append(clave_unica)
                continue

            img_final = ""
            try:
                detalle = requests.get(url_completa, headers=HEADERS, timeout=10)
                detalle_soup = BeautifulSoup(detalle.text, "html.parser")
                img_tag = detalle_soup.find("img", src=re.compile(r"products-image", re.I))
                if img_tag:
                    src = img_tag.get("src", "").split("?")[0]
                    img_final = descargar_y_redimensionar_imagen(src, nombre_normalizado)
            except Exception as e:
                registrar_log(f"Error obteniendo imagen desde ficha: {e}", "WARNING")

            if not img_final:
                img_tag = link.find("img")
                if img_tag:
                    for attr in ["src", "data-src", "data-original", "data-lazy"]:
                        src = img_tag.get(attr)
                        if src and "http" in src:
                            src = abs_url(START_URL, src).split("?")[0]
                            img_final = descargar_y_redimensionar_imagen(src, nombre_normalizado)
                            break

            precio_actual = 0
            precio_original = 0

            parent = link.parent
            for _ in range(5):
                if parent:
                    precios = parent.find_all(["span", "div"], class_=re.compile(r"price|precio", re.I))
                    for tag in precios:
                        texto = tag.get_text(strip=True)
                        if "€" in texto:
                            precio = parse_eur_int(texto)
                            if precio > 0:
                                if "tachado" in str(tag.get("class", "")):
                                    precio_original = precio
                                else:
                                    precio_actual = precio
                    if precio_actual > 0:
                        break
                parent = getattr(parent, "parent", None)

            if precio_actual == 0:
                precio_actual = 299
                precio_original = int(precio_actual * 1.15)

            producto = {
                "nombre": nombre_normalizado,
                "memoria": memoria,
                "capacidad": capacidad,
                "precio_actual": precio_actual,
                "precio_original": precio_original,
                "img": img_final,
                "url_imp": url_completa,
                "enviado_desde": ENVIADO_DESDE,
                "enviado_desde_tg": ENVIADO_DESDE_TG,
                "fecha": datetime.now().strftime("%d/%m/%Y"),
                "en_stock": True,
                "clave_unica": clave_unica,
                "version": "IOS" if es_iphone else "Android",
                "fuente": FUENTE,
                "codigo_descuento": CODIGO_DESCUENTO,
                "es_iphone": es_iphone
            }

            total_productos.append(producto)
            productos_procesados += 1

            msg = f"[{productos_procesados}] {producto['nombre']} | {precio_actual}€ | {capacidad} | {memoria}"
            registrar_log(msg, "INFO", False)
            print(">>>", msg)

        except Exception as e:
            registrar_log(f"Error procesando producto: {str(e)}", "WARNING")

    registrar_log(f"Productos válidos extraídos: {len(total_productos)}", "INFO")
    print(f">>> Productos válidos extraídos: {len(total_productos)}")
    return total_productos

# --- CREACIÓN DE PRODUCTOS EN WOOCOMMERCE ---
def crear_producto_woocommerce(producto, cache_categorias, max_intentos=10):
    intentos = 0
    while intentos < max_intentos:
        intentos += 1
        try:
            registrar_log(f"Intento {intentos}/{max_intentos} para crear: {producto['nombre']}", "INFO", False)

            url_importada_sin_afiliado = producto["url_imp"]
            url_con_afiliado = f"{url_importada_sin_afiliado}{ID_AFILIADO_PHONE_HOUSE}"
            url_oferta = acortar_url(url_con_afiliado)

            id_padre, id_hijo = resolver_jerarquia(
                producto["nombre"],
                cache_categorias,
                img_categoria=producto["img"]
            )

            data = {
                "name": producto["nombre"],
                "type": "simple",
                "status": "publish",
                "regular_price": str(producto["precio_original"]),
                "sale_price": str(producto["precio_actual"]),
                "description": f"{producto['nombre']} - Memoria: {producto['memoria']} - Capacidad: {producto['capacidad']}",
                "short_description": f"Precio especial: {producto['precio_actual']}€",
                "categories": [{"id": id_padre}, {"id": id_hijo}] if id_hijo else [{"id": id_padre}],
                "images": [{"src": producto["img"]}] if producto["img"] else [],
                "meta_data": [
                    {"key": "importado_de", "value": ID_IMPORTACION},
                    {"key": "fecha_importacion", "value": producto["fecha"]},
                    {"key": "memoria", "value": producto["memoria"]},
                    {"key": "capacidad", "value": producto["capacidad"]},
                    {"key": "fuente", "value": producto["fuente"]},
                    {"key": "precio_actual", "value": str(producto["precio_actual"])},
                    {"key": "precio_original", "value": str(producto["precio_original"])},
                    {"key": "codigo_de_descuento", "value": producto["codigo_descuento"]},
                    {"key": "enviado_desde", "value": producto["enviado_desde"]},
                    {"key": "enviado_desde_tg", "value": producto["enviado_desde_tg"]},
                    {"key": "enlace_de_compra_importado", "value": producto["url_imp"]},
                    {"key": "url_importada_sin_afiliado", "value": producto["url_imp"]},
                    {"key": "url_sin_acortar_con_mi_afiliado", "value": url_con_afiliado},
                    {"key": "url_oferta", "value": url_oferta},
                    {"key": "imagen_producto", "value": producto["img"]},
                    {"key": "version", "value": producto["version"]},
                    {"key": "es_iphone", "value": "1" if producto["es_iphone"] else "0"},
                ]
            }

            response = wcapi.post("products", data)

            if response.status_code in [200, 201]:
                producto_creado = response.json()
                product_id = producto_creado.get("id")
                product_url = producto_creado.get("permalink")

                if product_url:
                    url_producto_acortada = acortar_url(product_url)
                    wcapi.put(f"products/{product_id}", {
                        "meta_data": [{"key": "url_post_acortada", "value": url_producto_acortada}]
                    })

                registrar_log(f"✅ CREADO: {producto['nombre']} (ID: {product_id})", "SUCCESS")
                print(f">>> CREADO: {producto['nombre']} (ID: {product_id})")
                return True, product_id

            else:
                registrar_log(f"Error {response.status_code}: {response.text[:200]}", "WARNING", False)
                time.sleep(2 ** intentos)

        except Exception as e:
            registrar_log(f"Excepción en intento {intentos}: {str(e)}", "ERROR", False)
            time.sleep(5)

    registrar_log(f"❌ FALLIDO tras {max_intentos} intentos: {producto['nombre']}", "ERROR")
    print(f">>> FALLIDO tras {max_intentos} intentos: {producto['nombre']}")
    return False, None


# --- SINCRONIZACIÓN PRINCIPAL ---
def sincronizar_productos(productos_remotos):
    registrar_log("=" * 70, "INFO")
    registrar_log("INICIANDO SINCRONIZACIÓN CON WOOCOMMERCE", "INFO")
    registrar_log("=" * 70, "INFO")

    cache_categorias = obtener_todas_las_categorias()
    productos_existentes = []
    page = 1

    while True:
        try:
            response = wcapi.get("products", params={
                "per_page": 100,
                "page": page,
                "status": "any"
            }).json()

            if not response or isinstance(response, dict) and "message" in response:
                break

            for producto in response:
                meta_dict = {m["key"]: str(m["value"]) for m in producto.get("meta_data", []) if "key" in m}

                if "phonehouse.es" in meta_dict.get("importado_de", ""):
                    productos_existentes.append({
                        "id": producto["id"],
                        "nombre": producto.get("name", ""),
                        "url": meta_dict.get("enlace_de_compra_importado", ""),
                        "meta": meta_dict
                    })

            if len(response) < 100:
                break

            page += 1

        except Exception as e:
            registrar_log(f"Error obteniendo productos existentes: {str(e)}", "ERROR")
            break

    registrar_log(f"Productos Phone House existentes: {len(productos_existentes)}", "INFO")
    print(f">>> Productos Phone House existentes: {len(productos_existentes)}")

    for producto in productos_remotos:
        try:
            existente = next((p for p in productos_existentes if p["url"].strip() == producto["url_imp"].strip()), None)

            if existente:
                precio_actual_existente = float(existente["meta"].get("precio_actual", 0) or 0)

                if abs(producto["precio_actual"] - precio_actual_existente) > 1:
                    update_data = {
                        "sale_price": str(producto["precio_actual"]),
                        "regular_price": str(producto["precio_original"]),
                        "meta_data": [
                            {"key": "precio_actual", "value": str(producto["precio_actual"])},
                            {"key": "precio_original", "value": str(producto["precio_original"])},
                            {"key": "enviado_desde_tg", "value": producto["enviado_desde_tg"]},
                            {"key": "url_oferta", "value": acortar_url(f"{producto['url_imp']}{ID_AFILIADO_PHONE_HOUSE}")},
                        ]
                    }

                    wcapi.put(f"products/{existente['id']}", update_data)
                    summary_actualizados.append({
                        "nombre": producto["nombre"],
                        "id": existente["id"],
                        "cambio": f"{precio_actual_existente}€ → {producto['precio_actual']}€"
                    })

                    registrar_log(f"🔄 ACTUALIZADO: {producto['nombre']} (ID: {existente['id']})", "INFO")
                    print(f">>> ACTUALIZADO: {producto['nombre']} (ID: {existente['id']})")

                else:
                    summary_ignorados.append({
                        "nombre": producto["nombre"],
                        "id": existente["id"]
                    })
                    registrar_log(f"⏭️ IGNORADO: {producto['nombre']} (sin cambios)", "INFO", False)

            else:
                exito, product_id = crear_producto_woocommerce(producto, cache_categorias)

                if exito:
                    summary_creados.append({"nombre": producto["nombre"], "id": product_id})
                else:
                    summary_fallidos.append(producto["nombre"])

            time.sleep(1)

        except Exception as e:
            registrar_log(f"Error procesando {producto.get('nombre', 'desconocido')}: {str(e)}", "ERROR")
            summary_fallidos.append(producto.get("nombre", "desconocido"))

    mostrar_resumen_completo()


# --- RESUMEN FINAL ---
def mostrar_resumen_completo():
    total_procesados = (
        len(summary_creados)
        + len(summary_eliminados)
        + len(summary_actualizados)
        + len(summary_ignorados)
        + len(summary_fallidos)
    )

    registrar_log("=" * 70, "INFO")
    registrar_log("📋 RESUMEN FINAL DE EJECUCIÓN", "INFO")
    registrar_log(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", "INFO")
    registrar_log("=" * 70, "INFO")

    registrar_log(f"Total procesados: {total_procesados}", "INFO")
    registrar_log(f"Creados: {len(summary_creados)}", "SUCCESS")
    registrar_log(f"Actualizados: {len(summary_actualizados)}", "INFO")
    registrar_log(f"Ignorados: {len(summary_ignorados)}", "INFO")
    registrar_log(f"Fallidos: {len(summary_fallidos)}", "ERROR")

    registrar_log(f"iPhones en memoria: {len(iphones_memoria)}", "INFO")

    guardar_memoria_iphones()

    registrar_log("✅ PROCESO COMPLETADO", "SUCCESS")
    registrar_log("=" * 70, "INFO")
    print(">>> PROCESO COMPLETADO")


# --- EJECUCIÓN PRINCIPAL ---
def main():
    print("\n" + "=" * 80)
    print("🤖 SCRAPER PHONE HOUSE - VERSIÓN SIN SELENIUM")
    print("=" * 80)
    print(f"🔗 URL: {START_URL}")
    print(f"📏 Redimensión imágenes: {'SÍ' if REDIMENSIONAR_IMAGENES else 'NO'}")
    print(f"📱 Memoria iPhones: ACTIVADA")
    print("=" * 80 + "\n")

    try:
        productos = obtener_datos_remotos()

        if not productos:
            registrar_log("No se encontraron productos", "ERROR")
            print(">>> No se encontraron productos")
            return

        sincronizar_productos(productos)

        print("\n" + "=" * 80)
        print("🎉 ¡PROCESO COMPLETADO CON ÉXITO!")
        print("=" * 80)
        print(f"📊 Productos creados/actualizados: {len(summary_creados) + len(summary_actualizados)}")
        print(f"📁 Logs guardados en: {archivo_log}")
        print("=" * 80)

    except Exception as e:
        registrar_log(f"Error crítico en ejecución principal: {str(e)}", "ERROR")
        print(">>> Error crítico en ejecución principal:", e)

if __name__ == "__main__":
    print(">>> Llamando a main()...")
    main()

print("Hola")






