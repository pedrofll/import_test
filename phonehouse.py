import requests
from bs4 import BeautifulSoup
from woocommerce import API
import os
import sys  # <--- Necesario para sys.exit()
import time
import re
import smtplib
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from urllib.parse import urlparse, parse_qs, unquote, quote
from collections import defaultdict

import urllib.parse
import hashlib
import io
import json
from PIL import Image
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException


# ============================================================
#  CONFIGURACIÓN INICIAL (El orden es importante)
# ============================================================

# 1. Cargar la URL desde Secrets (Variables de entorno)
START_URL = os.environ.get("SOURCE_URL_PHONEHOUSE", "")

# 2. Comprobar si existe ANTES de seguir.
#    Si no existe, paramos el script aquí mismo para evitar errores después.
if not START_URL:
    print("❌ ERROR FATAL: No se ha recibido la variable 'SOURCE_URL_PHONEHOUSE'.", flush=True)
    print("   -> Revisa tu archivo .yml en GitHub Actions.", flush=True)
    print("   -> Asegúrate de haber añadido en 'env': SOURCE_URL_PHONEHOUSE: ${{ secrets.SOURCE_URL_PHONEHOUSE }}", flush=True)
    sys.exit(1)

# 3. Resto de constantes
FUENTE = "Phone House"
ID_IMPORTACION = "phonehouse.es"

# --- CONFIGURACIÓN WORDPRESS ---
wcapi = API(
    url=os.environ.get("WP_URL", ""),
    consumer_key=os.environ.get("WP_KEY", ""),
    consumer_secret=os.environ.get("WP_SECRET", ""),
    version="wc/v3",
    timeout=60
)

# --- PARÁMETROS DE AFILIADO ---
ID_AFILIADO_PHONE_HOUSE = os.environ.get("AFF_PHONEHOUSE", "")

ENVIADO_DESDE = "España"
ENVIADO_DESDE_TG = "🇪🇸 España"
CODIGO_DESCUENTO = "OFERTA PROMO"

# --- CONFIGURACIÓN REDIMENSIÓN IMÁGENES ---
REDIMENSIONAR_IMAGENES = True
TAMANO_IMAGEN = (600, 600)
CALIDAD_JPEG = 85
DIRECTORIO_IMAGENES = "imagenes_phonehouse_600x600"


# --- VARIABLES GLOBALES ---
summary_creados, summary_eliminados, summary_actualizados = [], [], []
summary_ignorados, summary_sin_stock_nuevos, summary_fallidos = [], [], []
summary_duplicados = []
iphones_memoria = {}  # Memoria para iPhones
logs = []  # Sistema de logs
archivo_log = f"scraper_phonehouse_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.phonehouse.es/"
}

# --- SISTEMA DE LOGS COMPLETO ---
def registrar_log(mensaje, nivel="INFO", mostrar=True):
    """Sistema completo de logs"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] [{nivel}] {mensaje}"
    logs.append(log_entry)
    
    # Guardar en archivo
    with open(archivo_log, "a", encoding="utf-8") as f:
        f.write(log_entry + "\n")
    
    # Mostrar en consola si está activado
    if mostrar:
        if nivel == "ERROR":
            print(f"\033[91m{log_entry}\033[0m", flush=True)
        elif nivel == "WARNING":
            print(f"\033[93m{log_entry}\033[0m", flush=True)
        elif nivel == "SUCCESS":
            print(f"\033[92m{log_entry}\033[0m", flush=True)
        else:
            print(log_entry), flush=True)
# --- FUNCIÓN REDIMENSIÓN IMÁGENES ---
def descargar_y_redimensionar_imagen(url_imagen, nombre_producto):
    """Descarga y redimensiona una imagen a 600x600 píxeles"""
    if not REDIMENSIONAR_IMAGENES or not url_imagen or not url_imagen.startswith('http'):
        return url_imagen
    
    try:
        # Crear directorio si no existe
        os.makedirs(DIRECTORIO_IMAGENES, exist_ok=True)
        
        # Generar nombre de archivo único
        nombre_seguro = re.sub(r'[^\w\-_]', '', nombre_producto[:50].lower().replace(' ', '_'))
        hash_url = hashlib.md5(url_imagen.encode()).hexdigest()[:8]
        nombre_archivo = f"{nombre_seguro}_{hash_url}.jpg"
        ruta_completa = os.path.join(DIRECTORIO_IMAGENES, nombre_archivo)
        
        # Si ya existe, devolver la ruta local
        if os.path.exists(ruta_completa):
            return ruta_completa
        
        # Descargar imagen
        headers = {'User-Agent': HEADERS['User-Agent']}
        response = requests.get(url_imagen, headers=headers, timeout=30)
        response.raise_for_status()
        
        # Abrir y redimensionar imagen
        imagen = Image.open(io.BytesIO(response.content))
        
        # Convertir a RGB si es necesario
        if imagen.mode in ('RGBA', 'LA', 'P'):
            imagen = imagen.convert('RGB')
        
        # Redimensionar manteniendo relación de aspecto
        imagen.thumbnail(TAMANO_IMAGEN, Image.Resampling.LANCZOS)
        
        # Crear imagen cuadrada 600x600 con fondo blanco
        imagen_cuadrada = Image.new('RGB', TAMANO_IMAGEN, (255, 255, 255))
        
        # Pegar la imagen redimensionada centrada
        x_offset = (TAMANO_IMAGEN[0] - imagen.size[0]) // 2
        y_offset = (TAMANO_IMAGEN[1] - imagen.size[1]) // 2
        imagen_cuadrada.paste(imagen, (x_offset, y_offset))
        
        # Guardar imagen
        imagen_cuadrada.save(ruta_completa, 'JPEG', quality=CALIDAD_JPEG, optimize=True)
        
        registrar_log(f"Imagen redimensionada: {nombre_archivo} (600x600)", "INFO", False)
        return ruta_completa
        
    except Exception as e:
        registrar_log(f"Error redimensionando imagen: {str(e)[:100]}", "WARNING", False)
        return url_imagen

# --- FUNCIÓN SCROLL AJAX ---
def obtener_html_con_scroll_ajax():
    """Usa Selenium para hacer scroll y cargar todos los 72 productos"""
    registrar_log("Iniciando navegador para scroll automático", "INFO")
    
    try:
        # Configurar Chrome headless
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument(f"user-agent={HEADERS['User-Agent']}")
        
        driver = webdriver.Chrome(options=chrome_options)
        try:
            driver.set_page_load_timeout(40)
        except Exception:
            pass
        try:
            driver.get(START_URL)
        except TimeoutException:
            registrar_log("Timeout cargando URL en Selenium (page_load_timeout)", "WARNING")
            try:
                driver.execute_script("window.stop();")
            except Exception:
                pass
        try:
            registrar_log(f"URL final (Selenium) tras redirecciones: {driver.current_url}", "INFO")
        except Exception:
            pass
        
        # Esperar a que cargue la página inicial
        time.sleep(3)
        
        # Hacer scroll hasta el final para cargar todos los productos
        registrar_log("Haciendo scroll hasta el final de la página", "INFO")
        
        last_height = driver.execute_script("return document.body.scrollHeight")
        scroll_attempts = 0
        max_scroll_attempts = 20
        
        while scroll_attempts < max_scroll_attempts:
            # Scroll hacia abajo
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            
            # Calcular nueva altura
            new_height = driver.execute_script("return document.body.scrollHeight")
            
            # Contar productos actuales
            product_count = driver.execute_script("""
                var links = document.querySelectorAll('a[href*="movil"]');
                return links.length;
            """)
            
            registrar_log(f"Scroll {scroll_attempts+1}/{max_scroll_attempts} | Productos: {product_count}/72", "INFO", False)
            
            if product_count >= 72:
                registrar_log(f"¡Encontrados {product_count} productos!", "SUCCESS")
                break
            
            if new_height == last_height:
                scroll_attempts += 1
                if scroll_attempts >= 5:
                    break
            else:
                last_height = new_height
                scroll_attempts = 0
        
        # Intentar hacer click en "Ver más" si existe
        try:
            ver_mas_button = driver.find_element(By.XPATH, "//button[contains(text(), 'Ver más')]")
            ver_mas_button.click()
            time.sleep(2)
            registrar_log("Clic en 'Ver más' realizado", "INFO")
        except:
            pass
        
        html = driver.page_source
        driver.quit()
        
        registrar_log("Scroll completado exitosamente", "SUCCESS")
        return html
        
    except Exception as e:
        registrar_log(f"Error en scroll AJAX: {str(e)}", "ERROR")
        return None

# --- UTILIDADES PRINCIPALES ---
def acortar_url(url_larga):
    """Acorta URL con is.gd"""
    try:
        url_encoded = urllib.parse.quote(url_larga)
        r = requests.get(f"https://is.gd/create.php?format=simple&url={url_encoded}", timeout=10)
        return r.text.strip() if r.status_code == 200 else url_larga
    except Exception as e:
        registrar_log(f"Error acortando URL: {str(e)}", "WARNING")
        return url_larga

def abs_url(base, href):
    """Convierte URL relativa a absoluta"""
    try:
        if href.startswith('//'):
            href = 'https:' + href
        return urllib.parse.urljoin(base, href)
    except Exception:
        return href

def parse_eur_int(txt):
    """Convierte texto de precio a entero"""
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

def normalize_spaces(s):
    """Normaliza espacios en texto"""
    return re.sub(r"\s+", " ", (s or "")).strip()

# --- MEMORIA PARA iPHONES ---
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
    """Devuelve la RAM en función del modelo de iPhone"""
    if not nombre:
        return None
    n = nombre.lower()
    if "iphone" not in n:
        return None
    for needle, ram in IPHONE_RAM_MAP:
        if needle in n:
            return ram
    return "8GB"  # Valor por defecto

def registrar_iphone_memoria(nombre, memoria):
    """Registra iPhone en memoria para evitar duplicados"""
    clave = f"{nombre}_{memoria}"
    if clave in iphones_memoria:
        return False
    iphones_memoria[clave] = {
        'nombre': nombre,
        'memoria': memoria,
        'fecha': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    return True

def guardar_memoria_iphones():
    """Guarda la memoria de iPhones en archivo JSON"""
    if iphones_memoria:
        archivo = f"memoria_iphones_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(archivo, 'w', encoding='utf-8') as f:
            json.dump(iphones_memoria, f, ensure_ascii=False, indent=2)
        registrar_log(f"Memoria iPhones guardada: {archivo}", "INFO")

# --- EXTRACCIÓN DE INFORMACIÓN ---
def extraer_nombre_memoria_capacidad(titulo):
    """
    Extrae nombre, capacidad (almacenamiento) y memoria (RAM) del título.

    Soporta formatos habituales:
      - "256GB+8GB RAM"
      - "128GB+4GB"
      - "8GB/256GB", "8/256GB"
      - "8+256GB"
      - "256GB 8GB RAM"

    Devuelve: (nombre, capacidad, memoria). Si no detecta algo, devuelve "" en ese campo.
    """
    t = normalize_spaces(titulo)

    # 1) Formatos combo (CAP+RAM o RAM+CAP) con separadores + o /
    m_combo = re.search(
        r"(?P<cap>\d{2,4})\s*(?P<unit>TB|GB)\s*[\+\/]\s*(?P<ram>\d{1,2})\s*GB(?:\s*RAM)?\b"
        r"|(?P<ram2>\d{1,2})\s*GB(?:\s*RAM)?\s*[\+\/]\s*(?P<cap2>\d{2,4})\s*(?P<unit2>TB|GB)\b",
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

    # 2) CAPACIDAD (almacenamiento) - restringimos a tamaños típicos para evitar falsos positivos (p.ej. "50GB" de datos)
    m_cap = re.search(r"\b(64|128|256|512|1024)\s*GB\b|\b(1|2)\s*TB\b", t, flags=re.I)
    capacidad = ""
    if m_cap:
        if m_cap.group(1):
            capacidad = f"{m_cap.group(1)}GB"
        else:
            capacidad = f"{m_cap.group(2)}TB"

    # 3) RAM (memoria) con o sin literal "RAM"
    m_ram = re.search(r"\b(4|6|8|12|16)\s*GB(?:\s*RAM)?\b", t, flags=re.I)
    memoria = f"{m_ram.group(1)}GB" if m_ram else ""

    # 4) Nombre: cortar por la primera aparición de capacidad o RAM
    cut_positions = []
    if m_cap:
        cut_positions.append(m_cap.start())
    if m_ram:
        cut_positions.append(m_ram.start())
    cut = min(cut_positions) if cut_positions else len(t)

    nombre = t[:cut].strip()
    return normalize_spaces(nombre), capacidad, memoria


# --- GESTIÓN DE CATEGORÍAS ---# --- GESTIÓN DE CATEGORÍAS ---
def obtener_todas_las_categorias():
    """Obtiene todas las categorías de WooCommerce"""
    categorias = []
    page = 1
    while True:
        try:
            res = wcapi.get("products/categories", params={"per_page": 100, "page": page}).json()
            if not res or "message" in res or len(res) == 0:
                break
            categorias.extend(res)
            page += 1
        except Exception as e:
            registrar_log(f"Error obteniendo categorías: {str(e)}", "ERROR")
            break
    return categorias

def resolver_jerarquia(nombre_completo, cache_categorias):
    """Resuelve la jerarquía de categorías"""
    palabras = (nombre_completo or "").split()
    nombre_padre = palabras[0] if palabras else "Smartphones"
    nombre_hijo = nombre_completo

    id_cat_padre = None
    id_cat_hijo = None

    # Buscar categoría padre
    for cat in cache_categorias:
        if cat.get("name", "").lower() == nombre_padre.lower() and cat.get("parent") == 0:
            id_cat_padre = cat.get("id")
            break
    
    # Crear categoría padre si no existe
    if not id_cat_padre:
        try:
            res = wcapi.post("products/categories", {"name": nombre_padre}).json()
            id_cat_padre = res.get("id")
            cache_categorias.append(res)
            registrar_log(f"Categoría creada: {nombre_padre} (ID: {id_cat_padre})", "INFO")
        except Exception as e:
            registrar_log(f"Error creando categoría {nombre_padre}: {str(e)}", "ERROR")

    # Buscar categoría hijo
    for cat in cache_categorias:
        if cat.get("name", "").lower() == nombre_hijo.lower() and cat.get("parent") == id_cat_padre:
            id_cat_hijo = cat.get("id")
            break
    
    # Crear categoría hijo si no existe
    if not id_cat_hijo and id_cat_padre:
        try:
            res = wcapi.post("products/categories", {
                "name": nombre_hijo,
                "parent": id_cat_padre
            }).json()
            id_cat_hijo = res.get("id")
            cache_categorias.append(res)
            registrar_log(f"Subcategoría creada: {nombre_hijo} (ID: {id_cat_hijo})", "INFO")
        except Exception as e:
            registrar_log(f"Error creando subcategoría {nombre_hijo}: {str(e)}", "ERROR")

    return id_cat_padre, id_cat_hijo

# ============================================================
#  FUNCIÓN DE EXTRACCIÓN (NUEVA Y LIMPIA)
# ============================================================

def obtener_datos_remotos():
    """Extrae productos de Phone House (modo robusto).

    Estrategia:
      1) Intentar obtener HTML con Selenium + scroll (mejor para listados con AJAX).
      2) Si Selenium falla, caer a requests.
      3) Localizar productos por enlaces y contenedores (múltiples heurísticas) para tolerar cambios de HTML.

    Devuelve: lista[dict] con los campos usados por crear_producto_woocommerce().
    """

    total_productos = []

    registrar_log("=" * 70, "INFO")
    registrar_log("INICIANDO EXTRACCIÓN DE PRODUCTOS", "INFO")
    registrar_log(f"URL: {START_URL}", "INFO")
    registrar_log(f"Redimensión imágenes: {'SÍ' if REDIMENSIONAR_IMAGENES else 'NO'}", "INFO")
    registrar_log("Scroll AJAX: INTENTANDO (fallback a modo directo)", "INFO")
    registrar_log("=" * 70, "INFO")

    try:
        # 1) Intentar Selenium + scroll
        html = None
        try:
            html = obtener_html_con_scroll_ajax()
        except Exception as e:
            registrar_log(f"Selenium/scroll no disponible: {str(e)[:160]}", "WARNING")
            html = None

        if not html:
            registrar_log("Usando método directo (requests)", "WARNING")
            session = requests.Session()
            session.headers.update(HEADERS)
            r = session.get(START_URL, timeout=30)
            try:
                u = urlparse(r.url)
                registrar_log(f"URL final (requests) tras redirecciones: {u.scheme}://{u.netloc}{u.path}", "INFO")
            except Exception:
                pass
            if r.status_code != 200:
                registrar_log(f"Error al cargar la página: HTTP {r.status_code}", "ERROR")
                return []
            soup = BeautifulSoup(r.text, "html.parser")
        else:
            soup = BeautifulSoup(html, "html.parser")
            registrar_log("HTML obtenido con Selenium/scroll", "SUCCESS")

        # 2) Descubrir SOLO fichas de producto (evita menús/categorías/servicios)
        #    Aceptamos únicamente URLs con patrón:
        #      /movil/<marca>/<slug>.html
        PRODUCT_PATH_RE = re.compile(r"^/movil/[^/]+/[^/?#]+\.html$", re.I)

        all_links = []
        seen_urls = set()

        for a in soup.find_all("a", href=True):
            href = (a.get("href") or "").strip()
            if not href:
                continue

            url_completa = abs_url(START_URL, href)

            try:
                path = urlparse(url_completa).path
            except Exception:
                continue

            if not PRODUCT_PATH_RE.match(path):
                continue

            low = url_completa.lower()
            if any(x in low for x in ["accesorio", "funda", "cargador", "protector", "kit", "reacondicionado", "seguro", "financiacion"]):
                continue

            if url_completa in seen_urls:
                continue

            seen_urls.add(url_completa)
            all_links.append(a)

        registrar_log(f"URLs de producto detectadas (/movil/.../*.html): {len(all_links)}", "INFO")

        if not all_links:
            registrar_log("No se encontraron enlaces de productos. Probable cambio de HTML o bloqueo anti-bot.", "ERROR")
            return []

        # 3) Procesar enlaces
        productos_procesados = 0
        urls_procesadas = set()

        for link in all_links:
            if productos_procesados >= 72:
                break

            try:
                href = link.get('href', '')
                if not href:
                    continue

                url_completa = abs_url(START_URL, href)
                if url_completa in urls_procesadas:
                    continue
                urls_procesadas.add(url_completa)

                # Extraer título (si es posible desde el mismo card)
                nombre_elemento = link.find(['h2', 'h3', 'h4', 'div', 'span'],
                                           class_=re.compile(r'title|name|product', re.I))
                titulo = nombre_elemento.get_text(strip=True) if nombre_elemento else link.get_text(strip=True)

                if not titulo or len(titulo) < 5:
                    continue

                if any(palabra in titulo.lower() for palabra in ['accesorio', 'funda', 'cargador', 'protector', 'kit']):
                    continue

                nombre, capacidad, memoria = extraer_nombre_memoria_capacidad(titulo)

                # Requisito: SOLO móviles con CAPACIDAD detectada en el título (no se inventa)
                if not capacidad:
                    continue

                es_iphone = 'iphone' in nombre.lower()

                # Requisito: SOLO móviles con RAM. Para iPhone permitimos derivarla por modelo.
                if es_iphone and (not memoria):
                    memoria = ram_por_modelo_iphone(nombre)

                    # Evitar duplicados de iPhone por RAM derivada
                    if memoria and not registrar_iphone_memoria(nombre, memoria):
                        summary_duplicados.append(f"{nombre} - {memoria}")
                        continue

                # Si no es iPhone y no hay RAM explícita, descartar
                if (not es_iphone) and (not memoria):
                    continue

                # Si es iPhone y aun así no se ha podido derivar RAM, descartar
                if es_iphone and (not memoria):
                    continue

                # Buscar precios cerca del enlace
                precio_actual = 0
                precio_original = 0

                parent = link.parent
                for _ in range(5):
                    if parent:
                        precio_tags = parent.find_all(['span', 'div'], class_=re.compile(r'price|precio', re.I))
                        for tag in precio_tags:
                            texto = tag.get_text(strip=True)
                            if '€' in texto:
                                precio = parse_eur_int(texto)
                                if precio > 0:
                                    if 'tachado' in str(tag.get('class', '')) or tag.name in ['s', 'del']:
                                        precio_original = precio
                                    else:
                                        precio_actual = precio
                        if precio_actual > 0:
                            break
                    parent = getattr(parent, 'parent', None)

                # Requisito: no inventar precios. Si no hay precio detectable en el listado, descartamos el candidato.
                if precio_actual == 0:
                    continue

                # Si no se detecta precio original, igualamos al actual (evita inventar "tachados")
                if precio_original == 0:
                    precio_original = precio_actual

                # Buscar imagen
                img_url = ""
                img_tag = link.find('img')
                if img_tag:
                    for attr in ['src', 'data-src', 'data-original', 'data-lazy']:
                        src = img_tag.get(attr)
                        if src and 'logo' not in src.lower():
                            img_url = abs_url(START_URL, src)
                            break

                if REDIMENSIONAR_IMAGENES and img_url:
                    img_final = descargar_y_redimensionar_imagen(img_url, nombre)
                else:
                    img_final = img_url

                version = "IOS" if es_iphone else "Android"

                key = f"{nombre}_{capacidad}_{memoria}"
                if any(p.get('clave_unica') == key for p in total_productos):
                    summary_duplicados.append(f"{nombre} {capacidad} {memoria}")
                    continue

                producto = {
                    "nombre": nombre,
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
                    "clave_unica": key,
                    "version": version,
                    "fuente": FUENTE,
                    "codigo_descuento": CODIGO_DESCUENTO,
                    "es_iphone": es_iphone,
                }

                total_productos.append(producto)
                productos_procesados += 1

                registrar_log(
                    f"[{productos_procesados}] {'iPhone' if es_iphone else 'Android'}: "
                    f"{nombre[:40]:40} | {precio_actual}€ | {capacidad} | {memoria}",
                    "INFO",
                    False,
                )

                time.sleep(0.1)

            except Exception as e:
                registrar_log(f"Error procesando candidato: {str(e)[:160]}", "WARNING", False)
                continue

        registrar_log("=" * 70, "INFO")
        registrar_log("RESUMEN DE EXTRACCIÓN", "INFO")
        registrar_log(
            f"Productos encontrados: {len(total_productos)}",
            "SUCCESS" if len(total_productos) >= 20 else "WARNING",
        )
        registrar_log("=" * 70, "INFO")

        return total_productos

    except Exception as e:
        registrar_log(f"Error crítico en extracción: {str(e)}", "ERROR")
        return []

# --- CREACIÓN DE PRODUCTOS EN WOOCOMMERCE ---
def crear_producto_woocommerce(producto, cache_categorias, max_intentos=10):
    """Crea un producto en WooCommerce con múltiples intentos"""
    intentos = 0
    while intentos < max_intentos:
        intentos += 1
        try:
            registrar_log(f"Intento {intentos}/{max_intentos} para crear: {producto['nombre']}", "INFO", False)
            
            # Preparar URLs con afiliado
            url_importada_sin_afiliado = producto["url_imp"]
            url_con_afiliado = f"{url_importada_sin_afiliado}{ID_AFILIADO_PHONE_HOUSE}"
            url_oferta = acortar_url(url_con_afiliado)
            
            # Resolver categorías
            id_padre, id_hijo = resolver_jerarquia(producto["nombre"], cache_categorias)
            
            # Preparar datos del producto
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
                    {"key": "url_oferta_sin_acortar", "value": producto["url_imp"]},
                    {"key": "url_importada_sin_afiliado", "value": producto["url_imp"]},
                    {"key": "url_sin_acortar_con_mi_afiliado", "value": url_con_afiliado},
                    {"key": "url_oferta", "value": url_oferta},
                    {"key": "imagen_producto", "value": producto["img"]},
                    {"key": "version", "value": producto["version"]},
                    {"key": "es_iphone", "value": "1" if producto["es_iphone"] else "0"},
                ]
            }
            
            # Crear producto
            response = wcapi.post("products", data)
            
            if response.status_code in [200, 201]:
                producto_creado = response.json()
                product_id = producto_creado.get("id")
                product_url = producto_creado.get("permalink")
                
                # Acortar URL del producto creado
                if product_url:
                    url_producto_acortada = acortar_url(product_url)
                    wcapi.put(f"products/{product_id}", {
                        "meta_data": [{"key": "url_post_acortada", "value": url_producto_acortada}]
                    })
                
                registrar_log(f"✅ CREADO: {producto['nombre']} (ID: {product_id})", "SUCCESS")
                return True, product_id
            
            else:
                error_msg = f"Error {response.status_code}: {response.text[:200]}"
                registrar_log(f"Intento {intentos} fallado: {error_msg}", "WARNING", False)
                
                if intentos < max_intentos:
                    tiempo_espera = 2 ** intentos  # Backoff exponencial
                    registrar_log(f"Esperando {tiempo_espera} segundos...", "INFO", False)
                    time.sleep(tiempo_espera)
        
        except Exception as e:
            registrar_log(f"Excepción en intento {intentos}: {str(e)}", "ERROR", False)
            if intentos < max_intentos:
                time.sleep(5)
    
    registrar_log(f"❌ FALLIDO tras {max_intentos} intentos: {producto['nombre']}", "ERROR")
    return False, None

# --- SINCRONIZACIÓN PRINCIPAL ---
def sincronizar_productos(productos_remotos):
    """Sincroniza productos remotos con WooCommerce"""
    registrar_log("=" * 70, "INFO")
    registrar_log("INICIANDO SINCRONIZACIÓN CON WOOCOMMERCE", "INFO")
    registrar_log("=" * 70, "INFO")
    
    # 1. Obtener productos existentes
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
                meta_dict = {}
                for meta in producto.get("meta_data", []):
                    if isinstance(meta, dict) and "key" in meta and "value" in meta:
                        meta_dict[meta["key"]] = str(meta["value"])
                
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
    
    # 2. Procesar cada producto remoto
    for producto in productos_remotos:
        try:
            # Buscar producto existente
            producto_existente = None
            for existente in productos_existentes:
                if existente["url"].strip() == producto["url_imp"].strip():
                    producto_existente = existente
                    break
            
            if producto_existente:
                # Actualizar producto existente
                precio_actual_existente = float(producto_existente["meta"].get("precio_actual", 0) or 0)
                
                if abs(producto["precio_actual"] - precio_actual_existente) > 1:
                    # Actualizar precios
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
                    
                    wcapi.put(f"products/{producto_existente['id']}", update_data)
                    summary_actualizados.append({
                        "nombre": producto["nombre"],
                        "id": producto_existente["id"],
                        "cambio": f"{precio_actual_existente}€ → {producto['precio_actual']}€"
                    })
                    registrar_log(f"🔄 ACTUALIZADO: {producto['nombre']} (ID: {producto_existente['id']})", "INFO")
                else:
                    summary_ignorados.append({
                        "nombre": producto["nombre"],
                        "id": producto_existente["id"]
                    })
                    registrar_log(f"⏭️ IGNORADO: {producto['nombre']} (sin cambios)", "INFO", False)
            
            else:
                # Crear nuevo producto
                exito, product_id = crear_producto_woocommerce(producto, cache_categorias)
                
                if exito:
                    summary_creados.append({
                        "nombre": producto["nombre"],
                        "id": product_id
                    })
                else:
                    summary_fallidos.append(producto["nombre"])
            
            # Pequeña pausa entre productos
            time.sleep(1)
            
        except Exception as e:
            registrar_log(f"Error procesando {producto.get('nombre', 'desconocido')}: {str(e)}", "ERROR")
            summary_fallidos.append(producto.get("nombre", "desconocido"))
    
    # 3. Eliminar productos obsoletos (opcional - comentado por seguridad)
    # registrar_log("Verificando productos obsoletos...", "INFO")
    # for existente in productos_existentes:
    #     encontrado = False
    #     for remoto in productos_remotos:
    #         if existente["url"].strip() == remoto["url_imp"].strip():
    #             encontrado = True
    #             break
    #     
    #     if not encontrado:
    #         try:
    #             wcapi.delete(f"products/{existente['id']}", params={"force": True})
    #             summary_eliminados.append({
    #                 "nombre": existente["nombre"],
    #                 "id": existente["id"]
    #             })
    #             registrar_log(f"🗑️ ELIMINADO: {existente['nombre']} (obsoleto)", "WARNING")
    #         except Exception as e:
    #             registrar_log(f"Error eliminando {existente['nombre']}: {str(e)}", "ERROR")
    
    # 4. Mostrar resumen final
    mostrar_resumen_completo()

def mostrar_resumen_completo():
    """Muestra un resumen completo de la ejecución"""
    total_procesados = (
        len(summary_creados) +
        len(summary_eliminados) +
        len(summary_actualizados) +
        len(summary_ignorados) +
        len(summary_fallidos)
    )
    
    registrar_log("=" * 70, "INFO")
    registrar_log("📋 RESUMEN FINAL DE EJECUCIÓN", "INFO")
    registrar_log(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", "INFO")
    registrar_log("=" * 70, "INFO")
    
    registrar_log(f"📊 ESTADÍSTICAS GENERALES:", "INFO")
    registrar_log(f"   Total productos procesados: {total_procesados}", "INFO")
    registrar_log(f"   Objetivo: 72 productos", "INFO")
    registrar_log(f"   Cobertura: {(total_procesados/72*100):.1f}%", 
                 "SUCCESS" if total_procesados >= 72 else "WARNING")
    
    registrar_log(f"\n📦 PRODUCTOS:", "INFO")
    registrar_log(f"   Creados: {len(summary_creados)}", "SUCCESS")
    for item in summary_creados:
        registrar_log(f"     • {item['nombre']} (ID: {item['id']})", "INFO")
    
    registrar_log(f"\n   Actualizados: {len(summary_actualizados)}", "INFO")
    for item in summary_actualizados:
        registrar_log(f"     • {item['nombre']}: {item['cambio']}", "INFO")
    
    registrar_log(f"\n   Ignorados (sin cambios): {len(summary_ignorados)}", "INFO")
    for item in summary_ignorados[:5]:
        registrar_log(f"     • {item['nombre']}", "INFO", False)
    if len(summary_ignorados) > 5:
        registrar_log(f"     ... y {len(summary_ignorados) - 5} más", "INFO", False)
    
    registrar_log(f"\n   Eliminados: {len(summary_eliminados)}", "WARNING")
    for item in summary_eliminados:
        registrar_log(f"     • {item['nombre']} (ID: {item['id']})", "INFO")
    
    registrar_log(f"\n   Fallidos: {len(summary_fallidos)}", "ERROR")
    for item in summary_fallidos[:5]:
        registrar_log(f"     • {item}", "INFO")
    if len(summary_fallidos) > 5:
        registrar_log(f"     ... y {len(summary_fallidos) - 5} más", "INFO")
    
    registrar_log(f"\n📱 iPHONES EN MEMORIA: {len(iphones_memoria)}", "INFO")
    
    if REDIMENSIONAR_IMAGENES:
        registrar_log(f"\n🖼️ IMÁGENES REDIMENSIONADAS:", "INFO")
        registrar_log(f"   Directorio: {DIRECTORIO_IMAGENES}/", "INFO")
        registrar_log(f"   Tamaño: {TAMANO_IMAGEN[0]}x{TAMANO_IMAGEN[1]}px", "INFO")
    
    registrar_log(f"\n📝 LOGS GUARDADOS EN: {archivo_log}", "INFO")
    
    registrar_log("\n" + "=" * 70, "INFO")
    registrar_log("✅ PROCESO COMPLETADO", "SUCCESS")
    registrar_log("=" * 70, "INFO")
    
    # Guardar memoria de iPhones
    guardar_memoria_iphones()

# --- EJECUCIÓN PRINCIPAL ---
def main():
    """Función principal del scraper"""
    print("\n" + "=" * 80, flush=True)
    print("🤖 SCRAPER PHONE HOUSE - VERSIÓN COMPLETA", flush=True)
    print("=" * 80, flush=True)
    print(f"🔗 URL: {START_URL}", flush=True)
    print(f"📏 Redimensión imágenes: {'SÍ' if REDIMENSIONAR_IMAGENES else 'NO'} ({TAMANO_IMAGEN[0]}x{TAMANO_IMAGEN[1]}px)")
    print(f"🔄 Scroll AJAX: ACTIVADO", flush=True)
    print(f"📱 Memoria iPhones: ACTIVADA", flush=True)
    print(f"📝 Sistema logs: ACTIVADO", flush=True)
    print(f"🎯 Objetivo: 72 productos", flush=True)
    print("=" * 80 + "\n")
   , flush=True)
    try:
        # 1. Extraer productos
        productos = obtener_datos_remotos()
        
        if not productos:
            registrar_log("No se encontraron productos", "ERROR")
            return
        
        # 2. Sincronizar con WooCommerce
        sincronizar_productos(productos)
        
        # 3. Mensaje final
        print("\n" + "=" * 80, flush=True)
        print("🎉 ¡PROCESO COMPLETADO CON ÉXITO!", flush=True)
        print("=" * 80, flush=True)
        print(f"📊 Productos procesados: {len(summary_creados) + len(summary_actualizados)}")
        print(f"📁 Logs guardados en: {archivo_log}", flush=True)
        if REDIMENSIONAR_IMAGENES:
            print(f"🖼️ Imágenes en: {DIRECTORIO_IMAGENES}/", flush=True)
        print("=" * 80)
       , flush=True)
    except KeyboardInterrupt:
        registrar_log("Proceso interrumpido por el usuario", "WARNING")
        mostrar_resumen_completo()
    except Exception as e:
        registrar_log(f"Error crítico en ejecución principal: {str(e)}", "ERROR")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Verificar dependencias
    try:
        import selenium
        from PIL import Image  # o import PIL
        registrar_log("Dependencias verificadas correctamente", "INFO", False)
    except ImportError as e:
        print(f"\033[91m❌ Error: Falta dependencia: {str(e)}\033[0m")
        print("Instala las dependencias con:", flush=True)
        print("pip install selenium Pillow woocommerce requests beautifulsoup4", flush=True)
        exit(1)
    
    # Ejecutar scraper
    main()
