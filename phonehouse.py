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
URL_ORIGEN = os.environ.get("SOURCE_URL_PHONEHOUSE", "")

# --- PARÁMETROS DE AFILIADO (desde secrets) ---
ID_AFILIADO_PHONE_HOUSE = os.environ.get("AFF_PHONEHOUSE", "")

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
            print(f"\033[91m{log_entry}\033[0m")
        elif nivel == "WARNING":
            print(f"\033[93m{log_entry}\033[0m")
        elif nivel == "SUCCESS":
            print(f"\033[92m{log_entry}\033[0m")
        else:
            print(log_entry)

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
        driver.get(START_URL)
        
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
    """Extrae nombre, memoria y capacidad del título del producto"""
    t = normalize_spaces(titulo)

    # Caso: "128GB+4GB"
    m_combo = re.search(r"(\d+)\s*(TB|GB)\s*\+\s*(\d+)\s*GB", t, flags=re.I)
    if m_combo:
        capacidad = f"{m_combo.group(1)}{m_combo.group(2).upper()}"
        memoria = f"{m_combo.group(3)}GB"
        nombre = t[:m_combo.start()].strip()
        return normalize_spaces(nombre), capacidad, memoria

    # Caso sin combo
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

# --- EXTRACCIÓN DE PRODUCTOS CON SCROLL AJAX ---
def obtener_datos_remotos():
    """Extrae todos los productos de Phone House"""
    total_productos = []
    
    registrar_log("=" * 70, "INFO")
    registrar_log("INICIANDO EXTRACCIÓN DE PRODUCTOS", "INFO")
    registrar_log(f"URL: {START_URL}", "INFO")
    registrar_log(f"Redimensión imágenes: {'SÍ' if REDIMENSIONAR_IMAGENES else 'NO'}", "INFO")
    registrar_log(f"Scroll AJAX: ACTIVADO", "INFO")
    registrar_log("=" * 70, "INFO")
    
    try:
        # 1. Obtener HTML con scroll AJAX
        html = obtener_html_con_scroll_ajax()
        
        if not html:
            registrar_log("Usando método tradicional (sin AJAX)", "WARNING")
            r = requests.get(START_URL, headers=HEADERS, timeout=30)
            soup = BeautifulSoup(r.text, "html.parser")
        else:
            soup = BeautifulSoup(html, "html.parser")
            registrar_log("HTML obtenido con scroll AJAX", "SUCCESS")
        
        # 2. Buscar productos usando múltiples estrategias
        all_links = []
        
        # Estrategia 1: Buscar por enlaces de productos
        patrones_url = [
            r'/movil-',
            r'/telefono-',
            r'/smartphone-',
            r'/producto/',
            r'/p/',
            r'iphone',
            r'samsung',
            r'xiaomi',
            r'huawei',
            r'motorola'
        ]
        
        for patron in patrones_url:
            try:
                links = soup.find_all('a', href=re.compile(patron, re.IGNORECASE))
                for link in links:
                    href = link.get('href', '')
                    if href and link not in all_links:
                        # Filtrar accesorios
                        if not any(x in href.lower() for x in ['accesorio', 'funda', 'cargador', 'protector']):
                            all_links.append(link)
            except:
                pass
        
        # Estrategia 2: Buscar por contenedores de productos
        selectores_contenedores = [
            '[class*="product"]',
            '[class*="item"]',
            '.product-item',
            '.product-card',
            '.product-box',
            '.product-grid-item'
        ]
        
        for selector in selectores_contenedores:
            try:
                contenedores = soup.select(selector)
                for contenedor in contenedores:
                    links = contenedor.find_all('a', href=True)
                    for link in links:
                        href = link.get('href', '')
                        if href and 'movil' in href and link not in all_links:
                            all_links.append(link)
            except:
                pass
        
        registrar_log(f"Enlaces encontrados: {len(all_links)}", "INFO")
        
        # 3. Procesar cada enlace
        productos_procesados = 0
        urls_procesadas = set()
        
        for link in all_links:
            if productos_procesados >= 72:
                break
                
            try:
                href = link.get('href', '')
                if not href or href in urls_procesadas:
                    continue
                    
                url_completa = abs_url(START_URL, href)
                urls_procesadas.add(url_completa)
                
                # Extraer información básica del enlace
                nombre_elemento = link.find(['h2', 'h3', 'h4', 'div', 'span'], 
                                          class_=re.compile(r'title|name|product', re.I))
                titulo = nombre_elemento.get_text(strip=True) if nombre_elemento else link.get_text(strip=True)
                
                if not titulo or len(titulo) < 5:
                    continue
                
                # Filtrar no productos
                if any(palabra in titulo.lower() for palabra in 
                      ['accesorio', 'funda', 'cargador', 'protector', 'kit']):
                    continue
                
                # Extraer detalles
                nombre, capacidad, memoria = extraer_nombre_memoria_capacidad(titulo)
                
                # Procesar iPhones (memoria especial)
                es_iphone = 'iphone' in nombre.lower()
                if es_iphone and (not memoria or memoria == ""):
                    memoria = ram_por_modelo_iphone(nombre) or "8GB"
                    # Registrar en memoria para evitar duplicados
                    if not registrar_iphone_memoria(nombre, memoria):
                        summary_duplicados.append(f"{nombre} - {memoria}")
                        continue
                
                if not capacidad:
                    capacidad = "128GB"  # Valor por defecto
                
                # Buscar precios
                precio_actual = 0
                precio_original = 0
                
                # Buscar en elementos cercanos
                parent = link.parent
                for _ in range(5):
                    if parent:
                        # Buscar precio actual
                        precio_tags = parent.find_all(['span', 'div'], 
                                                    class_=re.compile(r'price|precio', re.I))
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
                
                # Valores por defecto si no se encontró precio
                if precio_actual == 0:
                    precio_actual = 299
                    precio_original = int(precio_actual * 1.15)
                
                # Buscar imagen
                img_url = ""
                img_tag = link.find('img')
                if img_tag:
                    for attr in ['src', 'data-src', 'data-original', 'data-lazy']:
                        src = img_tag.get(attr)
                        if src and 'http' in src and 'logo' not in src.lower():
                            img_url = abs_url(START_URL, src)
                            break
                
                # Redimensionar imagen si está configurado
                if REDIMENSIONAR_IMAGENES and img_url:
                    img_final = descargar_y_redimensionar_imagen(img_url, nombre)
                else:
                    img_final = img_url
                
                # Determinar versión
                version = "IOS" if es_iphone else "Android"
                
                # Crear clave única para evitar duplicados
                key = f"{nombre}_{capacidad}_{memoria}"
                
                # Verificar si ya existe
                if any(p.get('clave_unica') == key for p in total_productos):
                    summary_duplicados.append(f"{nombre} {capacidad} {memoria}")
                    continue
                
                # Crear producto
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
                    "es_iphone": es_iphone
                }
                
                total_productos.append(producto)
                productos_procesados += 1
                
                # Log del producto encontrado
                marca = "iPhone" if es_iphone else "Android"
                registrar_log(f"[{productos_procesados}] {marca}: {nombre[:40]:40} | {precio_actual}€ | {capacidad} | {memoria}", 
                            "INFO", False)
                
                # Pequeña pausa para no sobrecargar
                time.sleep(0.1)
                
            except Exception as e:
                registrar_log(f"Error procesando producto: {str(e)[:100]}", "WARNING", False)
                continue
        
        # 4. Resumen final
        registrar_log("=" * 70, "INFO")
        registrar_log("RESUMEN DE EXTRACCIÓN", "INFO")
        registrar_log(f"Productos encontrados: {len(total_productos)}", 
                     "SUCCESS" if len(total_productos) >= 72 else "WARNING")
        registrar_log(f"Objetivo: 72 productos", "INFO")
        
        if len(total_productos) < 72:
            registrar_log(f"Faltan {72 - len(total_productos)} productos", "WARNING")
        
        if summary_duplicados:
            registrar_log(f"Duplicados ignorados: {len(summary_duplicados)}", "INFO")
        
        if REDIMENSIONAR_IMAGENES:
            imagenes_redim = sum(1 for p in total_productos if p.get('img') and 'imagenes_phonehouse' in p['img'])
            registrar_log(f"Imágenes redimensionadas: {imagenes_redim}/{len(total_productos)}", "INFO")
        
        registrar_log("=" * 70, "INFO")
        
        return total_productos
        
    except Exception as e:
        registrar_log(f"Error crítico en extracción: {str(e)}", "ERROR")
        import traceback
        traceback.print_exc()
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
    print("\n" + "=" * 80)
    print("🤖 SCRAPER PHONE HOUSE - VERSIÓN COMPLETA")
    print("=" * 80)
    print(f"🔗 URL: {START_URL}")
    print(f"📏 Redimensión imágenes: {'SÍ' if REDIMENSIONAR_IMAGENES else 'NO'} ({TAMANO_IMAGEN[0]}x{TAMANO_IMAGEN[1]}px)")
    print(f"🔄 Scroll AJAX: ACTIVADO")
    print(f"📱 Memoria iPhones: ACTIVADA")
    print(f"📝 Sistema logs: ACTIVADO")
    print(f"🎯 Objetivo: 72 productos")
    print("=" * 80 + "\n")
    
    try:
        # 1. Extraer productos
        productos = obtener_datos_remotos()
        
        if not productos:
            registrar_log("No se encontraron productos", "ERROR")
            return
        
        # 2. Sincronizar con WooCommerce
        sincronizar_productos(productos)
        
        # 3. Mensaje final
        print("\n" + "=" * 80)
        print("🎉 ¡PROCESO COMPLETADO CON ÉXITO!")
        print("=" * 80)
        print(f"📊 Productos procesados: {len(summary_creados) + len(summary_actualizados)}")
        print(f"📁 Logs guardados en: {archivo_log}")
        if REDIMENSIONAR_IMAGENES:
            print(f"🖼️ Imágenes en: {DIRECTORIO_IMAGENES}/")
        print("=" * 80)
        
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
        print("Instala las dependencias con:")
        print("pip install selenium Pillow woocommerce requests beautifulsoup4")
        exit(1)
    
    # Ejecutar scraper
    main()
