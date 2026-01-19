import os
import time
import re
import requests
import urllib.parse
from datetime import datetime
from bs4 import BeautifulSoup
from woocommerce import API

# ============================================================
#  SCRAPER PHONE HOUSE - VERSIÓN COMPLETA CON RESUMEN MEJORADO
# ============================================================

# --- CONFIGURACIÓN ---
START_URL = "https://www.phonehouse.es/moviles-y-telefonia/moviles/todos-los-smartphones.html"

FUENTE = "Phone House"
ID_IMPORTACION = "https://www.phonehouse.es"
ID_AFILIADO_PHONE_HOUSE = "?utm_source=awin&utm_medium=affiliate&utm_campaign=PH_es_ao_affiliate&utm_term=Cashback&utm_content=400137&sv1=affiliate&sv_campaign_id=400137&awc=14845_1768231977_96f87e9485c28c819aa35c90be13a913&sn=1"

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
    "Referer": "https://www.phonehouse.es/"
}


# --------------------------
# UTILIDADES
# --------------------------
def acortar_url(url_larga: str) -> str:
    """Acorta con is.gd."""
    try:
        url_encoded = urllib.parse.quote(url_larga)
        r = requests.get(f"https://is.gd/create.php?format=simple&url={url_encoded}", timeout=10)
        return r.text.strip() if r.status_code == 200 else url_larga
    except Exception:
        return url_larga


def abs_url(base: str, href: str) -> str:
    """Convierte URL relativa a absoluta."""
    try:
        if href.startswith('//'):
            href = 'https:' + href
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

def ram_por_modelo_iphone(nombre: str) -> str | None:
    """Devuelve la RAM en función del modelo de iPhone."""
    if not nombre:
        return None
    n = nombre.lower()
    if "iphone" not in n:
        return None
    for needle, ram in IPHONE_RAM_MAP:
        if needle in n:
            return ram
    return None


def extraer_nombre_memoria_capacidad(titulo: str):
    t = normalize_spaces(titulo)

    # Caso típico: "128GB+4GB"
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
# EXTRACCIÓN REMOTA
# --------------------------
def obtener_datos_remotos():
    total_productos = []
    hoy = datetime.now().strftime("%d/%m/%Y")

    print("--- FASE 1: ESCANEANDO PHONE HOUSE ---")
    print(f"URL: {START_URL}")
    
    try:
        print("🔍 Descargando página completa...")
        r = requests.get(START_URL, headers=HEADERS, timeout=30)
        soup = BeautifulSoup(r.text, "html.parser")
        
        # Buscar TODOS los enlaces a productos de móviles
        print("\n🔍 Buscando TODOS los enlaces a productos...")
        
        all_links = soup.find_all('a', href=re.compile(r'/movil/'))
        print(f"   🔗 Total enlaces /movil/: {len(all_links)}")
        
        # Filtrar enlaces únicos
        unique_links = {}
        for link in all_links:
            href = link.get('href', '')
            if href and '/movil/' in href:
                full_url = abs_url(START_URL, href)
                # Evitar parámetros duplicados
                base_url = full_url.split('?')[0]
                unique_links[base_url] = link
        
        print(f"   🔗 Enlaces únicos: {len(unique_links)}")
        
        # Procesar cada enlace único
        productos_procesados = 0
        productos_ignorados_color = 0  # Contador para productos ignorados por color
        
        for idx, (url, link) in enumerate(unique_links.items(), 1):
            try:
                # Extraer información del enlace
                nombre_element = link.find(['h2', 'h3', 'div', 'span'], class_=re.compile(r'name|title|product'))
                if nombre_element:
                    titulo = normalize_spaces(nombre_element.get_text())
                else:
                    titulo = normalize_spaces(link.get_text())
                
                if not titulo or len(titulo) < 5:
                    continue
                
                # Limpiar título
                titulo_limpio = titulo.replace("¡OFERTA!", "").replace("OFERTA", "").strip()
                
                # Extraer nombre, capacidad, memoria
                nombre, capacidad, memoria = extraer_nombre_memoria_capacidad(titulo_limpio)
                
                # CORRECCIÓN: Para iPhones, determinar memoria si no la tiene
                if "iphone" in nombre.lower() and (not memoria or memoria == ""):
                    memoria_iphone = ram_por_modelo_iphone(nombre)
                    if memoria_iphone:
                        memoria = memoria_iphone
                    else:
                        memoria = "-"
                
                if not nombre or not capacidad:
                    continue
                
                # Buscar precios
                precio_actual = 0
                precio_original = 0
                
                # Buscar en el elemento padre
                parent = link.parent
                for _ in range(3):
                    if parent:
                        precio_elements = parent.find_all(['span', 'div'], class_=re.compile(r'price|precio'))
                        for precio_el in precio_elements:
                            texto = normalize_spaces(precio_el.get_text())
                            if '€' in texto and parse_eur_int(texto) > 0:
                                if 'tachado' in str(precio_el.get('class', '')) or precio_el.name == 's' or precio_el.name == 'del':
                                    precio_original = parse_eur_int(texto)
                                else:
                                    precio_actual = parse_eur_int(texto)
                        
                        if precio_actual > 0:
                            break
                    parent = getattr(parent, 'parent', None)
                
                if precio_actual == 0:
                    precio_actual = 100
                    precio_original = int(precio_actual * 1.10)
                
                # Buscar imagen
                img_url = ""
                img_element = link.find('img')
                if img_element:
                    for attr in ['src', 'data-src', 'data-original', 'data-lazy']:
                        if img_element.get(attr):
                            candidate = img_element.get(attr)
                            if candidate and 'catalogo-blanco' not in candidate.lower():
                                img_url = abs_url(START_URL, candidate)
                                break
                
                # Determinar versión
                if "iphone" in nombre.lower():
                    version = "IOS"
                else:
                    version = "Global"
                
                key = (nombre.lower(), capacidad.upper(), (memoria or "").upper())
                
                # Verificar duplicados (variantes de color)
                if any(p["dedupe_key"] == key for p in total_productos):
                    summary_duplicados.append(f"{nombre} {capacidad} {memoria}".strip())
                    productos_ignorados_color += 1  # Incrementar contador
                    
                    # Mostrar en logs si es duplicado por color
                    if productos_ignorados_color <= 5:  # Mostrar solo los primeros 5
                        print(f"   🎨 IGNORADO (color): {nombre[:30]:30} | {capacidad} | {memoria}")
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
                    print(f"   [{productos_procesados}] {nombre[:30]:30} | {precio_actual:4d}€ | {capacidad} | {memoria}")
                
            except Exception as e:
                continue
        
        # Calcular total encontrado
        total_encontrados = len(total_productos) + len(summary_duplicados)
        
        print(f"\n📊 RESUMEN EXTRACCIÓN:")
        print(f"   Productos únicos encontrados: {len(total_productos)}")
        print(f"   Variantes de color ignoradas: {len(summary_duplicados)}")
        print(f"   Total productos detectados: {total_encontrados}")
        print(f"   Objetivo: 72 productos")
        
        if total_encontrados < 72:
            print(f"   ⚠️  Faltan {72 - total_encontrados} productos por encontrar")
            print(f"   💡 Posibles causas:")
            print(f"      - Productos cargados con JavaScript/AJAX")
            print(f"      - Scroll infinito no detectado")
            print(f"      - Estructura HTML diferente")
        elif total_encontrados > 72:
            print(f"   ⚠️  Se encontraron {total_encontrados - 72} productos más de lo esperado")
        else:
            print("   ✅ Se encontraron todos los productos esperados")
        
        # Mostrar algunos ejemplos de duplicados si hay
        if summary_duplicados:
            print(f"\n🎨 EJEMPLOS DE VARIANTES DE COLOR IGNORADAS:")
            for i, dup in enumerate(summary_duplicados[:5], 1):
                print(f"   {i}. {dup}")
            if len(summary_duplicados) > 5:
                print(f"   ... y {len(summary_duplicados) - 5} más")
        
        return total_productos
        
    except Exception as e:
        print(f"⚠️ Error en extracción: {e}")
        import traceback
        traceback.print_exc()
        return []


# --------------------------
# SINCRONIZACIÓN WP
# --------------------------
def sincronizar(remotos):
    print("\n--- FASE 2: SINCRONIZANDO ---")
    cache_categorias = obtener_todas_las_categorias()

    # Cargar productos locales
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

    print(f"📦 Productos Phone House existentes en la web: {len(locales)}")

    for r in remotos:
        try:
            # Saltar productos sin capacidad
            if not r.get('capacidad'):
                print(f"   ⏭️ OMITIDO: {r['nombre']} (sin capacidad)")
                continue
                
            # Preparar URLs
            url_importada_sin_afiliado = r["url_imp"]
            url_con_afiliado = f"{url_importada_sin_afiliado}{ID_AFILIADO_PHONE_HOUSE}"
            url_oferta = acortar_url(url_con_afiliado)
            
            # Preparar variables para logs
            nombre = r['nombre']
            memoria = r['memoria']
            capacidad = r['capacidad']
            version = r.get('version', 'Global')
            fuente = FUENTE
            precio_actual = r['precio_actual']
            precio_original = r['precio_original']
            codigo_de_descuento = CODIGO_DESCUENTO
            imagen_producto = r['img'] or "(vacía)"
            enlace_de_compra_importado = r['url_imp']
            url_oferta_sin_acortar = r['url_imp']
            url_importada_sin_afiliado_var = r['url_imp']
            url_sin_acortar_con_mi_afiliado = url_con_afiliado
            url_oferta_var = url_oferta
            enviado_desde = ENVIADO_DESDE

            # Mostrar logs detallados
            print("-" * 60)
            print(f"Detectado {nombre}")
            print(f"1) Nombre: {nombre}")
            print(f"2) Memoria: {memoria}")
            print(f"3) Capacidad: {capacidad}")
            print(f"4) Versión: {version}")
            print(f"5) Fuente: {fuente}")
            print(f"6) Precio actual: {precio_actual}€")
            print(f"7) Precio original: {precio_original}€")
            print(f"8) Código de descuento: {codigo_de_descuento}")
            print(f"9) URL Imagen: {imagen_producto[:80]}..." if imagen_producto != "(vacía)" else "9) URL Imagen: (vacía)")
            print(f"11) Enlace Importado: {enlace_de_compra_importado}")
            print(f"12) Enlace Expandido: {url_oferta_sin_acortar}")
            print(f"13) URL importada sin afiliado: {url_importada_sin_afiliado_var}")
            print(f"14) URL sin acortar con mi afiliado: {url_sin_acortar_con_mi_afiliado}")
            print(f"15) URL acortada con mi afiliado: {url_oferta_var}")
            print(f"16) Enviado desde: {enviado_desde}")
            print(f"17) Encolado para comparar con base de datos...")
            print("-" * 60)

            url_r = r["url_imp"].strip().rstrip("/")
            match = next(
                (
                    l for l in locales
                    if l["meta"].get("enlace_de_compra_importado", "").strip().rstrip("/") == url_r
                ),
                None
            )

            # Categorías
            id_padre, id_hijo = resolver_jerarquia(r["nombre"], cache_categorias)

            # Imagen por defecto
            img_subcat = obtener_imagen_categoria(cache_categorias, id_hijo)
            if not img_subcat and r.get("img"):
                actualizado = actualizar_imagen_categoria(cache_categorias, id_hijo, r["img"])
                img_subcat = obtener_imagen_categoria(cache_categorias, id_hijo) if actualizado else ""
            img_final_producto = img_subcat or r.get("img") or ""

            if match:
                if not r["en_stock"]:
                    wcapi.delete(f"products/{match['id']}", params={"force": True})
                    summary_eliminados.append({"nombre": r["nombre"], "id": match["id"], "razon": "Sin Stock"})
                    print("   ❌ ELIMINADO de la web por falta de stock.")
                    continue

                p_acf = int(float(match["meta"].get("precio_actual", 0) or 0))
                if r["precio_actual"] != p_acf:
                    cambio_str = f"{p_acf}€ -> {r['precio_actual']}€"
                    print(f"   🔄 ACTUALIZANDO: {cambio_str}")
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
                                {"key": "imagen_producto", "value": r["img"]},
                                {"key": "version", "value": version},
                            ],
                        },
                    )
                    summary_actualizados.append({"nombre": r["nombre"], "id": match["id"], "cambio": cambio_str})
                else:
                    summary_ignorados.append({"nombre": r["nombre"], "id": match["id"]})
                    print("   ⏭️ IGNORADO: Ya está actualizado.")

            elif r["en_stock"]:
                print("   🆕 CREANDO PRODUCTO NUEVO...")

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
                        {"key": "enlace_de_compra_importado", "value": r["url_imp"]},
                        {"key": "url_oferta_sin_acortar", "value": r["url_imp"]},
                        {"key": "url_importada_sin_afiliado", "value": r["url_imp"]},
                        {"key": "url_sin_acortar_con_mi_afiliado", "value": url_con_afiliado},
                        {"key": "url_oferta", "value": url_oferta},
                        {"key": "imagen_producto", "value": r["img"]},
                        {"key": "version", "value": version},
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
                        
                        if res.status_code in [200, 201]:
                            creado = True
                            prod_res = res.json()
                            summary_creados.append({"nombre": r["nombre"], "id": prod_res.get("id")})
                            
                            url_short = acortar_url(prod_res.get("permalink"))
                            if url_short:
                                wcapi.put(
                                    f"products/{prod_res.get('id')}",
                                    {"meta_data": [{"key": "url_post_acortada", "value": url_short}]},
                                )
                            print(f"   ✅ CREADO -> ID: {prod_res.get('id')}")
                        else:
                            body_preview = (res.text or "").replace("\n", " ")[:250]
                            print(f"   ⚠️  Error {res.status_code} al crear {r['nombre']}: {body_preview}")
                    except Exception as e:
                        print(f"   ⚠️  Excepción al crear {r['nombre']}: {e}")
                    
                    if (not creado) and (intentos < max_intentos):
                        print("    ⏳ Esperando 15s antes del siguiente reintento...", flush=True)
                        time.sleep(15)

                if not creado:
                    print(f"   ❌ NO SE PUDO CREAR tras {max_intentos} intentos -> {r['nombre']}", flush=True)
                    summary_fallidos.append(r.get("nombre", "desconocido"))

            else:
                summary_sin_stock_nuevos.append(r["nombre"])

        except Exception as e:
            print(f"   ❌ ERROR en {r.get('nombre')}: {e}")
            import traceback
            traceback.print_exc()
            summary_fallidos.append(r.get("nombre", "desconocido"))

    # Calcular total de productos procesados
    total_procesados = (
        len(summary_creados) +
        len(summary_eliminados) +
        len(summary_actualizados) +
        len(summary_ignorados) +
        len(summary_sin_stock_nuevos) +
        len(summary_fallidos) +
        len(summary_duplicados)
    )

    # RESUMEN MEJORADO
    print("\n" + "=" * 60 + f"\n📋 RESUMEN DE EJECUCIÓN ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})\n" + "=" * 60)
    print(f"📊 TOTAL PRODUCTOS PROCESADOS: {total_procesados}")
    print(f"   (Objetivo: 72 productos)")
    
    if total_procesados < 72:
        print(f"   ⚠️  Faltan {72 - total_procesados} productos por encontrar")
    elif total_procesados > 72:
        print(f"   ⚠️  Se encontraron {total_procesados - 72} productos más de lo esperado")
    else:
        print("   ✅ Se encontraron todos los productos esperados")
    
    print("\n" + "=" * 60)
    print(f"a) ARTÍCULOS CREADOS ({len(summary_creados)}):")
    for item in summary_creados:
        print(f"- {item['nombre']} (ID: {item['id']})")

    print("-" * 40 + f"\nb) ARTÍCULOS ELIMINADOS DE LA WEB ({len(summary_eliminados)}):")
    for item in summary_eliminados:
        print(f"- {item['nombre']} (ID: {item['id']}) - {item['razon']}")

    print("-" * 40 + f"\nc) ARTÍCULOS ACTUALIZADOS ({len(summary_actualizados)}):")
    for item in summary_actualizados:
        print(f"- {item['nombre']} (ID: {item['id']}): {item['cambio']}")

    print("-" * 40 + f"\nd) ARTÍCULOS IGNORADOS ({len(summary_ignorados)}):")
    for item in summary_ignorados:
        print(f"- {item['nombre']} (ID: {item['id']})")

    print("-" * 40 + f"\ne) OMITIDOS (NUEVOS SIN STOCK) ({len(summary_sin_stock_nuevos)}):")
    for item in summary_sin_stock_nuevos:
        print(f"- {item}")

    print("-" * 40 + f"\nf) VARIANTES DE COLOR IGNORADAS ({len(summary_duplicados)}):")
    if summary_duplicados:
        print("   (Solo se importa 1 producto por combinación nombre+capacidad+memoria)")
        for i, item in enumerate(summary_duplicados[:10], 1):
            print(f"   {i:2d}. {item}")
        if len(summary_duplicados) > 10:
            print(f"   ... y {len(summary_duplicados) - 10} más")
    else:
        print("   - No se encontraron variantes de color")

    print("-" * 40 + f"\ng) FALLIDOS ({len(summary_fallidos)}):")
    for item in summary_fallidos:
        print(f"- {item}")

    print("=" * 60)
    
    # Mostrar porcentaje de cobertura
    if total_procesados > 0:
        porcentaje = min(100, (total_procesados / 72) * 100)
        print(f"\n📈 COBERTURA: {porcentaje:.1f}% ({total_procesados}/72 productos)")
        if porcentaje < 100:
            print(f"   🔍 Se recomienda revisar si hay más productos cargados con JavaScript/AJAX")


if __name__ == "__main__":
    remotos = obtener_datos_remotos()
    if remotos:
        sincronizar(remotos)
