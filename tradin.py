import os
import time
import re
import requests
import urllib.parse
import traceback
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from woocommerce import API

# ============================================================
#   CONFIGURACIÓN ROBUSTA (BASE_URL, URLS_PAGINAS, IMPORTACIÓN)
# ============================================================

# 1) Intentar leer lista completa desde TSZ_URLS (secreto opcional)
#    Formato: url1,url2,url3
tsz_urls_raw = os.environ.get("TSZ_URLS", "").strip()

if tsz_urls_raw:
    # Si TSZ_URLS existe, se usa directamente
    URLS_PAGINAS = [u.strip() for u in tsz_urls_raw.split(",") if u.strip()]

    # BASE_URL derivado automáticamente de la primera URL
    # Ejemplo: https://tradingshenzhen.com/en/new → https://tradingshenzhen.com
    primera = URLS_PAGINAS[0]
    BASE_URL = primera.split("/en/")[0].rstrip("/")

else:
    # 2) Fallback: usar dominio base desde secreto SOURCE_URL_TRADINGSENZHEN
    BASE_URL = os.environ["SOURCE_URL_TRADINGSENZHEN"].rstrip("/")

    URLS_PAGINAS = [
        f"{BASE_URL}/en/new",
        f"{BASE_URL}/en/new?page=2",
        f"{BASE_URL}/en/new?page=3",
        f"{BASE_URL}/en/new?page=4",
        f"{BASE_URL}/en/new?page=5",
        f"{BASE_URL}/en/deal",
        f"{BASE_URL}/en/deal?page=2",
        f"{BASE_URL}/en/deal?page=3",
        f"{BASE_URL}/en/deal?page=4",
        f"{BASE_URL}/en/deal?page=5",
        f"{BASE_URL}/en/eu-warehouse"
    ]

# 3) Identificador de importación (oculto)
ID_IMPORTACION = f"{BASE_URL}/"

# 4) Afiliado oculto
ID_AFILIADO_TRADINGSENZHEN = os.environ.get("AFF_TRADINGSENZHEN", "")

# 5) WooCommerce API
wcapi = API(
    url=os.environ["WP_URL"],
    consumer_key=os.environ["WP_KEY"],
    consumer_secret=os.environ["WP_SECRET"],
    version="wc/v3",
    timeout=60
)

summary_creados, summary_eliminados, summary_actualizados = [], [], []
summary_ignorados, summary_sin_stock_nuevos, summary_fallidos = [], [], []

def acortar_url(url_larga):
    try:
        url_encoded = urllib.parse.quote(url_larga)
        r = requests.get(f"https://is.gd/create.php?format=simple&url={url_encoded}", timeout=10)
        return r.text.strip() if r.status_code == 200 else url_larga
    except:
        return url_larga

def obtener_version(nombre):
    primera = nombre.split(' ')[0].capitalize()
    mapping = {
        "Xiaomi": "CN (EN/CHN)", "Redmi": "CN (EN/CHN)", "Realme": "CN (EN/CHN)",
        "Honor": "Multiidioma", "Oneplus": "OxygenOs", "Oppo": "ColorOS Multiidioma",
        "Vivo": "OriginOS", "Nubia": "Nebula AIOS", "Motorola": "Original (EN/CHN)"
    }
    return mapping.get(primera, "Global Version")

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
            break
    if not id_cat_hijo:
        res = wcapi.post("products/categories", {"name": nombre_hijo, "parent": id_cat_padre}).json()
        id_cat_hijo = res.get('id')
        cache_categorias.append(res)
    
    return id_cat_padre, id_cat_hijo

# --- FASE 1: SCRAPING ---
def obtener_datos_remotos():
    total_productos = []
    hoy = datetime.now().strftime("%d/%m/%Y")
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'}

    print(f"--- FASE 1: ESCANEANDO {len(URLS_PAGINAS)} PÁGINAS ---")

    for idx, url in enumerate(URLS_PAGINAS, 1):
        try:
            print(f"   Scaneando página {idx}...")
            r = requests.get(url, headers=headers, timeout=20)
            soup = BeautifulSoup(r.text, 'html.parser')

            for item in soup.select("div.product_desc"):
                link_tag = item.select_one('h3[itemprop="name"] a')
                if not link_tag or " - " not in link_tag.text:
                    continue

                url_imp = link_tag['href']
                nombre = link_tag.text.split(" - ")[0].strip()

                if any(k in nombre.upper() for k in ["TAB", "IPAD", "PAD"]):
                    continue

                specs = link_tag.text.split(" - ")[1].strip()
                if "/" not in specs:
                    continue

                enviado_desde = "Europa" if "EU Warehouse" in link_tag.text else "China"
                memoria = specs.split("/")[0].strip()

                cap_raw = specs.split("/")[1].strip().upper()
                if "TB" in cap_raw:
                    capacidad = cap_raw
                elif "GB" in cap_raw:
                    capacidad = cap_raw
                else:
                    capacidad = cap_raw.replace("B", "GB") if cap_raw.endswith("B") else f"{cap_raw}GB"

                p_cont = item.find_next_sibling("div", class_="product-price-and-shipping") or item.parent.select_one(".product-price-and-shipping")
                p_act_el = p_cont.select_one(".price") if p_cont else item.parent.select_one(".price")
                p_act = int(float(re.sub(r'[^\d.]', '', p_act_el.get_text(strip=True)))) if p_act_el else 0

                p_reg_el = p_cont.select_one(".regular-price") if p_cont else None
                p_reg = int(float(re.sub(r'[^\d.]', '', p_reg_el.get_text(strip=True)))) if p_reg_el else int(p_act * 1.1)

                det_r = requests.get(url_imp, headers=headers, timeout=15)
                det_soup = BeautifulSoup(det_r.text, 'html.parser')
                img = det_soup.find("meta", property="og:image")["content"] if det_soup.find("meta", property="og:image") else ""

                avail_tag = det_soup.select_one("#product-availability, .product-quantities")
                stock_txt = avail_tag.get_text().strip() if avail_tag else det_soup.get_text()
                match_stock = re.search(r'(\d+)', stock_txt)
                cantidad = match_stock.group(1) if match_stock else ("Disponible" if "in stock" in stock_txt.lower() else "0")

                total_productos.append({
                    "nombre": nombre,
                    "memoria": memoria,
                    "capacidad": capacidad,
                    "precio_actual": p_act,
                    "precio_regular": p_reg,
                    "img": img,
                    "url_imp": url_imp,
                    "version": obtener_version(nombre),
                    "enviado_desde": enviado_desde,
                    "fecha": hoy,
                    "en_stock": (cantidad != "0"),
                    "cantidad": cantidad,
                    "pagina": idx
                })
        except:
            continue

    print(f"   ✅ Total productos encontrados: {len(total_productos)}")
    return total_productos

# --- FASE 2: SINCRONIZACIÓN ---
def sincronizar(remotos):
    print(f"--- FASE 2: SINCRONIZANDO ---")
    cache_categorias = obtener_todas_las_categorias()
    locales = []
    page = 1

    while True:
        res = wcapi.get("products", params={"per_page": 100, "page": page}).json()
        if not res or "message" in res:
            break

        for p in res:
            meta = {m['key']: str(m['value']) for m in p.get('meta_data', [])}
            if "tradingshenzhen.com" in meta.get('importado_de', '').lower():
                locales.append({"id": p['id'], "nombre": p['name'], "meta": meta})

        if len(res) < 100:
            break
        page += 1

    for r in remotos:
        try:
            print("-" * 60)
            print(f"Detectado {r['nombre']} (Página {r['pagina']})")
            print(f"1) Nombre:          {r['nombre']}")
            print(f"2) Memoria:         {r['memoria']}")
            print(f"3) Capacidad:       {r['capacidad']}")
            print(f"4) Versión ROM:     {r['version']}")
            print(f"5) Precio Actual:   {r['precio_actual']}€")
            print(f"6) Precio Original: {r['precio_regular']}€")
            print(f"7) Enviado desde:   {r['enviado_desde']}")
            print(f"8) Stock Real:      {r['cantidad']}")
            print(f"9) URL Imagen:      {r['img'][:75]}...")
            print(f"10) Enlace Compra:  {r['url_imp']}")

            url_r = r['url_imp'].strip().rstrip('/')
            match = next((l for l in locales if l['meta'].get('enlace_de_compra_importado', '').strip().rstrip('/') == url_r), None)

            url_aff = f"{r['url_imp']}{ID_AFILIADO_TRADINGSENZHEN}"
            url_final = acortar_url(url_aff)

            flag_emoji = "🇪🇺 " if r['enviado_desde'] == "Europa" else "🇨🇳 "
            envio_telegram = f"{flag_emoji}{r['enviado_desde']}"

            if match:
                if not r['en_stock']:
                    wcapi.delete(f"products/{match['id']}", params={"force": True})
                    summary_eliminados.append({"nombre": r['nombre'], "id": match['id'], "razon": "Sin Stock"})
                    print("   ❌ ELIMINADO de la web por falta de stock.")
                    continue

                p_acf = int(float(match['meta'].get('precio_actual', 0)))
                if r['precio_actual'] != p_acf:
                    cambio_str = f"{p_acf}€ -> {r['precio_actual']}€"
                    print(f"   🔄 ACTUALIZANDO: {cambio_str}")

                    wcapi.put(f"products/{match['id']}", {
                        "sale_price": str(r['precio_actual']),
                        "regular_price": str(r['precio_regular']),
                        "meta_data": [
                            {"key": "precio_actual", "value": str(r['precio_actual'])},
                            {"key": "enviado_desde_tg", "value": envio_telegram}
                        ]
                    })

                    summary_actualizados.append({"nombre": r['nombre'], "id": match['id'], "cambio": cambio_str})
                else:
                    summary_ignorados.append({"nombre": r['nombre'], "id": match['id']})
                    print("   ⏭️ IGNORADO: Ya está actualizado.")

            elif r['en_stock']:
                print("   🆕 CREANDO PRODUCTO NUEVO...")
                id_p, id_h = resolver_jerarquia(r['nombre'], cache_categorias)

                data = {
                    "name": r['nombre'],
                    "type": "simple",
                    "status": "publish",
                    "regular_price": str(r['precio_regular']),
                    "sale_price": str(r['precio_actual']),
                    "categories": [{"id": id_p}, {"id": id_h}] if id_h else [{"id": id_p}] if id_p else [],
                    "images": [{"src": r['img']}] if r['img'] else [],
                    "meta_data": [
                        {"key": "nombre_movil_final", "value": r['nombre']},
                        {"key": "importado_de", "value": ID_IMPORTACION},
                        {"key": "fecha", "value": r['fecha']},
                        {"key": "memoria", "value": r['memoria']},
                        {"key": "capacidad", "value": r['capacidad']},
                        {"key": "version", "value": r['version']},
                        {"key": "fuente", "value": "Tradingshenzhen"},
                        {"key": "precio_actual", "value": str(r['precio_actual'])},
                        {"key": "precio_original", "value": str(r['precio_regular'])},
                        {"key": "codigo_de_descuento", "value": "OFERTA PROMO"},
                        {"key": "enviado_desde", "value": r['enviado_desde']},
                        {"key": "enviado_desde_tg", "value": envio_telegram},
                        {"key": "enlace_de_compra_importado", "value": r['url_imp']},
                        {"key": "url_sin_acortar_con_mi_afiliado", "value": url_aff},
                        {"key": "url_oferta", "value": url_final}
                    ]
                }

                intentos = 0
                max_intentos = 10
                creado = False

                while intentos < max_intentos and not creado:
                    intentos += 1
                    try:
                        res = wcapi.post("products", data)
                        if res.status_code in [200, 201]:
                            creado = True
                            prod_res = res.json()
                            summary_creados.append({"nombre": r['nombre'], "id": prod_res.get('id')})

                            url_short = acortar_url(prod_res.get('permalink'))
                            if url_short:
                                wcapi.put(f"products/{prod_res.get('id')}", {
                                    "meta_data": [{"key": "url_post_acortada", "value": url_short}]
                                })

                            print(f"   ✅ CREADO -> ID: {prod_res.get('id')}")
                    except:
                        time.sleep(60)

            else:
                summary_sin_stock_nuevos.append(r['nombre'])

        except:
            print(f"   ❌ ERROR en {r['nombre']}")
            summary_fallidos.append(r['nombre'])

    print("\n" + "="*60)
    print(f"📋 RESUMEN DE EJECUCIÓN ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
    print("="*60)

    print(f"a) ARTÍCULOS CREADOS ({len(summary_creados)}):")
    for item in summary_creados:
        print(f"- {item['nombre']} (ID: {item['id']})")

    print("-" * 40)
    print(f"b) ARTÍCULOS ELIMINADOS ({len(summary_eliminados)}):")
    for item in summary_eliminados:
        print(f"- {item['nombre']} (ID: {item['id']}) - {item['razon']}")

    print("-" * 40)
    print(f"c) ARTÍCULOS ACTUALIZADOS ({len(summary_actualizados)}):")
    for item in summary_actualizados:
        print(f"- {item['nombre']} (ID: {item['id']}): {item['cambio']}")

    print("-" * 40)
    print(f"d) ARTÍCULOS IGNORADOS ({len(summary_ignorados)}):")
    for item in summary_ignorados:
        print(f"- {item['nombre']} (ID: {item['id']})")

    print("-" * 40)
    print(f"e) NUEVOS SIN STOCK ({len(summary_sin_stock_nuevos)}):")
    for item in summary_sin_stock_nuevos:
        print(f"- {item}")

    print("-" * 40)
    print(f"f) FALLIDOS ({len(summary_fallidos)}):")
    for item in summary_fallidos:
        print(f"- {item}")

    print("="*60)

if __name__ == '__main__':
    remotos = obtener_datos_remotos()
    if remotos:
        sincronizar(remotos)

