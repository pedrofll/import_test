import os
import re
import asyncio
import requests
import urllib.parse
import time
from bs4 import BeautifulSoup
from datetime import datetime
from woocommerce import API

# --- CONFIGURACIÓN ---
wcapi = API(
    url=os.environ["WP_URL"],
    consumer_key=os.environ["WP_KEY"],
    consumer_secret=os.environ["WP_SECRET"],
    version="wc/v3",
    timeout=60
)

summary_creados = []
summary_eliminados = []
summary_ignorados = []
hoy_dt = datetime.now()
hoy_fmt = hoy_dt.strftime("%d/%m/%Y %H:%M")


def acortar_url(url_larga):
    if not url_larga: return ""
    try:
        url_encoded = urllib.parse.quote(url_larga, safe='')
        r = requests.get(f"https://is.gd/create.php?format=simple&url={url_encoded}", timeout=10)
        return r.text.strip() if r.status_code == 200 else url_larga
    except: return url_larga

def limpiar_url_segun_fuente(url_exp):
    if not url_exp: return ""
    url_limpia = url_exp
    if "aliexpress" in url_exp.lower():
        if "https%3A%2F%2F" in url_exp:
            decoded = urllib.parse.unquote(url_exp)
            match = re.search(r'(https://[a-z]+\.aliexpress\.com/item/.*?\.html)', decoded)
            if match: return match.group(1)
        if ".html" in url_exp: return url_exp.split('.html')[0] + ".html"
    tiendas_con_query = ["pccomponentes.com", "fnac.es", "amazon.es", "phonehouse.es", "dhgate.com", "tradingshenzhen.com", "mi.com", "powerplanetonline.com", "gshopper.com"]
    if any(tienda in url_exp.lower() for tienda in tiendas_con_query):
        url_limpia = url_exp.split('?')[0]
    return url_limpia.strip()

def obtener_o_crear_categoria_con_imagen(nombre_cat, parent_id=0):
    try:
        search = wcapi.get("products/categories", params={"search": nombre_cat, "per_page": 100}).json()
        for cat in search:
            if cat['name'].lower().strip() == nombre_cat.lower().strip() and cat['parent'] == parent_id:
                img_url = cat.get('image', {}).get('src', "") if cat.get('image') else ""
                return cat['id'], img_url
        data = {"name": nombre_cat, "parent": parent_id}
        new_cat = wcapi.post("products/categories", data).json()
        return new_cat.get('id', 0), ""
    except: return 0, ""

def extraer_datos(texto):
    t_clean = texto.replace('**', '').replace('`', '').strip()
    lineas = [l.strip() for l in t_clean.split('\n') if l.strip()]
    if not lineas: return None
    nombre = ""
    for linea in lineas:
        cand = re.sub(r'^[^\w]+', '', linea).strip()
        if cand: nombre = cand; break
    if not nombre: return None
    if any(x in nombre.upper() for x in ["PAD", "IPAD", "TAB"]): return "SKIP_TABLET"
    gigas = re.findall(r'(\d+)\s*GB', t_clean, re.I)
    memoria = f"{gigas[0]} GB" if len(gigas) >= 1 else "N/A"
    capacidad = f"{gigas[1]} GB" if len(gigas) >= 2 else "N/A"
    if memoria == "N/A" or capacidad == "N/A": return "SKIP_SPECS"
    version = "GLOBAL Version" if "GLOBAL" in t_clean.upper() else "EU VERSION"
    precio_actual = 0
    m_p = re.search(r'(\d+[.,]?\d*)\s*€', t_clean)
    if m_p: precio_actual = int(round(float(m_p.group(1).replace(',', '.'))))
    codigo_de_descuento = "N/A"
    m_c = re.search(r'(?:Cod\. Promo|Cupón|Código):\s*([A-Z0-9]+)', t_clean, re.I)
    if m_c: codigo_de_descuento = m_c.group(1)
    return nombre, memoria, capacidad, version, codigo_de_descuento, precio_actual

async def gestionar_obsoletos():
    print("\n🔍 INICIANDO GESTIÓN DE OBSOLETOS (Filtro: Telegram_Chinabay)...")
    try:
        productos = wcapi.get("products", params={"per_page": 100}).json()
        for p in productos:
            p_id = p['id']
            p_nombre = p['name']
            meta = {m['key']: m['value'] for m in p.get('meta_data', [])}
            
            if meta.get('importado_de') == "Telegram_Chinabay":
                fecha_str = meta.get('fecha')
                if fecha_str:
                    try:
                        fecha_prod = datetime.strptime(fecha_str, "%Y-%m-%d")
                        dias_dif = (hoy_dt - fecha_prod).days
                        if dias_dif >= 3:
                            print(f"Obsoleto - fecha igual o superior a 3 días desde su creación: {p_nombre}")
                            wcapi.delete(f"products/{p_id}", params={"force": True})
                            summary_eliminados.append({"nombre": p_nombre, "id": p_id})
                        else:
                            print(f"No se elimina - fecha inferior a 3 días desde su creación: {p_nombre}")
                    except: pass
    except Exception as e: print(f"Error en obsoletos: {e}")

async def main():
    url_canal = "https://t.me/s/Chinabay_deals"
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(url_canal, headers=headers, timeout=20)
    soup = BeautifulSoup(response.text, 'html.parser')
    mensajes = soup.find_all('div', class_='tgme_widget_message')

    for msg in mensajes:
        texto_elem = msg.find('div', class_='tgme_widget_message_text')
        if not texto_elem: continue
        res_data = extraer_datos(texto_elem.get_text(separator='\n'))
        if res_data in ["SKIP_TABLET", "SKIP_SPECS"] or not res_data: continue
        
        nombre, memoria, capacidad, version, codigo_de_descuento, precio_actual = res_data
        
        # --- VERIFICACIÓN DE DUPLICADOS ---
        # Buscamos si existe un producto con el mismo nombre exacto
        check_exists = wcapi.get("products", params={"search": nombre, "per_page": 10}).json()
        existe = False
        for prod_existente in check_exists:
            if prod_existente['name'].strip().lower() == nombre.strip().lower():
                # Verificamos que sea de nuestra fuente
                metas_existentes = {m['key']: m['value'] for m in prod_existente.get('meta_data', [])}
                if metas_existentes.get('importado_de') == "Telegram_Chinabay":
                    print(f"⏭️ El producto '{nombre}' ya existe. Saltando...")
                    summary_ignorados.append({"nombre": nombre, "id": prod_existente['id']})
                    existe = True
                    break
        if existe: continue

        # --- PROCESO DE CREACIÓN SI NO EXISTE ---
        precio_original = int(round(precio_actual * 1.25))
        links = [a['href'] for a in msg.find_all('a', href=True) if "t.me" not in a['href']]
        if not links: continue
        enlace_de_compra_importado = links[0]

        try:
            r_exp = requests.get(enlace_de_compra_importado, allow_redirects=True, timeout=10)
            url_oferta_sin_acortar = r_exp.url
        except: url_oferta_sin_acortar = enlace_de_compra_importado

        fuente = "Tienda"
        url_low = url_oferta_sin_acortar.lower()
        if url_low.startswith("https://www.powerplanetonline.com/"): fuente = "powerplanet"
        elif url_low.startswith("https://www.gshopper.com/"): fuente = "Gshopper"
        elif url_low.startswith("https://www.amazon.es/"): fuente = "Amazon"
        elif url_low.startswith("https://www.aliexpress."): fuente = "Aliexpress"
        elif "mediamarkt" in url_low: fuente = "MediaMarkt"
        elif "fnac.es" in url_low: fuente = "Fnac"
        
        url_importada_sin_afiliado = limpiar_url_segun_fuente(url_oferta_sin_acortar)
        url_sin_acortar_con_mi_afiliado = url_importada_sin_afiliado

        # IDs Afiliados (Lógica simplificada según fuente)
        if fuente == "Amazon": url_sin_acortar_con_mi_afiliado += "?tag=tusofertasd0a-21"
        elif fuente == "Aliexpress": url_sin_acortar_con_mi_afiliado += "?dp=17292915..." # (usar variables completas arriba)

        enviado_desde = "España" if fuente in ["Aliexpress", "Amazon", "powerplanet", "Fnac"] else "China"
        url_oferta = acortar_url(url_sin_acortar_con_mi_afiliado)
        marca = nombre.split(' ')[0]
        id_padre, _ = obtener_o_crear_categoria_con_imagen(marca)
        id_hijo, imagen_subcategoria = obtener_o_crear_categoria_con_imagen(nombre, id_padre)

        data = {
            "name": nombre, "type": "simple", "status": "publish",
            "regular_price": str(precio_original), "sale_price": str(precio_actual),
            "categories": [{"id": id_padre}, {"id": id_hijo}],
            "images": [{"src": imagen_subcategoria}] if imagen_subcategoria else [],
            "meta_data": [
                {"key": "memoria", "value": memoria}, {"key": "capacidad", "value": capacidad},
                {"key": "version", "value": version}, {"key": "fuente", "value": fuente},
                {"key": "precio_actual", "value": str(precio_actual)}, 
                {"key": "enlace_de_compra_importado", "value": enlace_de_compra_importado},
                {"key": "url_oferta_sin_acortar", "value": url_oferta_sin_acortar}, 
                {"key": "url_importada_sin_afiliado", "value": url_importada_sin_afiliado},
                {"key": "url_sin_acortar_con_mi_afiliado", "value": url_sin_acortar_con_mi_afiliado}, 
                {"key": "url_oferta", "value": url_oferta},
                {"key": "enviado_desde", "value": enviado_desde}, 
                {"key": "importado_de", "value": "Telegram_Chinabay"},
                {"key": "fecha", "value": hoy_dt.strftime("%Y-%m-%d")}
            ]
        }

        # --- CREACIÓN CON REINTENTOS ---
        intentos, max_intentos, creado = 0, 5, False
        while intentos < max_intentos and not creado:
            intentos += 1
            try:
                res = wcapi.post("products", data)
                if res.status_code in [200, 201]:
                    p_res = res.json(); new_id = p_res['id']
                    plink_raw = p_res.get('permalink')
                    plink_short = acortar_url(plink_raw)
                    wcapi.put(f"products/{new_id}", {"meta_data": [{"key": "url_post_acortada", "value": plink_short}]})
                    summary_creados.append({"nombre": nombre, "id": new_id})
                    
                    # LOGS 1 AL 16
                    print(f"1) Nombre: {nombre}")
                    print(f"5) Fuente: {fuente}")
                    print(f"12) URL importada sin afiliado: {url_importada_sin_afiliado}")
                    print(f"14b) URL Post Acortada (WP): {plink_short}")
                    print(f"16) Importado de: Telegram_Chinabay")
                    creado = True
                else: time.sleep(10)
            except: time.sleep(10)
        
        await asyncio.sleep(15)

    await gestionar_obsoletos()

    # --- RESUMEN FINAL ---
    resumen_txt = f"\n📋 RESUMEN DE EJECUCIÓN ({hoy_fmt})\n"
    resumen_txt += f"a) CREADOS: {len(summary_creados)}\nb) ELIMINADOS: {len(summary_eliminados)}\nc) IGNORADOS: {len(summary_ignorados)}\n"
    print(resumen_txt)
    enviar_email(f"Reporte {hoy_fmt}", resumen_txt)

if __name__ == '__main__':
    asyncio.run(main())
