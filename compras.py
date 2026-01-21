# scraper_compras.py
import os
import time
import requests
import urllib.parse
import re
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
URL_ORIGEN = os.environ.get("SOURCE_URL_COMPRAS", "")  # ej. https://comprasmartphone.com/ofertas (no poner en repo)
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

def strip_query(url: str) -> str:
    """Devuelve la URL sin parámetros ni fragmento (todo lo posterior a '?' o '#')."""
    if not url:
        return url
    url = url.split('#', 1)[0]
    url = url.split('?', 1)[0]
    return url.rstrip()

def unir_afiliado(base: str, afiliado: str) -> str:
    """Concatena afiliado a una URL base, manejando prefijos típicos ('?', '&')."""
    if not afiliado:
        return base
    if afiliado.startswith('&'):
        return f"{base}?{afiliado[1:]}"
    if afiliado.startswith('?') or afiliado.startswith('#'):
        return f"{base}{afiliado}"
    return f"{base}{afiliado}"

def _extraer_url_tradedoubler(click_url: str) -> str | None:
    """Extrae y decodifica la URL real embebida en enlaces de pdt.tradedoubler.com/click ... url(ENCODED)."""
    if not click_url:
        return None
    m = re.search(r"url\(([^)]+)\)", click_url)
    if not m:
        return None
    encoded = m.group(1)
    try:
        return urllib.parse.unquote(encoded)
    except Exception:
        return encoded

def expandir_url(url):
    """Intenta expandir/normalizar URLs de tracking/shorteners para obtener la URL final de destino.

    Para TradeDoubler (clk/pdt) extrae la URL real del parámetro `url` aunque el click no redirija.
    """
    url = (url or "").strip()
    if not url:
        return ""

    def _extraer_destino_tradedoubler(click_url: str) -> str:
        if not click_url:
            return ""
        cu = click_url.strip()

        # Formato 1: https://clk.tradedoubler.com/click?...&url=https%3A%2F%2F...
        #          o   https://clk.tradedoubler.com/click?...&url=https://www....
        # Nota: `url` suele ser el último parámetro; capturamos hasta el final.
        m = re.search(r"(?:\?|&)url=([^#]+)$", cu)
        if m:
            return urllib.parse.unquote(m.group(1))

        # Formato 2 (legacy): https://pdt.tradedoubler.com/click?...url(https%3A%2F%2F...)
        m = re.search(r"url\(([^)]+)\)", cu)
        if m:
            return urllib.parse.unquote(m.group(1))

        return ""

    # Si ya es un click de tradedoubler, extraemos sin requests
    if "tradedoubler.com" in url and "/click" in url:
        destino = _extraer_destino_tradedoubler(url)
        if destino:
            return destino

    # Intento de expansión vía HTTP (shorteners, etc.)
    try:
        resp = requests.get(url, allow_redirects=True, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        final = resp.url or url

        # Si el resultado sigue siendo tradedoubler click, extraemos el destino
        if "tradedoubler.com" in final and "/click" in final:
            destino = _extraer_destino_tradedoubler(final)
            if destino:
                return destino

        return final
    except Exception:
        return url

    # Caso especial: TradeDoubler click con URL embebida (no siempre redirige en un GET simple)
    if url.startswith('https://pdt.tradedoubler.com/click'):
        real = _extraer_url_tradedoubler(url)
        if real:
            return real

    try:
        resp = requests.get(url, allow_redirects=True, timeout=10)
        return resp.url
    except Exception:
        return url

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
    print(f"--- FASE 1: Leyendo origen configurado ---")
    headers = {"User-Agent": "Mozilla/5.0"}
    productos_lista = []
    fuentes_6_principales = ["MediaMarkt", "AliExpress Plaza", "PcComponentes", "Fnac", "Amazon", "Phone House"]
    
    if not URL_ORIGEN:
        print("ERROR: SOURCE_URL no está configurada en las variables de entorno.")
        return []

    try:
        r = requests.get(URL_ORIGEN, headers=headers, timeout=30)
        soup = BeautifulSoup(r.text, "lxml")
        items = soup.select("ul.grid li")
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

                # Normalización de URL sin parámetros (sin '?' / '#') según fuente
                # - MediaMarkt: el enlace importado suele ser TradeDoubler; expandir_url ya extrae la URL real del merchant.
                # - AliExpress / AliExpress Plaza: expandir y eliminar parámetros.
                if fuente in ["MediaMarkt", "AliExpress", "AliExpress Plaza", "PcComponentes", "Fnac", "Amazon", "Phone House"]:
                    url_importada_sin_afiliado = strip_query(url_exp)
                else:
                    url_importada_sin_afiliado = url_exp

                # Construir URL con afiliado usando variables de entorno
                if fuente == "MediaMarkt" and ID_AFILIADO_MEDIAMARKT:
                    url_sin_acortar_con_mi_afiliado = unir_afiliado(url_importada_sin_afiliado, ID_AFILIADO_MEDIAMARKT)
                elif fuente in ["AliExpress", "AliExpress Plaza"] and ID_AFILIADO_ALIEXPRESS:
                    url_sin_acortar_con_mi_afiliado = unir_afiliado(url_importada_sin_afiliado, ID_AFILIADO_ALIEXPRESS)
                elif fuente == "Fnac" and ID_AFILIADO_FNAC:
                    url_sin_acortar_con_mi_afiliado = unir_afiliado(url_importada_sin_afiliado, ID_AFILIADO_FNAC)
                elif fuente == "Amazon" and ID_AFILIADO_AMAZON:
                    url_sin_acortar_con_mi_afiliado = unir_afiliado(url_importada_sin_afiliado, ID_AFILIADO_AMAZON)
                else:
                    url_sin_acortar_con_mi_afiliado = url_importada_sin_afiliado

                url_oferta = acortar_url(url_sin_acortar_con_mi_afiliado)

                # Enviado desde
                tiendas_espana = ["pccomponentes", "aliexpress plaza", "mediamarkt", "amazon", "fnac", "phone house", "powerplanet"]
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
                
                cup = item.select_one("button.border-fluor-green").get_text(strip=True).replace("Código", "").strip() if item.select_one("button.border-fluor-green") else "OFERTA PROMO"

                # Evitar imprimir URLs o códigos de afiliado en logs públicos
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


                productos_lista.append({
                    "nombre": nombre, "p_act": p_act, "p_reg": p_reg,
                    "ram": ram, "rom": rom, "ver": ver, "fuente": fuente,
                    "cup": cup, "url_exp": url_exp, "url_imp": url_imp,
                    "url_importada_sin_afiliado": url_importada_sin_afiliado,
                    "url_sin_acortar_con_mi_afiliado": url_sin_acortar_con_mi_afiliado,
                    "url_oferta": url_oferta,
                    "imagen": img_src,
                    "enviado_desde": enviado_desde,
                    "enviado_desde_tg": enviado_desde_tg
                })
            except Exception:
                continue
        return productos_lista
    except Exception:
        return []

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

    propios_en_wc = [p for p in locales_wc if any(m['key'] == 'importado_de' and m['value'] == URL_ORIGEN for m in p.get('meta_data', []))]

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
                {"key": "importado_de", "value": URL_ORIGEN},
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
            
            time.sleep(30)

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
