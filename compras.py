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


def expandir_url(url: str) -> str:
    """
    Expande enlaces soportando:
      - redirects 3xx (requests allow_redirects)
      - wrappers (Tradedoubler url=, Tradetracker u=)
      - redirecciones HTML/JS (meta refresh, window.location)
    Devuelve la URL final (idealmente la URL real de la tienda).
    """
    if not url:
        return ""

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "es-ES,es;q=0.9",
    }

    s = requests.Session()
    current = url

    # Evitar bucles
    for _hop in range(8):
        try:
            r = s.get(current, allow_redirects=True, headers=headers, timeout=20)
        except Exception:
            return current

        final_url = getattr(r, "url", "") or current

        # 1) Deswrapper por querystring (cuando el destino viene embebido)
        p = urllib.parse.urlparse(final_url)
        host = (p.netloc or "").lower()
        qs = urllib.parse.parse_qs(p.query)

        if "tradedoubler" in host and "url" in qs:
            nxt = urllib.parse.unquote(qs["url"][0])
            if nxt and nxt != current:
                current = nxt
                continue

        if "tradetracker" in host and "u" in qs:
            nxt = urllib.parse.unquote(qs["u"][0])
            if nxt and nxt != current:
                current = nxt
                continue

        body = (r.text or "")

        # 2) meta refresh: content="0;url=..."
        m = re.search(r'http-equiv=["\']refresh["\'][^>]*content=["\'][^"\']*url=([^"\']+)["\']', body, re.I)
        if m:
            nxt = m.group(1).strip()
            nxt = urllib.parse.urljoin(final_url, nxt)
            if nxt and nxt != current:
                current = nxt
                continue

        # 3) JS: window.location / location.href / document.location
        m = re.search(r'(?:window\.location|location\.href|document\.location)\s*=\s*["\']([^"\']+)["\']', body, re.I)
        if m:
            nxt = m.group(1).strip()
            nxt = urllib.parse.urljoin(final_url, nxt)
            if nxt and nxt != current:
                current = nxt
                continue

        return final_url

    return current


def deswrap_url(url: str) -> str:
    """Quita wrappers de afiliación y devuelve la URL destino real si viene en querystring."""
    u = url or ""
    for _ in range(5):
        p = urllib.parse.urlparse(u)
        host = (p.netloc or "").lower()
        qs = urllib.parse.parse_qs(p.query)

        candidate = None
        # Tradedoubler: .../click?...&url=https%3A%2F%2F...
        if "tradedoubler" in host and "url" in qs:
            candidate = qs["url"][0]
        # Tradetracker: ...?u=https%3A%2F%2F...
        elif "tradetracker" in host and "u" in qs:
            candidate = qs["u"][0]

        if not candidate:
            break

        candidate = urllib.parse.unquote(candidate)
        if candidate == u:
            break
        u = candidate

    return u


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
        if cat["name"].lower() == nombre_padre.lower() and cat["parent"] == 0:
            id_cat_padre = cat["id"]
            break

    if not id_cat_padre:
        payload = {"name": nombre_padre, "parent": 0}
        r = wcapi.post("products/categories", payload)
        if r.status_code in [200, 201]:
            nueva = r.json()
            id_cat_padre = nueva["id"]
            cache_categorias.append(nueva)

    # Buscar hijo exacto dentro del padre
    for cat in cache_categorias:
        if cat["name"].lower() == nombre_hijo.lower() and cat["parent"] == id_cat_padre:
            id_cat_hijo = cat["id"]
            # intentar reutilizar imagen de la subcategoría si existe
            img = cat.get("image") or {}
            foto_final = img.get("src")
            break

    if not id_cat_hijo:
        payload = {"name": nombre_hijo, "parent": id_cat_padre}
        r = wcapi.post("products/categories", payload)
        if r.status_code in [200, 201]:
            nueva = r.json()
            id_cat_hijo = nueva["id"]
            cache_categorias.append(nueva)

    return id_cat_padre, id_cat_hijo, foto_final


def obtener_productos_existentes():
    productos = []
    page = 1
    while True:
        res = wcapi.get("products", params={"per_page": 100, "page": page, "status": "publish"}).json()
        if not res or "message" in res:
            break
        productos.extend(res)
        if len(res) < 100:
            break
        page += 1
    return productos


def meta_get(meta_list, key):
    for m in meta_list:
        if m.get("key") == key:
            return m.get("value")
    return ""


def producto_match(p_existente, p_nuevo):
    meta = p_existente.get("meta_data", [])
    nombre = p_existente.get("name", "").strip()

    memoria = meta_get(meta, "memoria")
    capacidad = meta_get(meta, "capacidad")
    precio_actual = meta_get(meta, "precio_actual")
    precio_original = meta_get(meta, "precio_original")
    fuente = meta_get(meta, "fuente")
    importado_de = _norm_import_id(meta_get(meta, "importado_de"))

    return (
        nombre.lower() == p_nuevo["nombre"].lower()
        and (memoria or "").strip().lower() == p_nuevo["ram"].strip().lower()
        and (capacidad or "").strip().lower() == p_nuevo["rom"].strip().lower()
        and str(precio_original).strip() == str(p_nuevo["p_reg"]).strip()
        and str(precio_actual).strip() == str(p_nuevo["p_act"]).strip()
        and (fuente or "").strip().lower() == p_nuevo["fuente"].strip().lower()
        and importado_de == ID_IMPORTACION_NORM
    )


def obtener_datos_remotos():
    fuentes_6_principales = ["PcComponentes", "MediaMarkt", "AliExpress Plaza", "Amazon", "Fnac", "Phone House"]

    print("\n--- FASE 1: ESCANEANDO COMPRAS SMARTPHONE ---")
    print("-" * 60)

    productos_por_clave = {}
    total_paginas = len(URLS_PAGINAS)

    for i, url in enumerate(URLS_PAGINAS, start=1):
        try:
            print(f"Escaneando listado ({i}/{total_paginas}): {url}")
            html = requests.get(url, timeout=15).text
            soup = BeautifulSoup(html, "html.parser")

            items = soup.select("div.flex.flex-col.gap-4.rounded-2xl.bg-gray-800")
            print(f"✅ Items detectados: {len(items)}")

            for item in items:
                try:
                    # Nombre
                    nombre = item.select_one("h2").get_text(strip=True)

                    # Ignorar tablets/relojes: si hay TAB o IPAD en el nombre
                    nombre_upper = nombre.upper()
                    if " TAB" in f" {nombre_upper} " or " IPAD" in f" {nombre_upper} ":
                        continue

                    # RAM/ROM (debe existir si es móvil)
                    data = item.select_one("p.text-white.text-sm").get_text(strip=True)
                    if "/" not in data:
                        # si no tiene memoria/capacidad, no se importa
                        continue

                    ram_part, rom_part = data.split("/")
                    ram_part = ram_part.replace("RAM", "").strip()
                    rom_part = rom_part.replace("ROM", "").strip()

                    ram = ram_part if "TB" in ram_part else f"{ram_part} GB"
                    rom = rom_part if "TB" in rom_part else f"{rom_part} GB"

                    p_act = limpiar_precio(item.select_one("p.text-fluor-green").get_text(strip=True))
                    p_reg = limpiar_precio(item.select_one("span.line-through").get_text(strip=True)) if item.select_one("span.line-through") else p_act

                    btn = item.select_one("a.bg-fluor-green")
                    url_imp = btn["href"] if btn else ""

                    # ✅ Expandir SIEMPRE antes de guardar en ACF "url_oferta_sin_acortar"
                    url_oferta_sin_acortar = expandir_url(url_imp)
                    url_exp = url_oferta_sin_acortar  # mantenemos el nombre para no romper el resto del flujo

                    # Guardamos también el enlace original importado (acortador / afiliado de origen)
                    enlace_de_compra_importado = url_imp

                    fuente = btn.get_text(strip=True).replace("Cómpralo en", "").strip() if btn else "Tienda"
                    url_importada_sin_afiliado = url_oferta_sin_acortar

                    # Normalización de URL sin parámetros según fuente
                    if fuente == "MediaMarkt":
                        url_importada_sin_afiliado = url_exp.split("?")[0]
                    elif fuente == "AliExpress Plaza":
                        url_importada_sin_afiliado = (
                            url_exp.split(".html")[0] + ".html" if ".html" in url_exp else url_exp.split("?")[0]
                        )
                    elif fuente in ["PcComponentes", "Fnac", "Amazon", "Phone House"]:
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

                    # Imagen
                    img = item.select_one("img")
                    img_src = img["src"] if img and img.get("src") else ""

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

                    # clave para evitar duplicados en remoto
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
                            "enlace_de_compra_importado": enlace_de_compra_importado,
                            "url_importada_sin_afiliado": url_importada_sin_afiliado,
                            "url_sin_acortar_con_mi_afiliado": url_sin_acortar_con_mi_afiliado,
                            "url_oferta": url_oferta,
                            "imagen": img_src,
                            "enviado_desde": enviado_desde,
                            "enviado_desde_tg": enviado_desde_tg,
                            "paginas_origen": {url},
                        }
                    else:
                        productos_por_clave[clave]["paginas_origen"].add(url)

                except Exception as e:
                    continue

        except Exception as e:
            print(f"❌ Error al escanear {url}: {e}")
            continue

    return list(productos_por_clave.values())


def sincronizar(remotos):
    cache_categorias = obtener_todas_las_categorias()
    existentes = obtener_productos_existentes()

    # Conjunto de claves remotas para detectar obsoletos por importado_de + identidad (nombre+ram+rom+fuente)
    claves_remotas = set()
    for p in remotos:
        clave = f"{p['nombre']}|{p['ram']}|{p['rom']}|{p['fuente']}".lower()
        claves_remotas.add(clave)

    # 1) Crear/actualizar
    for p in remotos:
        # Buscar si ya existe un producto idéntico (según reglas del proyecto)
        encontrado_id = None
        encontrado = None

        for ex in existentes:
            meta = ex.get("meta_data", [])
            importado_de = _norm_import_id(meta_get(meta, "importado_de"))
            if importado_de != ID_IMPORTACION_NORM:
                continue

            # comparar por identidad mínima (nombre + memoria + capacidad + fuente)
            nombre = ex.get("name", "").strip()
            memoria = (meta_get(meta, "memoria") or "").strip()
            capacidad = (meta_get(meta, "capacidad") or "").strip()
            fuente = (meta_get(meta, "fuente") or "").strip()

            if (
                nombre.lower() == p["nombre"].lower()
                and memoria.lower() == p["ram"].lower()
                and capacidad.lower() == p["rom"].lower()
                and fuente.lower() == p["fuente"].lower()
            ):
                encontrado_id = ex["id"]
                encontrado = ex
                break

        # Si existe y es idéntico en TODOS los campos a comparar => ignorado
        if encontrado and producto_match(encontrado, p):
            summary_ignorados.append({"nombre": p["nombre"], "id": encontrado_id})
            continue

        # Resolver categorías
        id_cat_padre, id_cat_hijo, foto_subcat = resolver_jerarquia(p["nombre"], cache_categorias)

        # Foto: reutilizar subcat si existe, si no usar imagen del producto (si la trae)
        if foto_subcat:
            imagen_final = foto_subcat
        else:
            imagen_final = p["imagen"]

        # Payload producto
        data = {
            "name": p["nombre"],
            "type": "external",
            "status": "publish",
            "regular_price": str(p["p_reg"]),
            "sale_price": str(p["p_act"]),
            "external_url": p["url_oferta"],
            "button_text": "Comprar oferta",
            "categories": [{"id": id_cat_hijo}] if id_cat_hijo else [{"id": id_cat_padre}],
            "images": [{"src": imagen_final}] if imagen_final else [],
            "meta_data": [
                {"key": "importado_de", "value": ID_IMPORTACION_NORM},
                {"key": "memoria", "value": p["ram"]},
                {"key": "capacidad", "value": p["rom"]},
                {"key": "version", "value": p["ver"]},
                {"key": "fuente", "value": p["fuente"]},
                {"key": "imagen_producto", "value": p["imagen"]},
                {"key": "precio_actual", "value": str(p["p_act"])},
                {"key": "precio_original", "value": str(p["p_reg"])},
                {"key": "codigo_de_descuento", "value": p["cup"]},
                {"key": "enlace_de_compra_importado", "value": p.get("enlace_de_compra_importado", p.get("url_exp", ""))},
                {"key": "url_oferta_sin_acortar", "value": p.get("url_oferta_sin_acortar", p.get("url_exp", ""))},
                {"key": "url_importada_sin_afiliado", "value": p["url_importada_sin_afiliado"]},
                {"key": "url_sin_acortar_con_mi_afiliado", "value": p["url_sin_acortar_con_mi_afiliado"]},
                {"key": "url_oferta", "value": p["url_oferta"]},
                {"key": "enviado_desde", "value": p["enviado_desde"]},
                {"key": "enviado_desde_tg", "value": p["enviado_desde_tg"]},
                {"key": "fecha", "value": datetime.now().strftime("%d/%m/%Y")},
            ],
        }

        intentos = 0
        max_intentos = 10
        creado = False

        while intentos < max_intentos and not creado:
            intentos += 1
            try:
                if encontrado_id:
                    r = wcapi.put(f"products/{encontrado_id}", data)
                    if r.status_code in [200, 201]:
                        summary_actualizados.append({"nombre": p["nombre"], "id": encontrado_id, "cambios": ["precio/meta"]})
                        creado = True
                        break
                else:
                    r = wcapi.post("products", data)
                    if r.status_code in [200, 201]:
                        nuevo = r.json()
                        summary_creados.append({"nombre": p["nombre"], "id": nuevo["id"]})
                        creado = True
                        break
            except Exception:
                pass

            time.sleep(15)

        if not creado:
            print(f"❌ No se pudo crear/actualizar: {p['nombre']}")

    # 2) Eliminar obsoletos: SOLO los que son de esta importación y no están en remoto
    for ex in existentes:
        meta = ex.get("meta_data", [])
        importado_de = _norm_import_id(meta_get(meta, "importado_de"))
        if importado_de != ID_IMPORTACION_NORM:
            continue

        nombre = ex.get("name", "").strip()
        memoria = (meta_get(meta, "memoria") or "").strip()
        capacidad = (meta_get(meta, "capacidad") or "").strip()
        fuente = (meta_get(meta, "fuente") or "").strip()

        clave = f"{nombre}|{memoria}|{capacidad}|{fuente}".lower()
        if clave not in claves_remotas:
            try:
                wcapi.delete(f"products/{ex['id']}", params={"force": True})
                summary_eliminados.append({"nombre": nombre, "id": ex["id"]})
            except:
                pass

    # Resumen final
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
