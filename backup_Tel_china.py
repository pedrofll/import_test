import os
import re
import sys
import asyncio
import requests
import urllib.parse
import time
import math
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

# --- AFILIADOS (poner el query completo en variables de entorno) ---
# Ejemplos:
#   AFF_ALIEXPRESS="dp=XXXX&aff_fcid=...&aff_fsk=...&aff_platform=...&sk=...&aff_trace_key=..."
#   AFF_AMAZON="tag=tu-tag-21"
AFF_ALIEXPRESS = os.getenv("AFF_ALIEXPRESS", "").strip()
AFF_AMAZON = os.getenv("AFF_AMAZON", "").strip()
AFF_FNAC = os.getenv("AFF_FNAC", "").strip()
AFF_MEDIAMARKT = os.getenv("AFF_MEDIAMARKT", "").strip()
AFF_POWERPLANET = os.getenv("AFF_POWERPLANET", "").strip()
AFF_GSHOPPER = os.getenv("AFF_GSHOPPER", "").strip()
AFF_TRADINGSENZHEN = os.getenv("AFF_TRADINGSENZHEN", "").strip()

summary_creados = []
summary_eliminados = []
summary_ignorados = []
hoy_dt = datetime.now()
hoy_fmt = hoy_dt.strftime("%d/%m/%Y %H:%M")


# ============================================================
#   LOGS A FICHERO (print -> consola + /wp-content/importador-log.txt)
# ============================================================
LOG_PATH = os.environ.get("IMPORTADOR_LOG_PATH", "/wp-content/importador-log.txt")

try:
    with open(LOG_PATH, "a", encoding="utf-8") as _f:
        _f.write("")
except Exception:
    LOG_PATH = "importador-log.txt"


def _append_log(s: str) -> None:
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(s)
    except Exception:
        pass


def print(*args, sep=" ", end="\n", file=None, flush=False):
    # consola
    import builtins as _b
    _b.print(*args, sep=sep, end=end, file=file, flush=flush)

    # fichero (solo si no redirigen a otro 'file')
    try:
        if file is None or file in (sys.stdout, sys.stderr):
            msg = sep.join(str(a) for a in args)
            _append_log(msg + (end if end else ""))
    except Exception:
        pass


def log_bloque_inicio():
    print("\n" + "=" * 80)
    print(f"RUN: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)


def acortar_url(url_larga: str) -> str:
    if not url_larga:
        return ""
    try:
        url_encoded = urllib.parse.quote(url_larga, safe="")
        r = requests.get(f"https://is.gd/create.php?format=simple&url={url_encoded}", timeout=10)
        return r.text.strip() if r.status_code == 200 else url_larga
    except Exception:
        return url_larga


def _contiene_ellipsis(u: str) -> bool:
    return ("..." in (u or "")) or ("…" in (u or ""))


def normalizar_url_aliexpress(url: str) -> str:
    """Reconstruye canonical de AliExpress para evitar URLs truncadas o con query basura."""
    if not url:
        return ""
    u = str(url).strip().replace("&amp;", "&").replace("…", "...")
    # extraer item id
    m = re.search(r"/item/(\d+)\.html", u)
    if not m:
        m = re.search(r"item/(\d+)\.html", u)
    if not m:
        m = re.search(r"/i/(\d+)\.html", u)
    if m:
        return f"https://www.aliexpress.com/item/{m.group(1)}.html"
    # fallback: cortar a .html
    low = u.lower()
    pos = low.find(".html")
    if pos != -1:
        return u[:pos + 5]
    return u.split("?")[0]


def limpiar_url_segun_fuente(url_exp: str) -> str:
    """Elimina query de tracking/afiliado original según dominio."""
    if not url_exp:
        return ""

    url_exp = str(url_exp).strip().replace("&amp;", "&").replace("…", "...")
    url_limpia = url_exp

    # AliExpress: reconstruimos canonical
    if "aliexpress" in url_exp.lower():
        # a veces viene URL url-encoded dentro de otra
        if "https%3A%2F%2F" in url_exp:
            decoded = urllib.parse.unquote(url_exp)
            m = re.search(r"(https://[^\s]+aliexpress\.[^\s]+?/item/\d+\.html)", decoded, re.I)
            if m:
                return normalizar_url_aliexpress(m.group(1))
        return normalizar_url_aliexpress(url_exp)

    # tiendas donde queremos quitar query
    tiendas_con_query = [
        "pccomponentes.com",
        "fnac.es",
        "amazon.es",
        "phonehouse.es",
        "dhgate.com",
        "tradingshenzhen.com",
        "mi.com",
        "powerplanetonline.com",
        "gshopper.com",
        "mediamarkt.",
    ]
    if any(tienda in url_exp.lower() for tienda in tiendas_con_query):
        url_limpia = url_exp.split("?")[0]

    # si por algún motivo viene con '...'
    if _contiene_ellipsis(url_limpia):
        url_limpia = url_limpia.split("...")[0].split("…")[0]

    return url_limpia.strip()


def unir_afiliado(url_base: str, aff: str) -> str:
    """Concatena el query de afiliado completo sin truncarlo ni romper '?'"""
    base = (url_base or "").strip().replace("&amp;", "&")
    a = (aff or "").strip()
    if not base or not a:
        return base

    # si por error el afiliado es una URL completa
    if a.lower().startswith("http"):
        return a

    tiene_q = "?" in base
    if a.startswith("?"):
        return base + ("&" + a[1:] if tiene_q else a)
    if a.startswith("&"):
        return base + (a if tiene_q else "?" + a[1:])
    return base + ("&" + a if tiene_q else "?" + a)


def construir_url_con_mi_afiliado(fuente: str, url_base: str) -> str:
    f = (fuente or "").strip().lower()
    if f == "amazon":
        return unir_afiliado(url_base, AFF_AMAZON)
    if f == "aliexpress":
        # AliExpress: canonical + afiliado completo
        base = normalizar_url_aliexpress(url_base)
        return unir_afiliado(base, AFF_ALIEXPRESS)
    if f == "fnac":
        return unir_afiliado(url_base, AFF_FNAC)
    if f == "mediamarkt":
        return unir_afiliado(url_base, AFF_MEDIAMARKT)
    if f == "powerplanet":
        return unir_afiliado(url_base, AFF_POWERPLANET)
    if f == "gshopper":
        return unir_afiliado(url_base, AFF_GSHOPPER)
    if f == "tradingshenzhen":
        return unir_afiliado(url_base, AFF_TRADINGSENZHEN)
    return url_base


def asegurar_url_no_truncada(url: str, fuente: str) -> str:
    """Garantiza que no se guarde nada con '...' en ACF."""
    if not url:
        return ""
    u = url.replace("…", "...")
    if "..." not in u:
        return u
    # AliExpress: reconstruimos otra vez por seguridad
    if (fuente or "").strip().lower() == "aliexpress":
        base = normalizar_url_aliexpress(u)
        u2 = unir_afiliado(base, AFF_ALIEXPRESS)
        return u2.replace("…", "...")
    # resto: cortar al primer '...'
    return u.split("...")[0].rstrip("&?").strip()


def obtener_o_crear_categoria_con_imagen(nombre_cat, parent_id=0):
    try:
        search = wcapi.get("products/categories", params={"search": nombre_cat, "per_page": 100}).json()
        for cat in search:
            if cat["name"].lower().strip() == nombre_cat.lower().strip() and cat["parent"] == parent_id:
                img_url = cat.get("image", {}).get("src", "") if cat.get("image") else ""
                return cat["id"], img_url
        data = {"name": nombre_cat, "parent": parent_id}
        new_cat = wcapi.post("products/categories", data).json()
        return new_cat.get("id", 0), ""
    except Exception:
        return 0, ""


def extraer_datos(texto):
    t_clean = texto.replace("**", "").replace("`", "").strip()
    lineas = [l.strip() for l in t_clean.split("\n") if l.strip()]
    if not lineas:
        return None

    nombre = ""
    for linea in lineas:
        cand = re.sub(r"^[^\w]+", "", linea).strip()
        if cand:
            nombre = cand
            break
    if not nombre:
        return None

    # descartar tablets
    if any(x in nombre.upper() for x in ["PAD", "IPAD", "TAB"]):
        return "SKIP_TABLET"

    # Regla especial: iQOO (Vivo) — si empieza por "IQ" y no lleva "Vivo" delante,
    # forzamos marca/categoría Vivo.
    try:
        _parts = nombre.split()
        _first_raw = _parts[0] if _parts else ""
        _first_clean = re.sub(r"[^A-Za-z0-9]+", "", _first_raw)
        if _first_clean.upper().startswith("IQ") and not nombre.strip().lower().startswith("vivo "):
            if _parts:
                _parts[0] = _first_clean.upper() if _first_clean else _parts[0].upper()
                nombre = "Vivo " + " ".join(_parts)
    except Exception:
        pass


    # RAM / ROM
    gigas = re.findall(r"(\d+)\s*GB", t_clean, re.I)
    memoria = f"{gigas[0]} GB" if len(gigas) >= 1 else "N/A"
    capacidad = f"{gigas[1]} GB" if len(gigas) >= 2 else "N/A"
    if memoria == "N/A" or capacidad == "N/A":
        return "SKIP_SPECS"

    version = "GLOBAL Version" if "GLOBAL" in t_clean.upper() else "EU VERSION"

    # precio actual
    precio_actual = 0
    m_p = re.search(r"(\d+[.,]?\d*)\s*€", t_clean)
    if m_p:
        precio_actual = int(round(float(m_p.group(1).replace(",", "."))))

    # cupón
    codigo_de_descuento = "OFERTA: PROMO."
    m_c = re.search(r"(?:Cod\.\s*Promo|Cupón|Código)\s*:?\s*([A-Z0-9]+)", t_clean, re.I)
    if m_c:
        codigo_de_descuento = m_c.group(1)

    return nombre, memoria, capacidad, version, codigo_de_descuento, precio_actual


def calcular_precio_original(precio_actual: int, factor: float = 1.20) -> int:
    try:
        pa = float(precio_actual)
    except Exception:
        return 0
    if pa <= 0:
        return 0
    return int(math.ceil(pa * factor))


def detectar_fuente_por_url(url: str) -> str:
    u = (url or "").lower()
    if "powerplanetonline.com" in u:
        return "powerplanet"
    if "gshopper.com" in u:
        return "Gshopper"
    if "amazon.es" in u or "amazon." in u:
        return "Amazon"
    if "aliexpress" in u:
        return "Aliexpress"
    if "mediamarkt" in u:
        return "MediaMarkt"
    if "fnac.es" in u:
        return "Fnac"
    if "phonehouse.es" in u or "phonehouse." in u:
        return "Phone House"
    if "tradingshenzhen.com" in u:
        return "TradingShenzhen"
    return "Tienda"


def expandir_url(url: str) -> str:
    if not url:
        return ""
    try:
        r = requests.get(url, allow_redirects=True, timeout=15, stream=True, headers={"User-Agent": "Mozilla/5.0"})
        return r.url
    except Exception:
        return url


def enviar_email(asunto: str, cuerpo: str) -> None:
    # Opcional: si quieres email, implementa aquí (SMTP / API).
    # Evitamos NameError si no está configurado.
    try:
        _ = asunto, cuerpo
    except Exception:
        pass
    return


async def gestionar_obsoletos():
    print("\n🔍 INICIANDO GESTIÓN DE OBSOLETOS (Filtro: Telegram_Chinabay)...")
    try:
        productos = wcapi.get("products", params={"per_page": 100}).json()
        for p in productos:
            p_id = p["id"]
            p_nombre = p["name"]
            meta = {m["key"]: m["value"] for m in p.get("meta_data", [])}

            if meta.get("importado_de") == "Telegram_Chinabay":
                fecha_str = meta.get("fecha")
                if fecha_str:
                    try:
                        fecha_prod = datetime.strptime(fecha_str, "%Y-%m-%d")
                        dias_dif = (hoy_dt - fecha_prod).days
                        if dias_dif >= 5:
                            print(f"Obsoleto - fecha igual o superior a 5 días desde su creación: {p_nombre}")
                            wcapi.delete(f"products/{p_id}", params={"force": True})
                            summary_eliminados.append({"nombre": p_nombre, "id": p_id})
                        else:
                            print(f"No se elimina - fecha inferior a 5 días desde su creación: {p_nombre}")
                    except Exception:
                        pass
    except Exception as e:
        print(f"Error en obsoletos: {e}")


async def main():
    log_bloque_inicio()

    url_canal = "https://t.me/s/Chinabay_deals"
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url_canal, headers=headers, timeout=20)
    soup = BeautifulSoup(response.text, "html.parser")
    mensajes = soup.find_all("div", class_="tgme_widget_message")

    for msg in mensajes:
        texto_elem = msg.find("div", class_="tgme_widget_message_text")
        if not texto_elem:
            continue

        res_data = extraer_datos(texto_elem.get_text(separator="\n"))
        if res_data in ["SKIP_TABLET", "SKIP_SPECS"] or not res_data:
            continue

        nombre, memoria, capacidad, version, codigo_de_descuento, precio_actual = res_data

        # --- VERIFICACIÓN DE DUPLICADOS ---
        check_exists = wcapi.get("products", params={"search": nombre, "per_page": 10}).json()
        existe = False
        for prod_existente in check_exists:
            if prod_existente["name"].strip().lower() == nombre.strip().lower():
                metas_existentes = {m["key"]: m["value"] for m in prod_existente.get("meta_data", [])}
                if metas_existentes.get("importado_de") == "Telegram_Chinabay":
                    print(f"⏭️ El producto '{nombre}' ya existe. Saltando...")
                    summary_ignorados.append({"nombre": nombre, "id": prod_existente["id"]})
                    existe = True
                    break
        if existe:
            continue

        # --- PROCESO DE CREACIÓN SI NO EXISTE ---
        precio_original = calcular_precio_original(precio_actual, 1.20)

        # enlaces del mensaje (evitar t.me)
        links = [a["href"] for a in msg.find_all("a", href=True) if "t.me" not in a["href"]]
        if not links:
            continue
        enlace_de_compra_importado = links[0]

        # expandir (redirige a la URL final)
        url_oferta_sin_acortar = expandir_url(enlace_de_compra_importado)

        # fuente por dominio
        fuente = detectar_fuente_por_url(url_oferta_sin_acortar)

        # limpiar afiliado original y reconstruir canonical si aplica
        url_importada_sin_afiliado = limpiar_url_segun_fuente(url_oferta_sin_acortar)

        # construir URL con TU afiliado (completa)
        url_sin_acortar_con_mi_afiliado = construir_url_con_mi_afiliado(fuente, url_importada_sin_afiliado)
        url_sin_acortar_con_mi_afiliado = asegurar_url_no_truncada(url_sin_acortar_con_mi_afiliado, fuente)

        # acortar para 'url_oferta'
        url_oferta = acortar_url(url_sin_acortar_con_mi_afiliado) if url_sin_acortar_con_mi_afiliado else ""

        enviado_desde = "España" if fuente in ["Aliexpress", "Amazon", "powerplanet", "Fnac", "MediaMarkt", "Phone House"] else "China"

        # categorías
        marca = nombre.split(" ")[0]
        id_padre, _ = obtener_o_crear_categoria_con_imagen(marca)
        id_hijo, imagen_subcategoria = obtener_o_crear_categoria_con_imagen(nombre, id_padre)

        # --- LOGS DETALLADOS (guardados a fichero) ---
        print("# --- LOGS DETALLADOS SOLICITADOS ---")
        print(f"Detectado {nombre}")
        print(f"1) Nombre: {nombre}")
        print(f"2) Memoria: {memoria}")
        print(f"3) Capacidad: {capacidad}")
        print(f"4) Versión: {version}")
        print(f"5) Fuente: {fuente}")
        print(f"6) Precio actual: {precio_actual}")
        print(f"7) Precio original: {precio_original}")
        print(f"8) Código de descuento: {codigo_de_descuento}")
        print(f"10) URL Imagen: {imagen_subcategoria}")
        print(f"11) Enlace Importado: {enlace_de_compra_importado}")
        print(f"12) Enlace Expandido: {url_oferta_sin_acortar}")
        print(f"13) URL importada sin afiliado: {url_importada_sin_afiliado}")
        print(f"14) URL sin acortar con mi afiliado: {url_sin_acortar_con_mi_afiliado}")
        print(f"15) URL acortada con mi afiliado: {url_oferta}")
        print(f"16) Enviado desde: {enviado_desde}")
        print(f"17) Encolado para comparar con base de datos...")
        if _contiene_ellipsis(url_sin_acortar_con_mi_afiliado):
            print("⚠️ ATENCIÓN: La URL con afiliado contiene '...' (no debería ocurrir tras normalización).")
        print("-" * 60)
        # -----------------------------------

        data = {
            "name": nombre,
            "type": "simple",
            "status": "publish",
            "regular_price": str(precio_original),
            "sale_price": str(precio_actual),
            "categories": [{"id": id_padre}, {"id": id_hijo}],
            "images": [{"src": imagen_subcategoria}] if imagen_subcategoria else [],
            "meta_data": [
                {"key": "memoria", "value": memoria},
                {"key": "capacidad", "value": capacidad},
                {"key": "version", "value": version},
                {"key": "fuente", "value": fuente},
                {"key": "precio_actual", "value": str(precio_actual)},
                {"key": "precio_original", "value": str(precio_original)},
                {"key": "codigo_de_descuento", "value": codigo_de_descuento},
                {"key": "enlace_de_compra_importado", "value": enlace_de_compra_importado},
                {"key": "url_oferta_sin_acortar", "value": url_oferta_sin_acortar},
                {"key": "url_importada_sin_afiliado", "value": url_importada_sin_afiliado},
                # ✅ AQUÍ va siempre la URL completa con tu afiliado (sin '...')
                {"key": "url_sin_acortar_con_mi_afiliado", "value": url_sin_acortar_con_mi_afiliado},
                {"key": "url_oferta", "value": url_oferta},
                {"key": "enviado_desde", "value": enviado_desde},
                {"key": "importado_de", "value": "Telegram_Chinabay"},
                {"key": "fecha", "value": hoy_dt.strftime("%Y-%m-%d")},
            ],
        }

        # --- CREACIÓN CON REINTENTOS ---
        intentos, max_intentos, creado = 0, 10, False
        while intentos < max_intentos and not creado:
            intentos += 1
            try:
                res = wcapi.post("products", data)
                if res.status_code in [200, 201]:
                    p_res = res.json()
                    new_id = p_res["id"]
                    plink_raw = p_res.get("permalink", "")
                    plink_short = acortar_url(plink_raw) if plink_raw else ""
                    if plink_short:
                        wcapi.put(f"products/{new_id}", {"meta_data": [{"key": "url_post_acortada", "value": plink_short}]})
                    summary_creados.append({"nombre": nombre, "id": new_id})

                    print(f"✅ CREADO -> {nombre} (ID: {new_id})")
                    print(f"14b) URL Post Acortada (WP): {plink_short}")
                    creado = True
                else:
                    time.sleep(15)
            except Exception:
                time.sleep(15)

        await asyncio.sleep(15)

    await gestionar_obsoletos()

    # --- RESUMEN FINAL ---
    resumen_txt = f"\n📋 RESUMEN DE EJECUCIÓN ({hoy_fmt})\n"
    resumen_txt += f"a) CREADOS: {len(summary_creados)}\n"
    resumen_txt += f"b) ELIMINADOS: {len(summary_eliminados)}\n"
    resumen_txt += f"c) IGNORADOS: {len(summary_ignorados)}\n"
    print(resumen_txt)
    try:
        enviar_email(f"Reporte {hoy_fmt}", resumen_txt)
    except Exception:
        pass


if __name__ == "__main__":
    asyncio.run(main())
