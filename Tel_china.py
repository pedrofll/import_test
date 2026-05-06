import os
import re
import sys
import asyncio
import requests
import urllib.parse
import time
import math
import hashlib
import unicodedata
from bs4 import BeautifulSoup
from datetime import datetime
from woocommerce import API

SCRIPT_VERSION = "2026-04-12 hotfix-isgd-external-v5"

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
# ✅ NUEVO: DHGate
AFF_DHGATE = os.getenv("AFF_DHGATE", "").strip()

summary_creados = []
summary_eliminados = []
summary_ignorados = []
summary_actualizados = []  # lista de dicts: {nombre,id,cambios}
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


def _url_fingerprint(u: str) -> str:
    """Devuelve una huella corta para identificar la fuente sin revelar la URL."""
    try:
        s = (u or "").strip()
        if not s:
            return ""
        return hashlib.sha256(s.encode("utf-8", errors="ignore")).hexdigest()[:10]
    except Exception:
        return ""


def _safe_filename_from_url(u: str) -> str:
    """Devuelve solo el nombre de fichero (último segmento) sin dominio ni query."""
    try:
        if not u:
            return ""
        p = urllib.parse.urlparse(u)
        path = (p.path or "").strip("/")
        return path.split("/")[-1] if path else ""
    except Exception:
        return ""


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
    print(f"SCRIPT_VERSION: {SCRIPT_VERSION}")
    print("=" * 80)


def acortar_url(url_larga: str) -> str:
    """Acorta con is.gd, pero solo acepta respuestas que sean URL válidas.
    Si is.gd falla y devuelve textos como 'Error: database query failed', conserva la URL larga.
    """
    if not url_larga:
        return ""
    try:
        url_encoded = urllib.parse.quote(url_larga, safe="")
        r = requests.get(f"https://is.gd/create.php?format=simple&url={url_encoded}", timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        txt = (r.text or "").strip()
        if r.status_code != 200:
            return url_larga
        if not txt:
            return url_larga
        low = txt.lower()
        if low.startswith("error:") or "database query failed" in low or "please try again" in low:
            return url_larga
        if txt.startswith("http://") or txt.startswith("https://"):
            return txt
        return url_larga
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
        base = (url_base or "").split("?")[0]
        aff = (AFF_TRADINGSENZHEN or "").strip()
        # Permite configurar sólo el ID (p.ej. "176940") o el fragmento completo ("affp=176940")
        if aff and not aff.lower().startswith("http"):
            if "affp=" not in aff.lower():
                # si es sólo un token/ID, lo interpretamos como affp=<ID>
                if re.fullmatch(r"[A-Za-z0-9_-]+", aff):
                    aff = f"affp={aff}"
            # asegura que se concatena como query
            if not aff.startswith("?") and not aff.startswith("&"):
                aff = "?" + aff
        return unir_afiliado(base, aff)
    # ✅ NUEVO: DHGate
    if f == "dhgate":
        base = (url_base or "").split("?")[0]
        return unir_afiliado(base, AFF_DHGATE)
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


def _normalizar_espacios_nombre(s: str) -> str:
    return re.sub(r"\s+", " ", str(s or "")).strip()


def _token_base(tok: str) -> str:
    """Reduce un token a sus letras/números visibles, quitando marcas Unicode invisibles."""
    if not tok:
        return ""
    out = []
    for ch in unicodedata.normalize("NFKD", str(tok)):
        cat = unicodedata.category(ch)
        if cat in ("Mn", "Me", "Cf"):
            continue
        if ch.isalnum():
            out.append(ch)
    return "".join(out)


def _token_sin_letras_es_decorativo(tok: str) -> bool:
    """Detecta tokens iniciales decorativos como 1️⃣, 1), •, emojis, etc."""
    if not tok:
        return False

    t = str(tok).strip()
    if not t:
        return True

    base = _token_base(t)
    visible = "".join(
        ch for ch in unicodedata.normalize("NFKD", t)
        if unicodedata.category(ch) not in ("Mn", "Me", "Cf")
    ).strip()

    # Si contiene letras reales, no lo tocamos (p.ej. 1MORE, X200, iQOO)
    if any(ch.isalpha() for ch in base):
        return False

    # Token vacío o solo símbolos/emoji
    if not base:
        return True

    # Numeración corta típica de listas: 1, 2, 01, 10
    if base.isdigit() and len(base) <= 2:
        return True

    # Tokens muy cortos sin letras y con símbolos alrededor: (1), 1️⃣, 1., •
    if len(base) <= 3 and any(not ch.isalnum() for ch in visible):
        return True

    return False


def limpiar_prefijo_nombre(s: str) -> str:
    """Limpia prefijos/sufijos promocionales de Telegram sin tocar el nombre real."""
    if not s:
        return ""

    s = str(s)
    s = s.replace("**", "").replace("`", "")
    s = unicodedata.normalize("NFKC", s)
    s = _normalizar_espacios_nombre(s)

    # Quita bloques promocionales tipo [TOP VENTAS], [NOVEDAD], [PRECIO TOP], etc.
    s = re.sub(r"\[[^\]]{0,60}\]", " ", s, flags=re.I)
                    
                                                             
                                                                                                                   
                                                     
                                     
                                          
                         
                 

    # Quita prefijos decorativos/emojis/símbolos típicos de Telegram.
    s = re.sub(
        r"^[\s\u200b-\u200f\u2060\ufeff•·▪▫◦►▶★☆✅☑✔✳✴◆◇🔹🔸🔥💥📱📦🆕⭐⚡♦️🧡🧊🔘✨🚀ℹ️]+",
        "",
        s,
        flags=re.I,
    )

    # Quita numeración inicial tipo 1), 1., (1), etc.
    s = re.sub(r"^\(?\d{1,2}\)?[.)\-]+\s*", "", s)

    # Quita cualquier símbolo restante al inicio, pero conserva letras/números.
    s = re.sub(r"^[^\wA-Za-zÁÉÍÓÚÜáéíóúüÑñ]+", "", s)
                       
                        
                                 
                                                 
                               
                                                                
                                                                     
                     
                                                                                                                                      
                                                                     

    # Quita emojis/símbolos finales tipo ℹ️.
    s = re.sub(
        r"[\s\u200b-\u200f\u2060\ufeff•·▪▫◦►▶★☆✅☑✔✳✴◆◇🔹🔸🔥💥📱📦🆕⭐⚡♦️🧡🧊🔘✨🚀ℹ️]+$",
        "",
        s,
        flags=re.I,
    )

    s = _normalizar_espacios_nombre(s)

    # Limpieza de tokens iniciales decorativos que hayan sobrevivido.
    partes = s.split()
    while len(partes) > 1 and _token_sin_letras_es_decorativo(partes[0]):
        partes = partes[1:]

    return _normalizar_espacios_nombre(" ".join(partes))

def extraer_datos(texto):
    t_clean = texto.replace("**", "").replace("`", "").strip()
    lineas = [l.strip() for l in t_clean.split("\n") if l.strip()]
    if not lineas:
        return None

    nombre = ""

    def _es_parte_de_nombre(s: str) -> bool:
        if not s:
            return False
        s_str = s.strip()
        low = s_str.lower()
        if "http" in low or "www." in low:
            return False
        if ":" in s_str:
            return False
        if re.search(r"\b\d+[\.,]?\d*\s*€\b", s_str):
            return False
        for k in ("precio", "cup", "cupón", "cupon", "link", "ram", "rom", "cn version", "eu version", "visita", "síguenos", "siguenos", "follow"):
            if low.startswith(k):
                return False
        if re.match(r"^[\W_]+$", s_str):
            return False
        return True

    partes_nombre = []
    for linea in lineas:
        cand = limpiar_prefijo_nombre(linea)
        if _es_parte_de_nombre(cand):
            partes_nombre.append(cand)
        elif partes_nombre:
            break

    nombre = limpiar_prefijo_nombre(" ".join(partes_nombre)).strip()
    if not nombre:
        return None

    # descartar tablets
    nombre_upper = nombre.upper()
    if re.search(r"\b(PAD|IPAD|TAB)\b", nombre_upper):
        return "SKIP_TABLET"

    # Regla especial: iQOO (Vivo)
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
    # ✅ NUEVO: DHGate
    if "dhgate.com" in u:
        return "Dhgate"
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

    url_canal = os.getenv("TEL_SOURCE_URL", "").strip()
    if not url_canal:
        print("❌ Fuente no configurada (TEL_SOURCE_URL).")
        return

    print(f"📥 ORIGEN DATOS: Telegram (web) | TEL_SOURCE_URL: SI | src_hash={_url_fingerprint(url_canal)}")

    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url_canal, headers=headers, timeout=20)
    soup = BeautifulSoup(response.text, "html.parser")
    mensajes = soup.find_all("div", class_="tgme_widget_message")
    print(f"Mensajes Telegram detectados: {len(mensajes)}")
    if len(mensajes) == 0:
        titulo = (soup.title.string.strip() if soup.title and soup.title.string else "")
        print("⚠️ AVISO: No se detectan bloques tgme_widget_message. La fuente puede NO ser Telegram Web o el HTML ha cambiado/bloqueado.")
        if titulo:
            th = hashlib.sha256(titulo.encode("utf-8", errors="ignore")).hexdigest()[:10]
            print(f"   ℹ️ Title_hash={th} (no se muestra el título por confidencialidad)")

    for msg in mensajes:
        texto_elem = msg.find("div", class_="tgme_widget_message_text")
        if not texto_elem:
            continue

        res_data = extraer_datos(texto_elem.get_text(separator="\n"))
        if res_data in ["SKIP_TABLET", "SKIP_SPECS"] or not res_data:
            continue

        nombre, memoria, capacidad, version, codigo_de_descuento, precio_actual = res_data
        nombre_raw = nombre
        nombre = limpiar_prefijo_nombre(nombre)
        nombre = _normalizar_espacios_nombre(nombre)

        # descartar basura residual de cabeceras promocionales de Telegram
        nombre_upper = nombre.upper().strip()
        palabras_basura = [
            "TOP",
            "SUPERVENTAS",
            "SUPERTOP",
            "MINIMO TOP",
            "OFERTA TOP",
            "OFERTA",
            "TOP SCORE",
            "POTENCIA",
            "NOVEDAD",
            "PRECIO TOP",
        ]
        if (
            nombre_upper in palabras_basura
            or len(nombre.split()) <= 1
            or re.fullmatch(r"^[^\w]+$", nombre)
        ):
            print(f"⛔ IGNORADO POR BASURA: {nombre}")
            continue

        # --- VERIFICACIÓN DE DUPLICADOS / ACTUALIZACIÓN ---
        check_exists = wcapi.get("products", params={"search": nombre, "per_page": 20}).json()
        existe = False
        producto_existente_match = None
        for prod_existente in check_exists:
            nombre_existente_normalizado = limpiar_prefijo_nombre(prod_existente["name"]).strip().lower()
            nombre_nuevo_normalizado = limpiar_prefijo_nombre(nombre).strip().lower()
            metas_existentes = {m["key"]: m["value"] for m in prod_existente.get("meta_data", [])}
            if (
                nombre_existente_normalizado == nombre_nuevo_normalizado
                and str(metas_existentes.get("memoria", "")).strip() == memoria
                and str(metas_existentes.get("capacidad", "")).strip() == capacidad
                and metas_existentes.get("importado_de") == "Telegram_Chinabay"
            ):
                producto_existente_match = prod_existente
                break

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
        if url_oferta and not (url_oferta.startswith("http://") or url_oferta.startswith("https://")):
            print(f"⚠️ Acortador devolvió valor no válido, se conserva URL larga: {url_oferta}")
            url_oferta = url_sin_acortar_con_mi_afiliado

        enviado_desde = "España" if fuente in ["Aliexpress", "Amazon", "powerplanet", "Fnac", "MediaMarkt", "Phone House"] else "China"
        if enviado_desde == "España":
            enviado_desde_tg = "🇪🇸 España"
        elif enviado_desde == "Europa":
            enviado_desde_tg = "🇪🇺 Europa"
        else:
            enviado_desde_tg = "🇨🇳 China"

        # categorías
        marca = nombre.split(" ")[0]
        id_padre, _ = obtener_o_crear_categoria_con_imagen(marca)
        id_hijo, imagen_subcategoria = obtener_o_crear_categoria_con_imagen(nombre, id_padre)

        # --- LOGS DETALLADOS (guardados a fichero) ---
        print("# --- LOGS DETALLADOS SOLICITADOS ---")
        print(f"Detectado {nombre}")
        print(f"0) Nombre RAW parser: {nombre_raw}")
        print(f"0b) Nombre normalizado final: {nombre}")
        print(f"1) Nombre: {nombre}")
        print(f"2) Memoria: {memoria}")
        print(f"3) Capacidad: {capacidad}")
        print(f"4) Versión: {version}")
        print(f"5) Fuente: {fuente}")
        print(f"6) Precio actual: {precio_actual}")
        print(f"7) Precio original: {precio_original}")
        print(f"8) Código de descuento: {codigo_de_descuento}")
        print(f"10) Imagen (subcategoría Woo): {'SI' if imagen_subcategoria else 'NO'}")
        if imagen_subcategoria:
            print(f"10b) Imagen fichero: {_safe_filename_from_url(imagen_subcategoria)}")
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
            "type": "external",
            "status": "publish",
            "regular_price": str(precio_original),
            "sale_price": str(precio_actual),
            "external_url": url_oferta or url_sin_acortar_con_mi_afiliado or url_importada_sin_afiliado,
            "button_text": f"Comprar en {fuente}",
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
                {"key": "url_sin_acortar_con_mi_afiliado", "value": url_sin_acortar_con_mi_afiliado},
                {"key": "url_oferta", "value": url_oferta},
                {"key": "enviado_desde", "value": enviado_desde},
                {"key": "enviado_desde_tg", "value": enviado_desde_tg},
                {"key": "importado_de", "value": "Telegram_Chinabay"},
                {"key": "fecha", "value": hoy_dt.strftime("%Y-%m-%d")},
            ],
        }

        if producto_existente_match:
            exist_id = producto_existente_match["id"]
            metas_existentes = {m["key"]: m["value"] for m in producto_existente_match.get("meta_data", [])}
            cambios = []

            if str(metas_existentes.get("precio_actual", "")).strip() != str(precio_actual):
                cambios.append(f"precio_actual: {metas_existentes.get('precio_actual', '')} -> {precio_actual}")
            if str(metas_existentes.get("precio_original", "")).strip() != str(precio_original):
                cambios.append(f"precio_original: {metas_existentes.get('precio_original', '')} -> {precio_original}")
            if str(metas_existentes.get("url_oferta", "")).strip() != str(url_oferta).strip() and url_oferta:
                cambios.append("url_oferta")
            if str(metas_existentes.get("codigo_de_descuento", "")).strip() != str(codigo_de_descuento).strip():
                cambios.append("codigo_de_descuento")
            if str(metas_existentes.get("fuente", "")).strip() != str(fuente).strip():
                cambios.append("fuente")

            if cambios:
                payload_update = {
                    "type": "external",
                    "regular_price": str(precio_original),
                    "sale_price": str(precio_actual),
                    "external_url": url_oferta or url_sin_acortar_con_mi_afiliado or url_importada_sin_afiliado,
                    "button_text": f"Comprar en {fuente}",
                    "meta_data": data["meta_data"],
                }
                try:
                    wcapi.put(f"products/{exist_id}", payload_update)
                    print(f"♻️ ACTUALIZADO -> {nombre} (ID: {exist_id}) | Cambios: {', '.join(cambios)}")
                    summary_actualizados.append({"nombre": nombre, "id": exist_id, "cambios": cambios})
                except Exception as e:
                    print(f"❌ Error actualizando {nombre} (ID: {exist_id}): {e}")
                continue
            else:
                print(f"⏭️ El producto '{nombre}' ya existe. Sin cambios.")
                summary_ignorados.append({"nombre": producto_existente_match["name"], "id": exist_id})
                continue

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
                    if not plink_short and plink_raw:
                        plink_short = plink_raw
                    if plink_short:
                        wcapi.put(f"products/{new_id}", {"meta_data": [{"key": "url_post_acortada", "value": plink_short}]})
                    summary_creados.append({"nombre": nombre, "id": new_id})

                    print(f"✅ CREADO -> {nombre} (ID: {new_id})")
                    print(f"14b) URL Post Acortada (WP): {plink_short}")
                    creado = True
                else:
                    try:
                        print(f"❌ Error creando producto intento {intentos}/{max_intentos}: HTTP {res.status_code} | {res.text}")
                    except Exception:
                        print(f"❌ Error creando producto intento {intentos}/{max_intentos}: HTTP {getattr(res, 'status_code', 'N/A')}")
                    time.sleep(15)
            except Exception as e:
                print(f"❌ Excepción creando producto intento {intentos}/{max_intentos}: {e}")
                time.sleep(15)

        await asyncio.sleep(15)

    await gestionar_obsoletos()

    # --- RESUMEN FINAL ---
    hoy_fmt2 = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    lineas_resumen = []
    lineas_resumen.append("\n============================================================")
    lineas_resumen.append(f"📋 RESUMEN DE EJECUCIÓN ({hoy_fmt2})")
    lineas_resumen.append("============================================================")

    lineas_resumen.append(f"\na) ARTICULOS CREADOS: {len(summary_creados)}")
    for item in summary_creados:
        lineas_resumen.append(f"- {item['nombre']} (ID: {item['id']})")

    lineas_resumen.append(f"\nb) ARTICULOS ELIMINADOS (OBSOLETOS): {len(summary_eliminados)}")
    for item in summary_eliminados:
        lineas_resumen.append(f"- {item['nombre']} (ID: {item['id']})")

    lineas_resumen.append(f"\nc) ARTICULOS ACTUALIZADOS: {len(summary_actualizados)}")
    for item in summary_actualizados:
        cambios = item.get('cambios') or []
        cambios_txt = ", ".join(cambios) if isinstance(cambios, list) else str(cambios)
        lineas_resumen.append(f"- {item['nombre']} (ID: {item['id']}): {cambios_txt}".rstrip(": "))

    lineas_resumen.append(f"\nd) ARTICULOS IGNORADOS (SIN CAMBIOS): {len(summary_ignorados)}")
    for item in summary_ignorados:
        lineas_resumen.append(f"- {item['nombre']} (ID: {item['id']})")

    lineas_resumen.append("============================================================")
    resumen_txt = "\n".join(lineas_resumen)
    print(resumen_txt)

    try:
        enviar_email(f"Reporte {hoy_fmt2}", resumen_txt)
    except Exception:
        pass


if __name__ == "__main__":
    asyncio.run(main())
