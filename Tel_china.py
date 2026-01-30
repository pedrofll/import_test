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

# --- AFILIADOS (tu ID / params) ---
# (Mantengo tu lógica existente; aquí solo está el archivo completo con cambios de nombre/trading)
AFF_TRADINGSHENZHEN = "affp=57906"

TIENDAS_ESPANA = ["pccomponentes", "aliexpress plaza", "aliexpress", "mediamarkt", "amazon", "fnac", "phone house", "powerplanet", "xiaomi store"]
TIENDAS_CHINA = ["gshopper", "dhgate", "banggood", "tradingshenzhen"]

summary_creados = []
summary_eliminados = []
summary_actualizados = []
summary_ignorados = []

hoy_dt = datetime.now()
hoy_fmt = hoy_dt.strftime("%d/%m/%Y %H:%M")


def log_bloque_inicio():
    print("=" * 80)
    print(f"RUN: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)


def _contiene_ellipsis(url: str) -> bool:
    return "..." in (url or "")


def asegurar_url_no_truncada(url: str, fuente: str) -> str:
    """
    Evita que se cuele una URL truncada por logs/prints o por datos incompletos.
    Aquí, por si se detectara '...'.
    """
    if not url:
        return ""
    if "..." in url:
        # No podemos recuperar la URL real si ya viene truncada aquí.
        # Devolvemos tal cual, pero al menos se loguea la alerta.
        return url
    return url


def acortar_url(url: str) -> str:
    if not url:
        return ""
    try:
        r = requests.get("https://is.gd/create.php", params={"format": "simple", "url": url}, timeout=15)
        if r.status_code == 200:
            return r.text.strip()
    except Exception:
        pass
    return ""


def limpiar_url_segun_fuente(url: str) -> str:
    """
    Limpia parámetros de afiliado originales, dejando una URL canónica.
    """
    if not url:
        return ""
    try:
        u = urllib.parse.urlparse(url)
        qs = urllib.parse.parse_qs(u.query, keep_blank_values=True)

        host = (u.netloc or "").lower()

        # FNAC: quitamos awc/origin/aff y similares
        if "fnac.es" in host:
            # eliminamos params típicos
            for k in ["awc", "origin", "oref", "sv_campaign_id", "sv_tax1", "sv_tax2", "sv_tax3", "sv_tax4", "sv_affiliate_id"]:
                qs.pop(k, None)
            # reconstruimos
            new_q = urllib.parse.urlencode({k: v[0] for k, v in qs.items()}, doseq=False)
            return urllib.parse.urlunparse((u.scheme, u.netloc, u.path, u.params, new_q, u.fragment))

        # AliExpress: dejamos item base (quitamos tracking)
        if "aliexpress" in host:
            # conservamos lo mínimo: a veces es mejor dejar el path y quitar query
            return urllib.parse.urlunparse((u.scheme, u.netloc, u.path, "", "", ""))

        # Amazon: deja path y query mínima (aquí simplificamos)
        if "amazon." in host:
            return urllib.parse.urlunparse((u.scheme, u.netloc, u.path, "", "", ""))

        # TradingShenzhen: quitamos affp si existe
        if "tradingshenzhen.com" in host:
            qs.pop("affp", None)
            new_q = urllib.parse.urlencode({k: v[0] for k, v in qs.items()}, doseq=False)
            return urllib.parse.urlunparse((u.scheme, u.netloc, u.path, u.params, new_q, u.fragment))

        # Por defecto: devuelve sin cambios
        return url

    except Exception:
        return url


def construir_url_con_mi_afiliado(fuente: str, url_importada_sin_afiliado: str) -> str:
    """
    Construye la URL final con tu afiliado, según fuente.
    """
    if not url_importada_sin_afiliado:
        return ""

    try:
        u = urllib.parse.urlparse(url_importada_sin_afiliado)
        host = (u.netloc or "").lower()

        # TradingShenzhen -> añadir affp
        if fuente.lower() == "tradingshenzhen" or "tradingshenzhen.com" in host:
            qs = urllib.parse.parse_qs(u.query, keep_blank_values=True)
            qs["affp"] = [AFF_TRADINGSHENZHEN.split("=")[1]]
            new_q = urllib.parse.urlencode({k: v[0] for k, v in qs.items()}, doseq=False)
            return urllib.parse.urlunparse((u.scheme, u.netloc, u.path, u.params, new_q, u.fragment))

        # Fnac, AliExpress, Amazon etc -> en este scraper de Telegram, usamos la URL canónica tal cual
        return url_importada_sin_afiliado
    except Exception:
        return url_importada_sin_afiliado


def obtener_o_crear_categoria_con_imagen(nombre_cat: str, parent_id=None):
    """
    Simplificado: crea o recupera categoría y devuelve (id, image_url).
    En tu proyecto real, aquí tienes tu lógica completa.
    """
    # Buscar categoría existente
    params = {"search": nombre_cat, "per_page": 50}
    cats = wcapi.get("products/categories", params=params).json()
    for c in cats:
        if c["name"].strip().lower() == nombre_cat.strip().lower():
            img = ""
            if c.get("image") and isinstance(c["image"], dict):
                img = c["image"].get("src", "") or ""
            return c["id"], img

    # Si no existe, crear
    data = {"name": nombre_cat}
    if parent_id:
        data["parent"] = parent_id
    res = wcapi.post("products/categories", data).json()
    cid = res.get("id")
    img = ""
    if res.get("image") and isinstance(res["image"], dict):
        img = res["image"].get("src", "") or ""
    return cid, img


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

    # ✅ Telegram a veces parte el nombre en varias líneas (p.ej. "Xiaomi 17" + "PRO MAX").
    # Si la segunda línea parece un sufijo típico del modelo, la concatenamos.
    if len(lineas) >= 2 and nombre:
        l2 = re.sub(r"^[^\w]+", "", lineas[1]).strip()
        if re.fullmatch(r"(PRO(\s+MAX)?|MAX|ULTRA|PLUS|\+|EDGE|LITE|SE|FE|RSR)", l2, re.I):
            nombre = f"{nombre} {l2}".replace("  ", " ").strip()

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
        return int(math.ceil(pa * float(factor)))
    except Exception:
        return int(precio_actual or 0)


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


def _titlecase_model_words(s: str) -> str:
    """Title-case básico respetando tokens alfanuméricos tipo 14T, 5G, 4G, etc."""
    out = []
    for w in (s or "").split():
        if re.fullmatch(r"\d+[a-zA-Z]+", w) or re.fullmatch(r"\d+g", w, re.I):
            out.append(w.upper())
        else:
            out.append(w[:1].upper() + w[1:].lower() if w else w)
    return " ".join(out).strip()


def nombre_desde_slug_trading(url: str) -> str:
    """Intenta reconstruir el nombre (modelo) desde el slug de TradingShenzhen.

    Ej: /xiaomi-17-series/xiaomi-17-pro-max-12gb512gb -> "Xiaomi 17 Pro Max"
    """
    try:
        if not url:
            return ""
        path = urllib.parse.urlparse(url).path.strip("/")
        if not path:
            return ""
        last = path.split("/")[-1]  # xiaomi-17-pro-max-12gb512gb
        last = re.sub(r"-\d+gb\d+gb.*$", "", last, flags=re.I)  # quita -12gb512gb...
        last = last.replace("-", " ").strip()
        return _titlecase_model_words(last)
    except Exception:
        return ""


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
        page = 1
        productos = []
        while True:
            res = wcapi.get("products", params={"per_page": 100, "page": page}).json()
            if not res:
                break
            productos.extend(res)
            page += 1

        for p in productos:
            metas = {m["key"]: m["value"] for m in p.get("meta_data", [])}
            if metas.get("importado_de") != "Telegram_Chinabay":
                continue

            # no eliminar si creado hace menos de 5 días
            f = metas.get("fecha", "")
            try:
                dtp = datetime.strptime(f, "%Y-%m-%d")
                if (datetime.now() - dtp).days < 5:
                    print(f"No se elimina - fecha inferior a 5 días desde su creación: {p['name']}")
                    continue
            except Exception:
                pass

            # En este scraper, no tenemos listado de "actuales" del canal para comparar uno-a-uno,
            # así que lo dejamos como placeholder.
            # Si implementas comparación real, aquí eliminarías obsoletos.
            # wcapi.delete(f"products/{p['id']}", params={"force": True})
            # summary_eliminados.append({"nombre": p["name"], "id": p["id"]})
            # print(f"🗑️ ELIMINADO -> {p['name']} (ID: {p['id']})")
    except Exception:
        pass


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

        # ✅ Si la oferta viene de TradingShenzhen, el nombre en Telegram a veces llega truncado (p.ej. "Xiaomi 17").
        # En ese caso, reconstruimos desde el slug canonical para evitar productos mal nombrados.
        nombre_antes = nombre
        if fuente.lower() == "tradingshenzhen":
            n_slug = nombre_desde_slug_trading(url_importada_sin_afiliado or url_oferta_sin_acortar)
            if n_slug:
                # Si el slug contiene sufijos (pro/max/ultra/plus) y el nombre no, corregimos.
                if any(k in n_slug.lower() for k in [" pro", " max", " ultra", " plus"]) and not any(k in nombre.lower() for k in [" pro", " max", " ultra", " plus"]):
                    nombre = n_slug

        # Si hemos corregido el nombre, re-verificamos duplicados para evitar crear duplicados por nombre truncado.
        if nombre != nombre_antes:
            check_exists2 = wcapi.get("products", params={"search": nombre, "per_page": 10}).json()
            existe2 = False
            for prod_existente in check_exists2:
                if prod_existente["name"].strip().lower() == nombre.strip().lower():
                    metas_existentes = {m["key"]: m["value"] for m in prod_existente.get("meta_data", [])}
                    if metas_existentes.get("importado_de") == "Telegram_Chinabay":
                        print(f"⏭️ El producto '{nombre}' ya existe (tras corregir nombre). Saltando...")
                        summary_ignorados.append({"nombre": nombre, "id": prod_existente["id"]})
                        existe2 = True
                        break
            if existe2:
                continue

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
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
