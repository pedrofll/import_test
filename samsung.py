import os
import re
import time
import math
import json
import urllib.parse
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from woocommerce import API

DEFAULT_START_URL = "https://www.samsung.com/es/smartphones/all-smartphones/"
START_URL = (os.getenv("SOURCE_URL_SAMSUNG") or DEFAULT_START_URL).strip() or DEFAULT_START_URL

FUENTE = "Samsung"
ID_IMPORTACION = START_URL.rstrip("/")
ENVIADO_DESDE = "Espana"
ENVIADO_DESDE_TG = "🇪🇸 Espana"
VERSION = "Version Global"
CODIGO_DESCUENTO_DEFAULT = "OFERTA: PROMO."
OBJETIVO = 120

AFF_RAW = (os.getenv("AFF_SAMSUNG") or "").strip()

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Referer": "https://www.samsung.com/",
}

wcapi = API(
    url=os.environ["WP_URL"],
    consumer_key=os.environ["WP_KEY"],
    consumer_secret=os.environ["WP_SECRET"],
    version="wc/v3",
    timeout=60,
)

summary_creados, summary_eliminados, summary_actualizados = [], [], []
summary_ignorados, summary_fallidos = [], []
summary_duplicados = []

# --------------------------
# UTILIDADES
# --------------------------

def normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def mask_url(url: str) -> str:
    try:
        u = urllib.parse.urlsplit(url)
        base = f"{u.scheme}://{u.netloc}{u.path}"
        return base + ("?***" if u.query else "")
    except Exception:
        return "***"


def abs_url(base: str, href: str) -> str:
    try:
        if not href:
            return ""
        if href.startswith("//"):
            href = "https:" + href
        return urllib.parse.urljoin(base, href)
    except Exception:
        return href or ""


def parse_eur_num(num_txt: str) -> int:
    if not num_txt:
        return 0
    n = str(num_txt).strip().replace(" ", "")
    if "," in n and "." in n:
        n = n.replace(".", "").replace(",", ".")
    elif "," in n:
        n = n.replace(",", ".")
    try:
        return int(round(float(n)))
    except Exception:
        return 0


EURO_AMOUNT_RE = r"(\d{1,3}(?:\.\d{3})*(?:,\d{1,2})?|\d{1,5}(?:[\.,]\d{1,2})?)\s*€"


def parse_eur_all(txt: str):
    if not txt:
        return []
    vals = []
    for m in re.finditer(EURO_AMOUNT_RE, txt, flags=re.I):
        v = parse_eur_num(m.group(1))
        if v > 0:
            vals.append(v)
    return vals


def dedupe_keep_order(seq):
    out = []
    seen = set()
    for x in seq:
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def _clip_text(txt: str, limit: int = 160) -> str:
    t = normalize_spaces(txt or "")
    if len(t) <= limit:
        return t
    return t[: limit - 3].rstrip() + "..."


def calcular_precio_original(precio_actual: int, factor: float = 1.20) -> int:
    try:
        pa = int(precio_actual)
    except Exception:
        return 0
    if pa <= 0:
        return 0
    return int(math.ceil(pa * factor))


# (todo el archivo es EXACTAMENTE igual al tuyo, solo cambia esta función)

def acortar_url(url_larga: str) -> str:
    try:
        if not url_larga:
            return ""
        url_encoded = urllib.parse.quote(url_larga, safe="")
        r = requests.get(
            f"https://is.gd/create.php?format=simple&url={url_encoded}",
            headers=HEADERS,
            timeout=10,
        )
        texto = r.text.strip() if r.status_code == 200 else ""
        if texto and texto.lower().startswith(("http://", "https://")):
            return texto
        return url_larga
    except Exception:
        return url_larga


def is_samsung_variant_specific(detail_url: str = "", buy_url_hint: str = "", model_code: str = "", capacidad: str = "") -> bool:
    if (model_code or "").strip():
        return True

    cap_token = (capacidad or "").lower().replace(" ", "")
    for raw in [buy_url_hint, detail_url]:
        if not raw:
            continue
        try:
            u = urllib.parse.urlsplit(raw)
            path = (u.path or "").lower()
            query = (u.query or "").lower()

            if "modelcode=" in query:
                return True
            if re.search(r"\bsm-[a-z0-9]+\b", raw, flags=re.I):
                return True
            if cap_token and cap_token in path:
                return True
        except Exception:
            continue
    return False



SAMSUNG_BUY_VARIANTS_CACHE = {}


def sanitize_samsung_buy_url(url: str) -> str:
    if not url:
        return ""
    try:
        u = urllib.parse.urlsplit(url)
        scheme = u.scheme or "https"
        netloc = u.netloc
        path = (u.path or "").rstrip("/")
        if not re.search(r"/buy$", path, flags=re.I):
            path = re.sub(r"/buy$", "", path, flags=re.I).rstrip("/") + "/buy"
        qs = urllib.parse.parse_qs(u.query, keep_blank_values=True)
        keep = []
        for key in ["modelCode", "modelcode"]:
            for value in qs.get(key, []):
                if value:
                    keep.append(("modelCode", value))
        query = urllib.parse.urlencode(keep, doseq=True)
        return urllib.parse.urlunsplit((scheme, netloc, path + "/", query, ""))
    except Exception:
        return url


def build_samsung_buy_url(detail_url: str = "", buy_url_hint: str = "", model_code: str = "") -> str:
    hinted = sanitize_samsung_buy_url(buy_url_hint)
    if hinted and "modelCode=" in hinted and not model_code:
        return hinted

    base = normalize_product_url(detail_url or buy_url_hint or "")
    if not base:
        return hinted

    u = urllib.parse.urlsplit(base)
    scheme = u.scheme or "https"
    netloc = u.netloc
    path = re.sub(r"/buy$", "", (u.path or "").rstrip("/"), flags=re.I).rstrip("/") + "/buy/"
    query = urllib.parse.urlencode({"modelCode": model_code}) if (model_code or "").strip() else ""
    return urllib.parse.urlunsplit((scheme, netloc, path, query, ""))


def _search_jsonish_field(blob: str, field: str) -> str:
    if not blob:
        return ""
    patterns = [
        rf'"{re.escape(field)}"\s*:\s*"([^\"]*)"',
        rf"'{re.escape(field)}'\s*:\s*'([^']*)'",
        rf'"{re.escape(field)}"\s*:\s*([^,\}}\]]+)',
    ]
    for pat in patterns:
        m = re.search(pat, blob, flags=re.I | re.S)
        if m:
            return normalize_spaces(str(m.group(1)).strip().strip('"').strip("'"))
    return ""

def _extract_samsung_variant_blocks(html: str):
    seen = set()
    blocks = []

    object_re = re.compile(r'\{[^{}]{0,5000}"modelCode"\s*:\s*"SM-[A-Z0-9]+"[^{}]{0,5000}\}', re.I | re.S)
    for m in object_re.finditer(html or ""):
        blob = m.group(0)
        mc = _search_jsonish_field(blob, "modelCode").upper()
        sig = normalize_spaces(blob)
        if mc and sig not in seen:
            seen.add(sig)
            blocks.append(blob)

    if blocks:
        return blocks

    pair_re = re.compile(
        r'"displayName"\s*:\s*"(?P<display>[^"]+)"(?P<mid>.{0,2000}?)"modelCode"\s*:\s*"(?P<model>SM-[A-Z0-9]+)"(?P<tail>.{0,2000}?)"price"\s*:\s*"?(?P<price>\d+(?:[\.,]\d+)?)"?',
        re.I | re.S,
    )
    for m in pair_re.finditer(html or ""):
        blob = m.group(0)
        sig = normalize_spaces(blob)
        if sig in seen:
            continue
        seen.add(sig)
        blocks.append(blob)
    return blocks


def _extract_json_array_after_key(html: str, key: str):
    if not html or not key:
        return None
    m = re.search(rf'"{re.escape(key)}"\s*:\s*\[', html, flags=re.I)
    if not m:
        return None
    start = html.find("[", m.start())
    if start < 0:
        return None

    depth = 0
    in_string = False
    escape = False
    for idx in range(start, len(html)):
        ch = html[idx]
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            continue

        if ch == '"':
            in_string = True
        elif ch == '[':
            depth += 1
        elif ch == ']':
            depth -= 1
            if depth == 0:
                return html[start:idx + 1]
    return None


def _parse_samsung_buying_options(html: str):
    arr_txt = _extract_json_array_after_key(html or "", "buyingOptions")
    if not arr_txt:
        return []
    try:
        data = json.loads(arr_txt)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _variant_specificity_score(v: dict) -> int:
    return int(bool(v.get("capacidad"))) + int(bool(v.get("memoria"))) + int(bool(v.get("model_code")))


def _make_variant_candidate(model_code: str = "", display_name: str = "", english_name: str = "", capacidad: str = "", memoria: str = "", precio_actual: int = 0, precio_original: int = 0, detail_url: str = "", buy_url_hint: str = ""):
    mc = normalize_spaces(model_code).upper()
    cap = normalize_spaces(capacidad).upper().replace(" ", "")
    ram = normalize_spaces(memoria).upper().replace(" ", "")
    pa = int(precio_actual or 0)
    po = int(precio_original or 0)
    if po and pa and po < pa:
        po = pa
    if pa <= 0 and po > 0:
        pa = po
    if po <= 0 and pa > 0:
        po = pa
    return {
        "model_code": mc,
        "display_name": normalize_spaces(display_name),
        "english_name": normalize_spaces(english_name),
        "capacidad": cap,
        "memoria": ram,
        "precio_actual": pa,
        "precio_original": po,
        "buy_url": build_samsung_buy_url(detail_url=detail_url, buy_url_hint=buy_url_hint, model_code=mc),
    }


def _merge_variant_candidate(variants: list, candidate: dict):
    if not candidate:
        return
    if int(candidate.get("precio_actual") or 0) <= 0 and int(candidate.get("precio_original") or 0) <= 0:
        return

    mc = str(candidate.get("model_code") or "").upper().strip()
    cap = str(candidate.get("capacidad") or "").upper().strip()
    ram = str(candidate.get("memoria") or "").upper().strip()

    target = None
    for existing in variants:
        emc = str(existing.get("model_code") or "").upper().strip()
        ecap = str(existing.get("capacidad") or "").upper().strip()
        eram = str(existing.get("memoria") or "").upper().strip()
        same_model = bool(mc and emc and mc == emc)
        same_capram = bool(cap and ram and ecap == cap and eram == ram)
        if same_capram or same_model:
            target = existing
            break

    if not target:
        variants.append(candidate)
        return

    target_score = _variant_specificity_score(target)
    cand_score = _variant_specificity_score(candidate)

    if cand_score >= target_score:
        for k in ["display_name", "english_name", "capacidad", "memoria", "model_code", "buy_url"]:
            if candidate.get(k):
                target[k] = candidate[k]
    else:
        for k in ["display_name", "english_name", "capacidad", "memoria", "model_code", "buy_url"]:
            if not target.get(k) and candidate.get(k):
                target[k] = candidate[k]

    if cand_score >= target_score:
        if int(candidate.get("precio_actual") or 0) > 0:
            target["precio_actual"] = int(candidate.get("precio_actual") or 0)
        if int(candidate.get("precio_original") or 0) > 0:
            target["precio_original"] = int(candidate.get("precio_original") or 0)
    else:
        if int(target.get("precio_actual") or 0) <= 0 and int(candidate.get("precio_actual") or 0) > 0:
            target["precio_actual"] = int(candidate.get("precio_actual") or 0)
        if int(target.get("precio_original") or 0) <= 0 and int(candidate.get("precio_original") or 0) > 0:
            target["precio_original"] = int(candidate.get("precio_original") or 0)


def _variant_from_option_item(item: dict, detail_url: str = "", buy_url_hint: str = ""):
    if not isinstance(item, dict):
        return None
    model_code = normalize_spaces(str(item.get("modelCode") or item.get("pimModelCode") or "")).upper()
    display_name = normalize_spaces(str(item.get("displayName") or ""))
    english_name = normalize_spaces(str(item.get("englishName") or ""))
    display_text = normalize_spaces(f"{display_name} {english_name}")
    capacidad, memoria = parse_variant_option_text(display_text)

    price = parse_eur_num(str(item.get("price") or ""))
    promo = parse_eur_num(str(item.get("promotionPrice") or ""))
    list_price = parse_eur_num(str(item.get("listPrice") or ""))
    sale_price = parse_eur_num(str(item.get("salePrice") or ""))

    precio_actual = 0
    precio_original = 0
    if promo > 0 and (price == 0 or promo <= price):
        precio_actual = promo
        precio_original = price or list_price or promo
    elif sale_price > 0 and (price == 0 or sale_price <= price):
        precio_actual = sale_price
        precio_original = price or list_price or sale_price
    elif price > 0:
        precio_actual = price
        precio_original = list_price or price
    elif list_price > 0:
        precio_actual = list_price
        precio_original = list_price

    return _make_variant_candidate(
        model_code=model_code,
        display_name=display_name,
        english_name=english_name,
        capacidad=capacidad,
        memoria=memoria,
        precio_actual=precio_actual,
        precio_original=precio_original,
        detail_url=detail_url,
        buy_url_hint=buy_url_hint,
    )


def _variant_from_blob(blob: str, detail_url: str = "", buy_url_hint: str = ""):
    model_code = _search_jsonish_field(blob, "modelCode").upper()
    display_name = _search_jsonish_field(blob, "displayName")
    english_name = _search_jsonish_field(blob, "englishName")
    display_text = normalize_spaces(f"{display_name} {english_name}")
    capacidad, memoria = parse_variant_option_text(display_text)

    price = parse_eur_num(_search_jsonish_field(blob, "price"))
    promo = parse_eur_num(_search_jsonish_field(blob, "promotionPrice"))
    list_price = parse_eur_num(_search_jsonish_field(blob, "listPrice"))
    sale_price = parse_eur_num(_search_jsonish_field(blob, "salePrice"))

    precio_actual = 0
    precio_original = 0
    if promo > 0 and (price == 0 or promo <= price):
        precio_actual = promo
        precio_original = price or list_price or promo
    elif sale_price > 0 and (price == 0 or sale_price <= price):
        precio_actual = sale_price
        precio_original = price or list_price or sale_price
    elif price > 0:
        precio_actual = price
        precio_original = list_price or price
    elif list_price > 0:
        precio_actual = list_price
        precio_original = list_price

    return _make_variant_candidate(
        model_code=model_code,
        display_name=display_name,
        english_name=english_name,
        capacidad=capacidad,
        memoria=memoria,
        precio_actual=precio_actual,
        precio_original=precio_original,
        detail_url=detail_url,
        buy_url_hint=buy_url_hint,
    )


def _parse_visible_variant_prices(text: str):
    vals = [int(v) for v in parse_eur_all(text) if int(v) > 0]
    vals = sorted(set(vals), reverse=True)
    if not vals:
        return 0, 0
    if len(vals) == 1:
        return vals[0], calcular_precio_original(vals[0])
    top = vals[:2]
    precio_actual = min(top)
    precio_original = max(top)
    return int(precio_actual), int(precio_original)


def _extract_rendered_variant_elements(driver):
    js = r"""
const storageRe = /(?:64|128|256|512|1024)\s*GB\s*[｜|\\/]\s*(?:3|4|6|8|12|16)\s*GB/i;
const euroRe = /\d[\d\.,]*\s*€/;
const norm = (s) => (s || '').replace(/\s+/g, ' ').trim();
const nodes = Array.from(document.querySelectorAll('body *'));
const out = [];
for (const el of nodes) {
  const txt = norm(el.innerText);
  if (!txt || !storageRe.test(txt) || !euroRe.test(txt)) continue;
  const childHas = Array.from(el.children || []).some(ch => {
    const ct = norm(ch.innerText);
    return ct && storageRe.test(ct) && euroRe.test(ct);
  });
  if (childHas) continue;
  out.push({text: txt.slice(0, 1200), html: (el.outerHTML || '').slice(0, 4000)});
}
return out;
"""
    try:
        return driver.execute_script(js) or []
    except Exception:
        return []


def _get_samsung_buy_variants_rendered(detail_url: str = "", buy_url_hint: str = ""):
    buy_url = build_samsung_buy_url(detail_url=detail_url, buy_url_hint=buy_url_hint, model_code="")
    if not buy_url:
        return []

    variants = []
    driver = None
    try:
        driver = get_driver()
        driver.set_page_load_timeout(45)
        driver.get(buy_url)
        time.sleep(3)
        dismiss_overlays(driver)
        try:
            driver.execute_script("window.scrollTo(0, 1100);")
        except Exception:
            pass
        time.sleep(1.2)
        scroll_page(driver, rounds=6)
        time.sleep(1.2)

        html = driver.page_source or ""

        buying_options = _parse_samsung_buying_options(html)
        if buying_options:
            for group in buying_options:
                if not isinstance(group, dict):
                    continue
                option_items = group.get("optionItems") or []
                if not isinstance(option_items, list):
                    continue
                for item in option_items:
                    _merge_variant_candidate(variants, _variant_from_option_item(item, detail_url=detail_url, buy_url_hint=buy_url))

        if not variants:
            blocks = _extract_samsung_variant_blocks(html)
            for blob in blocks:
                _merge_variant_candidate(variants, _variant_from_blob(blob, detail_url=detail_url, buy_url_hint=buy_url))

        rendered_cards = _extract_rendered_variant_elements(driver)
        for card in rendered_cards:
            text = normalize_spaces(str(card.get("text") or ""))
            html_blob = str(card.get("html") or "")
            capacidad, memoria = parse_variant_option_text(text)
            if not capacidad or not memoria:
                continue
            precio_actual, precio_original = _parse_visible_variant_prices(text)
            if precio_actual <= 0 and precio_original <= 0:
                continue
            model_code = extraer_model_code(html_blob)
            _merge_variant_candidate(
                variants,
                _make_variant_candidate(
                    model_code=model_code,
                    display_name=capacidad + "｜" + memoria,
                    english_name="",
                    capacidad=capacidad,
                    memoria=memoria,
                    precio_actual=precio_actual,
                    precio_original=precio_original,
                    detail_url=detail_url,
                    buy_url_hint=buy_url,
                ),
            )

        return variants
    except Exception:
        return variants
    finally:
        try:
            if driver:
                driver.quit()
        except Exception:
            pass


def get_samsung_buy_variants(detail_url: str = "", buy_url_hint: str = ""):
    cache_key = normalize_product_url(detail_url or buy_url_hint or "")
    if cache_key in SAMSUNG_BUY_VARIANTS_CACHE:
        return SAMSUNG_BUY_VARIANTS_CACHE[cache_key]

    variants = []
    try:
        buy_url = build_samsung_buy_url(detail_url=detail_url, buy_url_hint=buy_url_hint, model_code="")
        if not buy_url:
            SAMSUNG_BUY_VARIANTS_CACHE[cache_key] = []
            return []

        r = requests.get(buy_url, headers=HEADERS, timeout=20)
        html = r.text or ""

        buying_options = _parse_samsung_buying_options(html)
        if buying_options:
            for group in buying_options:
                if not isinstance(group, dict):
                    continue
                option_items = group.get("optionItems") or []
                if not isinstance(option_items, list):
                    continue
                for item in option_items:
                    _merge_variant_candidate(variants, _variant_from_option_item(item, detail_url=detail_url, buy_url_hint=buy_url))

        if not variants:
            blocks = _extract_samsung_variant_blocks(html)
            for blob in blocks:
                _merge_variant_candidate(variants, _variant_from_blob(blob, detail_url=detail_url, buy_url_hint=buy_url))

        enough_specific = len([v for v in variants if v.get("capacidad") and v.get("memoria")])
        if enough_specific == 0:
            print(f"🧪 Samsung buy variants: HTML estático sin variantes suficientes, probando DOM renderizado -> {mask_url(buy_url)}", flush=True)
            rendered_variants = _get_samsung_buy_variants_rendered(detail_url=detail_url, buy_url_hint=buy_url)
            print(f"🧪 Samsung buy variants renderizadas: {len(rendered_variants)}", flush=True)
            for v in rendered_variants:
                _merge_variant_candidate(variants, v)

    except Exception:
        variants = []

    SAMSUNG_BUY_VARIANTS_CACHE[cache_key] = variants
    return variants


def resolve_samsung_variant(detail_url: str = "", buy_url_hint: str = "", capacidad: str = "", memoria: str = "", model_code: str = ""):
    variants = get_samsung_buy_variants(detail_url=detail_url, buy_url_hint=buy_url_hint)
    if not variants:
        return None

    cap = (capacidad or "").strip().upper()
    ram = (memoria or "").strip().upper()
    mc = (model_code or "").strip().upper()

    if cap and ram:
        exact_cr = [v for v in variants if str(v.get("capacidad", "")).upper() == cap and str(v.get("memoria", "")).upper() == ram]
        if len(exact_cr) == 1:
            return exact_cr[0]

    if mc:
        exact_mc = [v for v in variants if str(v.get("model_code", "")).upper() == mc]
        if len(exact_mc) == 1:
            candidate = exact_mc[0]
            candidate_cap = str(candidate.get("capacidad", "")).upper()
            candidate_ram = str(candidate.get("memoria", "")).upper()
            if (cap and candidate_cap and candidate_cap != cap) or (ram and candidate_ram and candidate_ram != ram):
                pass
            else:
                return candidate

    filtered = variants
    if cap:
        filtered = [v for v in filtered if str(v.get("capacidad", "")).upper() == cap]
    if ram:
        filtered = [v for v in filtered if str(v.get("memoria", "")).upper() == ram]
    if len(filtered) == 1:
        return filtered[0]

    if cap:
        cap_only = [v for v in variants if str(v.get("capacidad", "")).upper() == cap]
        if len(cap_only) == 1:
            return cap_only[0]

    return None


def obtener_precio_real_samsung(detail_url: str, buy_url_hint: str = "", model_code: str = "", capacidad: str = "", memoria: str = ""):
    try:
        variant = resolve_samsung_variant(
            detail_url=detail_url,
            buy_url_hint=buy_url_hint,
            capacidad=capacidad,
            memoria=memoria,
            model_code=model_code,
        )
        if variant:
            return int(variant.get("precio_actual") or 0), int(variant.get("precio_original") or 0)

        if not is_samsung_variant_specific(detail_url=detail_url, buy_url_hint=buy_url_hint, model_code=model_code, capacidad=capacidad):
            return 0, 0

        buy_url = build_samsung_buy_url(detail_url=detail_url, buy_url_hint=buy_url_hint, model_code=model_code)
        if not buy_url:
            return 0, 0

        r = requests.get(buy_url, headers=HEADERS, timeout=15)
        html = r.text

        patterns_actual = [
            r'digitalData\.product\.model_price\s*=\s*"([^"]+)"',
            r'"model_price"\s*:\s*"([^"]+)"',
        ]
        patterns_original = [
            r'digitalData\.product\.list_price\s*=\s*"([^"]+)"',
            r'"list_price"\s*:\s*"([^"]+)"',
        ]

        m_actual = None
        m_original = None
        for pat in patterns_actual:
            m_actual = re.search(pat, html)
            if m_actual:
                break
        for pat in patterns_original:
            m_original = re.search(pat, html)
            if m_original:
                break

        precio_actual = parse_eur_num(m_actual.group(1)) if m_actual else 0
        precio_original = parse_eur_num(m_original.group(1)) if m_original else precio_actual

        return precio_actual, precio_original
    except Exception:
        return 0, 0

        buy_url = build_samsung_buy_url(detail_url=detail_url, buy_url_hint=buy_url_hint, model_code=model_code)
        if not buy_url:
            return 0, 0

        r = requests.get(buy_url, headers=HEADERS, timeout=15)
        html = r.text

        patterns_actual = [
            r'digitalData\.product\.model_price\s*=\s*"([^\"]+)"',
            r'"model_price"\s*:\s*"([^\"]+)"',
        ]
        patterns_original = [
            r'digitalData\.product\.list_price\s*=\s*"([^\"]+)"',
            r'"list_price"\s*:\s*"([^\"]+)"',
        ]

        m_actual = None
        m_original = None
        for pat in patterns_actual:
            m_actual = re.search(pat, html)
            if m_actual:
                break
        for pat in patterns_original:
            m_original = re.search(pat, html)
            if m_original:
                break

        precio_actual = parse_eur_num(m_actual.group(1)) if m_actual else 0
        precio_original = parse_eur_num(m_original.group(1)) if m_original else precio_actual

        return precio_actual, precio_original
    except Exception:
        return 0, 0

def unir_afiliado(url_base: str, aff: str) -> str:
    """Une AFF_SAMSUNG sin generar /?/? ni dobles interrogaciones."""
    base = (url_base or "").strip().strip('"').strip("'")
    aff = (aff or "").strip().strip('"').strip("'")
    if not base or not aff:
        return base
    if aff.lower().startswith("http"):
        return aff

    aff = aff.replace("&amp;", "&")
    # elimina prefijos raros tipo /? , ?? , && , /&
    aff = re.sub(r'^[\s/]+', '', aff)
    aff = re.sub(r'^[?&/]+', '', aff)
    aff = re.sub(r'^[\s/]+', '', aff)

    u = urllib.parse.urlsplit(base)
    scheme = u.scheme or "https"
    netloc = u.netloc
    path = (u.path or "").split("?")[0].rstrip("/") + "/"
    existing_query = (u.query or "").strip()
    aff_query = aff.lstrip("?&")
    query_parts = [q for q in [existing_query, aff_query] if q]
    query = "&".join(query_parts)
    return urllib.parse.urlunsplit((scheme, netloc, path, query, ""))


def normalizar_nombre_samsung(nombre: str) -> str:
    t = normalize_spaces(nombre)
    if not t:
        return ""
    t = re.sub(r"\bExclusivo Online\b", "", t, flags=re.I)
    t = t.replace("()", "")
    t = normalize_spaces(t)
    # Normaliza FE / 5G / nombres comunes
    t = re.sub(r"\bfe\b", "FE", t, flags=re.I)
    t = re.sub(r"\b5g\b", "5G", t, flags=re.I)
    if not t.lower().startswith("samsung "):
        t = "Samsung " + t
    if "galaxy" not in t.lower():
        t = t.replace("Samsung ", "Samsung Galaxy ", 1)
    return normalize_spaces(t)


def limpiar_nombre_para_categoria(nombre: str) -> str:
    return normalize_spaces(nombre or "")


def parse_capacidad_desde_texto(txt: str) -> str:
    t = normalize_spaces(txt)
    m = re.search(r"\b(64|128|256|512|1024)\s*GB\b", t, flags=re.I)
    if m:
        return f"{m.group(1)}GB"
    m = re.search(r"\b(1|2)\s*TB\b", t, flags=re.I)
    if m:
        return f"{m.group(1)}TB"
    return ""


def parse_memoria_desde_texto(txt: str) -> str:
    t = normalize_spaces(txt)
    m = re.search(r"\b(4|6|8|12|16)\s*GB(?:\s*RAM)?\b", t, flags=re.I)
    if m:
        return f"{m.group(1)}GB"
    return ""


def parse_variant_option_text(option_text: str):
    text = normalize_spaces(option_text)
    cap = ""
    ram = ""
    m = re.search(
        r"(?P<cap>\d{2,4}\s*(?:GB|TB)).*?(?P<ram>\d{1,2}\s*GB)"
        r"|(?P<ram2>\d{1,2}\s*GB).*?(?P<cap2>\d{2,4}\s*(?:GB|TB))",
        text,
        flags=re.I,
    )
    if m:
        if m.group("cap") and m.group("ram"):
            cap = normalize_spaces(m.group("cap")).upper().replace(" ", "")
            ram = normalize_spaces(m.group("ram")).upper().replace(" ", "")
        else:
            cap = normalize_spaces(m.group("cap2")).upper().replace(" ", "")
            ram = normalize_spaces(m.group("ram2")).upper().replace(" ", "")
    return cap, ram


def extraer_model_code(text: str) -> str:
    m = re.search(r"\bSM-[A-Z0-9]+\b", text or "", flags=re.I)
    return m.group(0).upper() if m else ""


def should_skip_by_name(nombre: str) -> bool:
    u = (nombre or "").upper()
    return any(x in u for x in [" TAB", "IPAD", " PAD", "WATCH", "BUDS", "RING"]) or u.startswith("TAB ")


def source_key(nombre: str, memoria: str, capacidad: str, fuente: str = FUENTE) -> str:
    return f"{normalize_spaces(nombre).lower()}|{str(memoria).upper()}|{str(capacidad).upper()}|{fuente.lower()}"


# --------------------------
# MAPA RAM / CAPACIDADES
# --------------------------

RAM_BY_NAME_CAPACITY = {
    ("samsung galaxy s26", "256GB"): "12GB",
    ("samsung galaxy s26", "512GB"): "12GB",
    ("samsung galaxy s26+", "256GB"): "12GB",
    ("samsung galaxy s26+", "512GB"): "12GB",
    ("samsung galaxy s26 ultra", "256GB"): "12GB",
    ("samsung galaxy s26 ultra", "512GB"): "12GB",
    ("samsung galaxy s26 ultra", "1TB"): "16GB",
    ("samsung galaxy z fold7", "256GB"): "12GB",
    ("samsung galaxy z fold7", "512GB"): "12GB",
    ("samsung galaxy z fold7", "1TB"): "16GB",
    ("samsung galaxy z flip7", "256GB"): "12GB",
    ("samsung galaxy z flip7", "512GB"): "12GB",
    ("samsung galaxy z flip7 fe", "128GB"): "8GB",
    ("samsung galaxy z flip7 fe", "256GB"): "8GB",
    ("samsung galaxy s25", "128GB"): "12GB",
    ("samsung galaxy s25", "256GB"): "12GB",
    ("samsung galaxy s25", "512GB"): "12GB",
    ("samsung galaxy s25+", "256GB"): "12GB",
    ("samsung galaxy s25+", "512GB"): "12GB",
    ("samsung galaxy s25 ultra", "256GB"): "12GB",
    ("samsung galaxy s25 ultra", "512GB"): "12GB",
    ("samsung galaxy s25 ultra", "1TB"): "12GB",
    ("samsung galaxy s25 fe", "128GB"): "8GB",
    ("samsung galaxy s25 fe", "256GB"): "8GB",
    ("samsung galaxy s25 fe", "512GB"): "8GB",
    ("samsung galaxy s25 edge", "256GB"): "12GB",
    ("samsung galaxy s25 edge", "512GB"): "12GB",
    ("samsung galaxy s24", "128GB"): "8GB",
    ("samsung galaxy s24", "256GB"): "8GB",
    ("samsung galaxy s24+", "256GB"): "12GB",
    ("samsung galaxy s24+", "512GB"): "12GB",
    ("samsung galaxy s24 fe", "128GB"): "8GB",
    ("samsung galaxy s24 fe", "256GB"): "8GB",
    ("samsung galaxy z flip6", "256GB"): "12GB",
    ("samsung galaxy z flip6", "512GB"): "12GB",
    ("samsung galaxy z fold6", "256GB"): "12GB",
    ("samsung galaxy z fold6", "512GB"): "12GB",
    ("samsung galaxy z fold6", "1TB"): "16GB",
    ("samsung galaxy a57 5g", "128GB"): "8GB",
    ("samsung galaxy a57 5g", "256GB"): "8GB",
    ("samsung galaxy a57 5g", "512GB"): "12GB",
    ("samsung galaxy a56 5g", "128GB"): "8GB",
    ("samsung galaxy a56 5g", "256GB"): "8GB",
    ("samsung galaxy a37 5g", "256GB"): "8GB",
    ("samsung galaxy a36 5g", "128GB"): "8GB",
    ("samsung galaxy a36 5g", "256GB"): "8GB",
    ("samsung galaxy a26 5g", "128GB"): "6GB",
    ("samsung galaxy a26 5g", "256GB"): "8GB",
    ("samsung galaxy a17 5g", "256GB"): "8GB",
    ("samsung galaxy a17", "256GB"): "8GB",
    ("samsung galaxy a16", "256GB"): "8GB",
}
RAM_BY_NAME_CAPACITY = {(k[0].lower(), k[1].upper()): v for k, v in RAM_BY_NAME_CAPACITY.items()}


def known_capacities_for_name(nombre: str):
    n = normalize_spaces(nombre).lower()
    caps = [cap for (name, cap), _ in RAM_BY_NAME_CAPACITY.items() if name == n]
    caps = dedupe_keep_order(caps)
    return sorted(caps, key=lambda c: 2000 if c.endswith("TB") else int(re.sub(r"\D", "", c) or "0"))


def infer_memoria_samsung_desde_listing(nombre: str, capacidad: str, url_hint: str = "") -> str:
    key = (normalize_spaces(nombre).lower(), (capacidad or "").upper())
    if key in RAM_BY_NAME_CAPACITY:
        return RAM_BY_NAME_CAPACITY[key]
    low_name = normalize_spaces(nombre).lower()
    low_hint = (url_hint or "").lower()
    if "fold" in low_name or "galaxy-z-fold" in low_hint:
        return "16GB" if (capacidad or "").upper() == "1TB" else "12GB"
    if "flip" in low_name or "galaxy-z-flip" in low_hint:
        return "8GB" if "flip7 fe" in low_name else "12GB"
    if " ultra" in low_name:
        return "16GB" if (capacidad or "").upper() == "1TB" else "12GB"
    if "s25 edge" in low_name:
        return "12GB"
    return ""


# --------------------------
# JSON-LD
# --------------------------

def normalize_product_url(url: str) -> str:
    if not url:
        return ""
    try:
        u = urllib.parse.urlsplit(url)
        path = (u.path or "").rstrip("/")
        path = re.sub(r"/buy$", "", path, flags=re.I)
        return urllib.parse.urlunsplit((u.scheme, u.netloc, path, "", ""))
    except Exception:
        return (url or "").split("?")[0].rstrip("/")


def extract_jsonld_products(html: str):
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for script in soup.find_all("script", attrs={"type": re.compile(r"ld\+json", re.I)}):
        raw = script.string or script.get_text() or ""
        raw = raw.strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue

        stack = [data]
        while stack:
            node = stack.pop()
            if isinstance(node, list):
                stack.extend(node)
                continue
            if not isinstance(node, dict):
                continue
            if isinstance(node.get("itemListElement"), list):
                stack.extend(node["itemListElement"])
            if isinstance(node.get("item"), dict):
                stack.append(node["item"])
            if isinstance(node.get("@graph"), list):
                stack.extend(node["@graph"])

            t = node.get("@type")
            tlist = t if isinstance(t, list) else [t]
            if "Product" not in [str(x) for x in tlist if x]:
                continue

            name = normalizar_nombre_samsung(node.get("name") or "")
            if not name or should_skip_by_name(name):
                continue

            offers = node.get("offers") or {}
            if isinstance(offers, list):
                offers = offers[0] if offers else {}

            price = parse_eur_num(str(offers.get("price") or ""))
            raw_url = node.get("url") or node.get("@id") or ""
            expanded = abs_url(START_URL, raw_url) if raw_url else ""
            expanded = expanded.split("#")[0]
            detail_url = normalize_product_url(expanded)
            img = node.get("image") or ""
            if isinstance(img, list):
                img = img[0] if img else ""
            img = abs_url(START_URL, img) if img else ""
            desc = normalize_spaces(node.get("description") or "")
            cap = parse_capacidad_desde_texto(expanded + " " + detail_url + " " + desc + " " + name)
            model_code = extraer_model_code(expanded + " " + desc)

            out.append({
                "name": name,
                "price": price,
                "expanded_url": expanded,
                "detail_url": detail_url,
                "raw_url": raw_url,
                "image": img,
                "description": desc,
                "capacidad": cap,
                "capacidad_origen": "detectada_en_texto" if cap else "",
                "capacidad_debug_reason": (
                    f"extraida_de_url_desc_nombre:{cap}" if cap else "sin_capacidad_en_url_desc_nombre"
                ),
                "source_channel": "jsonld_listing",
                "model_code": model_code,
            })

    # Inferir capacidades ausentes por nombre y orden de precios
    grouped = {}
    for item in out:
        grouped.setdefault(normalize_spaces(item.get("name", "")).lower(), []).append(item)
    for _, group in grouped.items():
        missing = [it for it in group if not it.get("capacidad")]
        if not missing:
            continue
        known = known_capacities_for_name(group[0].get("name", ""))
        if not known:
            for it in missing:
                it["capacidad_debug_reason"] = "sin_capacidad_en_texto_y_sin_mapa"
            continue
        explicit = {it.get("capacidad") for it in group if it.get("capacidad")}
        remaining = [c for c in known if c not in explicit]
        if len(remaining) != len(missing):
            reason = (
                f"sin_capacidad_en_texto; mapa={','.join(known)}; "
                f"explicit={','.join(sorted([x for x in explicit if x])) or '-'}; "
                f"missing={len(missing)}; remaining={len(remaining)}"
            )
            for it in missing:
                it["capacidad_debug_reason"] = reason
            continue
        missing_sorted = sorted(missing, key=lambda x: int(x.get("price") or 0))
        def cap_sort(c):
            return 2000 if c.endswith("TB") else int(re.sub(r"\D", "", c) or "0")
        remaining_sorted = sorted(remaining, key=cap_sort)
        for it, cap in zip(missing_sorted, remaining_sorted):
            it["capacidad"] = cap
            it["capacidad_origen"] = "inferida_por_mapa"
            it["capacidad_debug_reason"] = f"inferida_por_mapa:{cap}"

    return out


# --------------------------
# SELENIUM / CARDS
# --------------------------

def get_driver():
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options

    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1440,2600")
    opts.add_argument(
        "--user-agent=Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
    return webdriver.Chrome(options=opts)


def dismiss_overlays(driver):
    from selenium.webdriver.common.by import By

    candidates = [
        "//button[contains(., 'Aceptar')]",
        "//button[contains(., 'Acepto')]",
        "//button[contains(., 'Aceptar todo')]",
        "//button[contains(., 'OK')]",
        "//button[contains(., 'Continuar')]",
        "//button[contains(., 'MAS TARDE')]",
        "//button[contains(., 'MÁS TARDE')]",
        "//a[contains(., 'IR A SAMSUNG.COM')]",
    ]
    for _ in range(3):
        for xp in candidates:
            try:
                for el in driver.find_elements(By.XPATH, xp)[:3]:
                    if el.is_displayed():
                        try:
                            driver.execute_script("arguments[0].click();", el)
                            time.sleep(0.6)
                        except Exception:
                            pass
            except Exception:
                pass


def scroll_page(driver, rounds: int = 20):
    last_h = 0
    stable = 0
    for _ in range(rounds):
        try:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1.0)
            h = driver.execute_script("return document.body.scrollHeight")
            if h == last_h:
                stable += 1
            else:
                stable = 0
            last_h = h
            if stable >= 3:
                break
        except Exception:
            break


def extract_title_from_card(text: str) -> str:
    lines = [normalize_spaces(x) for x in re.split(r"[\n\r]+", text or "") if normalize_spaces(x)]
    for line in lines:
        low = line.lower()
        if "galaxy" not in low:
            continue
        if any(x in low for x in ["comprar", "comparar", "ahorra", "estrena", "descubre", "ver mas", "ver más"]):
            continue
        if any(x in low for x in ["tab", "watch", "buds", "ring", "accesorios"]):
            continue
        line = re.split(r"\b(?:64|128|256|512|1024)\s*GB\b|\b(?:1|2)\s*TB\b", line, maxsplit=1, flags=re.I)[0]
        name = normalizar_nombre_samsung(line)
        if name and name != "Samsung Galaxy":
            return name
    return ""


def collect_listing_card_roots(driver):
    from selenium.webdriver.common.by import By

    buy_buttons = []
    for xp in [
        "//a[contains(normalize-space(.), 'Comprar')]",
        "//button[contains(normalize-space(.), 'Comprar')]",
    ]:
        try:
            buy_buttons.extend(driver.find_elements(By.XPATH, xp))
        except Exception:
            pass

    roots = []
    seen = set()
    for btn in buy_buttons:
        try:
            if not btn.is_displayed():
                continue
            cur = btn
            container = None
            for _ in range(10):
                cur = cur.find_element(By.XPATH, "..")
                txt = normalize_spaces(cur.text)
                if "galaxy" in txt.lower() and "€" in txt:
                    container = cur
                    break
            if not container:
                continue
            cid = container.id
            if cid in seen:
                continue
            seen.add(cid)
            roots.append(container)
        except Exception:
            pass
    return roots


def _capacity_candidate_score(el):
    score = 0
    try:
        cls = (el.get_attribute("class") or "").lower()
        attrs = " ".join(
            [(el.get_attribute(a) or "") for a in ["aria-selected", "aria-pressed", "selected", "checked"]]
        ).lower()
        if "true" in attrs:
            score += 100
        if any(k in cls for k in ["selected", "active", "checked", "current", "is-selected"]):
            score += 50
    except Exception:
        pass
    return score


def extract_selected_capacity_from_card(card):
    from selenium.webdriver.common.by import By

    storage_re = re.compile(r"^\d+\s*(GB|TB)$", re.I)
    candidates = []
    try:
        els = card.find_elements(By.XPATH, ".//*")
    except Exception:
        return "", []

    for el in els:
        try:
            if not el.is_displayed():
                continue
            txt = normalize_spaces(el.text)
            if not txt or len(txt) > 20 or not storage_re.fullmatch(txt):
                continue
            cap = parse_capacidad_desde_texto(txt)
            if not cap:
                continue
            candidates.append((_capacity_candidate_score(el), cap))
        except Exception:
            pass

    if not candidates:
        return "", []

    ordered = dedupe_keep_order([cap for _, cap in candidates])
    positives = [c for c in candidates if c[0] > 0]
    if positives:
        positives.sort(key=lambda x: (-x[0], x[1]))
        return positives[0][1], ordered
    if len(ordered) == 1:
        return ordered[0], ordered
    return "", ordered


def extract_urls_from_card(card):
    from selenium.webdriver.common.by import By

    buy = ""
    detail = ""
    try:
        anchors = card.find_elements(By.XPATH, ".//a[@href]")
    except Exception:
        anchors = []

    for a in anchors:
        try:
            href = normalize_spaces(a.get_attribute("href") or "")
            txt = normalize_spaces(a.text or "")
            if not href or "/es/smartphones/" not in href:
                continue
            if "/buy/" in href or "comprar" in txt.lower():
                buy = href
            if not detail:
                detail = normalize_product_url(href)
        except Exception:
            pass

    if buy and not detail:
        detail = normalize_product_url(buy)
    return buy, detail


def extract_card_price_info(text: str, fallback_price: int = 0):
    vals = [v for v in parse_eur_all(text) if 150 <= v <= 5000]
    vals = dedupe_keep_order(vals)

    current = 0
    original = 0
    if vals:
        plausible = vals
        if fallback_price > 0:
            low = max(150, int(fallback_price * 0.45))
            high = max(5000, int(fallback_price * 1.35))
            plausible = [v for v in vals if low <= v <= high] or vals

        # actual: el menor importe plausible, pero no dejes que suba mucho sobre el fallback
        current = min(plausible)
        if fallback_price > 0 and current > int(fallback_price * 1.05):
            current = fallback_price

        # original: un precio superior al actual y razonable respecto al fallback
        bigger = [v for v in plausible if v > current]
        if fallback_price > 0:
            bigger = [v for v in bigger if v <= int(max(fallback_price, current) * 1.35)] or bigger
        if bigger:
            original = max(bigger)

    if not current and fallback_price:
        current = fallback_price

    if current and (not original or original <= current):
        if fallback_price and fallback_price > current:
            original = fallback_price
        else:
            original = calcular_precio_original(current)

    return int(current or 0), int(original or 0)


def extract_products_from_main_listing(listing_url: str):
    driver = get_driver()
    try:
        driver.set_page_load_timeout(45)
        driver.get(listing_url)
        time.sleep(3)
        dismiss_overlays(driver)
        scroll_page(driver, rounds=20)
        html = driver.page_source
        jsonld_items = extract_jsonld_products(html)
        print(f"✅ Items JSON-LD Samsung detectados: {len(jsonld_items)}", flush=True)

        stats = {
            "total": 0,
            "skip_by_name": 0,
            "no_capacidad": 0,
            "no_ram": 0,
            "sin_precio": 0,
            "no_detail_url": 0,
            "validos": 0,
        }

        cards = collect_listing_card_roots(driver)
        print(f"✅ Cards Samsung detectadas en listing: {len(cards)}", flush=True)
        visible_card_names = set()
        for card in cards:
            try:
                nm = extract_title_from_card(normalize_spaces(card.text))
                if nm:
                    visible_card_names.add(nm.lower())
            except Exception:
                pass

        # Base principal: JSON-LD
        remote_by_key = {}
        jsonld_by_detail_and_cap = {}
        for item in jsonld_items:
            nombre = item.get("name") or ""
            stats["total"] += 1

            print(f"🔍 DETECTADO (RAW): {nombre}", flush=True)
            try:
                source_channel = item.get("source_channel") or "jsonld_listing"
                expanded_url_log = item.get("expanded_url") or item.get("detail_url") or listing_url
                raw_url_log = item.get("raw_url") or ""
                desc_log = _clip_text(item.get("description") or "", 180)
                cap_log = (item.get("capacidad") or "").upper()
                cap_origin = item.get("capacidad_origen") or "sin_detectar"
                cap_reason = item.get("capacidad_debug_reason") or ""
                known_caps_log = ", ".join(known_capacities_for_name(nombre)) or "-"
                visible_flag = "SI" if nombre.lower() in visible_card_names else "NO"
                print(
                    f"   ↳ Ruta búsqueda: canal={source_channel} | listado={mask_url(listing_url)} | expanded={mask_url(expanded_url_log)} | visible_en_cards={visible_flag}",
                    flush=True,
                )
                if raw_url_log:
                    print(f"   ↳ Raw URL JSON-LD: {raw_url_log}", flush=True)
                if desc_log:
                    print(f"   ↳ Descripción JSON-LD: {desc_log}", flush=True)
                print(
                    f"   ↳ Capacidad: detectada={cap_log or '-'} | origen={cap_origin} | mapa={known_caps_log}",
                    flush=True,
                )
                if cap_reason:
                    print(f"   ↳ Motivo capacidad: {cap_reason}", flush=True)
            except Exception:
                pass

            if not nombre:
                print("⛔ IGNORADO -> sin nombre", flush=True)
                continue

            if should_skip_by_name(nombre):
                stats["skip_by_name"] += 1
                print(f"⛔ IGNORADO -> {nombre} | motivo=skip_by_name", flush=True)
                continue

            capacidad = (item.get("capacidad") or "").upper()
            if not capacidad:
                stats["no_capacidad"] += 1
                print(f"⛔ IGNORADO -> {nombre} | motivo=no_capacidad", flush=True)
                continue

            detail_url = normalize_product_url(item.get("detail_url") or item.get("expanded_url") or "")
            buy_url = abs_url(listing_url, item.get("expanded_url") or "")
            if not detail_url:
                stats["no_detail_url"] += 1
                print(f"⛔ IGNORADO -> {nombre} | motivo=no_detail_url", flush=True)
                continue

            memoria = infer_memoria_samsung_desde_listing(nombre, capacidad, detail_url or buy_url)
            if not memoria:
                stats["no_ram"] += 1
                print(f"⛔ IGNORADO -> {nombre} {capacidad} | motivo=no_ram", flush=True)
                continue

            precio_actual = int(item.get("price") or 0)
            if precio_actual <= 0:
                stats["sin_precio"] += 1
                print(f"⛔ IGNORADO -> {nombre} | motivo=sin_precio", flush=True)
                continue

            precio_original = calcular_precio_original(precio_actual)

            current_model_code = item.get("model_code", "")
            resolved_variant = resolve_samsung_variant(
                detail_url=detail_url,
                buy_url_hint=buy_url,
                capacidad=capacidad,
                memoria=memoria,
                model_code=current_model_code,
            )
            if resolved_variant:
                if int(resolved_variant.get("precio_actual") or 0) > 0:
                    precio_actual = int(resolved_variant.get("precio_actual") or 0)
                if int(resolved_variant.get("precio_original") or 0) > 0:
                    precio_original = int(resolved_variant.get("precio_original") or 0)
                if (resolved_variant.get("model_code") or "").strip():
                    current_model_code = resolved_variant.get("model_code") or current_model_code
                if (resolved_variant.get("buy_url") or "").strip():
                    buy_url = resolved_variant.get("buy_url") or buy_url
                print(f"   ↳ VARIANTE SAMSUNG RESUELTA: cap={capacidad} ram={memoria} model={current_model_code or '-'} pa={precio_actual} po={precio_original}", flush=True)
            else:
                precio_real, precio_real_original = obtener_precio_real_samsung(
                    detail_url=detail_url,
                    buy_url_hint=buy_url,
                    model_code=current_model_code,
                    capacidad=capacidad,
                    memoria=memoria,
                )
                if precio_real > 0:
                    precio_actual = precio_real
                    precio_original = precio_real_original if precio_real_original > precio_real else calcular_precio_original(precio_real)

            print(f"✅ VALIDO -> {nombre} {memoria} {capacidad} | precio={precio_actual}", flush=True)
            stats["validos"] += 1

            key = source_key(nombre, memoria, capacidad, FUENTE)
            variant_import_url = (
                sanitize_samsung_buy_url(buy_url)
                if ((resolved_variant and resolved_variant.get("buy_url")) or is_samsung_variant_specific(
                    detail_url=detail_url,
                    buy_url_hint=buy_url,
                    model_code=current_model_code,
                    capacidad=capacidad,
                ))
                else detail_url
            )

            remote = {
                "nombre": nombre,
                "memoria": memoria,
                "capacidad": capacidad,
                "precio_actual": precio_actual,
                "precio_original": precio_original,
                "img": "",
                "url_imp": detail_url,
                "url_oferta_sin_acortar": buy_url or detail_url,
                "url_importada_sin_afiliado": variant_import_url or detail_url,
                "buy_url": buy_url or detail_url,
                "enviado_desde": ENVIADO_DESDE,
                "enviado_desde_tg": ENVIADO_DESDE_TG,
                "fecha": datetime.now().strftime("%d/%m/%Y"),
                "version": VERSION,
                "fuente": FUENTE,
                "codigo_descuento": CODIGO_DESCUENTO_DEFAULT,
                "origen_pagina": "1",
                "origen_listado": listing_url,
                "source_key": key,
                "model_code": current_model_code,
            }
            remote_by_key[key] = remote
            jsonld_by_detail_and_cap[(detail_url, capacidad)] = remote

        # Enriquecimiento con cards visibles: ajusta precio actual/original y cupón
        for card in cards:
            try:
                text = normalize_spaces(card.text)
                nombre = extract_title_from_card(text)
                if not nombre or should_skip_by_name(nombre):
                    continue

                selected_cap, all_caps = extract_selected_capacity_from_card(card)
                buy_url, detail_url = extract_urls_from_card(card)
                detail_url = normalize_product_url(detail_url or buy_url)
                if not detail_url:
                    continue

                candidates = []
                for key, remote in remote_by_key.items():
                    if remote["url_imp"] == detail_url and remote["nombre"].lower() == nombre.lower():
                        candidates.append(remote)

                buy_model_code = urllib.parse.parse_qs(urllib.parse.urlsplit(buy_url).query).get("modelCode", [""])[0]
                if buy_model_code:
                    exact = [r for r in candidates if str(r.get("model_code", "")).upper() == buy_model_code.upper()]
                    if exact:
                        candidates = exact

                if selected_cap:
                    candidates = [r for r in candidates if r["capacidad"] == selected_cap]

                if not candidates and selected_cap:
                    memoria = infer_memoria_samsung_desde_listing(nombre, selected_cap, detail_url or buy_url)
                    if memoria:
                        key = source_key(nombre, memoria, selected_cap, FUENTE)
                        if key in remote_by_key:
                            candidates = [remote_by_key[key]]

                # Si una card genérica apunta a varias variantes y no hemos podido identificar cuál es,
                # no debemos pisar el precio de todas con el mismo valor.
                if len(candidates) > 1 and not selected_cap and not buy_model_code:
                    continue

                if not candidates:
                    continue

                variants_catalog = get_samsung_buy_variants(detail_url=detail_url, buy_url_hint=buy_url)
                multi_variant = len([v for v in variants_catalog if (v.get("capacidad") or v.get("memoria") or v.get("model_code"))]) > 1

                fallback = min([int(r.get("precio_actual") or 0) for r in candidates if int(r.get("precio_actual") or 0) > 0] or [0])
                cur, orig = extract_card_price_info(text, fallback_price=fallback)
                codigo = extract_coupon_from_text(text) or CODIGO_DESCUENTO_DEFAULT

                for remote in candidates:
                    if not multi_variant and cur > 0:
                        # No sustituir por un precio claramente peor
                        if fallback and cur > int(fallback * 1.40):
                            continue
                        remote["precio_actual"] = cur
                    if not multi_variant:
                        if orig > 0:
                            remote["precio_original"] = max(orig, remote["precio_actual"] + 1)
                        else:
                            remote["precio_original"] = max(int(remote.get("precio_original") or 0), calcular_precio_original(remote["precio_actual"]))
                    remote["codigo_descuento"] = codigo
                    if buy_url:
                        mc = urllib.parse.parse_qs(urllib.parse.urlsplit(buy_url).query).get("modelCode", [""])[0]
                        if mc:
                            specific_buy = sanitize_samsung_buy_url(buy_url)
                            remote["url_oferta_sin_acortar"] = specific_buy
                            remote["buy_url"] = specific_buy
                            remote["url_importada_sin_afiliado"] = specific_buy
                            remote["model_code"] = mc
                        elif (not multi_variant) and (not str(remote.get("model_code", "") or "").strip()):
                            remote["url_oferta_sin_acortar"] = buy_url
                            remote["buy_url"] = buy_url
            except Exception:
                continue

        print("\n📊 DEBUG SAMSUNG:", flush=True)
        for k, v in stats.items():
            print(f"{k}: {v}", flush=True)

        # limpieza final
        final = []
        for key, remote in remote_by_key.items():
            if int(remote.get("precio_actual") or 0) <= 0:
                continue
            po = int(remote.get("precio_original") or 0)
            pa = int(remote.get("precio_actual") or 0)
            if po <= pa:
                remote["precio_original"] = calcular_precio_original(pa)
            final.append(remote)

        # límite razonable
        return final[:OBJETIVO]
    finally:
        try:
            driver.quit()
        except Exception:
            pass


def extract_coupon_from_text(txt: str) -> str:
    t = normalize_spaces(txt or "")
    m = re.search(r"(?:Código|Cup[oó]n|Promo)\s*:?\s*([A-Z0-9_-]{4,})", t, flags=re.I)
    if m:
        return m.group(1)
    return CODIGO_DESCUENTO_DEFAULT


# --------------------------
# EXTRACCIÓN REMOTA
# --------------------------

def obtener_datos_remotos():
    print("", flush=True)
    print("--- FASE 1: ESCANEANDO SAMSUNG ---", flush=True)
    print(f"URL base: {mask_url(START_URL)}", flush=True)
    print(f"🪄 Samsung listing-only: leyendo solo la pagina principal {mask_url(START_URL)}", flush=True)
    productos = extract_products_from_main_listing(START_URL)
    print("", flush=True)
    print("📊 RESUMEN EXTRACCION SAMSUNG:", flush=True)
    print("   URLs descubiertas: 1 (listing principal)", flush=True)
    print(f"   Productos unicos validos: {len(productos)}", flush=True)
    return productos


# --------------------------
# WOO: CATEGORÍAS / IMÁGENES
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
    nombre_hijo = limpiar_nombre_para_categoria(nombre_completo)

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


def _norm_image_ref(u: str) -> str:
    try:
        p = urllib.parse.urlsplit(u or "")
        return f"{p.netloc}{p.path}".lower().rstrip("/")
    except Exception:
        return (u or "").lower().rstrip("/")


def _looks_like_brand_image(u: str) -> bool:
    low = (u or "").lower()
    return any(x in low for x in ["logo", "marca", "samsung_logo", "brand"])


def seleccionar_imagen_subcategoria(cache_categorias, id_padre, id_hijo):
    """
    SOLO imagen de subcategoria exacta.
    Si la subcategoria no tiene imagen valida o coincide con la del padre, devuelve vacio.
    """
    if not id_hijo:
        return ""

    img_hijo = obtener_imagen_categoria(cache_categorias, id_hijo)
    if not img_hijo:
        return ""

    img_padre = obtener_imagen_categoria(cache_categorias, id_padre) if id_padre else ""
    if img_padre and _norm_image_ref(img_hijo) == _norm_image_ref(img_padre):
        return ""

    if _looks_like_brand_image(img_hijo):
        return ""

    return img_hijo


# --------------------------
# WOO / SYNC
# --------------------------

def cargar_locales_samsung():
    locales = []
    page = 1
    while True:
        try:
            res = wcapi.get("products", params={"per_page": 100, "page": page, "status": "any"}).json()
            if not res or "message" in res:
                break
            for p in res:
                meta = {m["key"]: str(m.get("value", "")) for m in p.get("meta_data", [])}
                if str(meta.get("importado_de", "")).strip().rstrip("/") == ID_IMPORTACION:
                    locales.append({"id": p["id"], "nombre": p.get("name", ""), "meta": meta})
            if len(res) < 100:
                break
            page += 1
        except Exception:
            break
    return locales


def build_local_key(local):
    meta = local.get("meta", {})
    hidden = str(meta.get("_odm_source_key", "")).strip()
    if hidden:
        return hidden
    return source_key(
        local.get("nombre", ""),
        meta.get("memoria", ""),
        meta.get("capacidad", ""),
        meta.get("fuente", FUENTE),
    )


def _num_meta(meta: dict, key: str) -> int:
    try:
        return int(round(float(str(meta.get(key, 0) or 0).replace(",", "."))))
    except Exception:
        return 0


def sincronizar(remotos):
    print("\n--- FASE 2: SINCRONIZANDO SAMSUNG ---", flush=True)
    cache_categorias = obtener_todas_las_categorias()
    locales = cargar_locales_samsung()

    print(f"📦 Productos Samsung existentes en la web: {len(locales)}", flush=True)
    print(f"📦 Productos remotos Samsung a procesar: {len(remotos)}", flush=True)

    remote_by_key = {r["source_key"]: r for r in remotos}
    local_by_key = {build_local_key(l): l for l in locales}

    # 1) Obsoletos
    for key, local in local_by_key.items():
        if key in remote_by_key:
            continue
        try:
            wcapi.delete(f"products/{local['id']}", params={"force": True})
            summary_eliminados.append({"nombre": local["nombre"], "id": local["id"]})
            print(f"🗑️ ELIMINADO (obsoleto) -> {local['nombre']} (ID: {local['id']})", flush=True)
        except Exception as e:
            print(f"❌ Error eliminando obsoleto {local['nombre']}: {e}", flush=True)
            summary_fallidos.append({"nombre": local["nombre"], "id": local["id"], "error": str(e)})

    # 2) Crear / actualizar
    for r in remotos:
        try:
            print("-" * 60, flush=True)
            print(f"Detectado {r.get('nombre', '(sin nombre)')}", flush=True)
            print(f"1) Nombre: {r.get('nombre', '')}", flush=True)
            print(f"2) Memoria: {r.get('memoria', '')}", flush=True)
            print(f"3) Capacidad: {r.get('capacidad', '')}", flush=True)
            print(f"4) Version: {r.get('version', VERSION)}", flush=True)
            print(f"5) Fuente: {r.get('fuente', FUENTE)}", flush=True)
            print(f"6) Precio actual: {r.get('precio_actual', 0)}", flush=True)
            print(f"7) Precio original: {r.get('precio_original', 0)}", flush=True)
            print(f"8) Codigo de descuento: {r.get('codigo_descuento', CODIGO_DESCUENTO_DEFAULT)}", flush=True)

            url_importada_raw = (r.get("url_importada_sin_afiliado") or "").strip()
            if url_importada_raw:
                url_base = sanitize_samsung_buy_url(url_importada_raw) if "modelCode=" in url_importada_raw else url_importada_raw.rstrip("/")
            else:
                url_base = normalize_product_url(r.get("url_imp") or "").rstrip("/")
                if (r.get("model_code") or "").strip():
                    url_base = build_samsung_buy_url(
                        detail_url=r.get("url_imp") or "",
                        buy_url_hint=r.get("url_oferta_sin_acortar") or r.get("buy_url") or "",
                        model_code=r.get("model_code") or "",
                    ).rstrip("/")

            url_con_afiliado = unir_afiliado(url_base, AFF_RAW) if AFF_RAW else (url_base.rstrip("/") + "/")
            url_oferta = acortar_url(url_con_afiliado)

            match = local_by_key.get(r["source_key"])
            id_padre, id_hijo = resolver_jerarquia(r["nombre"], cache_categorias)
            img_final_producto = seleccionar_imagen_subcategoria(cache_categorias, id_padre, id_hijo)

            print(f"9) URL Imagen: {img_final_producto}", flush=True)
            print(f"10) Enlace Importado: {mask_url(r.get('url_imp', ''))}", flush=True)
            print(f"11) Enlace Expandido: {mask_url(r.get('url_oferta_sin_acortar', ''))}", flush=True)
            print(f"12) URL importada sin afiliado: {mask_url(r.get('url_importada_sin_afiliado', ''))}", flush=True)
            print(f"13) URL sin acortar con mi afiliado: {mask_url(url_con_afiliado)}", flush=True)
            print(f"14) URL acortada con mi afiliado: {url_oferta}", flush=True)
            print(f"15) Enviado desde: {r.get('enviado_desde', ENVIADO_DESDE)}", flush=True)
            print(f"15) Importado de: {ID_IMPORTACION}", flush=True)
            print(f"16) Ruta búsqueda origen: canal=jsonld_listing | listado={mask_url(r.get('origen_listado', START_URL))} | pagina={r.get('origen_pagina', '1')}", flush=True)
            print("17) Encolado para comparar con base de datos...", flush=True)
            print("-" * 60, flush=True)

            if match:
                meta = match["meta"]
                cambios = []
                payload = {"meta_data": []}

                price_cur_old = _num_meta(meta, "precio_actual")
                price_org_old = _num_meta(meta, "precio_original")
                price_cur_new = int(r.get("precio_actual") or 0)
                price_org_new = int(r.get("precio_original") or 0)

                if price_cur_new != price_cur_old:
                    cambios.append(f"precio_actual: {price_cur_old} -> {price_cur_new}")
                    payload["sale_price"] = str(price_cur_new)
                    payload["meta_data"].append({"key": "precio_actual", "value": str(price_cur_new)})

                if price_org_new != price_org_old:
                    cambios.append(f"precio_original: {price_org_old} -> {price_org_new}")
                    payload["regular_price"] = str(price_org_new)
                    payload["meta_data"].append({"key": "precio_original", "value": str(price_org_new)})

                compare_meta = {
                    "codigo_de_descuento": r.get("codigo_descuento", CODIGO_DESCUENTO_DEFAULT),
                    "enviado_desde": r.get("enviado_desde", ENVIADO_DESDE),
                    "enviado_desde_tg": r.get("enviado_desde_tg", ENVIADO_DESDE_TG),
                    "version": r.get("version", VERSION),
                    "url_sin_acortar_con_mi_afiliado": url_con_afiliado,
                    "url_oferta": url_oferta,
                    "url_importada_sin_afiliado": url_base,
                    "url_oferta_sin_acortar": r.get("url_oferta_sin_acortar", url_base),
                    "_odm_source_model_code": r.get("model_code", ""),
                }
                for k, v in compare_meta.items():
                    if str(meta.get(k, "")) != str(v):
                        cambios.append(f"{k}: {meta.get(k, '')} -> {v}")
                        payload["meta_data"].append({"key": k, "value": v})

                current_img_meta = str(meta.get("imagen_producto", "") or "")
                if current_img_meta != str(img_final_producto):
                    payload["meta_data"].append({"key": "imagen_producto", "value": img_final_producto})
                    payload["images"] = ([{"src": img_final_producto}] if img_final_producto else [])

                if cambios:
                    wcapi.put(f"products/{match['id']}", payload)
                    summary_actualizados.append({"nombre": r["nombre"], "id": match["id"], "cambios": cambios})
                    print(f"🔄 ACTUALIZADO -> {r['nombre']} (ID: {match['id']})", flush=True)
                else:
                    summary_ignorados.append({"nombre": r["nombre"], "id": match["id"]})
                continue

            data = {
                "name": r["nombre"],
                "type": "simple",
                "status": "publish",
                "regular_price": str(r["precio_original"]),
                "sale_price": str(r["precio_actual"]),
                "categories": [{"id": id_padre}, {"id": id_hijo}] if id_hijo else ([{"id": id_padre}] if id_padre else []),
                "images": ([{"src": img_final_producto}] if img_final_producto else []),
                "meta_data": [
                    {"key": "importado_de", "value": ID_IMPORTACION},
                    {"key": "fecha", "value": r["fecha"]},
                    {"key": "memoria", "value": r["memoria"]},
                    {"key": "capacidad", "value": r["capacidad"]},
                    {"key": "fuente", "value": FUENTE},
                    {"key": "precio_actual", "value": str(r["precio_actual"])},
                    {"key": "precio_original", "value": str(r["precio_original"])},
                    {"key": "codigo_de_descuento", "value": r.get("codigo_descuento", CODIGO_DESCUENTO_DEFAULT)},
                    {"key": "enviado_desde", "value": r.get("enviado_desde", ENVIADO_DESDE)},
                    {"key": "enviado_desde_tg", "value": r.get("enviado_desde_tg", ENVIADO_DESDE_TG)},
                    {"key": "enlace_de_compra_importado", "value": url_base},
                    {"key": "url_importada_sin_afiliado", "value": url_base},
                    {"key": "url_oferta_sin_acortar", "value": r.get("url_oferta_sin_acortar", url_base)},
                    {"key": "url_sin_acortar_con_mi_afiliado", "value": url_con_afiliado},
                    {"key": "url_oferta", "value": url_oferta},
                    {"key": "imagen_producto", "value": img_final_producto},
                    {"key": "version", "value": r.get("version", VERSION)},
                    {"key": "_odm_source_key", "value": r["source_key"]},
                    {"key": "_odm_source_model_code", "value": r.get("model_code", "")},
                ],
            }

            intentos = 0
            max_intentos = 10
            creado = False

            while intentos < max_intentos and not creado:
                intentos += 1
                try:
                    res = wcapi.post("products", data)
                    if res.status_code in (200, 201):
                        prod = res.json()
                        creado = True
                        summary_creados.append({"nombre": r["nombre"], "id": prod.get("id")})
                        print(f"CREADO -> {r['nombre']} (ID: {prod.get('id')})", flush=True)

                        try:
                            url_short = acortar_url(prod.get("permalink", ""))
                            if url_short:
                                wcapi.put(
                                    f"products/{prod.get('id')}",
                                    {"meta_data": [{"key": "url_post_acortada", "value": url_short}]},
                                )
                        except Exception:
                            pass
                    else:
                        body_preview = (res.text or "").replace("\n", " ")[:250]
                        print(f"Woo error {res.status_code}: {body_preview}", flush=True)
                except Exception as e:
                    print(f"Excepcion Woo: {e}", flush=True)

                if (not creado) and (intentos < max_intentos):
                    time.sleep(15)

            if not creado:
                summary_fallidos.append(r.get("nombre", "desconocido"))
                print(f"NO SE PUDO CREAR: {r.get('nombre', '?')}", flush=True)

        except Exception as e:
            summary_fallidos.append(r.get("nombre", "desconocido"))
            print(f"ERROR en {r.get('nombre', '?')}: {e}", flush=True)

    hoy_fmt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("============================================================", flush=True)
    print(f"RESUMEN DE EJECUCION ({hoy_fmt})", flush=True)
    print("============================================================", flush=True)
    print(f"a) ARTICULOS CREADOS: {len(summary_creados)}", flush=True)
    for item in summary_creados:
        print(f"- {item.get('nombre', '?')} (ID: {item.get('id', '?')})", flush=True)
    print(f"b) ARTICULOS ELIMINADOS (OBSOLETOS): {len(summary_eliminados)}", flush=True)
    for item in summary_eliminados:
        print(f"- {item.get('nombre', '?')} (ID: {item.get('id', '?')})", flush=True)
    print(f"c) ARTICULOS ACTUALIZADOS: {len(summary_actualizados)}", flush=True)
    for item in summary_actualizados:
        print(f"- {item.get('nombre', '?')} (ID: {item.get('id', '?')}): {', '.join(item.get('cambios', []))}", flush=True)
    print(f"d) ARTICULOS IGNORADOS (SIN CAMBIOS): {len(summary_ignorados)}", flush=True)
    for item in summary_ignorados:
        print(f"- {item.get('nombre', '?')} (ID: {item.get('id', '?')})", flush=True)
    print(f"e) DUPLICADOS DETECTADOS: {len(summary_duplicados)}", flush=True)
    for item in summary_duplicados:
        print(f"- {item}", flush=True)
    print(f"f) FALLIDOS: {len(summary_fallidos)}", flush=True)
    print("============================================================", flush=True)


def main():
    remotos = obtener_datos_remotos()
    if remotos:
        sincronizar(remotos)
    else:
        print("No se han obtenido productos remotos de Samsung.", flush=True)


if __name__ == "__main__":
    main()
