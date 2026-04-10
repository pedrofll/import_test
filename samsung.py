import os,re,time,math,json,urllib.parse
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from woocommerce import API

START_URL=(os.getenv('SOURCE_URL_SAMSUNG') or 'https://www.samsung.com/es/smartphones/all-smartphones/').strip()
FUENTE='Samsung'
ID_IMPORTACION=START_URL.rstrip('/')
ENVIADO_DESDE='España'
ENVIADO_DESDE_TG='🇪🇸 España'
VERSION='Versión Global'
CODIGO='OFERTA: PROMO.'
AFF_SAMSUNG=(os.getenv('AFF_SAMSUNG') or '').strip()
HEADERS={'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36','Accept-Language':'es-ES,es;q=0.9'}
wcapi=API(url=os.environ['WP_URL'],consumer_key=os.environ['WP_KEY'],consumer_secret=os.environ['WP_SECRET'],version='wc/v3',timeout=60)
summary_creados=[];summary_eliminados=[];summary_actualizados=[];summary_ignorados=[];summary_fallidos=[];summary_duplicados=[]
EURO_RE=r'(\d{1,3}(?:\.\d{3})*(?:,\d{1,2})?|\d{1,5}(?:[\.,]\d{1,2})?)\s*€'
CAP_EXACT=re.compile(r'^(?:64|128|256|512|1024)\s*GB$|^(?:1|2)\s*TB$',re.I)
CAP_ANY=re.compile(r'\b(?:64|128|256|512|1024)\s*GB\b|\b(?:1|2)\s*TB\b',re.I)
BUY_URL_RE=re.compile(r'(https?://[^\s\"\'<>]+/es/smartphones/[^\s\"\'<>]+/buy/\?[^\s\"\'<>]+|/es/smartphones/[^\s\"\'<>]+/buy/\?[^\s\"\'<>]+)',re.I)
BASE_URL_RE=re.compile(r'(https?://[^\s\"\'<>]+/es/smartphones/[^\s\"\'<>]+|/es/smartphones/[^\s\"\'<>]+)',re.I)
RAM_MAP={
('SAMSUNG GALAXY S26','256GB'):'12GB',('SAMSUNG GALAXY S26','512GB'):'12GB',('SAMSUNG GALAXY S26+','256GB'):'12GB',('SAMSUNG GALAXY S26+','512GB'):'12GB',('SAMSUNG GALAXY S26 ULTRA','256GB'):'12GB',('SAMSUNG GALAXY S26 ULTRA','512GB'):'12GB',('SAMSUNG GALAXY S26 ULTRA','1TB'):'16GB',('SAMSUNG GALAXY Z FOLD7','256GB'):'12GB',('SAMSUNG GALAXY Z FOLD7','512GB'):'12GB',('SAMSUNG GALAXY Z FOLD7','1TB'):'16GB',('SAMSUNG GALAXY Z FLIP7','256GB'):'12GB',('SAMSUNG GALAXY Z FLIP7','512GB'):'12GB',('SAMSUNG GALAXY Z FLIP7 FE','128GB'):'8GB',('SAMSUNG GALAXY Z FLIP7 FE','256GB'):'8GB',('SAMSUNG GALAXY S25','128GB'):'12GB',('SAMSUNG GALAXY S25','256GB'):'12GB',('SAMSUNG GALAXY S25','512GB'):'12GB',('SAMSUNG GALAXY S25+','256GB'):'12GB',('SAMSUNG GALAXY S25+','512GB'):'12GB',('SAMSUNG GALAXY S25 ULTRA','256GB'):'12GB',('SAMSUNG GALAXY S25 ULTRA','512GB'):'12GB',('SAMSUNG GALAXY S25 ULTRA','1TB'):'12GB',('SAMSUNG GALAXY S25 FE','128GB'):'8GB',('SAMSUNG GALAXY S25 FE','256GB'):'8GB',('SAMSUNG GALAXY S25 FE','512GB'):'8GB',('SAMSUNG GALAXY S25 EDGE','256GB'):'12GB',('SAMSUNG GALAXY S25 EDGE','512GB'):'12GB',('SAMSUNG GALAXY A57 5G','128GB'):'8GB',('SAMSUNG GALAXY A57 5G','256GB'):'8GB',('SAMSUNG GALAXY A57 5G','512GB'):'12GB',('SAMSUNG GALAXY A56 5G','128GB'):'8GB',('SAMSUNG GALAXY A56 5G','256GB'):'8GB',('SAMSUNG GALAXY A37 5G','128GB'):'8GB',('SAMSUNG GALAXY A37 5G','256GB'):'8GB',('SAMSUNG GALAXY A36 5G','128GB'):'8GB',('SAMSUNG GALAXY A36 5G','256GB'):'8GB',('SAMSUNG GALAXY A26 5G','128GB'):'8GB',('SAMSUNG GALAXY A26 5G','256GB'):'8GB',('SAMSUNG GALAXY A17 5G','128GB'):'8GB',('SAMSUNG GALAXY A17 5G','256GB'):'8GB',('SAMSUNG GALAXY A17','128GB'):'8GB',('SAMSUNG GALAXY A17','256GB'):'8GB',('SAMSUNG GALAXY A16','128GB'):'8GB',('SAMSUNG GALAXY A16','256GB'):'8GB',('SAMSUNG GALAXY S24','128GB'):'8GB',('SAMSUNG GALAXY S24','256GB'):'8GB',('SAMSUNG GALAXY S24+','256GB'):'12GB',('SAMSUNG GALAXY S24+','512GB'):'12GB',('SAMSUNG GALAXY S24 FE','128GB'):'8GB',('SAMSUNG GALAXY S24 FE','256GB'):'8GB',('SAMSUNG GALAXY Z FLIP6','256GB'):'12GB',('SAMSUNG GALAXY Z FLIP6','512GB'):'12GB',('SAMSUNG GALAXY Z FOLD6','256GB'):'12GB',('SAMSUNG GALAXY Z FOLD6','512GB'):'12GB',('SAMSUNG GALAXY Z FOLD6','1TB'):'16GB'}

def ns(s): return re.sub(r'\s+',' ',(s or '')).strip()
def mask(u):
    try:
        p=urllib.parse.urlsplit(u);base=f'{p.scheme}://{p.netloc}{p.path}';return base+('?***' if p.query else '')
    except: return '***'
def parse_num(t):
    if not t:return 0
    t=str(t).strip().replace(' ','').replace('.','').replace(',','.')
    try:return int(round(float(t)))
    except:return 0
def parse_all(t): return [parse_num(m.group(1)) for m in re.finditer(EURO_RE,t or '',re.I) if parse_num(m.group(1))>0]
def pvp(v,f=1.2):
    try:v=int(v)
    except:return 0
    return int(math.ceil(v*f)) if v>0 else 0
def shorten(u):
    try:
        if not u:return ''
        q=urllib.parse.quote(u,safe='')
        r=requests.get(f'https://is.gd/create.php?format=simple&url={q}',headers=HEADERS,timeout=10)
        return r.text.strip() if r.status_code==200 else u
    except:return u
def norm_name(name):
    t=ns(re.sub(r'\bExclusivo Online\b','',name or '',flags=re.I))
    if not t:return ''
    if t.lower().startswith('samsung '): t=t[8:]
    out=[]
    for w in t.split():
        if re.search(r'\d',w) and re.search(r'[A-Za-z]',w): w=''.join(ch.upper() if ch.isalpha() else ch for ch in w)
        elif w.lower() in {'gb','tb'}: w=w.upper()
        else: w=w[:1].upper()+w[1:].lower()
        out.append(w)
    base=' '.join(out)
    return ns(f'Samsung {base}')
def cap_from(txt):
    m=re.search(r'\b(64|128|256|512|1024)\s*GB\b',txt or '',re.I)
    if m:return f'{m.group(1)}GB'
    m=re.search(r'\b(1|2)\s*TB\b',txt or '',re.I)
    return f'{m.group(1)}TB' if m else ''
def mem_for(name,cap): return RAM_MAP.get((ns(name).upper(),(cap or '').upper()),'')
def src_key(name,mem,cap,fuente=FUENTE): return f'{ns(name).lower()}|{str(mem).upper()}|{str(cap).upper()}|{fuente.lower()}'
def base_url(url):
    if not url:return ''
    try:
        p=urllib.parse.urlsplit(url);path=(p.path or '').rstrip('/');path=re.sub(r'/buy$','',path,flags=re.I);return f'{p.scheme}://{p.netloc}{path}'
    except:return (url or '').split('?')[0].rstrip('/')
def aff_url(base,aff):
    base=(base or '').strip();aff=(aff or '').strip().lstrip('?&')
    if not base:return ''
    if not aff:return base
    return base.rstrip('/')+'/?'+aff
def clean_img(u):
    try:
        p=urllib.parse.urlsplit(u or '');return f'{p.netloc}{p.path}'.lower().rstrip('/')
    except:return (u or '').lower().split('?')[0].rstrip('/')
def same_img(a,b):
    a=clean_img(a);b=clean_img(b)
    return bool(a and b and (a==b or a.split('/')[-1]==b.split('/')[-1]))
def should_skip(name):
    u=(name or '').upper();return any(x in u for x in [' TAB','IPAD',' PAD']) or u.startswith('TAB ')

def get_driver():
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    o=Options();o.add_argument('--headless=new');o.add_argument('--no-sandbox');o.add_argument('--disable-dev-shm-usage');o.add_argument('--disable-gpu');o.add_argument('--window-size=1440,3200');o.add_argument('--lang=es-ES');o.add_argument('--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36')
    return webdriver.Chrome(options=o)

def dismiss(driver):
    from selenium.webdriver.common.by import By
    xps=["//button[contains(., 'Aceptar')]","//button[contains(., 'Acepto')]","//button[contains(., 'Aceptar todo')]","//button[contains(., 'MAS TARDE')]","//button[contains(., 'MÁS TARDE')]","//a[contains(., 'IR A SAMSUNG.COM')]"]
    for _ in range(3):
        for xp in xps:
            try:
                for el in driver.find_elements(By.XPATH,xp)[:4]:
                    if el.is_displayed():
                        try: driver.execute_script('arguments[0].click();',el); time.sleep(0.4)
                        except: pass
            except: pass

def scroll(driver,rounds=22):
    last=0;stable=0
    for _ in range(rounds):
        try:
            driver.execute_script('window.scrollTo(0, document.body.scrollHeight);');time.sleep(0.9);h=driver.execute_script('return document.body.scrollHeight')
            stable=stable+1 if h==last else 0; last=h
            if stable>=3: break
        except: break

def lines(root):
    try: raw=root.text or ''
    except: raw=''
    return [ns(x) for x in raw.splitlines() if ns(x)]

def json_items(html):
    soup=BeautifulSoup(html,'html.parser');out=[]
    def walk(x):
        if isinstance(x,dict):
            yield x
            for v in x.values():
                for y in walk(v): yield y
        elif isinstance(x,list):
            for v in x:
                for y in walk(v): yield y
    for sc in soup.find_all('script',attrs={'type':re.compile(r'ld\+json',re.I)}):
        raw=(sc.string or sc.get_text() or '').strip()
        if not raw: continue
        try:data=json.loads(raw)
        except: continue
        for node in walk(data):
            if not isinstance(node,dict) or str(node.get('@type') or '')!='Product': continue
            raw_name=ns(node.get('name') or '');clean=norm_name(raw_name);offers=node.get('offers') or {};price=parse_num(str(offers.get('price') or '')) if isinstance(offers,dict) else 0;url=node.get('url') or ''
            if clean: out.append({'raw_name':raw_name,'clean_name':clean,'price':price,'buy_url':url,'base_url':base_url(url)})
    seen={};
    for it in out: seen[(it['raw_name'],it['price'],it['buy_url'])]=it
    return list(seen.values())

def item_key(name): return re.sub(r'[^a-z0-9+]','',ns(name).lower())

def collect_roots(driver):
    from selenium.webdriver.common.by import By
    dismiss(driver);scroll(driver);dismiss(driver);html=driver.page_source;roots=[];seen=set()
    try: buys=driver.find_elements(By.XPATH,"//*[self::a or self::button][contains(normalize-space(.), 'Comprar')]")
    except: buys=[]
    for btn in buys:
        try:
            if not btn.is_displayed(): continue
        except: continue
        cur=btn;chosen=None
        for _ in range(12):
            try: cur=cur.find_element(By.XPATH,'./..')
            except: break
            txt=ns(getattr(cur,'text','') or '')
            if not txt or 'Galaxy' not in txt or '€' not in txt or not CAP_ANY.search(txt): continue
            try: bc=len(cur.find_elements(By.XPATH,".//*[self::a or self::button][contains(normalize-space(.), 'Comprar') ]"))
            except: bc=1
            if bc<=2 and len(txt)<1400: chosen=cur; break
        if not chosen: continue
        try: sig=chosen.get_attribute('outerHTML') or chosen.text
        except: sig=txt
        h=str(hash(sig[:5000]))
        if h in seen: continue
        seen.add(h); roots.append(chosen)
    return html,roots

def card_name(root_lines):
    for ln in root_lines:
        if 'Galaxy' in ln and '€' not in ln and 'Comprar' not in ln and 'Más información' not in ln and 'Mas informacion' not in ln and 'Ficha de información' not in ln and 'Ficha de informacion' not in ln:
            return ln
    return ''

def cap_elems(root):
    els=[];seen=set()
    try:nodes=root.find_elements('xpath',".//*[self::button or self::a or self::span or self::div or self::label][string-length(normalize-space(.)) <= 10]")
    except: nodes=[]
    for el in nodes:
        try:
            if not el.is_displayed(): continue
            txt=ns(el.text or '')
        except: continue
        if not CAP_EXACT.match(txt): continue
        cap=cap_from(txt)
        if not cap or cap in seen: continue
        seen.add(cap); els.append((cap,el))
    return els

def is_selected(el):
    vals=[]
    for a in ['class','aria-selected','aria-pressed','aria-checked','data-selected','data-state']:
        try: vals.append(str(el.get_attribute(a) or ''))
        except: vals.append('')
    joined=' '.join(vals).lower()
    return any(x in joined for x in ['selected','active','checked','true','current'])

def click_cap(driver,el):
    if el is None: return
    try: driver.execute_script("arguments[0].scrollIntoView({block:'center'});",el); time.sleep(0.1)
    except: pass
    try: el.click()
    except:
        try: driver.execute_script('arguments[0].click();',el)
        except: pass
    time.sleep(0.5)

def price_from_lines(ls):
    bad=['ahorra','antes','paypal','dto','%','/mes','extra en carrito']
    current=0
    for ln in ls:
        low=ln.lower()
        if '€' not in ln or any(b in low for b in bad): continue
        vals=[v for v in parse_all(ln) if v>0]
        if vals:
            big=[v for v in vals if v>=100]
            current=big[0] if big else vals[0]
            break
    if current==0:
        vals=[]
        for ln in ls:
            low=ln.lower()
            if '€' not in ln or any(b in low for b in ['ahorra','antes','paypal','%']): continue
            vals.extend([v for v in parse_all(ln) if v>=100])
        if vals: current=vals[0]
    bigger=[]
    for ln in ls:
        for v in parse_all(ln):
            if current and v>current: bigger.append(v)
    original=max(bigger) if bigger else (pvp(current) if current else 0)
    return int(current or 0),int(original or 0)

def urls_from_root(root,page_url,clean_name,current_price,items):
    buy=''; info=''
    def attrs(node):
        out=[]
        for a in ['href','data-href','data-url','data-link','onclick','data-buy-url','data-target-url']:
            try:v=node.get_attribute(a)
            except:v=''
            if v: out.append(v)
        return out
    try: buy_nodes=root.find_elements('xpath',".//*[self::a or self::button][contains(normalize-space(.), 'Comprar')]")
    except: buy_nodes=[]
    for n in buy_nodes:
        for v in attrs(n):
            m=BUY_URL_RE.search(v)
            if m: buy=urllib.parse.urljoin(page_url,m.group(1)); break
        if buy: break
    try: info_nodes=root.find_elements('xpath',".//*[self::a or self::button][contains(normalize-space(.), 'Más información') or contains(normalize-space(.), 'Mas informacion') or contains(normalize-space(.), 'Ficha de información') or contains(normalize-space(.), 'Ficha de informacion')]")
    except: info_nodes=[]
    for n in info_nodes:
        for v in attrs(n):
            if v.lower().startswith('javascript'): continue
            if '/es/smartphones/' in v and '/buy/' not in v: info=urllib.parse.urljoin(page_url,v); break
        if info: break
    try: outer=root.get_attribute('outerHTML') or ''
    except: outer=''
    if not buy:
        m=BUY_URL_RE.search(outer)
        if m: buy=urllib.parse.urljoin(page_url,m.group(1))
    if not info:
        for m in BASE_URL_RE.findall(outer):
            u=urllib.parse.urljoin(page_url,m)
            if '/buy/' in u: continue
            info=u; break
    if not info and buy: info=base_url(buy)
    if not info:
        cands=[x for x in items if item_key(x.get('clean_name') or x.get('raw_name') or '')==item_key(clean_name)]
        if cands:
            if current_price: cands=sorted(cands,key=lambda x: abs(int(x.get('price') or 0)-int(current_price)))
            info=cands[0].get('base_url') or ''
            if not buy: buy=cands[0].get('buy_url') or ''
    return buy,base_url(info)

def obtener_todas_las_categorias():
    out=[];page=1
    while True:
        try: res=wcapi.get('products/categories',params={'per_page':100,'page':page}).json()
        except: break
        if not res or 'message' in res or len(res)==0: break
        out.extend(res); page+=1
    return out

def resolver_jerarquia(nombre,cache):
    parts=(nombre or '').split(); padre=parts[0] if parts else 'Otros'; hijo=nombre; idp=None; idh=None
    for c in cache:
        if c.get('name','').lower()==padre.lower() and c.get('parent')==0: idp=c.get('id'); break
    if not idp:
        r=wcapi.post('products/categories',{'name':padre}).json(); idp=r.get('id'); cache.append(r)
    for c in cache:
        if c.get('name','').lower()==hijo.lower() and c.get('parent')==idp: idh=c.get('id'); break
    if not idh:
        r=wcapi.post('products/categories',{'name':hijo,'parent':idp}).json(); idh=r.get('id'); cache.append(r)
    return idp,idh

def img_cat(cache,cat_id):
    if not cat_id:return ''
    for c in cache:
        if c.get('id')==cat_id:
            img=c.get('image') or {}
            return img.get('src') or ''
    return ''

def cargar_locales():
    out=[];page=1
    while True:
        try: res=wcapi.get('products',params={'per_page':100,'page':page,'status':'any'}).json()
        except: break
        if not res or 'message' in res: break
        for p in res:
            meta={m.get('key'):str(m.get('value','')) for m in p.get('meta_data',[]) if isinstance(m,dict)}
            if meta.get('importado_de','').rstrip('/')==ID_IMPORTACION.rstrip('/'): out.append({'id':p.get('id'),'nombre':p.get('name',''),'meta':meta})
        if len(res)<100: break
        page+=1
    return out

def meta_key(nombre,meta): return src_key(nombre,meta.get('memoria',''),meta.get('capacidad',''),meta.get('fuente',FUENTE))

def obtener_datos_remotos():
    print('--- FASE 1: ESCANEANDO SAMSUNG ---',flush=True)
    print(f'URL base: {mask(START_URL)}',flush=True)
    print(f'Samsung listing-only: leyendo solo la pagina principal {mask(START_URL)}',flush=True)
    driver=None
    try:
        driver=get_driver(); driver.get(START_URL); time.sleep(2); html,roots=collect_roots(driver)
    except Exception as e:
        print(f'Error renderizando listing Samsung: {e}',flush=True)
        try:
            if driver: driver.quit()
        except: pass
        return []
    items=json_items(html)
    print(f'Items JSON-LD Samsung detectados: {len(items)}',flush=True)
    print(f'Cards Samsung detectadas en listing: {len(roots)}',flush=True)
    productos={}; hoy=datetime.now().strftime('%d/%m/%Y')
    try:
        for root in roots:
            ls=lines(root); raw=card_name(ls)
            if not raw: continue
            name=norm_name(raw)
            if should_skip(name): continue
            opts=cap_elems(root)
            if not opts:
                cap=cap_from(' '.join(ls)); opts=[(cap,None)] if cap else []
            if not opts: continue
            defaults=[x for x in opts if x[1] is not None and is_selected(x[1])]
            ordered=defaults+[x for x in opts if x not in defaults]
            if not ordered: ordered=opts
            seen_sig=set()
            for cap,el in ordered:
                click_cap(driver,el)
                cur_ls=lines(root)
                price,orig=price_from_lines(cur_ls)
                if price<=0:
                    print(f'Card Samsung sin precio usable para {name} {cap}. Se ignora.',flush=True); continue
                mem=mem_for(name,cap)
                if not mem:
                    print(f'Card Samsung sin RAM resoluble para {name} {cap}. Se ignora.',flush=True); continue
                buy,base=urls_from_root(root,START_URL,name,price,items)
                if not base and buy: base=base_url(buy)
                aff=aff_url(base,AFF_SAMSUNG) if base else ''
                short=shorten(aff) if aff else ''
                key=src_key(name,mem,cap,FUENTE)
                sig=(price,orig,buy or base)
                if sig in seen_sig and key not in productos:
                    summary_duplicados.append(f'{name} {cap} {mem}'); continue
                seen_sig.add(sig)
                if key in productos:
                    summary_duplicados.append(f'{name} {cap} {mem}'); continue
                productos[key]={'nombre':name,'memoria':mem,'capacidad':cap,'precio_actual':int(price),'precio_original':int(orig),'fecha':hoy,'fuente':FUENTE,'version':VERSION,'codigo_descuento':CODIGO,'enviado_desde':ENVIADO_DESDE,'enviado_desde_tg':ENVIADO_DESDE_TG,'enlace_de_compra_importado':base,'url_oferta_sin_acortar':buy,'url_importada_sin_afiliado':base,'url_sin_acortar_con_mi_afiliado':aff,'url_oferta':short,'importado_de':ID_IMPORTACION,'source_key':key}
    finally:
        try: driver.quit()
        except: pass
    rem=list(productos.values())
    print('RESUMEN EXTRACCION SAMSUNG:',flush=True)
    print('   URLs descubiertas: 1 (listing principal)',flush=True)
    print(f'   Productos unicos validos: {len(rem)}',flush=True)
    return rem

def sincronizar(remotos):
    print('--- FASE 2: SINCRONIZANDO SAMSUNG ---',flush=True)
    cache=obtener_todas_las_categorias(); locales=cargar_locales(); print(f'Productos Samsung existentes en la web: {len(locales)}',flush=True); print(f'Productos remotos Samsung a procesar: {len(remotos)}',flush=True)
    rem={r['source_key']:r for r in remotos}; loc={meta_key(l['nombre'],l['meta']):l for l in locales}
    for k,l in loc.items():
        r=rem.get(k)
        if not r:
            try: wcapi.delete(f"products/{l['id']}",params={'force':True}); summary_eliminados.append({'nombre':l['nombre'],'id':l['id']}); print(f'ELIMINADO -> {l["nombre"]} (ID: {l["id"]})',flush=True)
            except Exception as e: summary_fallidos.append({'nombre':l['nombre'],'error':str(e)})
            continue
        meta=l['meta']; cambios=[]; payload={'meta_data':[]}
        def add(k,v): payload['meta_data'].append({'key':k,'value':v})
        try:
            if int(float(meta.get('precio_actual',0) or 0))!=int(r['precio_actual']): cambios.append('precio_actual'); payload['sale_price']=str(r['precio_actual']); add('precio_actual',str(r['precio_actual']))
        except: pass
        try:
            if int(float(meta.get('precio_original',0) or 0))!=int(r['precio_original']): cambios.append('precio_original'); payload['regular_price']=str(r['precio_original']); add('precio_original',str(r['precio_original']))
        except: pass
        for mk in ['enlace_de_compra_importado','url_oferta_sin_acortar','url_importada_sin_afiliado','url_sin_acortar_con_mi_afiliado','url_oferta']:
            if str(meta.get(mk,''))!=str(r.get(mk,'')): cambios.append(mk); add(mk,r.get(mk,''))
        if cambios:
            try: wcapi.put(f"products/{l['id']}",payload); summary_actualizados.append({'nombre':l['nombre'],'id':l['id'],'cambios':cambios}); print(f'ACTUALIZADO -> {l["nombre"]} (ID: {l["id"]})',flush=True)
            except Exception as e: summary_fallidos.append({'nombre':l['nombre'],'error':str(e)})
        else: summary_ignorados.append({'nombre':l['nombre'],'id':l['id']})
    for k,r in rem.items():
        if k in loc: continue
        try:
            idp,idh=resolver_jerarquia(r['nombre'],cache); imgp=img_cat(cache,idp); imgh=img_cat(cache,idh); imgs=imgh if (imgh and not same_img(imgh,imgp)) else ''
            print('------------------------------------------------------------',flush=True)
            print(f'Detectado {r["nombre"]}',flush=True)
            print(f'1) Nombre: {r["nombre"]}',flush=True)
            print(f'2) Memoria: {r["memoria"]}',flush=True)
            print(f'3) Capacidad: {r["capacidad"]}',flush=True)
            print(f'4) Versión: {r["version"]}',flush=True)
            print(f'5) Fuente: {r["fuente"]}',flush=True)
            print(f'6) Precio actual: {r["precio_actual"]}',flush=True)
            print(f'7) Precio original: {r["precio_original"]}',flush=True)
            print(f'8) Código de descuento: {r["codigo_descuento"]}',flush=True)
            print(f'9) URL Imagen: {mask(imgs) if imgs else ""}',flush=True)
            print(f'10) Enlace Importado: {r["enlace_de_compra_importado"]}',flush=True)
            print(f'11) Enlace Expandido: {r["url_oferta_sin_acortar"]}',flush=True)
            print(f'12) URL importada sin afiliado: {r["url_importada_sin_afiliado"]}',flush=True)
            print(f'13) URL sin acortar con mi afiliado: {mask(r["url_sin_acortar_con_mi_afiliado"]) if r["url_sin_acortar_con_mi_afiliado"] else ""}',flush=True)
            print(f'14) URL acortada con mi afiliado: {r["url_oferta"]}',flush=True)
            print(f'15) Enviado desde: {r["enviado_desde"]}',flush=True)
            print(f'15) Importado de: {r["importado_de"]}',flush=True)
            print('16) Encolado para comparar con base de datos...',flush=True)
            print('------------------------------------------------------------',flush=True)
            data={'name':r['nombre'],'type':'simple','status':'publish','regular_price':str(r['precio_original']),'sale_price':str(r['precio_actual']),'categories':[{'id':idp},{'id':idh}] if idh else ([{'id':idp}] if idp else []),'images':[{'src':imgs}] if imgs else [],'meta_data':[{'key':'importado_de','value':r['importado_de']},{'key':'fecha','value':r['fecha']},{'key':'memoria','value':r['memoria']},{'key':'capacidad','value':r['capacidad']},{'key':'version','value':r['version']},{'key':'fuente','value':r['fuente']},{'key':'precio_actual','value':str(r['precio_actual'])},{'key':'precio_original','value':str(r['precio_original'])},{'key':'codigo_de_descuento','value':r['codigo_descuento']},{'key':'enlace_de_compra_importado','value':r['enlace_de_compra_importado']},{'key':'url_oferta_sin_acortar','value':r['url_oferta_sin_acortar']},{'key':'url_importada_sin_afiliado','value':r['url_importada_sin_afiliado']},{'key':'url_sin_acortar_con_mi_afiliado','value':r['url_sin_acortar_con_mi_afiliado']},{'key':'url_oferta','value':r['url_oferta']},{'key':'enviado_desde','value':ENVIADO_DESDE},{'key':'enviado_desde_tg','value':ENVIADO_DESDE_TG},{'key':'imagen_producto','value':imgs},{'key':'_odm_source_key','value':r['source_key']} ]}
            creado=False; intentos=0
            while intentos<10 and not creado:
                intentos+=1
                try:
                    res=wcapi.post('products',data)
                    if res.status_code in (200,201):
                        prod=res.json(); new_id=prod.get('id'); summary_creados.append({'nombre':r['nombre'],'id':new_id}); creado=True; print(f'CREADO -> {r["nombre"]} (ID: {new_id})',flush=True)
                        try:
                            link=prod.get('permalink',''); short_post=shorten(link) if link else ''
                            if short_post: wcapi.put(f'products/{new_id}',{'meta_data':[{'key':'url_post_acortada','value':short_post}]})
                        except: pass
                    else:
                        body=(res.text or '').replace('\n',' ')[:250]; print(f'Woo error {res.status_code}: {body}',flush=True)
                        if intentos<10: time.sleep(15)
                except Exception as e:
                    print(f'Excepcion Woo: {e}',flush=True)
                    if intentos<10: time.sleep(15)
            if not creado: summary_fallidos.append({'nombre':r['nombre'],'error':'No se pudo crear'})
        except Exception as e:
            summary_fallidos.append({'nombre':r.get('nombre','?'),'error':str(e)}); print(f'ERROR en {r.get("nombre","?")}: {e}',flush=True)
    now=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print('\n============================================================',flush=True)
    print(f'RESUMEN DE EJECUCIÓN ({now})',flush=True)
    print('============================================================',flush=True)
    print(f'\na) ARTICULOS CREADOS: {len(summary_creados)}',flush=True)
    for i in summary_creados: print(f'- {i["nombre"]} (ID: {i["id"]})',flush=True)
    print(f'\nb) ARTICULOS ELIMINADOS (OBSOLETOS): {len(summary_eliminados)}',flush=True)
    for i in summary_eliminados: print(f'- {i["nombre"]} (ID: {i["id"]})',flush=True)
    print(f'\nc) ARTICULOS ACTUALIZADOS: {len(summary_actualizados)}',flush=True)
    for i in summary_actualizados: print(f'- {i["nombre"]} (ID: {i["id"]}): {", ".join(i["cambios"])}',flush=True)
    print(f'\nd) ARTICULOS IGNORADOS (SIN CAMBIOS): {len(summary_ignorados)}',flush=True)
    for i in summary_ignorados: print(f'- {i["nombre"]} (ID: {i["id"]})',flush=True)
    print(f'\ne) DUPLICADOS DETECTADOS: {len(summary_duplicados)}',flush=True)
    for i in summary_duplicados: print(f'- {i}',flush=True)
    print(f'\nf) FALLIDOS: {len(summary_fallidos)}',flush=True)
    print('============================================================',flush=True)

def main():
    rem=obtener_datos_remotos()
    if rem: sincronizar(rem)
    else: print('No se han obtenido productos remotos de Samsung.',flush=True)
if __name__=='__main__': main()
