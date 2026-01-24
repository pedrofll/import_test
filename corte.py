"""
Scraper para El Corte Inglés — Móviles
ESTRATEGIA: Playwright con bypass de HTTP/2 y evasión de huella digital.
"""

import os
import re
import time
import json
import random
from dataclasses import dataclass
from typing import List, Optional, Tuple
from urllib.parse import urlparse, urlunparse, urljoin

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

# ... (Mantén las clases y helpers de limpieza que ya tenías) ...

def main():
    print("--- FASE 1: ECI (NAVEGADOR REAL CON BYPASS) ---", flush=True)
    
    with sync_playwright() as p:
        print("🚀 Lanzando navegador con parámetros de evasión...", flush=True)
        
        # FORZAMOS HTTP/1.1 para evitar el ERR_HTTP2_PROTOCOL_ERROR
        # Además añadimos argumentos para ocultar que es un servidor
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--disable-http2", 
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox"
            ]
        )
        
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 720},
            locale="es-ES",
            timezone_id="Europe/Madrid"
        )
        
        # Script para ocultar Playwright del JavaScript de la web
        page = context.new_page()
        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        total = 0
        
        # URLs de la 1 a la 5 para probar
        urls = [f"https://www.elcorteingles.es/electronica/moviles-y-smartphones/{i+'/' if i>1 else ''}" for i in [1, 2, 3, 4, 5]]
        
        for i, url in enumerate(urls, start=1):
            print(f"\n📂 Intentando Página {i}...", flush=True)
            
            try:
                # Aumentamos el timeout y usamos un referer real
                response = page.goto(
                    url, 
                    timeout=90000, 
                    wait_until="load", 
                    referer="https://www.google.es/"
                )
                
                if response.status != 200:
                    print(f"      ⚠️  Error de respuesta: {response.status}")
                    continue

                # Espera aleatoria para imitar humano
                time.sleep(random.uniform(4, 7))
                
                # Scroll suave para cargar imágenes y datos
                page.mouse.wheel(0, 2000)
                time.sleep(2)
                
                html = page.content()
                
                if "Access Denied" in html:
                    print("      ⛔ BLOQUEO: IP Denegada por Akamai.")
                    continue
                
                # Usamos la función de parseo que ya teníamos
                prods = parse_productos_from_html(html, str(i))
                
                print(f"      ✅ Encontrados: {len(prods)}", flush=True)
                total += len(prods)
                
                for p in prods:
                    print(f"      📱 {p.nombre} | {p.precio_actual}€")
                
            except Exception as e:
                print(f"      ❌ Error navegación: {str(e)[:100]}...", flush=True)
        
        browser.close()
        print(f"\n📋 TOTAL PRODUCTOS ESCANEADOS: {total}")

# ... (El resto del script de parseo se mantiene igual) ...

if __name__ == "__main__":
    main()
