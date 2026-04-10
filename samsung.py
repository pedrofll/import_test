Añade este bloque cerca de tus utilidades / mapas locales:

```python
# RAM local para modelos que en el listing no la muestran.
# Nota: A57 5G 512 GB = 12 GB está soportado por Samsung España.
# A37 5G 256 GB y A17/A17 5G 256 GB se resuelven aquí para no ignorarlos en listing-only.
SAMSUNG_LISTING_RAM_MAP = {
    ("samsung galaxy a57 5g", "512gb"): "12GB",
    ("samsung galaxy a37 5g", "256gb"): "8GB",
    ("samsung galaxy a17 5g", "256gb"): "8GB",
    ("samsung galaxy a17", "256gb"): "8GB",
}


def resolver_ram_samsung_listing(nombre: str, capacidad: str, model_code: str = "") -> str:
    n = normalize_spaces(nombre).lower()
    c = (capacidad or "").lower().replace(" ", "")
    mc = (model_code or "").upper().strip()

    # Primero, si en el futuro quieres afinar por modelCode, este es el sitio.
    model_overrides = {
        # Ejemplo:
        # ("SM-A376BLVGEUB", "256gb"): "8GB",
    }
    if (mc, c) in model_overrides:
        return model_overrides[(mc, c)]

    return SAMSUNG_LISTING_RAM_MAP.get((n, c), "")
```

Y sustituye el tramo donde ahora haces algo parecido a:

```python
if not memoria:
    print(f"⚠️ Card Samsung sin RAM resoluble para {nombre} {capacidad}. Se ignora.")
    continue
```

por esto:

```python
if not memoria:
    memoria = resolver_ram_samsung_listing(nombre, capacidad, model_code)

if not memoria:
    print(f"⚠️ Card Samsung sin RAM resoluble para {nombre} {capacidad}. Se ignora.")
    continue
```

Si además quieres dejar trazabilidad en logs cuando la RAM venga del mapa local, usa esto:

```python
ram_from_map = False
if not memoria:
    memoria = resolver_ram_samsung_listing(nombre, capacidad, model_code)
    ram_from_map = bool(memoria)

if not memoria:
    print(f"⚠️ Card Samsung sin RAM resoluble para {nombre} {capacidad}. Se ignora.")
    continue

if ram_from_map:
    print(f"ℹ️ RAM resuelta por mapa local para {nombre} {capacidad}: {memoria}")
```

Opcional recomendado para evitar errores futuros:
- NO mapees A57 5G 256 GB ni A37 5G 256 GB a 12 GB por nombre+capacidad salvo que tengas modelCode exacto,
  porque en Samsung hay combinaciones 8/256 y 12/256 para esas familias.
