#!/usr/bin/env python
# coding: utf-8

import json
import time
import hashlib
import subprocess
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import requests


# ========= TZ =========
TZ = ZoneInfo("Europe/Madrid")

# ========= CONFIG =========
REPO_PATH = Path(".")                 # repo git clonado

OUT_GEOJSON = REPO_PATH / "viajes.geojson"
CACHE_PATH = REPO_PATH / "geocache.json"
LOG_PATH = REPO_PATH / "generacion.log"

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_SLEEP_SEC = 1.0
COUNTRY_CODES = "es"
USER_AGENT = "viajes-mapa/1.0 (contacto: tu-email@empresa.com)"

OSRM_URL = "https://router.project-osrm.org/route/v1/driving"

FECHA_COL   = "Fecha viaje"
SALIDA_COL  = "Salida"
LLEGADA_COL = "Llegada"
ORIGEN_COL  = "Origen"
DESTINO_COL = "Destino"
PARADA1_COL = "Parada1"
PARADA2_COL = "Parada2"


CSV_PATH = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQCgW2AsouOAq3YY65x7sHMtdursK_BhnZ3g4AD0DaCZ_NJ2DWsotnKr4YBXyiIrT-4K5eWzSnvHtRY/pub?output=csv"


# In[10]:


# ========= UTIL =========


def get_text(row, col_name: str) -> str:
    """
    Devuelve texto limpio. Trata NaN/None/"" y 'nan' como vacío.
    """
    if col_name not in row:
        return ""

    v = row.get(col_name)

    # NaN real de pandas
    if pd.isna(v):
        return ""

    s = str(v).strip()
    if not s:
        return ""

    # Por seguridad: si algo ya venía como string "nan"
    if s.lower() == "nan":
        return ""

    return s



def log(msg: str) -> None:
    ts = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg}\n")


def load_cache() -> dict:
    if CACHE_PATH.exists():
        return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    return {}


def save_cache(cache: dict) -> None:
    CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


def normalize_place(s: str) -> str:
    return " ".join(str(s).strip().lower().split())


def geocode(place: str, cache: dict):
    key = normalize_place(place)
    if not key:
        return None

    if key in cache:
        return cache[key]  # puede ser dict o None

    params = {
        "q": place,
        "format": "jsonv2",
        "limit": 1,
        "countrycodes": COUNTRY_CODES,
    }
    headers = {"User-Agent": USER_AGENT}

    r = requests.get(NOMINATIM_URL, params=params, headers=headers, timeout=20)
    r.raise_for_status()
    data = r.json()

    time.sleep(NOMINATIM_SLEEP_SEC)

    if not data:
        cache[key] = None
        return None

    item = data[0]
    result = {
        "lat": float(item["lat"]),
        "lon": float(item["lon"]),
        "display_name": item.get("display_name"),
    }
    cache[key] = result
    return result


def parse_fecha(fecha_str: str) -> str:
    # "18/02/2026" -> "2026-02-18"
    return datetime.strptime(str(fecha_str).strip(), "%d/%m/%Y").date().isoformat()


def parse_hora(h: str) -> str:
    # acepta "9:20", "09:20", "09:20:00"
    s = str(h).strip()
    for fmt in ("%H:%M", "%H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).strftime("%H:%M")
        except ValueError:
            pass
    # fallback: intenta rellenar si viene "9:20"
    parts = s.split(":")
    if len(parts) >= 2:
        return f"{parts[0].zfill(2)}:{parts[1].zfill(2)}"
    return s


def viaje_vigente(fecha_iso: str, hora_salida_hhmm: str | None) -> bool:
    ahora = datetime.now(TZ)
    hoy = ahora.date()
    fecha = datetime.strptime(fecha_iso, "%Y-%m-%d").date()

    if fecha > hoy:
        return True
    if fecha < hoy:
        return False

    # si es hoy, ocultar si ya pasó la hora de salida
    if hora_salida_hhmm:
        try:
            hs = datetime.strptime(hora_salida_hhmm, "%H:%M").time()
            return hs >= ahora.time()
        except ValueError:
            return True

    return True



def obtener_ruta_osrm_multi(stops_geo):
    coords = ";".join([f"{p['lon']},{p['lat']}" for p in stops_geo])
    url = f"{OSRM_URL}/{coords}?overview=simplified&geometries=geojson"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    data = r.json()
    if data.get("routes"):
        return data["routes"][0]["geometry"]
    return None


def build_feature_route(viaje_id, fecha, hora_salida, hora_llegada, stops_txt, stops_geo):
    try:
        ruta = obtener_ruta_osrm_multi(stops_geo)
    except Exception as e:
        log(f"OSRM fallo; uso polilinea por puntos. viaje_id={viaje_id} err={e}")
        ruta = None

    geometry = ruta if ruta else {
        "type": "LineString",
        "coordinates": [[p["lon"], p["lat"]] for p in stops_geo],
    }

    origen_txt = stops_txt[0]
    destino_txt = stops_txt[-1]
    paradas_txt = stops_txt[1:-1]

    return {
        "type": "Feature",
        "geometry": geometry,
        "properties": {
            "name": f"{origen_txt} → {destino_txt} ({hora_salida}-{hora_llegada})",
            "viaje_id": viaje_id,
            "fecha": fecha,
            "hora_salida": hora_salida,
            "hora_llegada": hora_llegada,
            "origen": origen_txt,
            "destino": destino_txt,
            "paradas": " | ".join(paradas_txt) if paradas_txt else "",
            "num_paradas": len(paradas_txt),
        },
    }


def git_commit_push():
    subprocess.run(["git", "-C", str(REPO_PATH), "add", "viajes.geojson", "geocache.json", "generacion.log"], check=True)
    msg = f"Actualizar viajes.geojson {datetime.now(TZ).strftime('%Y-%m-%d')}"
    subprocess.run(["git", "-C", str(REPO_PATH), "commit", "-m", msg], check=False)
    subprocess.run(["git", "-C", str(REPO_PATH), "push"], check=True)


def main():
    cache = load_cache()

    try:
        df = pd.read_csv(CSV_PATH, encoding="utf-8", sep=",")
    except UnicodeDecodeError:
        df = pd.read_csv(CSV_PATH, encoding="ISO-8859-1", sep=",")


    RENOMBRA = {
    # Forms/Sheets típicos
    "Primera parada": "Parada1",
    "Segunda parada": "Parada2",
    # si tuvieras “Llegada” con otro nombre, lo mapearías aquí
    # "Hora de llegada": "Llegada",
}

    df = df.rename(columns=RENOMBRA)


    features = []
    descartados = 0

    for _, row in df.iterrows():
        # 1) leer SIEMPRE con get_text (evita "nan")
        destino = get_text(row, DESTINO_COL)
        origen  = get_text(row, ORIGEN_COL)
        p1      = get_text(row, PARADA1_COL)
        p2      = get_text(row, PARADA2_COL)
    
        if not destino:
            descartados += 1
            log(f"Descartado (sin destino). Id={row.get('Id')}")
            continue
    
        if not origen:
            descartados += 1
            log(f"Descartado (sin origen). Id={row.get('Id')} destino='{destino}'")
            continue
    
        # 2) definir aquí fecha/hora (antes de usarlas)
        fecha_iso    = parse_fecha(row.get(FECHA_COL))
        hora_salida  = parse_hora(row.get(SALIDA_COL))
        hora_llegada = parse_hora(row.get(LLEGADA_COL))
    
        # 3) leer viaje_id (y validar) antes de descartar por caducidad si lo quieres en logs
        viaje_id = get_text(row, "viaje_id")
        if not viaje_id:
            descartados += 1
            log(f"Descartado (sin viaje_id). Id={row.get('Id')}")
            continue
    
        # 4) caducidad
        if not viaje_vigente(fecha_iso, hora_salida):
            descartados += 1
            log(f"Descartado (caducado). viaje_id={viaje_id} fecha={fecha_iso} salida={hora_salida}")
            continue
    
        # 5) construir stops y filtrar basura
        stops_txt = [origen]
        if p1:
            stops_txt.append(p1)
        if p2:
            stops_txt.append(p2)
        stops_txt.append(destino)
    
        stops_txt = [s for s in stops_txt if s and s.strip() and s.strip().lower() != "nan"]


        viaje_id = get_text(row, "viaje_id")
        if not viaje_id:
            descartados += 1
            log(f"Descartado (sin viaje_id). Id={row.get('Id')}")
            continue

        stops_geo = []
        geocode_ok = True
        for s in stops_txt:
            g = geocode(s, cache)
            if g is None:
                geocode_ok = False
                log(f"Descartado (no geocodifica stop). viaje_id={viaje_id} stop='{s}' stops={stops_txt}")
                break
            stops_geo.append(g)

        if not geocode_ok:
            descartados += 1
            continue

        features.append(build_feature_route(viaje_id, fecha_iso, hora_salida, hora_llegada, stops_txt, stops_geo))

    fc = {"type": "FeatureCollection", "features": features}
    OUT_GEOJSON.write_text(json.dumps(fc, ensure_ascii=False, indent=2), encoding="utf-8")
    save_cache(cache)

    log(f"Generado {OUT_GEOJSON.name}: features={len(features)} descartados={descartados}")

    git_commit_push()
    log("Publicado en GitHub (push OK)")


if __name__ == "__main__":
    main()


# In[ ]:





# In[ ]:




