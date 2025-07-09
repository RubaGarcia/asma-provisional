import requests
import pandas as pd
import os
import time

API_KEY = 'eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnYXJjaWFyQHByZWRpY3RpYS5lcyIsImp0aSI6ImQyMmZjOTdmLTcxM2ItNGU3OS1iNmY5LTMxNzY3MzM2ZDM0YyIsImlzcyI6IkFFTUVUIiwiaWF0IjoxNzUyMDUwNzg0LCJ1c2VySWQiOiJkMjJmYzk3Zi03MTNiLTRlNzktYjZmOS0zMTc2NzMzNmQzNGMiLCJyb2xlIjoiIn0.nKa6KIPqSopPMLVu1fj188VyKQpFgOhwJujr0MfZt_Q'  # Cambia por tu clave real

headers = {'api_key': API_KEY}

# Paso 1: Obtener inventario completo y filtrar estaciones de Madrid

def obtener_estaciones_madrid():
    """
    Obtiene las estaciones meteorológicas de Madrid desde el inventario de AEMET.
    """
    url_inventario = 'https://opendata.aemet.es/opendata/api/valores/climatologicos/inventarioestaciones/todasestaciones'

    response = requests.get(url_inventario, headers=headers)
    data = response.json()

    if 'datos' in data:
        url_datos = data['datos']
        
        response_estaciones = requests.get(url_datos)
        estaciones_json = response_estaciones.json()
        
        df_estaciones = pd.DataFrame(estaciones_json)
        df_madrid = df_estaciones[df_estaciones['provincia'].str.lower() == 'madrid']
        estaciones = df_madrid['indicativo'].tolist()
        
        print(f"Estaciones de Madrid encontradas: {len(estaciones)}")
    else:
        print("No se pudo obtener la URL para descargar el inventario.")
        estaciones = []
    return estaciones

# URL base para descargar datos mensuales/anuales (con placeholders para año y estación)

def descargar_y_guardar_datos(estacion, anio, max_retries=3):
    base_url = ("https://opendata.aemet.es/opendata/api/valores/climatologicos/mensualesanuales/datos/"
            "anioini/{anio}/aniofin/{anio}/estacion/{estacion}?api_key={apikey}")
    carpeta = f"downloads/metereological-data/{estacion}"
    os.makedirs(carpeta, exist_ok=True)
    archivo = os.path.join(carpeta, f"datos_{estacion}_{anio}.csv")

    # Evitar volver a descargar si ya existe
    if os.path.isfile(archivo):
        print(f"[{anio}] Datos estación {estacion} ya descargados. Saltando.")
        return True

    url = base_url.format(anio=anio, estacion=estacion, apikey=API_KEY)
    
    for intento in range(max_retries):
        try:
            response = requests.get(url, timeout=10)
            time.sleep(1.25)  # Espera para evitar sobrecargar el servidor
            if response.status_code == 200:
                data = response.json()
                url_datos = data.get('datos')
                if url_datos:
                    resp_datos = requests.get(url_datos, timeout=20)
                    # time.sleep(2)  # Espera para evitar sobrecargar el servidor
                    if resp_datos.status_code == 200:
                        datos_reales = resp_datos.json()
                        if datos_reales:
                            df = pd.DataFrame(datos_reales)
                            df.to_csv(archivo, index=False)
                            # print(f"[{anio}] Datos estación {estacion} guardados en {archivo}")
                            return True
                        else:
                            # print(f"[{anio}] No hay datos reales para estación {estacion}")
                            return False
                    else:
                        # print(f"[{anio}] Error descargando datos reales: {resp_datos.status_code}")
                        return False
                else:
                    # print(f"[{anio}] No hay URL de datos para estación {estacion}")
                    return False
            elif response.status_code == 429:
                # print(f"[{anio}] Límite de peticiones alcanzado, esperando 60 segundos...")
                time.sleep(60)
        except requests.exceptions.RequestException as e:
            # print(f"[{anio}] Error de conexión (intento {intento+1}/{max_retries}): {e}")
            # print('')
            time.sleep(10 * (intento + 1))  # backoff exponencial

    # print(f"[{anio}] Fallo definitivo para estación {estacion}")
    return False


# Ejemplo de llamada para cada estación y año (asumiendo que tienes la lista `estaciones`):

no_descargados = []

def download_aemet_files():
    """
    Descarga los datos de las estaciones meteorológicas de Madrid para los años 2001 a 2024.
    """
    estaciones = obtener_estaciones_madrid()
    if not estaciones:
        # print("No se encontraron estaciones de Madrid.")
        return

    for estacion in estaciones:
        for anio in range(2001, 2025):
            exito = descargar_y_guardar_datos(estacion, anio)
            if not exito:
                no_descargados.append((estacion, anio))

