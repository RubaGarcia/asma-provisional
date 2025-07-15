import requests
import pandas as pd
import os
import time
from dotenv import load_dotenv

load_dotenv()  # Cargar variables de entorno desde .env
API_KEY = os.getenv('API_KEY')

# Paso 1: Obtener inventario completo y filtrar estaciones de Madrid
def obtener_estaciones_madrid():
    """
    Obtiene las estaciones meteorológicas de Madrid desde el inventario de AEMET.
    """
    url_inventario = 'https://opendata.aemet.es/opendata/api/valores/climatologicos/inventarioestaciones/todasestaciones'

    response = requests.get(url_inventario, headers={'api_key': API_KEY})
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

# Función para descargar datos de varias estaciones
def descargar_y_guardar_datos_multiples(estaciones, anio, mes_inicio, mes_fin, max_retries=3):
    base_url = ("https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/"
                "fechaini/{fecha_inicio}/fechafin/{fecha_fin}/estacion/{estaciones}?api_key={apikey}")
    
    # Crear el formato de fecha para la solicitud
    fecha_inicio = f"{anio}-{mes_inicio:02d}-01T00:00:00UTC"
    fecha_fin = f"{anio}-{mes_fin:02d}-01T00:00:00UTC"
    
    # Carpeta de descarga general
    carpeta_principal = f"downloads/metereological-data"
    os.makedirs(carpeta_principal, exist_ok=True)

    # Evitar volver a descargar si ya existe
    archivo = os.path.join(carpeta_principal, f"datos_{anio}_{mes_inicio}_{mes_fin}.csv")

    if os.path.isfile(archivo):
        print(f"[{anio}-{mes_inicio}] Datos ya descargados. Saltando.")
        return True

    # Unir todas las estaciones en una sola cadena separada por comas
    estaciones_str = ','.join(estaciones)

    # Asegurarse de que API_KEY esté definida
    if not API_KEY:
        raise ValueError("API_KEY no está definida. Asegúrate de que esté cargada correctamente.")

    url = base_url.format(fecha_inicio=fecha_inicio, fecha_fin=fecha_fin, estaciones=estaciones_str, apikey=API_KEY)

    for intento in range(max_retries):
        try:
            response = requests.get(url, timeout=10)
            # time.sleep(1.25)  # Espera para evitar sobrecargar el servidor
            if response.status_code == 200:
                data = response.json()
                url_datos = data.get('datos')
                if url_datos:
                    resp_datos = requests.get(url_datos, timeout=20)
                    if resp_datos.status_code == 200:
                        datos_reales = resp_datos.json()
                        if datos_reales:
                            # Crear un DataFrame con los datos obtenidos
                            df = pd.DataFrame(datos_reales)
                            
                            # Segregar los datos por estación
                            for estacion in estaciones:
                                # Crear la carpeta para la estación si no existe
                                carpeta_estacion = os.path.join(carpeta_principal, estacion)
                                os.makedirs(carpeta_estacion, exist_ok=True)

                                # Filtrar los datos de la estación y guardarlos en su carpeta
                                df_estacion = df[df['indicativo'] == estacion]
                                archivo_estacion = os.path.join(carpeta_estacion, f"datos_{estacion}_{anio}_{mes_inicio}_{mes_fin}.csv")
                                df_estacion.to_csv(archivo_estacion, index=False)
                                print(f"Datos de estación {estacion} guardados en {archivo_estacion}")
                            return True
                        else:
                            return False
                    else:
                        return False
                else:
                    return False
            elif response.status_code == 429:
                time.sleep(60)
        except requests.exceptions.RequestException as e:
            time.sleep(10 * (intento + 1))  # backoff exponencial

    return False

# Función principal para descargar los datos diarios de las estaciones
def download_aemet_files():
    """
    Descarga los datos diarios de las estaciones meteorológicas de Madrid para los años 2001 a 2024.
    """
    estaciones = obtener_estaciones_madrid()
    if not estaciones:
        return

    no_descargados = []
    for anio in range(2001, 2025):
        for mes_inicio in range(1, 13, 6):  # Intervalos de 6 meses
            mes_fin = mes_inicio + 5
            if mes_fin > 12:
                mes_fin = 12
            exito = descargar_y_guardar_datos_multiples(estaciones, anio, mes_inicio, mes_fin)
            if not exito:
                no_descargados.append((anio, mes_inicio, mes_fin))
    
    # Si hay descargas fallidas, puedes guardarlas en un archivo o manejarlo de alguna forma
    if no_descargados:
        print("No se pudieron descargar los siguientes datos:")
        for item in no_descargados:
            print(item)
    else:
        print("Todos los datos se descargaron con éxito.")
