import os
import pandas as pd
from glob import glob

# Carpeta raíz con todas las subcarpetas (una por estación)
carpeta_raiz = "downloads/metereological-data/"
carpeta_salida = "./output/meteo/"
os.makedirs(carpeta_salida, exist_ok=True)

# Recorremos cada subcarpeta (una por estación)
for carpeta_estacion in os.listdir(carpeta_raiz):
    ruta_estacion = os.path.join(carpeta_raiz, carpeta_estacion)
    if not os.path.isdir(ruta_estacion):
        continue  # Saltamos si no es una carpeta

    archivos_csv = glob(os.path.join(ruta_estacion, "*.csv"))
    dfs = []

    for archivo in archivos_csv:
        try:
            # Intentar leer el archivo CSV
            df = pd.read_csv(archivo, sep=",", engine='python')  # Asegurarse de que usa el separador correcto

            df.columns = df.columns.str.strip().str.lower()  # Asegurarnos que los nombres de las columnas no tengan espacios

            # Procesar fecha
            if 'fecha' in df.columns:
                df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce', dayfirst=False)
                df = df.dropna(subset=['fecha'])  # Eliminar filas con fechas no válidas

            # Eliminar columnas no necesarias
            columnas_a_eliminar = ['indicativo', 'nombre', 'provincia']
            df = df.drop(columns=[col for col in columnas_a_eliminar if col in df.columns])

            # Añadir la estación (indicativo) como nombre del archivo
            cod_estacion = df['indicativo'].iloc[0] if 'indicativo' in df.columns else carpeta_estacion

            dfs.append(df)

        except Exception as e:
            print(f"❌ Error procesando archivo {archivo}: {e}")

    # Eliminar columnas vacías antes de concatenar
    dfs = [df.dropna(axis=1, how='all') for df in dfs]

    # Unir todos los archivos de la estación
    if dfs:
        df_estacion = pd.concat(dfs, ignore_index=True)
        df_estacion = df_estacion.sort_values(by='fecha')  # Orden cronológico

        # Asegurarse de que la fecha está en formato ISO (sin necesidad de hacer un 'strftime')
        if 'fecha' in df_estacion.columns:
            df_estacion['fecha'] = df_estacion['fecha'].dt.date  # Convierte a tipo 'date'

            # Mover la columna fecha al inicio
            cols = df_estacion.columns.tolist()
            cols.insert(0, cols.pop(cols.index('fecha')))
            df_estacion = df_estacion[cols]

        # Guardar archivo final con el nombre del indicativo
        salida = os.path.join(carpeta_salida, f"{cod_estacion}.csv")
        df_estacion.to_csv(salida, index=False, sep=';')
        print(f"✅ Estación {cod_estacion} procesada y guardada en {salida}")
    else:
        print(f"⚠️ No se encontraron datos válidos para estación: {carpeta_estacion}")
