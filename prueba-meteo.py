import os
import pandas as pd
from glob import glob

# Carpeta raíz con todas las subcarpetas (una por estación)
carpeta_raiz = "downloads/metereological-data/"
carpeta_salida = "./output/meteo/"
os.makedirs(carpeta_salida, exist_ok=True)

# Columnas numéricas a convertir
columnas_numericas = [
    'p_max', 'hr', 'nw_55', 'tm_min', 'ta_max', 'ts_min', 'nt_30',
    'w_racha', 'np_100', 'nw_91', 'np_001', 'ta_min', 'w_rec', 'e',
    'np_300', 'p_mes', 'w_med', 'nt_00', 'ti_max', 'tm_mes', 'tm_max', 'np_010'
]

# Recorremos cada subcarpeta (una por estación)
for carpeta_estacion in os.listdir(carpeta_raiz):
    ruta_estacion = os.path.join(carpeta_raiz, carpeta_estacion)
    if not os.path.isdir(ruta_estacion):
        continue  # Saltamos si no es una carpeta

    archivos_csv = glob(os.path.join(ruta_estacion, "*.csv"))
    dfs = []

    for archivo in archivos_csv:
        try:
            df = pd.read_csv(archivo)
            df.columns = df.columns.str.strip().str.lower()

            # Procesar fecha
            if 'fecha' in df.columns:
                df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce', dayfirst=True)
                df = df.dropna(subset=['fecha'])
                df['fecha'] = df['fecha'].dt.strftime('%d-%m-%Y')

            # Convertir columnas numéricas
            for col in columnas_numericas:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            # Agregar a la lista
            dfs.append(df)

        except Exception as e:
            print(f"❌ Error procesando archivo {archivo}: {e}")

    # Unir todos los archivos de la estación
    if dfs:
        df_estacion = pd.concat(dfs, ignore_index=True)
        df_estacion = df_estacion.sort_values(by='fecha')
        cod_estacion = df_estacion['indicativo'].iloc[0] if 'indicativo' in df_estacion.columns else carpeta_estacion

        # Guardar archivo final
        salida = os.path.join(carpeta_salida, f"{cod_estacion}_completo.csv")
        df_estacion.to_csv(salida, index=False)
        print(f"✅ Estación {cod_estacion} procesada y guardada en {salida}")
    else:
        print(f"⚠️ No se encontraron datos válidos para estación: {carpeta_estacion}")
