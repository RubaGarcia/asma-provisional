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
            # Leer CSV sin asumir formato decimal
            df = pd.read_csv(
                archivo,
                sep=",",
                engine='python',
                dtype=str  
            )

            df.columns = df.columns.str.strip().str.lower()  # Limpiar nombres de columnas

            # Procesar fecha
            if 'fecha' in df.columns:
                df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce', dayfirst=False)
                df = df.dropna(subset=['fecha'])  # Eliminar filas con fechas no válidas

            for col in df.columns:
                if col == 'fecha':
                    continue  # Saltar columna de fecha

                # Reemplazar ',' decimal por '.' solo en números (no texto general)
                df[col] = df[col].str.replace(r'(\d+),(\d+)', r'\1.\2', regex=True)

                # Eliminar separadores de miles (puntos en medio de números largos)
                df[col] = df[col].str.replace(r'(?<=\d)\.(?=\d{3}(?:\D|$))', '', regex=True)

                # Convertir a numérico donde sea posible
                df[col] = pd.to_numeric(df[col], errors='ignore')

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

        # Asegurarse de que la fecha está en formato ISO (YYYY-MM-DD)
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
