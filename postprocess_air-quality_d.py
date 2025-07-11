import os
import pandas as pd
from glob import glob
from calendar import monthrange

# Rutas
input_folder = 'downloads/air_quality-daily'  # Ajusta si es necesario
output_root = 'output/air_quality-daily'

# Leer todos los archivos
csv_files = glob(os.path.join(input_folder, '*.csv'))

# Diccionario para agrupar datos: {(año, mes, punto_base): {magnitud: {fecha: valor}}}
datos = {}

for file in csv_files:
    df = pd.read_csv(file, sep=';', encoding='latin1')

    for _, row in df.iterrows():
        punto_completo = row['PUNTO_MUESTREO']  # ej: 28079004_6_48
        punto_base = punto_completo.split('_')[0]  # ej: 28079004
        magnitud = str(row['MAGNITUD'])
        año = int(row['ANO'])
        mes = int(row['MES'])
        dias_validos = monthrange(año, mes)[1]

        clave = (año, mes, punto_base)
        if clave not in datos:
            datos[clave] = {}

        if magnitud not in datos[clave]:
            datos[clave][magnitud] = {}

        for dia in range(1, dias_validos + 1):
            col = f"D{dia:02d}"
            valor = row.get(col, "")
            fecha = f"{dia:02d}-{mes:02d}-{año}"
            datos[clave][magnitud][fecha] = valor

# Escribir archivos
for (año, mes, punto_base), magnitudes_dict in datos.items():
    fechas = [f"{dia:02d}-{mes:02d}-{año}" for dia in range(1, monthrange(año, mes)[1] + 1)]

    filas = []
    for magnitud, valores_dict in magnitudes_dict.items():
        fila = [punto_base, magnitud] + [valores_dict.get(fecha, "") for fecha in fechas]
        filas.append(fila)

    columnas = ['PUNTO_MUESTREO', 'MAGNITUD'] + fechas

    # Crear carpeta por año y punto_base
    output_folder = os.path.join(output_root, str(año), punto_base)
    os.makedirs(output_folder, exist_ok=True)

    # Guardar el archivo mensual
    nombre_archivo = f"{mes:02d}-{año}.csv"
    output_path = os.path.join(output_folder, nombre_archivo)
    df_out = pd.DataFrame(filas, columns=columnas)
    df_out.to_csv(output_path, index=False, sep=';')
