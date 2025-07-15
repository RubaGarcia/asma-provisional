import os
import pandas as pd
from glob import glob
from calendar import monthrange

# Rutas
input_folder = 'downloads/air_quality-daily'  # Ajusta si es necesario
output_root = 'output/calidad_aire-diaria'

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
            # Formato ISO para fecha: YYYY-MM-DD
            fecha_iso = f"{año:04d}-{mes:02d}-{dia:02d}"
            datos[clave][magnitud][fecha_iso] = valor

# Reorganizar datos por punto_base para escritura unificada
puntos_dict = {}

for (año, mes, punto_base), magnitudes_dict in datos.items():
    dias_validos = monthrange(año, mes)[1]
    fechas = [f"{año:04d}-{mes:02d}-{dia:02d}" for dia in range(1, dias_validos + 1)]

    # Asegurarse de que el punto_base esté en puntos_dict antes de agregar datos
    if punto_base not in puntos_dict:
        puntos_dict[punto_base] = []  # Inicializar con lista vacía si no existe

    for fecha in fechas:
        fila = {"FECHA": fecha}  # Ya no agregamos el PUNTO_MUESTREO
        for magnitud, valores_dict in magnitudes_dict.items():
            val_key = f"VAL{magnitud.zfill(3)}"  # Solo mantener el valor bajo una clave específica
            fila[val_key] = valores_dict.get(fecha, "")
        puntos_dict[punto_base].append(fila)

# Escribir un único archivo por punto_base (sin el punto de muestreo y sin la columna de magnitud innecesaria)
for punto_base, filas in puntos_dict.items():
    df_out = pd.DataFrame(filas)
    df_out.sort_values(by="FECHA", inplace=True)

    output_folder = output_root
    os.makedirs(output_folder, exist_ok=True)

    nombre_archivo = f"{punto_base}.csv"
    output_path = os.path.join(output_folder, nombre_archivo)
    df_out.to_csv(output_path, index=False, sep=';')
