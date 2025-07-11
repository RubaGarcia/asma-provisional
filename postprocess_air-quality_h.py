import os
import pandas as pd
from glob import glob

# Carpetas
input_folder = 'downloads/air_quality-hourly'
output_root = 'output/air_quality-hourly'
# Buscar ficheros que empiezan por mes_mo
csv_files = glob(os.path.join(input_folder, 'mes_mo*.csv'))

for file in csv_files:
    df = pd.read_csv(file, sep=';', encoding='latin1')

    for _, row in df.iterrows():
        punto_completo = row['PUNTO_MUESTREO']         # ej: 28079004_6_48
        punto_base = punto_completo.split('_')[0]      # ej: 28079004
        magnitud = str(row['MAGNITUD'])
        año = int(row['ANO'])
        mes = int(row['MES'])
        dia = int(row['DIA'])

        fecha = f"{dia:02d}-{mes:02d}-{año}"
        output_folder = os.path.join(output_root, str(año), punto_base)
        os.makedirs(output_folder, exist_ok=True)
        output_file = os.path.join(output_folder, f"{mes:02d}-{año}.csv")

        filas = []
        for h in range(1, 25):  # De H01 a H24
            col_valor = f"V{h:02d}"
            valor = row.get(col_valor, "")
            hora = f"{h:02d}"
            filas.append([punto_base, magnitud, fecha, hora, valor])

        df_out = pd.DataFrame(filas, columns=['PUNTO_MUESTREO', 'MAGNITUD', 'FECHA', 'HORA', 'VALOR'])

        # Añadir o crear archivo mensual
        if os.path.exists(output_file):
            df_existente = pd.read_csv(output_file, sep=';', encoding='utf-8')
            df_out = pd.concat([df_existente, df_out], ignore_index=True)

        df_out.to_csv(output_file, index=False, sep=';', encoding='utf-8')
