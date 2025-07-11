import os
import pandas as pd

# Ruta de entrada y salida
folder_path = os.path.expanduser("downloads/polen_quantity")

output_folder = os.path.join(os.getcwd(), "output", "polen")
os.makedirs(output_folder, exist_ok=True)

# Lista para guardar los dataframes leídos
all_data = []

# Leer todos los ficheros CSV o TXT
for filename in os.listdir(folder_path):
    if filename.endswith(".csv") or filename.endswith(".txt"):
        file_path = os.path.join(folder_path, filename)
        try:
            df = pd.read_csv(file_path, sep=';', encoding='utf-8')
        except UnicodeDecodeError:
            df = pd.read_csv(file_path, sep=';', encoding='latin1')
        all_data.append(df)

# Unir todos los datos
df_all = pd.concat(all_data, ignore_index=True)

# Asegurar que los nombres de columnas están bien
df_all.columns = df_all.columns.str.strip()

# Convertir fecha al formato dd-mm-aaaa
df_all["fecha_lectura"] = pd.to_datetime(df_all["fecha_lectura"], errors='coerce')
df_all = df_all.dropna(subset=["fecha_lectura"])  # Eliminar filas con fechas no válidas
df_all["fecha_lectura_str"] = df_all["fecha_lectura"].dt.strftime("%d-%m-%Y")

# Pivotar: una columna por tipo polínico
df_pivoted = df_all.pivot_table(
    index=["captador", "fecha_lectura", "fecha_lectura_str"],
    columns="tipo_polinico",
    values="granos_de_polen_x_metro_cubico",
    aggfunc="sum",
    fill_value=0
).reset_index()

# Agrupar por estación
grouped = df_pivoted.groupby("captador")

# Procesar cada estación
for captador, group_df in grouped:
    group_df = group_df.drop(columns=["captador"])  # Eliminamos captador y fecha original
    # Ordenar por fecha datetime
    group_df = group_df.sort_values(by="fecha_lectura")

    # Convertir la fecha datetime a string para guardar
    group_df["fecha_lectura"] = group_df["fecha_lectura"].dt.strftime("%d-%m-%Y")

    # Si existía columna 'fecha_lectura_str', eliminarla para evitar confusiones
    if "fecha_lectura_str" in group_df.columns:
        group_df = group_df.drop(columns=["fecha_lectura_str"])

    # Mover la columna fecha_lectura al principio
    cols = list(group_df.columns)
    cols.insert(0, cols.pop(cols.index("fecha_lectura")))
    group_df = group_df[cols]

    
    output_file = os.path.join(output_folder, f"{captador}.csv")
    group_df.to_csv(output_file, sep=';', index=False)

print("✅ Archivos generados por estación en:", output_folder)
