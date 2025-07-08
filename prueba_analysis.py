import pandas as pd
from glob import glob
import matplotlib.pyplot as plt


def leer_csv_con_codificacion(f):
    try:
        return pd.read_csv(f, sep=';')
    except UnicodeDecodeError:
        return pd.read_csv(f, sep=';', encoding='latin1')

# Leer todos los archivos CSV descargados
archivos = glob("downloads_polen/*.csv")
dfs = [leer_csv_con_codificacion(f) for f in archivos]


# Unir todos los dataframes
df = pd.concat(dfs, ignore_index=True)

# Convertir fecha a datetime
df['fecha_lectura'] = pd.to_datetime(df['fecha_lectura'], errors='coerce')

for tipo in df['tipo_polinico'].unique():
    df_tipo = df[df['tipo_polinico'] == tipo]
    print(f"\nFilas para {tipo}: {len(df_tipo)}")
    print(df_tipo[['fecha_lectura', 'granos_de_polen_x_metro_cubico']].head())

    df_tipo_grouped = df_tipo.groupby('fecha_lectura')['granos_de_polen_x_metro_cubico'].mean()
    print(f"Datos agrupados por fecha para {tipo}:")
    print(df_tipo_grouped.head())

    df_tipo_grouped.plot(title=f'Evolución del polen de {tipo}', figsize=(12,6))
    plt.ylabel("Granos de polen por m³")
    plt.grid(True)
    plt.show()


df_grouped = df.groupby('tipo_polinico')['granos_de_polen_x_metro_cubico'].mean().sort_values(ascending=False)

df_grouped.plot(kind='bar', title='Concentración media por tipo de polen', figsize=(12,6))
plt.ylabel("Granos de polen por m³")
plt.show()
# Definir umbral alto
umbral = 10000

df_alto = df[df['granos_de_polen_x_metro_cubico'] > umbral]
print(df_alto[['fecha_lectura', 'captador', 'tipo_polinico', 'granos_de_polen_x_metro_cubico']])

polen_mas_habitual = (
    df.groupby(['captador', 'tipo_polinico'])
      .size()
      .reset_index(name='conteo')
      .sort_values(['captador', 'conteo'], ascending=[True, False])
      .drop_duplicates('captador')
      .set_index('captador')
)

print("\nTipo de polen más habitual por captador:")
print(polen_mas_habitual[['tipo_polinico', 'conteo']].sort_values('conteo', ascending=False))

# Visualización con matplotlib
plt.figure(figsize=(12,6))
polen_mas_habitual['tipo_polinico'].value_counts().plot(kind='bar')
plt.title('Tipo de polen más habitual por captador')
plt.xlabel('Tipo de polen')
plt.ylabel('Número de captadores donde es el más habitual')
plt.grid(axis='y')
plt.tight_layout()
plt.show()