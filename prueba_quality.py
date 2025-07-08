import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# Crear carpeta para reportes si no existe
output_dir = "reports-air-quality"
os.makedirs(output_dir, exist_ok=True)

def guardar_grafico(nombre_archivo):
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, nombre_archivo), dpi=300)
    plt.close()

def cargar_todos_los_csv(directorio):
    archivos = [f for f in os.listdir(directorio) if f.endswith(".csv")]
    dfs = []
    
    for archivo in archivos:
        print(f"Cargando {archivo}...")
        path = os.path.join(directorio, archivo)
        try:
            df = pd.read_csv(path, sep=";")
            dfs.append(df)
        except Exception as e:
            print(f"Error leyendo {archivo}: {e}")
    
    df_total = pd.concat(dfs, ignore_index=True)
    return df_total

def transformar_a_formato_largo(df):
    dias = [f"{i:02d}" for i in range(1, 32)]
    registros = []

    for _, fila in df.iterrows():
        for dia in dias:
            if f"D{dia}" in df.columns and f"V{dia}" in df.columns:
                valor = fila[f"D{dia}"]
                validez = fila[f"V{dia}"]
                try:
                    fecha = pd.to_datetime(f"{int(fila['ANO'])}-{int(fila['MES']):02d}-{int(dia)}")
                    registros.append({
                        "fecha": fecha,
                        "provincia": str(fila["PROVINCIA"]).zfill(2),
                        "municipio": str(fila["MUNICIPIO"]).zfill(3),
                        "estacion": str(fila["ESTACION"]).zfill(3),
                        "magnitud": int(fila["MAGNITUD"]),
                        "valor": valor,
                        "validacion": validez
                    })
                except:
                    continue  # evita fechas inválidas
    
    df_largo = pd.DataFrame(registros)
    return df_largo

def matriz_correlacion(df_largo):
    pivot = df_largo.pivot_table(index="fecha", columns="magnitud", values="valor", aggfunc="mean")
    corr = pivot.corr()
    
    plt.figure(figsize=(10, 8))
    sns.heatmap(corr, annot=True, cmap="coolwarm", center=0, fmt=".2f")
    plt.title("Matriz de correlación entre contaminantes")
    guardar_grafico("matriz_correlacion_contaminantes.png")

def mapa_calor_provincias(df_largo, magnitud=8):
    df = df_largo[df_largo["magnitud"] == magnitud].copy()
    pivot = df.groupby(["provincia", "año"])["valor"].mean().unstack()
    
    plt.figure(figsize=(12, 8))
    sns.heatmap(pivot, annot=True, fmt=".1f", cmap="YlOrRd", cbar_kws={'label': 'µg/m³'})
    plt.title(f"Mapa de calor anual: concentración media magnitud {magnitud} por provincia")
    plt.xlabel("Año")
    plt.ylabel("Provincia")
    plt.yticks(rotation=0)
    guardar_grafico(f"mapa_calor_provincias_magnitud_{magnitud}.png")

def evolucion_por_provincia(df_largo, magnitud=8):
    df = df_largo[df_largo["magnitud"] == magnitud]
    evolucion_prov = df.groupby(["año", "provincia"])["valor"].mean().reset_index()
    
    plt.figure(figsize=(14, 6))
    sns.lineplot(data=evolucion_prov, x="año", y="valor", hue="provincia", palette="tab10")
    plt.title(f"Evolución anual magnitud {magnitud} por provincia")
    plt.ylabel("Concentración media (µg/m³)")
    plt.grid(True)
    guardar_grafico(f"evolucion_provincia_magnitud_{magnitud}.png")

def evolucion_por_municipio(df_largo, provincia, municipio, magnitud=8):
    df = df_largo[(df_largo["provincia"] == provincia) & 
                  (df_largo["municipio"] == municipio) &
                  (df_largo["magnitud"] == magnitud)]
    evol_muni = df.groupby("año")["valor"].mean().reset_index()
    
    plt.figure(figsize=(10, 5))
    sns.lineplot(data=evol_muni, x="año", y="valor")
    plt.title(f"Evolución anual magnitud {magnitud} en municipio {provincia}-{municipio}")
    plt.ylabel("µg/m³")
    plt.grid(True)
    guardar_grafico(f"evolucion_municipio_{provincia}_{municipio}_magnitud_{magnitud}.png")

def plot_tendencia_estacion(df_largo, estacion, magnitud=8):
    tendencia = df_largo.groupby(["año", "estacion", "magnitud"])["valor"].mean().reset_index()
    df_est = tendencia[(tendencia["estacion"] == estacion) & (tendencia["magnitud"] == magnitud)]
    
    if df_est.empty:
        print(f"No hay datos para estación {estacion} y magnitud {magnitud}")
        return
    
    plt.figure(figsize=(10, 5))
    sns.lineplot(data=df_est, x="año", y="valor")
    plt.title(f"Tendencia magnitud {magnitud} en estación {estacion}")
    plt.ylabel("µg/m³")
    plt.grid(True)
    guardar_grafico(f"tendencia_estacion_{estacion}_magnitud_{magnitud}.png")

def contar_dias_exceso(df_largo, limites):
    excesos = df_largo[df_largo.apply(lambda row: row["valor"] > limites.get(row["magnitud"], float('inf')), axis=1)]
    excesos["fecha"] = pd.to_datetime(excesos["fecha"])
    excesos_por_año = excesos.groupby(["año", "magnitud"])["fecha"].nunique().reset_index(name="dias_exceso")
    print("Días con exceso por año y contaminante:")
    print(excesos_por_año)
    return excesos_por_año

def generar_reportes(df_largo):
    matriz_correlacion(df_largo)
    
    for magn in [8, 9]:
        mapa_calor_provincias(df_largo, magnitud=magn)
        evolucion_por_provincia(df_largo, magnitud=magn)
    
    # Top municipios PM10 último año
    ultimo_año = df_largo["año"].max()
    pm10 = df_largo[df_largo["magnitud"] == 8]
    top_munis = (pm10[pm10["año"] == ultimo_año]
                 .groupby(["provincia", "municipio"])["valor"]
                 .mean()
                 .reset_index()
                 .sort_values("valor", ascending=False)
                 .head(10))
    
    print(f"Top 10 municipios con mayor PM10 en {ultimo_año}")
    print(top_munis)
    
    # Ejemplo evolución municipio Madrid (28-079)
    evolucion_por_municipio(df_largo, provincia="28", municipio="079", magnitud=8)
    
    # Tendencia estación ejemplo
    plot_tendencia_estacion(df_largo, estacion="001", magnitud=8)

# --- Programa principal ---
directorio = "downloads/air_quality-daily"

df_todo = cargar_todos_los_csv(directorio)
df_largo = transformar_a_formato_largo(df_todo)
df_largo["valor"] = pd.to_numeric(df_largo["valor"], errors="coerce")
df_largo = df_largo.dropna(subset=["valor"])

df_largo["año"] = df_largo["fecha"].dt.year

# Evolución general
evolucion = df_largo.groupby(["año", "magnitud"])["valor"].mean().reset_index()

plt.figure(figsize=(14, 6))
sns.lineplot(data=evolucion, x="año", y="valor", hue="magnitud")
plt.title("Evolución anual de contaminantes (media diaria)")
plt.ylabel("Concentración media")
plt.grid(True)
guardar_grafico("evolucion_anual_contaminantes.png")

# Limites para excesos
limites = {
    8: 50,   # PM10 diario
    9: 200,  # NO2 horario (aproximación diaria)
}

excesos_por_año = contar_dias_exceso(df_largo, limites)

generar_reportes(df_largo)

print("Análisis y reportes generados. Revisa la carpeta 'reports-air-quality'.")
