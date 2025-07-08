import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

def cargar_todos_los_csv(directorio):
    archivos = [f for f in os.listdir(directorio) if f.endswith(".csv")]
    dfs = []
    
    for archivo in archivos:
        print(archivo)
        path = os.path.join(directorio, archivo)
        try:
            df = pd.read_csv(path, sep=";")
            dfs.append(df)
        except Exception as e:
            print(f"Error leyendo {archivo}: {e}")
    
    df_total = pd.concat(dfs, ignore_index=True)
    return df_total

def transformar_a_formato_largo(df):
    # Crear lista de días
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
                        "provincia": fila["PROVINCIA"],
                        "municipio": fila["MUNICIPIO"],
                        "estacion": fila["ESTACION"],
                        "magnitud": fila["MAGNITUD"],
                        "valor": valor,
                        "validacion": validez
                    })
                except:
                    continue  # Evita errores por fechas no válidas (ej: 30/02)
    
    df_largo = pd.DataFrame(registros)
    return df_largo


# Ruta a tu carpeta
df_todo = cargar_todos_los_csv("downloads/air_quality-daily")

df_largo = transformar_a_formato_largo(df_todo)
df_largo["valor"] = pd.to_numeric(df_largo["valor"], errors="coerce")
df_largo = df_largo.dropna(subset=["valor"])


df_largo["año"] = df_largo["fecha"].dt.year
evolucion = df_largo.groupby(["año", "magnitud"])["valor"].mean().reset_index()



plt.figure(figsize=(14, 6))
sns.lineplot(data=evolucion, x="año", y="valor", hue="magnitud")
plt.title("Evolución anual de contaminantes (media diaria)")
plt.ylabel("Concentración media")
plt.grid(True)
plt.tight_layout()
plt.show()

# Suponiendo: 8 = PM10, 9 = NO2
limites = {
    8: 50,   # PM10 diario
    9: 200,  # NO2 horario (usamos diario como aproximación)
}

excesos = df_largo[df_largo.apply(lambda row: row["valor"] > limites.get(row["magnitud"], float('inf')), axis=1)]

# Número de días por año con exceso
# Contar días únicos con exceso por año y contaminante
excesos["fecha"] = pd.to_datetime(excesos["fecha"])
excesos_por_año = excesos.groupby(["año", "magnitud"])["fecha"].nunique().reset_index(name="dias_exceso")


print(excesos_por_año)

tendencia_estaciones = df_largo.groupby(["año", "estacion", "magnitud"])["valor"].mean().reset_index()

# Gráfico para una estación y contaminante concretos
df_est = tendencia_estaciones[(tendencia_estaciones["estacion"] == "001") & (tendencia_estaciones["magnitud"] == 8)]

plt.figure(figsize=(10, 5))
sns.lineplot(data=df_est, x="año", y="valor")
plt.title("Tendencia de PM10 en estación 001")
plt.ylabel("µg/m³")
plt.grid(True)
plt.show()


df_largo.sort_values("fecha", inplace=True)
df_largo["media_movil_30d"] = df_largo.groupby("magnitud")["valor"].transform(lambda x: x.rolling(window=30, min_periods=1).mean())


# Pivot para tener una columna por contaminante
pivot = df_largo.pivot_table(index="fecha", columns="magnitud", values="valor", aggfunc="mean")
correlacion = pivot.corr()
plt.figure(figsize=(10, 8))
sns.heatmap(correlacion, annot=True, cmap="coolwarm", center=0, fmt=".2f")
plt.title("Matriz de correlación entre contaminantes")
plt.tight_layout()
plt.show()


df_largo["provincia"] = df_largo["provincia"].astype(str).str.zfill(2)

evol_prov = df_largo.groupby(["año", "provincia", "magnitud"])["valor"].mean().reset_index()

# Por ejemplo, evolución de PM10 (magnitud 8) en varias provincias
pm10_prov = evol_prov[evol_prov["magnitud"] == 8]

plt.figure(figsize=(14, 6))
sns.lineplot(data=pm10_prov, x="año", y="valor", hue="provincia")
plt.title("Evolución anual de PM10 por provincia")
plt.ylabel("µg/m³")
plt.grid(True)
plt.tight_layout()
plt.show()


media_muni = df_largo.groupby(["año", "provincia", "municipio", "magnitud"])["valor"].mean().reset_index()
print(media_muni.head())


ultimo_año = df_largo["año"].max()
pm10_ultimo = media_muni[(media_muni["magnitud"] == 8) & (media_muni["año"] == ultimo_año)]

top_municipios = pm10_ultimo.sort_values("valor", ascending=False).head(10)
print(top_municipios)

df_largo["municipio"] = df_largo["municipio"].astype(str).str.zfill(3)

muni = "079"  # Ejemplo: Madrid ciudad
prov = "28"   # Comunidad de Madrid

pm10_muni = df_largo[(df_largo["provincia"] == prov) & 
                     (df_largo["municipio"] == muni) &
                     (df_largo["magnitud"] == 8)]

pm10_muni_anual = pm10_muni.groupby("año")["valor"].mean().reset_index()

plt.figure(figsize=(10, 5))
sns.lineplot(data=pm10_muni_anual, x="año", y="valor")
plt.title(f"Evolución anual PM10 en municipio {prov}-{muni}")
plt.ylabel("µg/m³")
plt.grid(True)
plt.tight_layout()
plt.show()