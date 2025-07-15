#!/bin/bash

# Activa el entorno de Conda
source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate asma-env

# Descarga de datos (se ejecuta siempre)
if [ "$1" == "descarga" ]; then
    python download_files.py
fi

# Verifica si el argumento "procesar" fue pasado
if [ "$1" == "procesar" ]; then
  echo "Ejecutando scripts de procesamiento..."
  python postprocess_air-quality_d.py
  python postprocess_polen.py
  python prueba-meteo.py
else
  echo "Argumento no proporcionado o incorrecto. Si deseas procesar los archivos, ejecuta:"
  echo "./run_pipeline.sh procesar"
fi