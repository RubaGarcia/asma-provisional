#!/bin/bash

conda activate asma-env

python download_files.py 

python postprocess_air-quality_d.py
python postprocess_polen.py
python prueba_meteo.py