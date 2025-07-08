import os
import requests
from urllib.parse import urlparse
import zipfile
from io import BytesIO

def download_files(name, url_folder='urls', download_folder='downloads', filter_ext=None):
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DOWNLOAD_DIR = os.path.join(BASE_DIR, download_folder, name)
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    URLS_FILE = os.path.join(BASE_DIR, url_folder, name + '.txt')
    with open(URLS_FILE, 'r') as f:
        urls = [line.strip() for line in f if line.strip()]

    for url in urls:
        try:
            response = requests.get(url)
            response.raise_for_status()

            parsed_url = urlparse(url)
            filename = os.path.basename(parsed_url.path)
            if not filename:
                filename = 'archivo_descargado'

            if filename.lower().endswith('.zip'):
                # Si es polen_quantity, extrae en la carpeta principal, si no, en subcarpeta
                if name == 'polen_quantity':
                    extract_path = DOWNLOAD_DIR
                else:
                    folder_name = os.path.splitext(filename)[0]
                    extract_path = os.path.join(DOWNLOAD_DIR, folder_name)
                    os.makedirs(extract_path, exist_ok=True)
                with zipfile.ZipFile(BytesIO(response.content)) as z:
                    if filter_ext:
                        files = [f for f in z.namelist() if f.lower().endswith(filter_ext)]
                    else:
                        files = z.namelist()
                    for file in files:
                        z.extract(file, extract_path)
                print(f'Descargado y descomprimido en: {extract_path}')
            else:
                filepath = os.path.join(DOWNLOAD_DIR, filename)
                with open(filepath, 'wb') as f_out:
                    f_out.write(response.content)
                print(f'Descargado: {filename}')
        except Exception as e:
            print(f'Error al descargar {url}: {e}')