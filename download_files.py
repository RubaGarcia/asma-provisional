import argparse
from utilities import download_files
from download_aemet import download_aemet_files

def main():
    parser = argparse.ArgumentParser(description="Descarga ficheros de calidad del aire o polen.")
    parser.add_argument(
        "--type",
        choices=["air_quality-hourly", "air_quality-daily", "polen_quantity", "aemet"],
        help="Tipo de descarga: air_quality-hourly, air_quality-daily, polen_quantity o aemet.",
    )
    args = parser.parse_args()

    if args.type == "air_quality-hourly":
        download_files('air_quality-hourly', filter_ext='.csv')
    elif args.type == "air_quality-daily":
        download_files('air_quality-daily', filter_ext='.csv')
    elif args.type == "polen_quantity":
        download_files('polen_quantity')
    elif args.type == "aemet":
        download_aemet_files()
    else:
        # Si no se especifica --type, se descargan todos
        download_files('air_quality-hourly', filter_ext='.csv')
        download_files('air_quality-daily', filter_ext='.csv')
        download_files('polen_quantity')
        download_aemet_files()

if __name__ == "__main__":
    main()