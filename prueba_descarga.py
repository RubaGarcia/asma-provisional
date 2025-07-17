import datetime
import warnings
from pathlib import Path
import os
import sys
import cdsapi
import logging
from concurrent.futures import ThreadPoolExecutor

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

warnings.filterwarnings("ignore")

def download_file(catalogue_id: str, catalogue_entry: dict, output_path: Path) -> Path:
    """
    Download a file from a given catalogue ID with the given parameters.

    This method retrieves the file from the CDS API, saves it in a temporary
    directory, and returns the `Path` object pointing to the downloaded file.

    Parameters
    ----------
    catalogue_id : str
        A string containing the CDS catalogue id. For instance: projections-cmip6.
    catalogue_entry: dict
        A dictionary containing the fields of the requested data.
    output_path: Path
        Path associated with the request params.

    Returns
    -------
    output_path: Path
        A `Path` object pointing to the downloaded file.

    Raises
    ------
    Exception
        If the download request failed.
    """
    start_time = datetime.datetime.now()
    c = cdsapi.Client(timeout=500, quiet=True)

    logging.info(f"Downloading the data from {catalogue_id} with parameters {catalogue_entry}")
    r = c.retrieve(catalogue_id, catalogue_entry)

    r.download(output_path)

    end_time = datetime.datetime.now()
    final_time = end_time - start_time
    logging.info(f"Duration of the process to download data: {final_time}")

    return output_path

def create_request(var,   year):
    return {
        "variable": [var],
        "level_type": ["surface"],
        "product_type": ["analysis"],
        "year": year,
        "month": [
        "01", "02", "03",
        "04", "05", "06",
        "07", "08", "09",
        "10", "11", "12"
        ],
        "day": [
            "01", "02", "03", "04", "05", "06",
            "07", "08", "09", "10", "11", "12",
            "13", "14", "15", "16", "17", "18",
            "19", "20", "21", "22", "23", "24",
            "25", "26", "27", "28", "29", "30",
            "31"
        ],
        "time": ["06:00"],
        "data_format": "netcdf"
    }

def get_output_filename(var, dataset,  year):
    return f"{var}-{dataset}-surface_or_atmosphere-forecast-{year[0]}.nc"

def download_cerra_files(var,root, dataset,  year_list, month_list):
    """
    Download CERRA files for the specified variables, years, and months.

    Parameters
    ----------
    var : str
        The variable to be downloaded.
    root : str
        The root directory where the files will be saved.
    dataset : str
        The dataset name.
    Dict : dict
        A dictionary containing the time and leadtime_hour for different specifications.
    year_list : list
        List of years to download data for.
    month_list : list
        List of months to download data for.
    spec_list : list
        List of specifications to download data for.
    """
    dest_dir = Path(root) /var
    dest_dir.mkdir(parents=True, exist_ok=True)

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for year in year_list:
                request = create_request(var,  [year], )
                file = get_output_filename(var, dataset,  [year])
                path_file = dest_dir / file
                if path_file.exists():
                    logging.info(f"{path_file} already exists, next")
                    continue
                futures.append(executor.submit(download_file, dataset, request, path_file))

        for future in futures:
            try:
                future.result()
            except Exception as e:
                logging.error(f"Failed to download file: {e}")

def main():
    var="total_precipitation"
    var_out="pr"

    root=f"/home/garciar/Desktop/prueba"
    dataset = "reanalysis-cerra-land"
    year_list = [str(year) for year in range(1984, 2000)]
    month_list = [f"{month:02d}" for month in range(1, 13)]

    try:
        index = int(sys.argv[1])
        logging.info(f"Downloading CERRA files for {var} and spec ")
    except:
        logging.info("Downloading all specs")

    download_cerra_files(var,  root, dataset,  year_list, month_list)

if __name__ == "__main__":
   main()






