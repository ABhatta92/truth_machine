import requests
import zipfile
import os
import glob
from prefect import task

project_root = os.path.dirname(os.getcwd())
data_root = os.path.join(project_root, 'data')
raw_path = os.path.join(data_root, 'raw')
print(data_root)
print(raw_path)

def get_zip_data(url):
    zip_file_name = url.split('/')[-1]
    folder_name = os.path.join(raw_path, zip_file_name.split('.')[0])
    response = requests.get(url)

    with open(zip_file_name, 'wb') as f:
        f.write(response.content)

    with zipfile.ZipFile(zip_file_name, 'r') as zip_ref:
        zip_ref.extractall(f'{folder_name}/unzipped')
    
    print(f'extracted and unzipped files to {folder_name}/unzipped')

def get_file_data(url):
    file_name = url.split('/')[-1]
    folder_name = os.path.join(raw_path, file_name.split('.')[0])
    response = requests.get(url)
    
    os.makedirs(folder_name, exist_ok=True)
    file_path = os.path.join(folder_name, file_name)
    with open(file_path, 'wb') as f:
        f.write(response.content)

    print(f'extracted file to {folder_name}')

# 'https://datacatalogfiles.worldbank.org/ddh-published-v2/0037712/35/DR0045575/WDI_CSV_2025_01_28.zip'

@task
def ingest_raw():
    zip_urls = ['https://datacatalogfiles.worldbank.org/ddh-published-v2/0037798/7/DR0092042/GemDataEXTR.zip',
            'https://databank.worldbank.org/data/download/IDS_CSV.zip',
            'https://databank.worldbank.org/data/download/HNP_Stats_CSV.zip',
            'https://databank.worldbank.org/data/download/Gender_Stats_CSV.zip',
            'https://databank.worldbank.org/data/download/Jobs_CSV.zip']

    file_urls = ['https://datacatalogfiles.worldbank.org/ddh-published-v2/0038480/4/DR0047022/Edstats_Updated.csv',
                'https://dataunodc.un.org/sites/dataunodc.un.org/files/data_cts_violent_and_sexual_crime.xlsx',
                'https://dataunodc.un.org/sites/dataunodc.un.org/files/data_cts_access_and_functioning_of_justice.xlsx',
                'https://dataunodc.un.org/sites/dataunodc.un.org/files/data_cts_corruption_and_economic_crime.xlsx'
                ]

    api_url = 'https://api.worldbank.org/pip/v1/pip'

    for url in zip_urls:
        get_zip_data(url)

    for url in file_urls:
        get_file_data(url)

    zip_files = glob.glob("*.zip")

    for file in zip_files:
        try:
            os.remove(file)
            print(f"✅ Deleted: {file}")
        except Exception as e:
            print(f"❌ Failed to delete {file}: {e}")