import io
import zipfile
import requests
import polars as pl
from bs4 import BeautifulSoup, Tag
import zipfile
import io
import fnmatch
import unicodedata
import re

def get_zip_links(BASE_URL):
    response = requests.get(BASE_URL)
    soup = BeautifulSoup(response.text, 'html.parser')
    zip_files = []
    for link in soup.find_all('a'):
        if isinstance(link, Tag):
            href = link.get('href')
            if isinstance(href, str) and href.endswith('.zip'):
                zip_files.append(BASE_URL+href)
    return zip_files

def extract_csv_from_zip(zip_bytes, name_file):
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        for name in z.namelist():
            if fnmatch.fnmatch(name, name_file):
                print("Extracting:", name)
                return io.BytesIO(z.read(name))
    return None

def process_files(BASE_URL, col_map, name_file):
    links = get_zip_links(BASE_URL)
    dfs = []
    for link in links:
        resp = requests.get(link)
        csv_bytes = extract_csv_from_zip(resp.content, name_file)
        if csv_bytes:
            df = pl.read_csv(csv_bytes, separator=';', encoding='iso-8859-1')
            df = rename_columns(df, col_map)
            dfs.append(df)
    if dfs:
        return pl.concat(dfs, how='vertical')
    return pl.DataFrame()

def rename_columns(df, col_map):
    df = df.rename({k: v for k, v in col_map.items() if k in df.columns})
    return df

def normalize_text(text):
    if not isinstance(text, str):
        return text
    text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII')
    text = re.sub(r'[^\w\s]', '', text)
    text = ' '.join(text.lower().split())
    return text