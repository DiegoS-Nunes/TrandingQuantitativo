import re
import requests
import polars as pl
from bs4 import BeautifulSoup
from typing import Dict, List
from ..repositories.dfp_repository import DfpRepository
from ..utils.file_utils import download_and_extract_zip

class DfpService:
    def __init__(self):
        self.BASE_URL_DFP = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/"

    def update_dfp_data(self) -> str:  #-> Dict[str, str]:
        self.repository = dfp_repository()
        results = {}
        zip_files = self._get_available_zip_files()
        
        for zip_file in zip_files:
            year = self._extract_year_from_filename(zip_file)
            csv_files = download_and_extract_zip(f"{self.BASE_URL_DFP}{zip_file}")
            
            for csv_name, csv_content in csv_files.items():
                report_type = self._identify_report_type(csv_name)
                if not report_type:
                    continue
                    
                df = pl.read_csv(csv_content, separator=";", encoding="iso-8859-1")
                df = df.with_columns([pl.col(col).str.to_uppercase() for col in df.columns if pl.col(col).dtype == pl.Utf8])
                
                consolidation_type = 'IND' if 'ind' in csv_name.lower() else 'CON'
                method = 'MD' if 'MD' in csv_name else 'MI' if 'MI' in csv_name else None
                
                processed_df = self._process_dataframe(df, report_type, year, consolidation_type, method)
                self.repository.save_report_data(processed_df, report_type, year, consolidation_type, method)
                
                results[f"{report_type}_{year}_{consolidation_type}"] = "Processed"
        
        return results

    def _get_available_zip_files(self) -> List[str]:
        response = requests.get(self.BASE_URL_DFP)
        if response.status_code != 200:
            return []
        soup = BeautifulSoup(response.text, 'html.parser')
        from bs4 import Tag
        zip_files = []
        for link in soup.find_all('a'):
            if isinstance(link, Tag):
                href = link.get('href')
                if isinstance(href, str) and href.endswith('.zip'):
                    zip_files.append(href)
        return zip_files

    def _extract_year_from_filename(self, filename: str) -> str:
        match = re.search(r'(\d{4})', filename)
        return match.group(1) if match else ""

    def _identify_report_type(self, filename: str) -> str:
        for report in ['DRE', 'BPA', 'BPP', 'DFC', 'BPP', 'DRA', 'DMPL', 'DVA']:
            if report in filename:
                return report
        return None

    def _process_dataframe(self, df: pl.DataFrame, report_type: str, year: str, consolidation_type: str, method: str) -> pl.DataFrame:
        # Filtra as colunas obrigatórias
        colunas_obrigatorias = [
            'CNPJ_CIA', 'DENOM_CIA', 'CD_CVM', 'MOEDA', 'ESCALA_MOEDA',
            'ORDEM_EXERC', 'CD_CONTA', 'DS_CONTA'
        ]
        # Filtra apenas a empresa específica se necessário (exemplo: pode receber como parâmetro)
        # df = df.filter(pl.col('DENOM_CIA') == empresa)

        # Filtra apenas o último exercício
        if 'ORDEM_EXERC' in df.columns:
            df = df.filter(pl.col('ORDEM_EXERC') == 'ÚLTIMO')

        # Converte DS_CONTA para minúsculo
        if 'DS_CONTA' in df.columns:
            df = df.with_columns(pl.col('DS_CONTA').str.to_lowercase())

        # Adiciona o ano e o tipo de consolidação como sufixo nas colunas de valores
        new_columns = []
        for col in df.columns:
            if col.startswith('VL_CONTA') and report_type != 'DFC':
                new_columns.append(f"{col}_{consolidation_type}_{year}")
            else:
                if method:
                    new_columns.append(f"{col}_{consolidation_type}_{method}_{year}")
                else:
                    new_columns.append(f"{col}_{consolidation_type}_{year}")
        df.columns = new_columns

        # Filtra as colunas finais
        colunas_finais = [col for col in new_columns if any(base in col for base in colunas_obrigatorias) or col.startswith('VL_CONTA')]
        df = df.select(colunas_finais)