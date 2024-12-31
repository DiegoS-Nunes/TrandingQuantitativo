import os
import re
import json
import zipfile
import requests
import pandas as pd
import pyarrow as pa
from io import BytesIO
import MetaTrader5 as mt5
import pyarrow.parquet as pq
from bs4 import BeautifulSoup
from lib import timeframe_dict
from datetime import datetime, timedelta

class bd():
    def __init__(self, file_path=None, login=None, password=None, server=None):
        if file_path:
            if os.path.exists(file_path):
                try:
                    with open(file_path) as f:
                        credentials = json.load(f)
                    self.login = credentials.get('login')
                    self.password = credentials.get('password')
                    self.server = credentials.get('server')
                except Exception as e:
                    print(f"Erro ao ler as credenciais: {e}")
                    quit()
                else:
                    print("Sucesso ao carregar as credenciais!")
            else:
                print(f"Arquivo {file_path} não encontrado!")
                quit()
        else:
            self.login = login
            self.password = password
            self.server = server
            if not self.login or not self.password or not self.server:
                print("Erro ao ler as credenciais!")
                quit()
        
        if not mt5.initialize(login=self.login, password=self.password, server=self.server):
            print("initialize() failed, error code: ", mt5.last_error())
            mt5.shutdown()
            quit()
        else:
            print("Sucesso ao logar no Meta Trader 5")

        if not os.path.isdir('ohlc'):
            os.makedirs('ohlc')
            for timeframe_dir in timeframe_dict.keys():
                try:
                    os.makedirs(f'ohlc/{timeframe_dir}')
                except Exception as e:
                    print(f"Erro ao criar as pastas de timeframe: {e}")
        if not os.path.isdir('ticks'):
            try:
                os.makedirs('ticks')
                print("Sucesso ao criar o diretório 'ticks'!")
            except Exception as e:
                print(f"Erro ao criar o diretório 'ticks': {e}")

    def update_ohlc(self, symbol, timeframe):
        initial_date = datetime(2000, 1, 1)
        final_date = datetime.now()
    
        path = f'ohlc/{timeframe}/{symbol}_{timeframe}.parquet'

        if not os.path.exists(path):
            df = pd.DataFrame(columns=['time', 'open', 'high', 'low', 'close', 'tick_volume', 'spread', 'real_volume'])
        else:
            try:
                df = pq.ParquetFile(path).read().to_pandas()
                df['time'] = pd.to_datetime(df['time'])
                if df['time'].max() < datetime.now(): 
                    initial_date = df['time'].max()
                else: 
                    return
            except Exception as e:
                return f"Erro ao ler o arquivo Parquet: {e}"

        timedelta_default = timedelta(hours=timeframe_dict[timeframe][1])
        final_date_aux = initial_date + timedelta_default
        timeframe = timeframe_dict[timeframe][0]

        while True:
            data_aux = mt5.copy_rates_range(symbol, timeframe, initial_date, min(final_date_aux, final_date)) 
            df_aux = pd.DataFrame(data_aux)
            df_aux['time'] = pd.to_datetime(df_aux['time'], unit='s')
            df = pd.concat([df_aux, df], ignore_index=True)
            print(df)
            if final_date_aux > final_date: break
            
            initial_date = df_aux['time'].max()
            final_date_aux = initial_date + timedelta_default
        
        df.sort_values(by='time', ascending=False, inplace=True)
        print(f"Salvando {len(df)} linhas no arquivo Parquet.")
        self.salvar_parquet(path, df)

    def update_ticks(self, symbol):
        initial_date = datetime(2020, 1, 1)
        final_date = datetime.now()
        path = f'ticks/{symbol}_ticksrange.parquet'

        if not os.path.exists(path):
            df = pd.DataFrame(columns=['time', 'bid', 'ask', 'last', 'volume', 'time_msc', 'flags', 'volume_real'])
        else:
            df = pq.ParquetFile(path).read().to_pandas()
            df['time'] = pd.to_datetime(df['time'])
            df.sort_values(by='time', ascending=False, inplace=True)
            if df['time'].max() < datetime.now(): 
                initial_date = df['time'].max()

        try:
            ticks_data = mt5.copy_ticks_range(symbol, initial_date, final_date, mt5.COPY_TICKS_TRADE)
            df_aux = pd.DataFrame(ticks_data)
        except Exception as e:
            print('Erro ao obter os dados de ticks: ' + str(e))
            return

        if len(df_aux) > 0:
            df_aux['time'] = pd.to_datetime(df_aux['time'], unit='s')
            df_aux['time_msc'] = pd.to_datetime(df['time_msc'])
            df_aux.sort_values(by='time', ascending=False, inplace=True)
            df = pd.concat([df_aux, df], ignore_index=True)
            self.salvar_parquet(path, df)
        else:
            print("Nenhum dado novo encontrado.")

    def listar_empresas(self):
        BASE_URL = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/"
        response = requests.get(BASE_URL)
        if response.status_code != 200:
            print("Erro ao acessar o site da CVM.")
            return []
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        zip_files = [link.get('href') for link in soup.find_all('a') if link.get('href').endswith('.zip')]
        if not zip_files:
            print("Nenhum arquivo ZIP encontrado.")
            return []
        
        empresas = set()
        
        for zip_file in zip_files:
            url = f"{BASE_URL}{zip_file}"
            response = requests.get(url)
            if response.status_code != 200:
                print(f"Erro ao baixar o arquivo ZIP: {zip_file}")
                continue
            
            with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
                csv_name = next((name for name in zip_file.namelist() if name.endswith('.csv')), None)
                if not csv_name:
                    print(f"Nenhum arquivo CSV encontrado no ZIP: {zip_file}")
                    continue
                
                with zip_file.open(csv_name) as f:
                    df = pd.read_csv(f, sep=';', encoding='ISO-8859-1')
                
                empresas.update(df['DENOM_CIA'].unique())
        
        return sorted(empresas)    

    def update_fundamentus(self, empresa):
        BASE_URL = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/"
        BASE_DIR = "./fundamentus"

        if empresa.endswith('.'):
            empresa_corrigida = empresa[:-1]

        # Cria o diretório da empresa dentro da pasta fundamentus
        empresa_dir = os.path.join(BASE_DIR,empresa_corrigida)
        os.makedirs(empresa_dir, exist_ok=True)

        response = requests.get(BASE_URL)

        if response.status_code != 200:
            print("Erro ao acessar o site da CVM.")
            return
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        zip_files = [link.get('href') for link in soup.find_all('a') if link.get('href').endswith('.zip')]
        
        # Tipos de relatórios a serem processados
        relatorios = ['DRE', 'BPA', 'BPP', 'DFC']
        
        for relatorio in relatorios:
            # Define o caminho do arquivo Parquet
            parquet_path = os.path.join(empresa_dir, f"{empresa}_{relatorio}.parquet")
            
            # Verifica se o arquivo Parquet já existe
            if os.path.exists(parquet_path):
                # Carrega os dados existentes
                df_existente = pd.read_parquet(parquet_path)
                # Extrai os anos já presentes no arquivo Parquet
                anos_existentes = df_existente['ano'].unique()
                print(f"Anos existentes para {relatorio}: {anos_existentes} \n Pulando...")
            else:
                # Se o arquivo não existir, cria um DataFrame vazio
                df_existente = pd.DataFrame()
                anos_existentes = []
            
            # DataFrame temporário para acumular os dados
            df_final = pd.DataFrame()
            
            try:
                for zip_file_name in zip_files:
                    # Extrai o ano do nome do arquivo ZIP
                    ano = int(zip_file_name.split('_')[-1].split('.')[0])
                    
                    # Verifica se o ano já está presente no arquivo Parquet
                    if ano in anos_existentes:
                        continue  # Pula para o próximo arquivo ZIP
                    
                    url = f"{BASE_URL}{zip_file_name}"
                    response = requests.get(url)
                    if response.status_code == 200:
                        with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
                            for csv_name in zip_file.namelist():
                                # Verifica se o arquivo CSV é do relatório atual
                                if csv_name.endswith('.csv') and relatorio in csv_name:
                                    with zip_file.open(csv_name) as f:
                                        df = pd.read_csv(f, sep=';', encoding='ISO-8859-1')
                                    
                                    # Normaliza o nome da empresa para comparação
                                    df_empresa = df[df['DENOM_CIA'].apply(lambda x: re.sub(r'\s+', ' ', x.strip()).lower()) == re.sub(r'\s+', ' ', empresa.strip()).lower()]
                                    
                                    if not df_empresa.empty:
                                        # Evita o SettingWithCopyWarning
                                        df_empresa = df_empresa.copy()
                                        
                                        # Adiciona a coluna 'ANO' ao DataFrame
                                        df_empresa.loc[:, 'ANO'] = ano
                                        
                                        # Adiciona a coluna 'TIPO' ao DataFrame
                                        tipo_consolidacao = 'con' if 'con' in csv_name else 'ind'
                                        df_empresa.loc[:, 'TIPO'] = tipo_consolidacao

                                        # Adiciona a coluna 'METODO' ao DataFrame
                                        if '_MD' in csv_name:
                                            metodo = 'Direto'
                                            df_empresa.loc[:, 'METODO'] = metodo
                                        elif '_MI' in csv_name:
                                            metodo = 'Indireto'
                                            df_empresa.loc[:, 'METODO'] = metodo
                                        
                                        # Filtra apenas o último exercício
                                        df_empresa = df_empresa[df_empresa['ORDEM_EXERC'] == 'ÚLTIMO']
                                        
                                        # Verifica se há dados após a filtragem
                                        if df_empresa.empty:
                                            print(f"Nenhum dado encontrado para o último exercício da empresa {empresa} no ano {ano}.")
                                            continue
                                        
                                        # Define as colunas do índice com base no relatório
                                        if relatorio in ['BPA', 'BPP']:
                                            # Para BPA e BPP, não inclui 'DT_INI_EXERC' no índice
                                            index_cols = ['ANO', 'TIPO', 'CNPJ_CIA', 'CD_CVM', 'DT_REFER', 'DENOM_CIA', 'ESCALA_MOEDA', 'DT_FIM_EXERC', 'CD_CONTA']
                                        elif relatorio in ['DRE']:
                                            # Para DRE, inclui 'DT_INI_EXERC' no índice
                                            index_cols = ['ANO', 'TIPO', 'CNPJ_CIA', 'CD_CVM', 'DT_REFER', 'DENOM_CIA', 'ESCALA_MOEDA', 'DT_INI_EXERC', 'DT_FIM_EXERC', 'CD_CONTA']
                                        else:
                                            # Para DRE, inclui 'DT_INI_EXERC' e 'METODO' no índice
                                            index_cols = ['ANO', 'TIPO', 'METODO', 'CD_CVM', 'CNPJ_CIA', 'DT_REFER', 'DENOM_CIA', 'ESCALA_MOEDA', 'DT_INI_EXERC', 'DT_FIM_EXERC', 'CD_CONTA']
                                        
                                        # Pivotar o DataFrame para transformar as contas em colunas
                                        df_pivot = df_empresa.pivot_table(
                                            index=index_cols,
                                            columns='DS_CONTA',
                                            values='VL_CONTA',
                                            aggfunc='first'  # Assume que cada combinação de índice e coluna tem apenas um valor
                                        ).reset_index()
                                        
                                        # Renomear o índice para facilitar a leitura
                                        df_pivot.columns.name = None  # Remove o nome das colunas (DS_CONTA)
                                        df_pivot = df_pivot.rename_axis(None, axis=1)
                                        
                                        # Concatena os dados no DataFrame final
                                        df_final = pd.concat([df_final, df_pivot], ignore_index=True)
                                        print(f"Concatenado dados do relatório {relatorio} para o ano {ano}.")
                    else:
                        print(f"Erro ao baixar o arquivo ZIP: {zip_file_name}")
                    
                # Verifica se há dados no DataFrame final
                if df_final.empty:
                    print(f"Nenhum dado novo de encontrado para a empresa {empresa} no relatório {relatorio} {ano}.")
                    continue
                
                # Concatena os novos dados com os existentes
                df_final = pd.concat([df_existente, df_final], ignore_index=True)
                try:
                    df_final = df_final.apply(pd.to_numeric)
                except Exception as e:
                    pass
                df_final = df_final.dropna(axis=1, how='all')
                df_final = df_final.fillna(0)

                # Salva o DataFrame final em Parquet
                self.salvar_parquet(parquet_path, df_final)
                print(f"Dados de {empresa} para o relatório {relatorio} salvos em Parquet com sucesso.")
            except Exception as e:
                print(f"Erro ao obter fundamentos da empresa {empresa} para o relatório {relatorio}: {str(e)}")
            
            print(f"FINALIZADO!")

    def update_indicators():
        pass

    def slice(self, type, symbol, initial_date, final_date, timeframe=None):
        path = f'ohlc\\{timeframe}\\{symbol}_{timeframe}.parquet' if type == 'ohlc' else f'ticks\\{symbol}_ticksrange.parquet'
        if not os.path.exists(path):
            print(f"O ativo {symbol} não está registrado, favor criá-lo utilizando a função .update_{type}()")
        else:
            df = pq.ParquetFile(path).read().to_pandas()
            colunas = df.columns.tolist()
            if 'time' in colunas:
                df['time'] = pd.to_datetime(df['time'], unit='s')
                if 'time_msc' in colunas:
                    df['time_msc'] = pd.to_datetime(df['time_msc'], unit='ms')
                return df.loc[(df['time'] >= initial_date) & (df['time'] < final_date)]

    def read_ohlc(self, symbol, timeframe, initial_date=datetime(2012, 1, 1), final_date=datetime.now()):
        return self.slice('ohlc', symbol, initial_date, final_date, timeframe)

    def read_ticks(self, symbol, initial_date=datetime(1970, 1, 1), final_date=datetime.now()):
        return self.slice('ticks', symbol, initial_date, final_date)

    def read_fundamentus(self, empresa, relatorio='DRE'):
        if empresa.endswith('.'):
            empresa_corrigida = empresa[:-1]
        
        path = f'fundamentus\\{empresa_corrigida}\\{empresa}_{relatorio}.parquet'
        df = pq.ParquetFile(path).read().to_pandas()
        return df
    
    def get_symbols(self):
        pass

    def salvar_parquet(self, path, df):
        try:
            table = pa.Table.from_pandas(df)
            writer = pq.ParquetWriter(path, table.schema, compression='snappy')
            writer.write_table(table)
            writer.close()
            print(f"Sucesso ao salvar o arquivo Parquet!")
        except Exception as e:
            print(f"Erro ao escrever o arquivo Parquet: {e}")
