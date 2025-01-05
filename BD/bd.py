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
class bd:
    # URLs base para DFP e FRE
    BASE_URL_DFP = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/"
    BASE_URL_FRE = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/FRE/DADOS/"

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

        self.criar_diretorios()

    def criar_diretorios(self):
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

    def processar_arquivo_dfp(self, zip_file_name, empresa, dfs_relatorios, empresa_dir, ano):
        url = f"{self.BASE_URL_DFP}{zip_file_name}"
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Erro ao baixar o arquivo ZIP: {zip_file_name}")
            return

        with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
            for csv_name in zip_file.namelist():
                if csv_name.endswith('.csv'):
                    for relatorio in ['DRE', 'BPA', 'BPP', 'DFC']:
                        if relatorio in csv_name:
                            # Verifica se o ano já foi processado para este relatório
                            parquet_path = os.path.join(empresa_dir, f"{empresa}_{relatorio.upper()}.parquet")
                            tipo_consolidacao = 'IND' if 'ind' in csv_name else 'CON'
                            
                            if 'MD' in csv_name:
                                metodo = 'MD'
                            elif 'MI' in csv_name:
                                metodo = 'MI'
                            else: None
                                                    
                            if os.path.exists(parquet_path):
                                df_existente = pd.read_parquet(parquet_path)
                                if any(str(ano) in col for col in df_existente.columns):
                                    print(f"Ano {ano} já processado para o relatório {relatorio.upper()} {tipo_consolidacao}. Pulando...")
                                    continue

                            with zip_file.open(csv_name) as f:
                                df = pd.read_csv(f, sep=';', encoding='ISO-8859-1')

                            # Converte os nomes das colunas para maiúsculas
                            df.columns = df.columns.str.upper()

                            # Filtra os dados da empresa específica
                            df_empresa = df[df['DENOM_CIA'].str.strip() == empresa.strip()]

                            # Filtra apenas o último exercício
                            if 'ORDEM_EXERC' in df_empresa.columns:
                                df_empresa = df_empresa[df_empresa['ORDEM_EXERC'] == 'ÚLTIMO']

                            if not df_empresa.empty:
                                # Cria uma cópia explícita do DataFrame para evitar SettingWithCopyWarning
                                df_empresa = df_empresa.copy()

                                # Converte DS_CONTA para minúsculo
                                if 'DS_CONTA' in df_empresa.columns:
                                    df_empresa['DS_CONTA'] = df_empresa['DS_CONTA'].str.lower()

                                # Adiciona o ano e o tipo de consolidação como sufixo nas colunas de valores
                                for col in df_empresa.columns:
                                    if col.startswith('VL_CONTA') and relatorio != 'DFC':
                                        df_empresa.rename(columns={col: f"{col}_{tipo_consolidacao}_{ano}"}, inplace=True)
                                    else:
                                        df_empresa.rename(columns={col: f"{col}_{tipo_consolidacao}_{metodo}_{ano}"}, inplace=True)

                                # Define as colunas obrigatórias
                                colunas_obrigatorias = [
                                    'CNPJ_CIA', 'DENOM_CIA', 'CD_CVM', 'MOEDA', 'ESCALA_MOEDA',
                                    'ORDEM_EXERC', 'CD_CONTA', 'DS_CONTA'
                                ]

                                # Filtra as colunas do DataFrame
                                colunas_finais = [col for col in colunas_obrigatorias if col in df_empresa.columns]
                                colunas_finais.extend([col for col in df_empresa.columns if col.startswith('VL_CONTA')])
                                df_empresa = df_empresa[colunas_finais]

                                # Concatena os dados no DataFrame correspondente ao relatório
                                if dfs_relatorios[relatorio].empty:
                                    dfs_relatorios[relatorio] = df_empresa
                                    print(f"{relatorio} {ano} criado com sucesso.")
                                    print(dfs_relatorios[relatorio])
                                else:
                                    dfs_relatorios[relatorio] = pd.merge(
                                        dfs_relatorios[relatorio],
                                        df_empresa,
                                        on=['CNPJ_CIA', 'DENOM_CIA', 'CD_CVM', 'MOEDA', 'ESCALA_MOEDA', 'ORDEM_EXERC', 'CD_CONTA', 'DS_CONTA'],
                                        how='outer'
                                    )
                                    print(f"Dados de {relatorio} {ano} atualizados.")

    def processar_arquivo_fre(self, zip_file_name, empresa, dfs_relatorios, ano):
        url = f"{self.BASE_URL_FRE}{zip_file_name}"
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Erro ao baixar o arquivo ZIP: {zip_file_name}")
            return

        print(f'Baixou o arquivo {ano}')

        with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
            # Processa cada CSV dentro do ZIP
            for csv_name in zip_file.namelist():
                if csv_name.endswith('.csv') and f'distribuicao_capital_{ano}' in csv_name.lower():
                    print(f'Atendeu a condição {ano}')

                    with zip_file.open(csv_name) as f:
                        df = pd.read_csv(f, sep=';', encoding='ISO-8859-1')

                    # Converte os nomes das colunas para maiúsculas e remove espaços extras
                    df.columns = df.columns.str.upper().str.strip()

                    # Filtra os dados da empresa específica
                    df_empresa = df[df['NOME_COMPANHIA'].str.strip().str.upper() == empresa.strip().upper()]

                    if not df_empresa.empty:
                        print(f'Encontrou dados {ano}')
                        # Define as colunas necessárias
                        colunas_necessarias = [
                            'CNPJ_COMPANHIA', 'DATA_REFERENCIA', 'ID_DOCUMENTO', 'NOME_COMPANHIA',
                            'QUANTIDADE_ACIONISTAS_PF', 'QUANTIDADE_ACIONISTAS_PJ', 'QUANTIDADE_ACIONISTAS_INVESTIDORES_INSTITUCIONAIS',
                            'QUANTIDADE_ACOES_ORDINARIAS_CIRCULACAO', 'PERCENTUAL_ACOES_ORDINARIAS_CIRCULACAO',
                            'QUANTIDADE_ACOES_PREFERENCIAIS_CIRCULACAO', 'PERCENTUAL_ACOES_PREFERENCIAIS_CIRCULACAO',
                            'QUANTIDADE_TOTAL_ACOES_CIRCULACAO', 'PERCENTUAL_TOTAL_ACOES_CIRCULACAO'
                        ]

                        # Verifica se as colunas necessárias existem no DataFrame
                        colunas_disponiveis = [col for col in colunas_necessarias if col in df_empresa.columns]
                        if len(colunas_disponiveis) < len(colunas_necessarias):
                            print(f"Aviso: Algumas colunas necessárias não foram encontradas no arquivo {csv_name}.")
                            print(f"Colunas disponíveis: {df_empresa.columns.tolist()}")
                            continue

                        # Filtra as colunas do DataFrame
                        df_empresa = df_empresa[colunas_disponiveis]

                        # Calcula as quantidades totais de ações usando regra de três
                        if 'QUANTIDADE_ACOES_ORDINARIAS_CIRCULACAO' in df_empresa.columns and 'PERCENTUAL_ACOES_ORDINARIAS_CIRCULACAO' in df_empresa.columns:
                            df_empresa['QUANTIDADE_ACOES_ORDINARIAS'] = (
                                df_empresa['QUANTIDADE_ACOES_ORDINARIAS_CIRCULACAO'] / df_empresa['PERCENTUAL_ACOES_ORDINARIAS_CIRCULACAO'] * 100
                            )
                        if 'QUANTIDADE_ACOES_PREFERENCIAIS_CIRCULACAO' in df_empresa.columns and 'PERCENTUAL_ACOES_PREFERENCIAIS_CIRCULACAO' in df_empresa.columns:
                            df_empresa['QUANTIDADE_ACOES_PREFERENCIAIS'] = (
                                df_empresa['QUANTIDADE_ACOES_PREFERENCIAIS_CIRCULACAO'] / df_empresa['PERCENTUAL_ACOES_PREFERENCIAIS_CIRCULACAO'] * 100
                            )
                        if 'QUANTIDADE_TOTAL_ACOES_CIRCULACAO' in df_empresa.columns and 'PERCENTUAL_TOTAL_ACOES_CIRCULACAO' in df_empresa.columns:
                            df_empresa['QUANTIDADE_TOTAL_ACOES'] = (
                                df_empresa['QUANTIDADE_TOTAL_ACOES_CIRCULACAO'] / df_empresa['PERCENTUAL_TOTAL_ACOES_CIRCULACAO'] * 100
                            )

                        # Adiciona os dados de distribuição de capital ao dfs_relatorios
                        if 'DISTRIBUICAO CAPITAL' not in dfs_relatorios:
                            dfs_relatorios['DISTRIBUICAO CAPITAL'] = pd.DataFrame()

                        # Concatena os dados verticalmente (axis=0)
                        dfs_relatorios['DISTRIBUICAO CAPITAL'] = pd.concat(
                            [dfs_relatorios['DISTRIBUICAO CAPITAL'], df_empresa],
                            axis=0,
                            ignore_index=True 
                        )

    def update_fundamentus(self, empresa):
        BASE_DIR = "./fundamentus"

        # Corrige o nome da empresa se terminar com '.'
        if empresa.endswith('.'):
            empresa_corrigida = empresa[:-1]
        else:
            empresa_corrigida = empresa

        # Cria o diretório da empresa dentro da pasta fundamentus
        empresa_dir = os.path.join(BASE_DIR, empresa_corrigida)
        os.makedirs(empresa_dir, exist_ok=True)

        # Obtém a lista de arquivos ZIP disponíveis no site da CVM (DFP e FRE)
        response_dfp = requests.get(self.BASE_URL_DFP)
        response_fre = requests.get(self.BASE_URL_FRE)
        if response_dfp.status_code != 200 or response_fre.status_code != 200:
            print("Erro ao acessar o site da CVM.")
            return

        soup_dfp = BeautifulSoup(response_dfp.text, 'html.parser')
        soup_fre = BeautifulSoup(response_fre.text, 'html.parser')
        zip_files_dfp = [link.get('href') for link in soup_dfp.find_all('a') if link.get('href').endswith('.zip')]
        zip_files_fre = [link.get('href') for link in soup_fre.find_all('a') if link.get('href').endswith('.zip')]

        # Dicionário para armazenar os DataFrames de cada relatório
        dfs_relatorios = {relatorio: pd.DataFrame() for relatorio in ['DRE', 'BPA', 'BPP', 'DFC']}

        # Processa cada arquivo ZIP do DFP
        for zip_file_name in zip_files_dfp:
            # Extrai o ano do nome do arquivo ZIP
            ano = int(zip_file_name.split('_')[-1].split('.')[0])

            # Verifica se o ano já foi processado para todos os relatórios
            todos_ja_processados = True
            for relatorio in dfs_relatorios.keys():
                parquet_path = os.path.join(empresa_dir, f"{empresa}_{relatorio.upper()}.parquet")
                if os.path.exists(parquet_path):
                    df_existente = pd.read_parquet(parquet_path)
                    colunas_ano = [col for col in df_existente.columns if f"_{ano}" in col]
                    if not colunas_ano:
                        # Se o ano não estiver presente, marca para atualizar
                        todos_ja_processados = False
                        break
                else:
                    # Se o arquivo Parquet não existir, marca para atualizar
                    todos_ja_processados = False
                    break

            if todos_ja_processados:
                print(f"Ano {ano} já processado para todos os relatórios. Pulando...")
                continue

            # Processa o arquivo ZIP
            self.processar_arquivo_dfp(zip_file_name, empresa, dfs_relatorios, empresa_dir, ano)

        # Processa cada arquivo ZIP do FRE
        for zip_file_name in zip_files_fre:
            # Extrai o ano do nome do arquivo ZIP
            ano = int(zip_file_name.split('_')[-1].split('.')[0])

            # Verifica se o ano já foi processado para DISTRIBUICAO CAPITAL
            parquet_path = os.path.join(empresa_dir, f"{empresa}_DISTRIBUICAO CAPITAL.parquet")
            if os.path.exists(parquet_path):
                df_existente = pd.read_parquet(parquet_path)
                if not df_existente.empty:
                    ultima_data = pd.to_datetime(df_existente['DATA_REFERENCIA']).max()
                    if pd.to_datetime(f"{ano}-12-31") <= ultima_data:
                        print(f"Ano {ano} já processado para DISTRIBUICAO CAPITAL. Pulando...")
                        continue

            # Processa o arquivo ZIP
            self.processar_arquivo_fre(zip_file_name, empresa, dfs_relatorios, ano)

        # Salva os DataFrames processados em arquivos Parquet
        for relatorio, df in dfs_relatorios.items():
            if not df.empty:
                parquet_path = os.path.join(empresa_dir, f"{empresa}_{relatorio.upper()}.parquet")
                self.salvar_parquet(parquet_path, df)
                print(f"Dados de {empresa} para o relatório {relatorio.upper()} salvos em Parquet com sucesso.")
            else:
                print(f"Nenhum dado encontrado para o relatório {relatorio.upper()} da empresa {empresa}.")

        print("Processamento concluído!")

    def salvar_parquet(self, path, df):
        try:
            table = pa.Table.from_pandas(df)
            writer = pq.ParquetWriter(path, table.schema, compression='snappy')
            writer.write_table(table)
            writer.close()
            print(f"Sucesso ao salvar o arquivo Parquet!")
        except Exception as e:
            print(f"Erro ao escrever o arquivo Parquet: {e}")

    def read_ohlc(self, symbol, timeframe, initial_date=datetime(2012, 1, 1), final_date=datetime.now()):
        return self.slice('ohlc', symbol, initial_date, final_date, timeframe)

    def read_ticks(self, symbol, initial_date=datetime(1970, 1, 1), final_date=datetime.now()):
        return self.slice('ticks', symbol, initial_date, final_date)

    def read_fundamentus(self, empresa, relatorio, ano_inicial=2000, ano_final=None):
        # Corrige o nome da empresa se terminar com '.'
        if empresa.endswith('.'):
            empresa_corrigida = empresa[:-1]
        else:
            empresa_corrigida = empresa

        # Define o caminho do arquivo
        path = f'fundamentus\\{empresa_corrigida}\\{empresa}_{relatorio}.parquet'

        # Lê o arquivo Parquet e converte para DataFrame
        df = pq.ParquetFile(path).read().to_pandas()

        # Define o ano_final como o ano atual se não for fornecido
        if ano_final is None:
            ano_final = datetime.now().year

        # Identifica as colunas 'VL_CONTA' que estão no intervalo de anos
        colunas_vl_conta_filtradas = [
            col for col in df.columns
            if 'VL_CONTA' in col and any(str(ano) in col for ano in range(ano_inicial, ano_final + 1))
        ]

        # Filtra as linhas onde pelo menos uma linha das colunas de 'VL_CONTA' não é nula ou zero
        if colunas_vl_conta_filtradas:
            df = df[df[colunas_vl_conta_filtradas].notna().any(axis=1) & (df[colunas_vl_conta_filtradas] != 0).any(axis=1)]

        # Remove as colunas 'VL_CONTA' que não estão no intervalo de anos
        colunas_nao_vl_conta = [col for col in df.columns if 'VL_CONTA' not in col]
        colunas_finais = colunas_nao_vl_conta + colunas_vl_conta_filtradas
        df = df[colunas_finais]

        return df

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

    def calcular_indicadores_anuais(self, empresa):
        # Carrega os dados dos relatórios
        dre = self.read_fundamentus(empresa, 'DRE')
        bpa = self.read_fundamentus(empresa, 'BPA')
        bpp = self.read_fundamentus(empresa, 'BPP')
        dfc = self.read_fundamentus(empresa, 'DFC')
        distribuicao_capital = self.read_fundamentus(empresa, 'DISTRIBUICAO CAPITAL')

        # Verifica se os dados estão disponíveis
        if dre.empty or bpa.empty or bpp.empty or dfc.empty or distribuicao_capital.empty:
            print(f"Dados insuficientes para calcular os indicadores da empresa {empresa}.")
            return pd.DataFrame()

        # Extrai os valores necessários dos relatórios
        def extrair_valor(df, ds_conta, ano):
            """Extrai o valor de uma conta específica do DataFrame para um ano específico."""
            filtro = df[df['DS_CONTA'].str.lower() == ds_conta.lower()]
            if not filtro.empty:
                coluna_valor = filtro.filter(regex=f'VL_CONTA.*{ano}').columns[0]
                return filtro[coluna_valor].values[0]
            return None

        # Obtém a lista de anos disponíveis
        anos = sorted(set(int(col.split('_')[-1]) for col in dre.columns if 'VL_CONTA' in col))

        # DataFrame para armazenar os indicadores
        indicadores_df = pd.DataFrame(columns=['DS_CONTA'] + [str(ano) for ano in anos])

        # Preço da ação (exemplo, você pode obter isso de uma API de mercado)
        preco_acao = 100  # Substitua pelo valor real

        # Calcula os indicadores para cada ano
        for ano in anos:
            # Dados de 5 meses anteriores (evitando look-ahead bias)
            if ano == min(anos):
                continue  # Não há dados anteriores para o primeiro ano

            # Extrai os valores para o ano anterior
            lucro_liquido = extrair_valor(dre, 'lucro/prejuízo do período', ano - 1)
            receita_liquida = extrair_valor(dre, 'receitas', ano - 1)
            ebitda = extrair_valor(dre, 'resultado antes dos tributos sobre o lucro', ano - 1)  # Aproximação do EBITDA
            ebit = extrair_valor(dre, 'resultado operacional', ano - 1)  # Aproximação do EBIT
            patrimonio_liquido = extrair_valor(bpp, 'patrimônio líquido', ano - 1)
            ativo_total = extrair_valor(bpa, 'ativo total', ano - 1)
            passivo_total = extrair_valor(bpp, 'passivo total', ano - 1)
            divida_liquida = extrair_valor(dfc, 'dívida subordinada e instrumentos de dívida elegíveis a capital', ano - 1)

            # Extrai o número de ações para o ano anterior
            distribuicao_capital['DATA_REFERENCIA'] = pd.to_datetime(distribuicao_capital['DATA_REFERENCIA'])
            distribuicao_ano_anterior = distribuicao_capital[distribuicao_capital['DATA_REFERENCIA'].dt.year == (ano - 1)]
            if not distribuicao_ano_anterior.empty:
                numero_acoes = distribuicao_ano_anterior['QUANTIDADE_TOTAL_ACOES'].iloc[0]
            else:
                print(f"Aviso: Dados de distribuição de capital não encontrados para o ano {ano - 1}.")
                numero_acoes = None

            # Cálculo dos indicadores
            indicadores = {
                'DS_CONTA': [
                    'D.Y', 'P/L', 'P/VP', 'EV/EBITDA', 'EV/EBIT', 'P/EBITDA', 'P/EBIT',
                    'VPA', 'P/Ativo', 'LPA', 'P/SR', 'Dív. líquida/PL', 'Dív. líquida/EBITDA',
                    'Dív. líquida/EBIT', 'PL/Ativos', 'Passivos/Ativos', 'M. Bruta', 'M. EBITDA',
                    'M. EBIT', 'M. Líquida', 'ROE', 'ROA', 'Giro ativos'
                ],
                str(ano): [
                    (extrair_valor(dfc, 'dividendos distribuídos', ano - 1) / preco_acao) * 100 if extrair_valor(dfc, 'dividendos distribuídos', ano - 1) else None,
                    preco_acao / (lucro_liquido / numero_acoes) if lucro_liquido and numero_acoes else None,
                    preco_acao / (patrimonio_liquido / numero_acoes) if patrimonio_liquido and numero_acoes else None,
                    (preco_acao * numero_acoes + divida_liquida) / ebitda if ebitda and numero_acoes else None,
                    (preco_acao * numero_acoes + divida_liquida) / ebit if ebit and numero_acoes else None,
                    preco_acao / (ebitda / numero_acoes) if ebitda and numero_acoes else None,
                    preco_acao / (ebit / numero_acoes) if ebit and numero_acoes else None,
                    patrimonio_liquido / numero_acoes if patrimonio_liquido and numero_acoes else None,
                    preco_acao / (ativo_total / numero_acoes) if ativo_total and numero_acoes else None,
                    lucro_liquido / numero_acoes if lucro_liquido and numero_acoes else None,
                    preco_acao / (receita_liquida / numero_acoes) if receita_liquida and numero_acoes else None,
                    divida_liquida / patrimonio_liquido if patrimonio_liquido else None,
                    divida_liquida / ebitda if ebitda else None,
                    divida_liquida / ebit if ebit else None,
                    patrimonio_liquido / ativo_total if ativo_total else None,
                    passivo_total / ativo_total if ativo_total else None,
                    (receita_liquida - extrair_valor(dre, 'despesas', ano - 1)) / receita_liquida if receita_liquida else None,
                    ebitda / receita_liquida if receita_liquida else None,
                    ebit / receita_liquida if receita_liquida else None,
                    lucro_liquido / receita_liquida if receita_liquida else None,
                    lucro_liquido / patrimonio_liquido if patrimonio_liquido else None,
                    lucro_liquido / ativo_total if ativo_total else None,
                    receita_liquida / ativo_total if ativo_total else None,
                ]
            }

            # Adiciona os indicadores ao DataFrame
            df_ano = pd.DataFrame(indicadores)
            if indicadores_df.empty:
                indicadores_df = df_ano
            else:
                indicadores_df = pd.merge(indicadores_df, df_ano, on='DS_CONTA', how='outer')

        return indicadores_df

    def get_symbols(self):
        pass

bd_instance = bd('D:/Downloads/Projetos/TrandingQuantitativo/credential.json')

ti = datetime.now() 
bd_instance.update_fundamentus('BCO BTG PACTUAL S.A.')
tf= datetime.now()
print (tf-ti)

df = bd_instance.calcular_indicadores_anuais('BCO BTG PACTUAL S.A.')
df

df = bd_instance.read_fundamentus('BCO BTG PACTUAL S.A.', relatorio='DISTRIBUICAO CAPITAL')
df = df.dropna(axis=1, how='all')
df

df2 = bd_instance.read_fundamentus('BCO BTG PACTUAL S.A.', relatorio='DRE')
# pd.Series(df2['DS_CONTA'].unique()).sort_values(ascending=True).tolist()
# df2 = df[(df['DS_CONTA'] == 'ON') | (df['DS_CONTA'] =='PN')]
df2 = df2.dropna(axis=1, how='all')
df2 = df2.sort_values(by='CD_CONTA', ascending=True)
df2

df3 = bd_instance.read_fundamentus('BCO BTG PACTUAL S.A.', relatorio='DFC')
pd.Series(df3['DS_CONTA'].unique()).sort_values(ascending=True).tolist()
df3 = df3.dropna(axis=1, how='all')
df3 = df3.sort_values(by='CD_CONTA', ascending=True)
df3 = df3[df3['DS_CONTA'] == 'dividendos distribuídos'] 
df3

df4 = bd_instance.read_fundamentus('BCO BTG PACTUAL S.A.', relatorio='BPP')
pd.Series(df4['DS_CONTA'].unique()).sort_values(ascending=True).tolist()
df4 = df4.dropna(axis=1, how='all')
df4 = df4.sort_values(by='CD_CONTA', ascending=True)
df4

df5 = bd_instance.read_fundamentus('BCO BTG PACTUAL S.A.', relatorio='BPA')
pd.Series(df5['DS_CONTA'].unique()).sort_values(ascending=True).tolist()
df5 = df5.dropna(axis=1, how='all')
df5 = df5.sort_values(by='CD_CONTA', ascending=True)
df5

bd_instance.update_ohlc('BPAC3', 'TIMEFRAME_D1')
df5 = bd_instance.read_ohlc('BPAC3', 'TIMEFRAME_D1')
df5.head()

bd_instance.update_ticks('PETR4')
df6 = bd_instance.read_ticks('PETR4')
df6