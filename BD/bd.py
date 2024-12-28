import os
import json
import pandas as pd
import pyarrow as pa
import MetaTrader5 as mt5
import pyarrow.parquet as pq
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
            os.mkdir('ohlc')
            for timeframe_dir in timeframe_dict.keys():
                try:
                    os.mkdir(f'ohlc\\{timeframe_dir}')
                except Exception as e:
                    print(f"Erro ao criar as pastas de timeframe: {e}")
        if not os.path.isdir('ticks'):
            try:
                os.mkdir('ticks')
                print("Sucesso ao criar o diretório 'ticks'!")
            except Exception as e:
                print(f"Erro ao criar o diretório 'ticks': {e}")

    def update_ohlc(self, symbol, timeframe):
        initial_date = datetime(2000, 1, 1)
        final_date = datetime.now()
    
        path = f'ohlc\\{timeframe}\\{symbol}_{timeframe}.parquet'

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
        path = f'ticks\\{symbol}_ticksrange.parquet'

        if not os.path.exists(path):
            df = pd.DataFrame(columns=['time', 'bid', 'ask', 'last', 'volume', 'time_msc', 'flags', 'volume_real'])
        else:
            df = pq.ParquetFile(path).read().to_pandas()
            df['time'] = pd.to_datetime(df['time'])
            if df['time'].max() < datetime.now(): 
                initial_date = df['time'].max()

        try:
            ticks_data = mt5.copy_ticks_range(symbol, initial_date, final_date, mt5.COPY_TICKS_TRADE)
            df_aux = pd.DataFrame(ticks_data)
            print(f"Coletados {len(df_aux)} novos ticks para {symbol} entre {initial_date} e {final_date}.")
        except Exception as e:
            print('Erro ao obter os dados de ticks: ' + str(e))
            return

        if len(df_aux) > 0:
            df_aux['time'] = pd.to_datetime(df_aux['time'], unit='s')
            df = pd.concat([df_aux, df], ignore_index=True)
            self.salvar_parquet(path, df)
        else:
            print("Nenhum dado novo encontrado.")

    def slice(self, type, symbol, initial_date, final_date, timeframe=None):
        path = f'ohlc\\{timeframe}\\{symbol}_{timeframe}.parquet' if type == 'ohlc' else f'ticks\\{symbol}_ticksrange.parquet'
        if not os.path.exists(path):
            print(f"O ativo {symbol} não está registrado, favor criá-lo utilizando a função .update_{type}()")
        else:
            df = pq.ParquetFile(path).read().to_pandas()
            df['time'] = pd.to_datetime(df['time'], unit='s')
            df['time_msc'] = pd.to_datetime(df['time_msc'], unit='ms')
            return df.loc[(df['time'] >= initial_date) & (df['time'] < final_date)]

    def read_ohlc(self, symbol, timeframe, initial_date=datetime(2012, 1, 1), final_date=datetime.now()):
        return self.slice('ohlc', symbol, initial_date, final_date, timeframe)

    def read_ticks(self, symbol, initial_date=datetime(1970, 1, 1), final_date=datetime.now()):
        return self.slice('ticks', symbol, initial_date, final_date)

    def get_symbols(self):
        pass

    def salvar_parquet(self, path, df):
        try:
            table = pa.Table.from_pandas(df.sort_values(by='time', ascending=False))
            pq.write_table(table, path, compression='snappy')
            print(f"Sucesso ao salvar o arquivo Parquet!")
        except Exception as e:
            print(f"Erro ao escrever o arquivo Parquet: {e}")