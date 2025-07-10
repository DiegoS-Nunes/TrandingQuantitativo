# TrandingQuantitativo

## Descrição do Projeto

O projeto **TrandingQuantitativo** tem como objetivo construir uma base de dados robusta para análise e trading quantitativo utilizando a plataforma MetaTrader 5. Através deste projeto, é possível coletar, armazenar e analisar dados de mercado, como OHLC (Open, High, Low, Close) e ticks, de forma eficiente e automatizada.

## Funcionalidades

- **Coleta de Dados OHLC**: Coleta dados históricos de OHLC para diferentes timeframes e os armazena em arquivos Parquet.
- **Coleta de Dados de Ticks**: Coleta dados de ticks, que representam negociações efetivadas, e os armazena em arquivos Parquet.
- **Leitura de Dados**: Leitura dos dados armazenados para análise posterior.
- **Atualização de Dados**: Atualiza os dados existentes com novas informações de mercado.
- **Gerenciamento de Credenciais**: Gerencia credenciais de login para acesso à plataforma MetaTrader 5.

## Estrutura do Projeto

- **BD**: Contém a classe principal para nteração com a plataforma MetaTrader 5, gerenciamento de dados e funções auxiliares.
- **lib.py**: Contém dicionários e funções auxiliares, como o cálculo de feriados.

## Como Utilizar

1. **Configuração Inicial**:
   - Insira suas credenciais de login no arquivo `credential.json`.
   - Certifique-se de que o MetaTrader 5 e python estão instalados e configurados corretamente.

2. **Execução do Projeto**:

    ## Exemplo de Uso

    ```python

    # Inicializando a classe com credenciais
    bd_instance = bd(file_path='path/to/credential.json')

    # Atualizando dados OHLC
    bd_instance.update_ohlc('PETR4', 'TIMEFRAME_D1')

    # Lendo dados OHLC
    df_ohlc = bd_instance.read_ohlc('PETR4', 'TIMEFRAME_D1')

    # Atualizando dados de ticks
    bd_instance.update_ticks('PETR4')

    # Lendo dados de ticks
    df_ticks = bd_instance.read_ticks(symbol, initial_date=datetime(1970, 1, 1), final_date=datetime.now()) //substitua `initial_date=datetime(aaaa, m, d)` e `final_date(aaaa, m, d)` pela data desejada.