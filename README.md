# TrandingQuantitativo

## Propósito do Projeto

O **TrandingQuantitativo** é um projeto para construção de uma base de dados robusta para análise e estratégias de trading quantitativo fundamentalista, integrando dados do mercado financeiro (OHLC, ticks, cadastros da CVM). O objetivo é facilitar a coleta, armazenamento, atualização e análise de dados financeiros para estratégias quantitativas, além de expor esses dados via API para consumo externo.

---

## Principais Funcionalidades

- **API RESTful**: Disponibiliza endpoints para consulta e atualização dos dados, incluindo filtros dinâmicos para companhias abertas da CVM.
- **Coleta de Dados de Ticks**: Coleta dados de negociações efetivadas (ticks) e armazena em Parquet.
- **Leitura e Atualização de Dados**: Permite leitura e atualização dos dados armazenados.
- **Validação e Normalização**: Utiliza Pydantic para validação dos dados e Polars para manipulação eficiente de DataFrames.

---

## Estrutura do Projeto

- **.venv/src/**: Serviços, repositórios, validadores e utilitários para ingestão, processamento e exposição dos dados via API.
- **routes/**: Rotas FastAPI para expor os dados.
- **middlewares/validators/**: Schemas Pydantic para validação dos dados.
- **repositories/**: Camada de acesso ao banco de dados (DuckDB).
- **utils/**: Funções utilitárias para manipulação de arquivos, datas, normalização de texto, etc.

---

## Como Utilizar

### 1. Configuração Inicial

- Instale o **DBeaver** para visualizar o banco (opcional) ([Download](https://dbeaver.io/download/))
- Baixe o **Git Bash** ([Download](https://git-scm.com/downloads))
- Crie uma pasta, inicie o Git Bash dentro dela e clone o repositório: 
   ```bash
   git clone https://github.com/DiegoS-Nunes/TrandingQuantitativo.git
   ```
- Baixe o **Visual Studio Code** ([Download](https://code.visualstudio.com/))
- Dentro da pasta clonada, inicie o VSCode e instale as extensões dentro da loja:
   - Extensão Python ([ms-python.python](https://marketplace.visualstudio.com/items?itemName=ms-python.python))
   - Extensão ThunderClient ([rangav.vscode-thunder-client](https://marketplace.visualstudio.com/items?itemName=rangav.vscode-thunder-client)) para testar rotas da API.
- No terminal do VSCode, instale as dependências e o banco:
   ```bash
   pip install -r requirements.txt
   pip install duckdb --upgrade
   ```

### 2. Executando o Projeto

#### Subindo a API

No terminal, ative o ambiente virtual (se aplicável) e execute:

```bash
.venv/scripts/activate.ps1  # Para Windows PowerShell
# Ou
source .venv/bin/activate   # Para Linux/Mac

uvicorn src.main:app --reload --log-level debug
```

---

## Como Usar as Rotas da API

No ThunderClient ou outro cliente HTTP, acesse a URL: http://localhost:8000

### Atualizar o banco de dados

```http
POST /atualizar_db
```
Atualiza todas as tabelas do banco com os dados mais recentes da CVM.

---

### Buscar companhias

```http
GET /companies?limit=50&offset=0
```
- `limit` e `offset`: paginação dos resultados, sendo o mínimo 1 e máximo 50

---

### Buscar companhia por CNPJ

```http
GET /company/00.000.000/0001-91
# OU
GET /company/00000000000191
```
Retorna os dados detalhados de uma companhia específica.

---

## Como visualizar o banco

---

Na pasta .venv duplo clique no arquivo "cvm_data.db", ou clique com direito > abrir com > dbeaver  

---

## Observações Importantes

- O projeto é modular e facilmente expansível para novos tipos de dados ou integrações.

---

## Links Úteis

- [DuckDB Documentation](https://duckdb.org/docs/)
- [Polars Documentation](https://pola-rs.github.io/polars-book/)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [DBeaver Download](https://dbeaver.io/download/)

---

## Contribuição

Pull requests são bem-vindos! Para reportar bugs ou sugerir melhorias, abra uma