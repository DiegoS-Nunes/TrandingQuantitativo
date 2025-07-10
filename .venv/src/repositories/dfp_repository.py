import duckdb
import polars as pl
from typing import Dict

class DfpRepository:
    def __init__(self):
        self.conn = duckdb.connect(database='cvm_data.db', read_only=False)
        self._initialize_database()

    def _initialize_database(self):
        # Create tables if they don't exist
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS dim_financas (
            cnpj_cia VARCHAR,
            denom_cia VARCHAR,
            cd_cvm VARCHAR,
            moeda VARCHAR,
            escala_moeda VARCHAR,
            ordem_exerc VARCHAR,
            cd_conta VARCHAR,
            ds_conta VARCHAR,
            vl_conta DECIMAL,
            tipo_consolidacao VARCHAR,
            metodo VARCHAR,
            ano VARCHAR,
            PRIMARY KEY (cnpj_cia, cd_conta, tipo_consolidacao, metodo, ano)
        );
        """)
        # Similar CREATE TABLE statements for other report types

    def save_report_data(self, df: pl.DataFrame, report_type: str):
        table_name = f"{report_type.lower()}_reports"
        self.conn.register('new_data', df.to_arrow())
        
        # Upsert logic
        self.conn.execute(f"""
        INSERT OR REPLACE INTO {table_name}
        SELECT * FROM new_data
        """)