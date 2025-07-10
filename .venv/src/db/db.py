import os
import duckdb

def get_conn():
    return duckdb.connect(database='cvm_data.db', read_only=False)

def delete_db():
    db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../cvm_data.db'))
    print("Caminho do banco de dados:", db_path)
    if os.path.exists(db_path):
        os.remove(db_path)