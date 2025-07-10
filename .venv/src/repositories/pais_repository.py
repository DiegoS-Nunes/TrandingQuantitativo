class PaisRepository:
    @staticmethod
    def create_pais(conn):
        return conn.execute("""
            CREATE TABLE IF NOT EXISTS pais (
                pais_id INTEGER PRIMARY KEY,
                pais TEXT
            )
            """)

    @staticmethod
    def delete_pais(conn):
        return conn.execute("DELETE FROM pais")

    @staticmethod
    def insert_pais(df, conn):
        conn.register("df_pais", df.to_arrow())
        return conn.execute("""
            INSERT INTO pais
            SELECT
                row_number() OVER () as pais_id,
                pais
            FROM df_pais
            """)
    
    @staticmethod
    def get_all_paises(conn):
        result = conn.execute("""
            SELECT pais_id, pais
            FROM pais
            """).fetchall()
        return {pais: id for id, pais in result}