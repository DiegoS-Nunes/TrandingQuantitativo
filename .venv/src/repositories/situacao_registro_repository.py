class SituacaoRegistroRepository:
    @staticmethod
    def create_situacao_registro(conn):
        return conn.execute("""
            CREATE TABLE IF NOT EXISTS situacao_registro (
                situacao_registro_id INTEGER PRIMARY KEY,
                situacao_registro TEXT
            )
            """)

    @staticmethod
    def delete_situacao_registro(conn):
        return conn.execute("DELETE FROM situacao_registro")

    @staticmethod
    def insert_situacao_registro(df, conn):
        conn.register("df_situacao_registro", df.to_arrow())
        return conn.execute("""
            INSERT INTO situacao_registro
            SELECT
                row_number() OVER () as situacao_registro_id,
                situacao_registro
            FROM df_situacao_registro
            """)
    
    @staticmethod
    def get_situacao_registro_id_by_situacao(conn, situacao):
        result = conn.execute("""
            SELECT
                situacao_registro_id
            FROM situacao_registro
            WHERE situacao_registro = ?
            """,
            [situacao]
        ).fetchone()
        return result[0] if result else None
    
    @staticmethod
    def get_all_situacao_registro(conn):
        result = conn.execute("""
            SELECT situacao_registro_id, situacao_registro
            FROM situacao_registro
            """).fetchall()
        return {sit: id for id, sit in result}