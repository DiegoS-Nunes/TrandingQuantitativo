class SituacaoEmissorRepository:
    @staticmethod
    def create_situacao_emissor(conn):
        return conn.execute("""
            CREATE TABLE IF NOT EXISTS situacao_emissor (
                situacao_emissor_id INTEGER PRIMARY KEY,
                situacao_emissor TEXT,
            )
            """)
    
    @staticmethod
    def delete_situacao_emissor(conn):
        return conn.execute("DELETE FROM situacao_emissor")

    @staticmethod
    def insert_situacao_emissor(df, conn):
        conn.register("df_situacao_emissor", df.to_arrow())
        return conn.execute("""
            INSERT INTO situacao_emissor
            SELECT
                row_number() OVER () as situacao_emissor_id,
                situacao_emissor
            FROM df_situacao_emissor
            """)
    
    @staticmethod
    def get_situacao_emissor_id_by_situacao(conn, situacao):
        result = conn.execute("""
            SELECT
                situacao_emissor_id
            FROM situacao_emissor
            WHERE situacao_emissor = ?
            """,
            [situacao]
        ).fetchone()
        return result[0] if result else None
    
    @staticmethod
    def get_all_situacao_emissor(conn):
        result = conn.execute("""
            SELECT situacao_emissor_id, situacao_emissor
            FROM situacao_emissor
            """).fetchall()
        return {sit: id for id, sit in result}