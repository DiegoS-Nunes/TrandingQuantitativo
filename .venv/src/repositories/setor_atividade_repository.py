class SetorAtividadeRepository:
    @staticmethod
    def create_setor_atividade(conn):
        return conn.execute("""
            CREATE TABLE IF NOT EXISTS setor_atividade (
                setor_atividade_id INTEGER PRIMARY KEY,
                setor_atividade TEXT
            )
            """)
    
    @staticmethod
    def delete_setor_atividade(conn):
        return conn.execute("DELETE FROM setor_atividade")

    @staticmethod
    def insert_setor_atividade(df, conn):
        conn.register("df_setor_atividade", df.to_arrow())
        return conn.execute("""
            INSERT INTO setor_atividade
            SELECT
                row_number() OVER () as setor_atividade_id,
                setor_atividade
            FROM df_setor_atividade
            """)
    
    @staticmethod
    def get_setor_atividade_id_by_setor(conn, setor):
        result = conn.execute("""
            SELECT
                setor_atividade_id
            FROM setor_atividade
            WHERE setor_atividade = ?
            """,
            [setor]
        ).fetchone()
        return result[0] if result else None
    
    @staticmethod
    def get_all_setores_atividade(conn):
        result = conn.execute("""
            SELECT setor_atividade_id, setor_atividade
            FROM setor_atividade
            """).fetchall()
        return {set: id for id, set in result}