class EspecieControleRepository:
    @staticmethod
    def create_especie_controle(conn):
        return conn.execute("""
            CREATE TABLE IF NOT EXISTS especie_controle (
                especie_controle_id INTEGER PRIMARY KEY,
                especie_controle TEXT,
            )
            """)

    @staticmethod
    def delete_especie_controle(conn):
        return conn.execute("DELETE FROM especie_controle")
    
    @staticmethod
    def insert_especie_controle(df, conn):
        conn.register("df_especie_controle", df.to_arrow())
        return conn.execute("""
            INSERT INTO especie_controle
            SELECT
                row_number() OVER () as especie_controle_id,
                especie_controle,
            FROM df_especie_controle
            """)
    
    @staticmethod
    def get_especie_controle_id_by_especie(conn, especie):
        result = conn.execute("""
            SELECT
                especie_controle_id
            FROM especie_controle
            WHERE especie_controle = ?
            """,
            [especie]
        ).fetchone()
        return result[0] if result else None
    
    @staticmethod
    def get_all_especie_controle(conn):
        result = conn.execute("""
            SELECT especie_controle_id, especie_controle
            FROM especie_controle
            """).fetchall()
        return {esp: id for id, esp in result}