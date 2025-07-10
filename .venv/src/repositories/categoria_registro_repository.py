class CategoriaRegistroRepository:
    @staticmethod
    def create_categoria_registro(conn):
        return conn.execute("""
            CREATE TABLE IF NOT EXISTS categoria_registro(
                categoria_registro_id INTEGER PRIMARY KEY,
                categoria_registro TEXT,
            )
            """)

    @staticmethod
    def delete_categoria_registro(conn):
        return conn.execute("DELETE FROM categoria_registro")

    @staticmethod
    def insert_categoria_registro(df, conn):
        conn.register("df_categoria_registro", df.to_arrow())
        return conn.execute("""
            INSERT INTO categoria_registro
            SELECT
                row_number() OVER () as categoria_registro_id,
                categoria_registro
            FROM df_categoria_registro
            """)
    
    @staticmethod
    def get_categoria_registro_id_by_categoria(conn, categoria):
        result = conn.execute("""
            SELECT
                categoria_registro_id
            FROM categoria_registro
            WHERE categoria_registro = ?
            """,
            [categoria]
            ).fetchone()
        return result[0] if result else None
    
    @staticmethod
    def get_all_categorias_registro(conn):
        result = conn.execute("""
            SELECT 
                categoria_registro_id, 
                categoria_registro
            FROM categoria_registro
            """).fetchall()
        return {cat: id for id, cat in result}