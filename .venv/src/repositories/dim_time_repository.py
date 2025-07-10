class DimTimeRepository:
    @staticmethod
    def create_dim_time(conn):
        return conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_time (
                data_id INTEGER PRIMARY KEY,
                data_completa DATE UNIQUE,
                dia INTEGER,
                mes INTEGER,
                trimestre INTEGER,
                ano INTEGER,
                nome_mes VARCHAR,
                dia_semana VARCHAR,
                flag_fim_semana BOOLEAN
            )
            """)
    
    @staticmethod
    def delete_dim_time(conn):
        return conn.execute("DELETE FROM dim_time")

    @staticmethod
    def insert_dim_time(df, conn):
        conn.register("df_time", df.to_arrow())
        return conn.execute("""
            INSERT INTO dim_time
            SELECT
                row_number() OVER () as data_id,
                data_completa,
                dia,
                mes,
                trimestre,
                ano,
                nome_mes,
                dia_semana,
                flag_fim_semana
            FROM df_time
            """)
    
    @staticmethod
    def get_dim_time_id_by_data_completa (conn, date=None):
        result = conn.execute(
            """
                SELECT data_id
                FROM dim_time
                WHERE data_completa = ?
            """,
            [date]
        ).fetchone()
        return result[0] if result else None