class DimCompanhiaRepository:
    @staticmethod
    def create_dim_companhia(conn):
        print("Creating dim_companhia table...")
        return conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_companhia (
                companhia_id INTEGER PRIMARY KEY,
                cnpj VARCHAR(20),
                data_constituicao_id INTEGER REFERENCES dim_time(data_id),
                pais_origem_id INTEGER REFERENCES pais(pais_id)
            )
            """)
    
    @staticmethod
    def delete_dim_companhia(conn):
        return conn.execute("DELETE FROM dim_companhia")
    
    @staticmethod
    def get_companhia_by_cnpj(conn, cnpj):
        result = conn.execute("""
            SELECT 
                dc.*,
                dt.data_completa AS data_constituicao
            FROM 
                dim_companhia dc
            LEFT JOIN 
                dim_time dt 
                ON dc.data_constituicao_id = dt.data_id
            LEFT JOIN 
                pais p On dc.pais_origem_id = p.pais_id
            WHERE
                dc.cnpj = ?
            """, 
            (cnpj,)
        )
        row = result.fetchone()
        return row if row else None
    
    @staticmethod
    def get_all_companies(conn, limit=None, offset=None):
        query = """
            SELECT 
                companhia_id,
                cnpj,
                dt.data_completa AS data_constituicao,
                p.pais AS pais_origem
            FROM
                dim_companhia dc
            LEFT JOIN
                dim_time dt ON dc.data_constituicao_id = dt.data_id
            LEFT JOIN
                pais p ON dc.pais_origem_id = p.pais_id
            """
        params = []
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        if offset is not None:
            query += " OFFSET ?"
            params.append(offset)
        result = conn.execute(query, params).fetchall()
        return result

    @staticmethod
    def insert_dim_companhia(df, conn):
        conn.register("df_companhia", df.to_arrow())
        return conn.execute("""
            INSERT INTO dim_companhia
            SELECT
                row_number() OVER () as companhia_id,
                cnpj,
                data_constituicao_id,
                pais_origem_id
            FROM df_companhia
            """)