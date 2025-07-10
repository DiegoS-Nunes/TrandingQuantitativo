class DimCadastroRepository:
    @staticmethod
    def create_dim_cadastro(conn):
        return conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_cadastro (
                cadastro_id INTEGER PRIMARY KEY,
                dim_companhia_id INTEGER REFERENCES dim_companhia(companhia_id),
                data_referencia_id INTEGER REFERENCES dim_time(data_id),
                numero_documento INTEGER,
                pais_custodia_id INTEGER REFERENCES pais(pais_id),
                categoria_registro_id INTEGER REFERENCES categoria_registro(categoria_registro_id),
                situacao_registro_id INTEGER REFERENCES situacao_registro(situacao_registro_id),
                setor_atividade_id INTEGER REFERENCES setor_atividade(setor_atividade_id),
                descricao_atividade TEXT,
                situacao_emissor_id INTEGER REFERENCES situacao_emissor(situacao_emissor_id),
                especie_controle_id INTEGER REFERENCES especie_controle(especie_controle_id),
                nome_empresarial TEXT,
                site TEXT,
                codigo_cvm INTEGER,
                data_registro_cvm_id INTEGER REFERENCES dim_time(data_id),
                data_categoria_registro_id INTEGER REFERENCES dim_time(data_id),
                data_situacao_registro_id INTEGER REFERENCES dim_time(data_id),
                data_situacao_emissor_id INTEGER REFERENCES dim_time(data_id),
                data_especie_controle_id INTEGER REFERENCES dim_time(data_id),
                data_alteracao_exercicio_id INTEGER REFERENCES dim_time(data_id),
                dia_encerramento_exercicio INTEGER,
                mes_encerramento_exercicio INTEGER,
                versao_documento INTEGER,
                data_atualizacao_id INTEGER REFERENCES dim_time(data_id)
            )
            """)
    
    @staticmethod
    def delete_dim_cadastro(conn):
        return conn.execute("DELETE FROM dim_cadastro")

    @staticmethod
    def insert_dim_cadastro(df, conn):
        conn.register("df_cadastro", df.to_arrow())
        return conn.execute("""
            INSERT INTO dim_cadastro
            SELECT
                row_number() OVER () as cadastro_id,
                dim_companhia_id,
                data_referencia_id,
                numero_documento,
                pais_custodia_id,
                categoria_registro_id,
                situacao_registro_id,
                setor_atividade_id,
                descricao_atividade,
                situacao_emissor_id,
                especie_controle_id,
                nome_empresarial,
                site,
                codigo_cvm,
                data_registro_cvm_id,
                data_categoria_registro_id,
                data_situacao_registro_id,
                data_situacao_emissor_id,
                data_especie_controle_id,
                data_alteracao_exercicio_id,
                dia_encerramento_exercicio,
                mes_encerramento_exercicio,
                versao_documento,
                data_atualizacao_id
            FROM df_cadastro
            """)
    
    @staticmethod
    def get_company_by_company_id(conn, companhia_id):
        result = conn.execute("""
            SELECT 
                dc.*,
                t_ref.data_completa AS data_referencia,
                t_reg.data_completa AS data_registro_cvm,
                t_cat.data_completa AS data_categoria_registro,
                t_sitreg.data_completa AS data_situacao_registro,
                t_sitemiss.data_completa AS data_situacao_emissor,
                t_esp.data_completa AS data_especie_controle,
                t_alt.data_completa AS data_alteracao_exercicio,
                t_atual.data_completa AS data_atualizacao,
                categoria_registro.categoria_registro,
                situacao_registro.situacao_registro,
                setor_atividade.setor_atividade,
                situacao_emissor.situacao_emissor,
                especie_controle.especie_controle,
                pais.pais AS pais_custodia
            
            FROM 
                dim_cadastro dc
            
            LEFT JOIN dim_time t_ref
                ON dc.data_referencia_id = t_ref.data_id
            LEFT JOIN dim_time t_reg
                ON dc.data_registro_cvm_id = t_reg.data_id
            LEFT JOIN dim_time t_cat
                ON dc.data_categoria_registro_id = t_cat.data_id
            LEFT JOIN dim_time t_sitreg
                ON dc.data_situacao_registro_id = t_sitreg.data_id
            LEFT JOIN dim_time t_sitemiss
                ON dc.data_situacao_emissor_id = t_sitemiss.data_id
            LEFT JOIN dim_time t_esp
                ON dc.data_especie_controle_id = t_esp.data_id
            LEFT JOIN dim_time t_alt
                ON dc.data_alteracao_exercicio_id = t_alt.data_id
            LEFT JOIN dim_time t_atual 
                ON dc.data_atualizacao_id = t_atual.data_id
            LEFT JOIN categoria_registro
                ON dc.categoria_registro_id = categoria_registro.categoria_registro_id
            LEFT JOIN situacao_registro
                ON dc.situacao_registro_id = situacao_registro.situacao_registro_id
            LEFT JOIN setor_atividade
                ON dc.setor_atividade_id = setor_atividade.setor_atividade_id
            LEFT JOIN situacao_emissor
                ON dc.situacao_emissor_id = situacao_emissor.situacao_emissor_id
            LEFT JOIN especie_controle
                ON dc.especie_controle_id = especie_controle.especie_controle_id
            LEFT JOIN pais
                ON dc.pais_custodia_id = pais.pais_id
            
            WHERE 
                dc.dim_companhia_id = ?
            
            ORDER BY 
                dc.data_atualizacao_id DESC
            """, 
            (companhia_id,)
        )
        return result.fetchall() if result else []