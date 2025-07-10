import polars as pl
from pydantic import ValidationError
from ..utils.dates_utils import replace_dates_with_ids
from ..utils.zip_files_utils import process_files, rename_columns, normalize_text
from ..utils.dates_utils import convert_dates
from ..utils.db_utils import format_cnpj
from ..repositories.pais_repository import PaisRepository
from ..repositories.dim_cadastro_repository import DimCadastroRepository
from ..repositories.dim_companhia_repository import DimCompanhiaRepository
from ..repositories.setor_atividade_repository import SetorAtividadeRepository
from ..repositories.especie_controle_repository import EspecieControleRepository
from ..repositories.situacao_emissor_repository import SituacaoEmissorRepository
from ..repositories.situacao_registro_repository import SituacaoRegistroRepository
from ..repositories.categoria_registro_repository import CategoriaRegistroRepository
from ..middlewares.validators.pais_validator import Pais
from ..middlewares.validators.fca_validator import Fca
from ..middlewares.validators.setor_atividade_validator import SetorAtividade
from ..middlewares.validators.dim_cadastro_validator import DimCadastro
from ..middlewares.validators.dim_companhia_validator import DimCompanhia
from ..middlewares.validators.situacao_emissor_validator import SituacaoEmissor
from ..middlewares.validators.situacao_registro_cvm_validator import SituacaoRegistroCVM
from ..middlewares.validators.categoria_registro_cvm_validator import CategoriaRegistroCVM
from ..middlewares.validators.especie_controle_acionario_validator import EspecieControleAcionario

class FcaService:
    @staticmethod
    def update_fca(conn):

        BASE_URL_FCA = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/FCA/DADOS/"

        col_map_raw = {
            "CNPJ_Companhia": "cnpj",
            "Data_Constituicao": "data_constituicao",
            "Pais_Origem": "pais_origem",
            "Pais_Custodia_Valores_Mobiliarios": "pais_custodia",
            "Data_Referencia": "data_referencia",
            "ID_Documento": "numero_documento",
            "Categoria_Registro_CVM": "categoria_registro",
            "Situacao_Registro_CVM": "situacao_registro",
            "Setor_Atividade": "setor_atividade",
            "Descricao_Atividade": "descricao_atividade",
            "Situacao_Emissor": "situacao_emissor",
            "Especie_Controle_Acionario": "especie_controle",
            "Dia_Encerramento_Exercicio_Social": "dia_encerramento_exercicio",
            "Mes_Encerramento_Exercicio_Social": "mes_encerramento_exercicio",
            "Nome_Empresarial": "nome_empresarial",
            "Pagina_Web": "site",
            "Codigo_CVM": "codigo_cvm",
            "Data_Registro_CVM": "data_registro_cvm",
            "Data_Categoria_Registro_CVM": "data_categoria_registro",
            "Data_Situacao_Registro_CVM": "data_situacao_registro",
            "Data_Situacao_Emissor": "data_situacao_emissor",
            "Data_Especie_Controle_Acionario": "data_especie_controle",
            "Data_Alteracao_Exercicio_Social": "data_alteracao_exercicio",
            "Versao": "versao_documento"
        }

        col_map_receb = {
            "ID_DOC": "numero_documento",
            "DT_RECEB": "data_atualizacao"
        }

        df = process_files(BASE_URL_FCA, col_map_raw, name_file="fca_cia_aberta_geral*.csv")
        df_atualizacao = process_files(BASE_URL_FCA, col_map_receb, name_file="fca_cia_aberta_*.csv")
        df_atualizacao = df_atualizacao.select(["numero_documento", "data_atualizacao"])
        df = df.join(df_atualizacao, on="numero_documento", how="left")

        del df_atualizacao

        df_categoria_registro = df.with_columns([
            pl.col("categoria_registro").str.strip_chars()]).unique(subset=["categoria_registro"]).select(["categoria_registro"]).sort("categoria_registro")
        df_situacao_registro = df.with_columns([
            pl.col("situacao_registro").str.strip_chars()]).unique(subset=["situacao_registro"]).select(["situacao_registro"]).sort("situacao_registro")
        df_setor_atividade = df.with_columns([
            pl.col("setor_atividade").str.strip_chars()]).unique(subset=["setor_atividade"]).select(["setor_atividade"]).sort("setor_atividade")
        df_situacao_emissor = df.with_columns([
            pl.col("situacao_emissor").str.strip_chars()]).unique(subset=["situacao_emissor"]).select(["situacao_emissor"]).sort("situacao_emissor")
        df_especie_controle = df.with_columns([
            pl.col("especie_controle").str.strip_chars()]).unique(subset=["especie_controle"]).select(["especie_controle"]).sort("especie_controle")
        
        pais_origem = df.select(pl.col("pais_origem").str.strip_chars().alias("pais"))
        pais_custodia = df.select(pl.col("pais_custodia").str.strip_chars().alias("pais"))
        df_pais = pl.concat([pais_origem, pais_custodia]).unique().filter(pl.col("pais").is_not_null()).sort("pais")

        for row in df_setor_atividade.to_dicts():
            try:
                SetorAtividade(**row)
            except ValidationError as e:
                raise Exception("Erro ao validar SetorAtividade: ", e)
        for row in df_situacao_emissor.to_dicts():
            try:
                SituacaoEmissor(**row)
            except ValidationError as e:
                raise Exception("Erro ao validar SituacaoEmissor: ", e)
        for row in df_situacao_registro.to_dicts():
            try:
                SituacaoRegistroCVM(**row)
            except ValidationError as e:
                raise Exception("Erro ao validar SituacaoRegistroCVM: ", e)
        for row in df_categoria_registro.to_dicts():
            try:
                CategoriaRegistroCVM(**row)
            except ValidationError as e:
                raise Exception("Erro ao validar CategoriaRegistroCVM: ", e)
        for row in df_especie_controle.to_dicts():
            try:
                EspecieControleAcionario(**row)
            except ValidationError as e:
                raise Exception("Erro ao validar EspecieControleAcionario: ", e)
        for row in df_pais.to_dicts():
            try:
                Pais(**row)
            except ValidationError as e:
                raise Exception("Erro ao validar Pais: ", e)

        create_categoria_registro = CategoriaRegistroRepository.create_categoria_registro(conn)
        if not create_categoria_registro:
            raise Exception("Erro ao criar tabela categoria_registro")

        insert_categoria_registro = CategoriaRegistroRepository.insert_categoria_registro(df_categoria_registro, conn)
        if not insert_categoria_registro:
            raise Exception("Erro ao inserir dados em categoria_registro")
        
        create_situacao_registro = SituacaoRegistroRepository.create_situacao_registro(conn)
        if not create_situacao_registro:
            raise Exception("Erro ao criar tabela situacao_registro")

        insert_situacao_registro = SituacaoRegistroRepository.insert_situacao_registro(df_situacao_registro, conn)
        if not insert_situacao_registro:
            raise Exception("Erro ao inserir dados em situacao_registro")
        
        create_setor_atividade = SetorAtividadeRepository.create_setor_atividade(conn)
        if not create_setor_atividade:
            raise Exception("")

        insert_setor_atividade = SetorAtividadeRepository.insert_setor_atividade(df_setor_atividade, conn)
        if not insert_setor_atividade:
            raise Exception("Erro ao inserir dados em setor_atividade")
        
        create_situacao_emissor = SituacaoEmissorRepository.create_situacao_emissor(conn)
        if not create_situacao_emissor:
            raise Exception("Erro ao criar tabela situacao_emissor")

        insert_situacao_emissor = SituacaoEmissorRepository.insert_situacao_emissor(df_situacao_emissor, conn)
        if not insert_situacao_emissor:
            raise Exception("Erro ao inserir dados em situacao_emissor")
        
        create_especie_controle = EspecieControleRepository.create_especie_controle(conn)
        if not create_especie_controle:
            raise Exception("Erro ao criar tabela especie_controle")

        insert_especie_controle = EspecieControleRepository.insert_especie_controle(df_especie_controle, conn)
        if not insert_especie_controle:
            raise Exception("Erro ao inserir dados em especie_controle")
        
        create_pais = PaisRepository.create_pais(conn)
        if not create_pais:
            raise Exception("Erro ao criar tabela pais")
        insert_pais = PaisRepository.insert_pais(df_pais, conn)
        if not insert_pais:
            raise Exception("Erro ao inserir dados em pais")
        
        col_map = {
            "data_referencia": "data_referencia_id",
            "categoria_registro": "categoria_registro_id",
            "situacao_registro": "situacao_registro_id",
            "setor_atividade": "setor_atividade_id",
            "situacao_emissor": "situacao_emissor_id",
            "especie_controle": "especie_controle_id",
            "pais_origem": "pais_origem_id",
            "pais_custodia": "pais_custodia_id",
            "data_registro_cvm": "data_registro_cvm_id",
            "data_categoria_registro": "data_categoria_registro_id",
            "data_situacao_registro": "data_situacao_registro_id",
            "data_situacao_emissor": "data_situacao_emissor_id",
            "data_especie_controle": "data_especie_controle_id",
            "data_alteracao_exercicio": "data_alteracao_exercicio_id",
            "data_atualizacao": "data_atualizacao_id",
            "data_constituicao": "data_constituicao_id",
        }

        df = rename_columns(df, col_map)

        map_categoria = CategoriaRegistroRepository.get_all_categorias_registro(conn)
        map_situacao = SituacaoRegistroRepository.get_all_situacao_registro(conn)
        map_setor = SetorAtividadeRepository.get_all_setores_atividade(conn)
        map_emissor = SituacaoEmissorRepository.get_all_situacao_emissor(conn)
        map_especie = EspecieControleRepository.get_all_especie_controle(conn)
        map_pais = PaisRepository.get_all_paises(conn)

        df = df.with_columns([
            pl.col("categoria_registro_id").replace(map_categoria).cast(pl.Int64),
            pl.col("situacao_registro_id").replace(map_situacao).cast(pl.Int64),
            pl.col("setor_atividade_id").replace(map_setor).cast(pl.Int64),
            pl.col("situacao_emissor_id").replace(map_emissor).cast(pl.Int64),
            pl.col("especie_controle_id").replace(map_especie).cast(pl.Int64),
            pl.col("pais_origem_id").replace(map_pais).cast(pl.Int64),
            pl.col("pais_custodia_id").replace(map_pais).cast(pl.Int64),
        ])

        data_cols = [
            "data_referencia_id",
            "data_registro_cvm_id",
            "data_categoria_registro_id",
            "data_situacao_registro_id",
            "data_situacao_emissor_id",
            "data_especie_controle_id",
            "data_alteracao_exercicio_id",
            "data_constituicao_id",
            "data_atualizacao_id"
        ]

        df = replace_dates_with_ids(df, data_cols, conn)

        print(df.columns)

        cnpjs_vazios = df.filter(
            (pl.col("nome_empresarial").is_null()) | 
            (pl.col("codigo_cvm").is_null())
        ).select("cnpj").unique().to_series().to_list()


        fill_values = df.filter(
            pl.col("cnpj").is_in(cnpjs_vazios)
        ).sort("data_atualizacao_id", descending=True).group_by("cnpj").agg([
            pl.col("nome_empresarial").drop_nulls().first().alias("nome_preencher"),
            pl.col("codigo_cvm").drop_nulls().first().alias("codigo_preencher")
        ])
        df = df.join(fill_values, on="cnpj", how="left").with_columns([
            pl.when(pl.col("nome_empresarial").is_null())
            .then(pl.col("nome_preencher"))
            .otherwise(pl.col("nome_empresarial"))
            .alias("nome_empresarial"),
            
            pl.when(pl.col("codigo_cvm").is_null())
            .then(pl.col("codigo_preencher"))
            .otherwise(pl.col("codigo_cvm"))
            .alias("codigo_cvm")
        ]).drop(["nome_preencher", "codigo_preencher"]).sort("nome_empresarial", descending=True)
        
        for col in df.columns:
            if df[col].dtype == pl.Utf8 and col != "site":
                df = df.with_columns([
                    pl.col(col).map_elements(normalize_text, return_dtype=pl.Utf8).alias(col)
                ])

        for row in df.to_dicts():
            try:
                Fca(**row)
            except ValidationError as e:
                raise Exception("Erro ao validar Fca", e)

        df_companhia = (
            df.sort("nome_empresarial", descending=True)
              .group_by("cnpj")
              .agg([
                pl.col("data_constituicao_id").first(),
                pl.col("pais_origem_id").first()
              ])
              .with_columns([
                pl.col("cnpj").str.replace_all(r'\D', ''),
                pl.col("data_constituicao_id").cast(pl.Int64),
                pl.col("pais_origem_id").cast(pl.Int64)
              ])
        )

        for row in df_companhia.to_dicts():
            try:
                DimCompanhia(**row)
            except ValidationError as e:
                raise Exception("Erro ao validar DimCompanhia: ", e)
        
        create_dim_companhia = DimCompanhiaRepository.create_dim_companhia(conn)

        if not create_dim_companhia:
            raise Exception("Erro ao criar tabela dim_companhia")
        
        insert_dim_companhia = DimCompanhiaRepository.insert_dim_companhia(df_companhia, conn)
        if not insert_dim_companhia:
            raise Exception("Erro ao inserir dados em dim_companhia")
        
        map_companhia = {
            row[1]: row[0]
            for row in DimCompanhiaRepository.get_all_companies(conn)
        }

        df_cadastro = df.with_columns([
            pl.col("data_referencia_id").cast(pl.Int64),
            pl.col("cnpj").str.replace_all(r'\D', '').replace(map_companhia).cast(pl.Int64).alias("dim_companhia_id"),
            pl.col("numero_documento").cast(pl.Int64),
            pl.col("pais_custodia_id").cast(pl.Int64),
            pl.col("categoria_registro_id").cast(pl.Int64),
            pl.col("situacao_registro_id").cast(pl.Int64),
            pl.col("setor_atividade_id").cast(pl.Int64),
            pl.col("descricao_atividade").str.strip_chars(),
            pl.col("situacao_emissor_id").cast(pl.Int64),
            pl.col("especie_controle_id").cast(pl.Int64),
            pl.col("dia_encerramento_exercicio").cast(pl.Int64),
            pl.col("mes_encerramento_exercicio").cast(pl.Int64),
            pl.col("nome_empresarial").str.strip_chars(),
            pl.col("site").str.strip_chars(),
            pl.col("codigo_cvm").cast(pl.Int64),
            pl.col("data_registro_cvm_id").cast(pl.Int64),
            pl.col("data_categoria_registro_id").cast(pl.Int64),
            pl.col("data_situacao_registro_id").cast(pl.Int64),
            pl.col("data_situacao_emissor_id").cast(pl.Int64),
            pl.col("data_especie_controle_id").cast(pl.Int64),
            pl.col("data_alteracao_exercicio_id").cast(pl.Int64),
            pl.col("versao_documento").cast(pl.Int64),
            pl.col("data_atualizacao_id").cast(pl.Int64)
        ])

        for row in df_cadastro.to_dicts():
            try:
                DimCadastro(**row)
            except ValidationError as e:
                raise Exception("Erro ao validar DimCadastro", e)
        
        create_dim_cadastro = DimCadastroRepository.create_dim_cadastro(conn)
        if not create_dim_cadastro:
            raise Exception("Erro ao criar tabela dim_cadastro")

        insert_dim_cadastro = DimCadastroRepository.insert_dim_cadastro(df_cadastro, conn)
        if not insert_dim_cadastro:
            raise Exception("Erro ao inserir dados em dim_cadastro")
        
        return {
            insert_dim_cadastro,
            insert_dim_companhia,
            insert_categoria_registro,
            insert_situacao_registro,
            insert_setor_atividade,
            insert_situacao_emissor,
            insert_especie_controle
        }
    
    @staticmethod
    def get_company_by_cnpj(conn, cnpj):
        cnpj_num = ''.join(filter(str.isdigit, cnpj))

        companhia_row = DimCompanhiaRepository.get_companhia_by_cnpj(conn, cnpj_num)
        if companhia_row is None:
            raise Exception(f"Companhia com CNPJ {cnpj} não encontrada.")

        col_names = [desc[0] for desc in conn.description]

        companhia = dict(zip(col_names, companhia_row))
        if "cnpj" in companhia:
            companhia["cnpj"] = format_cnpj(companhia["cnpj"])

        cadastros = DimCadastroRepository.get_company_by_company_id(conn, companhia["companhia_id"])
        cadastro_cols = [desc[0] for desc in conn.description]
        cadastros_dicts = [dict(zip(cadastro_cols, row)) for row in cadastros] if cadastros is not None else []

        response = {k: v for k, v in companhia.items() if not k.endswith("_id") or k == "companhia_id"}

        # Para cada coluna de cadastro, monta histórico de alterações
        historico = {}
        skip_cols = {"data_atualizacao"}

        for col in cadastro_cols:
            if col in skip_cols or col.endswith("_id"):
                continue
            historico_col = {}
            last_value = None
            for cadastro in reversed(cadastros_dicts):
                valor = cadastro.get(col)
                data_atualizacao = cadastro.get("data_atualizacao")
                norm = lambda x: (x or "").strip().lower() if isinstance(x, str) else x
                if norm(valor) != norm(last_value):
                    if data_atualizacao:
                        key = data_atualizacao.isoformat() if hasattr(data_atualizacao, "isoformat") else str(data_atualizacao)
                        historico_col[key] = valor
                    last_value = valor
            if historico_col:
                historico[col] = historico_col

        # Agrupa documentos
        if "versao_documento" in historico and "numero_documento" in historico and "data_referencia" in historico:
            documentos = {}
            for data, versao in historico["versao_documento"].items():
                numero = historico["numero_documento"].get(data)
                data_referencia = historico["data_referencia"].get(data)
                if numero is not None:
                    documentos[data] = {
                        "numero_documento": numero,
                        "versao_documento": versao,
                        "data_referencia": data_referencia
                    }
            historico["documentos"] = documentos
            historico.pop("versao_documento")
            historico.pop("numero_documento")
            historico.pop("data_referencia")

        # Para cada campo do histórico, coloca o valor mais recente fora do histórico
        historico_final = {}
        for campo, valores in historico.items():
            data_mais_recente = max(valores.keys())
            valor_mais_recente = valores[data_mais_recente]
            if campo == "documentos":
                response[campo] = valor_mais_recente
                if len(valores) > 1:
                    historico_final[campo] = valores
                else:
                    # Se só tem uma versão, pode colocar fora ou omitir do histórico, escolha:
                    response[campo] = list(valores.values())[0]
            else:
                response[campo] = valor_mais_recente
                if len(valores) > 1:
                    historico_final[campo] = valores

        # Campos principais de data resolvida
        response["data_constituicao"] = companhia.get("data_constituicao")
        response["data_atualizacao"] = cadastros_dicts[0].get("data_atualizacao") if cadastros_dicts else None

        # Junta o histórico ao response
        response["historico"] = historico_final

        response = convert_dates(response)
        return response
    
    @staticmethod
    def get_all_companies(conn, limit, offset):
        offset = (offset - 1) * limit
        companhias = DimCompanhiaRepository.get_all_companies(conn, limit, offset)
        if not companhias:
            return []
        col_names = [desc[0] for desc in conn.description]
        companhias_list = [dict(zip(col_names, row)) for row in companhias]

        for companhia in companhias_list:
            if companhia.get("data_constituicao") and hasattr(companhia["data_constituicao"], "isoformat"):
                companhia["data_constituicao"] = companhia["data_constituicao"].isoformat()
            
            companhia_id = companhia.get("companhia_id")
            if companhia_id:
                cadastros = DimCadastroRepository.get_company_by_company_id(conn, companhia_id)
                if cadastros:
                    cadastro_cols = [desc[0] for desc in conn.description]
                    cadastro_dicts = [dict(zip(cadastro_cols, row)) for row in cadastros]
                    nome_empresarial = cadastro_dicts[0].get("nome_empresarial")
                    if nome_empresarial:
                        companhia["nome_empresarial"] = nome_empresarial
            
            companhia["cnpj"] = format_cnpj(companhia["cnpj"]) if "cnpj" in companhia else None

        return companhias_list