from pydantic import BaseModel
from typing import Optional

class Fca(BaseModel):
    cnpj: str
    data_constituicao_id: Optional[int] = None
    pais_origem: Optional[str] = None
    data_referencia_id: int
    numero_documento: int
    categoria_registro_id: Optional[int] = None
    situacao_registro_id: Optional[int] = None
    setor_atividade_id: Optional[int] = None
    descricao_atividade: Optional[str] = None
    situacao_emissor_id: Optional[int] = None
    especie_controle_id: Optional[int] = None
    dia_encerramento_exercicio: Optional[int] = None
    mes_encerramento_exercicio: Optional[int] = None
    nome_empresarial: str
    site: Optional[str] = None
    codigo_cvm: int
    data_registro_cvm: Optional[int] = None
    data_documento: Optional[int] = None
    data_categoria_registro: Optional[int] = None
    data_situacao_registro: Optional[int] = None
    data_situacao_emissor: Optional[int] = None
    data_especie_controle: Optional[int] = None
    data_alteracao_exercicio: Optional[int] = None
    versao_documento: Optional[int] = None