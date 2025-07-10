from pydantic import BaseModel

class FatoAcao(BaseModel):
    acao_id: int
    data_referencia_id: int
    quantidade_acoes: int