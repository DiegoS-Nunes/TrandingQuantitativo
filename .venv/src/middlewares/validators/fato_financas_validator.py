from pydantic import BaseModel
from decimal import Decimal

class FatoFinancas(BaseModel):
    companhia_id: int
    data_apuracao_id: int
    codigo_conta: str
    descricao_conta: str
    valor_conta: Decimal
    conta_fixa: bool