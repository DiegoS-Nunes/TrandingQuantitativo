from pydantic import BaseModel
from decimal import Decimal

class FatoAcaoPreco(BaseModel):
    acao_id: int
    data_referencia_id: int
    preco: Decimal