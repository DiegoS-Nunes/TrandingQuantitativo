from pydantic import BaseModel
from decimal import Decimal

class FatoIndicador(BaseModel):
    companhia_id: int
    data_referencia_id: int
    nome_indicador: str
    valor_indicador: Decimal