from pydantic import BaseModel
from typing import Optional
from datetime import date

class DimAcao(BaseModel):
    companhia_id: int
    ticker: str
    tipo_acao: Optional[str] = None
    data_inicio_negociacao: date
    data_fim_negociacao: Optional[date] = None