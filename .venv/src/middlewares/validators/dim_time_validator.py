from pydantic import BaseModel
from datetime import date

class DimTime(BaseModel):
    data_completa: date
    dia: int
    mes: int
    trimestre: int
    ano: int
    nome_mes: str
    dia_semana: str
    flag_fim_semana: bool