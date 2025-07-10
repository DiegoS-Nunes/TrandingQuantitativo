from pydantic import BaseModel
from typing import Optional

class DimCompanhia(BaseModel):
    cnpj: str
    data_constituicao_id: Optional[int] = None
    pais_origem: Optional[str] = None