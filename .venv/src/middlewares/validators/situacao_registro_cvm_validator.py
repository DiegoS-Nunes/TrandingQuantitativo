from pydantic import BaseModel
from typing import Optional

class SituacaoRegistroCVM(BaseModel):
    situacao_registro: Optional[str] = None