from pydantic import BaseModel
from typing import Optional

class SetorAtividade(BaseModel):
    setor_atividade: Optional[str] = None