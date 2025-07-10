from pydantic import BaseModel
from typing import Optional

class EspecieControleAcionario(BaseModel):
    especie_controle: Optional[str] = None