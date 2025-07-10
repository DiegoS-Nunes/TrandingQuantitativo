from pydantic import BaseModel
from typing import Optional

class CategoriaRegistroCVM(BaseModel):
    categoria_registro: Optional[str] = None