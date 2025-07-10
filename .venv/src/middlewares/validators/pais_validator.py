from pydantic import BaseModel
from typing import Optional

class Pais(BaseModel):
    pais: Optional[str] = None