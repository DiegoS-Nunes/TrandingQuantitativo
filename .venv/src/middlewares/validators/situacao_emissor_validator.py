from pydantic import BaseModel
from typing import Optional

class SituacaoEmissor(BaseModel):
    situacao_emissor: Optional[str] = None