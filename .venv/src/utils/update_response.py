from pydantic import BaseModel
from typing import Any

class UpdateResponse(BaseModel):
    success: bool
    message: str
    details: Any