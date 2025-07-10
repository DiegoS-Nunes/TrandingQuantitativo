from ..services.dim_time_service import DimTimeService
from fastapi.responses import JSONResponse

class DimTimeController:
    @staticmethod
    def update_dim_time(conn):
        try:
            DimTimeService.update_dim_time(conn)
            return JSONResponse(
                status_code=200,
                content={
                    "success": True,
                    "message": "Banco de dados atualizado com sucesso.",
                    "details": {"dim_time": "Atualização da dimensão tempo concluída."}
                }
            )
        except Exception as e:
            return JSONResponse(
                status_code=400,
                content={
                    "success": False,
                    "message": "Erro ao atualizar o DimTime.",
                    "details": {"error": str(e)}
                }
            )