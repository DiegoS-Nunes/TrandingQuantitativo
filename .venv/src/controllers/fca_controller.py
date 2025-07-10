from ..services.fca_service import FcaService
from fastapi.responses import JSONResponse

class FcaController:
    @staticmethod
    def update_fca(conn):
        try:
            FcaService.update_fca(conn)
            return JSONResponse(
                status_code=200,
                content={
                    "success": True,
                    "message": "Banco de dados atualizado com sucesso.",
                    "details": {"fca": "Atualização dos dados cadastrais concluída."}
                }
            )
        except Exception as e:
            print(e)
            return JSONResponse(
                status_code=400,
                content={
                    "success": False,
                    "message": "Erro ao atualizar dados cadastrais.",
                    "details": {"error": str(e)}
                }
            )
        
    @staticmethod
    def get_all_companies(conn, limit, offset):
        try:
            return JSONResponse(
                content=FcaService.get_all_companies(conn, limit, offset)
            )
        except Exception as e:
            print(e)
            return JSONResponse(
                status_code=400,
                content={
                    "success": False,
                    "message": "Erro ao obter dados das empresas.",
                    "details": {"error": str(e)}
                }
            )
        
    @staticmethod
    def get_company_by_cnpj(conn, cnpj):
        try:
            if not cnpj:
                return JSONResponse(
                status_code=404,
                content={
                    "success": False,
                    "message": "Missing CNPJ"
                }
            )
            return JSONResponse(
                content=FcaService.get_company_by_cnpj(conn, cnpj)
            )
        except Exception as e:
            print(e)
            return JSONResponse(
                status_code=400,
                content={
                    "success": False,
                    "message": "Erro ao obter dados da empresa pelo cnpj.",
                    "details": {"error": str(e)}
                }
            )