from fastapi import Query
from fastapi import APIRouter
from ..db.db import get_conn, delete_db
from ..controllers.fca_controller import FcaController
from ..controllers.dim_time_controller import DimTimeController

router = APIRouter()

@router.post("/atualizar_db")
def atualizar_db():
    delete_db()
    conn = get_conn()
    try:
        UpdateDimTime = DimTimeController.update_dim_time(conn)
        if UpdateDimTime.status_code != 200:
            return UpdateDimTime
        UpdateFca = FcaController.update_fca(conn)
        if UpdateFca.status_code != 200:
            return UpdateFca
        return {
            UpdateDimTime.body,
            UpdateFca.body
        }
    finally:
        conn.close()

@router.get("/companies")
def get_all_companies(
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(1, ge=1)
    ):
    conn = get_conn()
    try:
        return FcaController.get_all_companies(conn, limit=limit, offset=offset)
    finally:
        conn.close()

@router.get("/company/{cnpj:path}")
def get_company_by_cnpj(cnpj: str):
    print(f"Buscando companhia com CNPJ: {cnpj}")
    conn = get_conn()
    try:
        return FcaController.get_company_by_cnpj(conn, cnpj)
    finally:
        conn.close()