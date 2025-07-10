import polars as pl
from datetime import datetime
from ..repositories.dim_time_repository import DimTimeRepository
from fastapi import HTTPException
from ..middlewares.validators.dim_time_validator import DimTime
from pydantic import ValidationError

class DimTimeService:
    @staticmethod
    def update_dim_time(conn):
        date_range = pl.concat([
            pl.date_range(
                start=datetime.strptime('1900-01-01', "%Y-%m-%d"),
                end=datetime.now(),
                interval="1d",
                eager=True
            ),
            pl.date_range(
                start=datetime.strptime('1808-10-12', "%Y-%m-%d"),
                end=datetime.strptime('1808-10-12', "%Y-%m-%d"),
                interval="1d",
                eager=True
            )
        ])
        
        df = pl.DataFrame({
            "data_completa": date_range,
        }).with_columns(
            dia=pl.col("data_completa").dt.day(),
            dia_semana=pl.col("data_completa").dt.strftime("%A"),
            flag_fim_semana=pl.col("data_completa").dt.weekday() > 5,
            mes=pl.col("data_completa").dt.month(),
            nome_mes=pl.col("data_completa").dt.strftime("%B"),
            trimestre=pl.col("data_completa").dt.quarter(),
            ano=pl.col("data_completa").dt.year()
        )

        for row in df.to_dicts():
            try:
                DimTime(**row)
            except ValidationError as e:
                raise HTTPException(
                    status_code=400,
                    detail= f"Erro ao validar dados da tabela dim_time: {e}"
                )

        create_dim_time = DimTimeRepository.create_dim_time(conn)
        if not create_dim_time:
            raise HTTPException(
                status_code=500,
                detail="Erro ao criar a tabela dim_time."
            )
        
        DimTimeRepository.delete_dim_time(conn)
        
        insert_dim_time = DimTimeRepository.insert_dim_time(df, conn)
        if not insert_dim_time:
            raise HTTPException(
                status_code=500,
                detail="Erro ao inserir dados na tabela dim_time."
            )
        return insert_dim_time