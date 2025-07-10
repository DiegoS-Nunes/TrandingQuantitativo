import polars as pl
from ..repositories.dim_time_repository import DimTimeRepository
from datetime import datetime, date

def replace_dates_with_ids(df, data_cols, conn):
    print("Coletando datas únicas do DataFrame...")
    datas_unicas = set()
    for col in data_cols:
        if col in df.columns:
            datas_unicas.update(df[col].drop_nulls().unique().to_list())
    print("Datas únicas encontradas:", len(datas_unicas))

    print("Mapeando datas para IDs...")
    data_map = {}
    for data in datas_unicas:
        if data:
            data_id = DimTimeRepository.get_dim_time_id_by_data_completa(conn, data)
            data_map[data] = data_id
    print("Mapeamento de datas concluído:")

    print("Substituindo datas no DataFrame...")
    for col in data_cols:
        if col in df.columns:
            df = df.with_columns([
                pl.col(col).replace(data_map).alias(col)
            ])
    return df

def convert_dates(obj):
    if isinstance(obj, dict):
        return {k: convert_dates(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_dates(v) for v in obj]
    elif isinstance(obj, (date, datetime)):
        return obj.isoformat()
    else:
        return obj