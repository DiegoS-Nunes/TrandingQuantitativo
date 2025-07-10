def resolve_id(conn, table, id_value, id_col, value_col):
    if id_value is None:
        return None
    row = conn.execute(
        f"SELECT {value_col} FROM {table} WHERE {id_col} = ?", (id_value,)
    ).fetchone()
    return row[0] if row else None

def normalize_str(s):
    if not s:
        return ""
    return ''.join(s.split()).lower()

def format_cnpj(cnpj: str) -> str:
    cnpj = ''.join(filter(str.isdigit, str(cnpj)))
    if len(cnpj) == 14:
        return f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:14]}"
    return cnpj