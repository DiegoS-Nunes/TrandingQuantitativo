from datetime import date, datetime
import MetaTrader5 as mt5

timeframe_dict = {
            'TIMEFRAME_M1': [mt5.TIMEFRAME_M1, 60],
            'TIMEFRAME_M2': [mt5.TIMEFRAME_M2, 120],
            'TIMEFRAME_M3': [mt5.TIMEFRAME_M3, 180],
            'TIMEFRAME_M4': [mt5.TIMEFRAME_M4, 240],
            'TIMEFRAME_M5': [mt5.TIMEFRAME_M5, 300],
            'TIMEFRAME_M6': [mt5.TIMEFRAME_M6, 360],
            'TIMEFRAME_M10': [mt5.TIMEFRAME_M10, 600],
            'TIMEFRAME_M12': [mt5.TIMEFRAME_M12, 720],
            'TIMEFRAME_M15': [mt5.TIMEFRAME_M15, 900],
            'TIMEFRAME_M20': [mt5.TIMEFRAME_M20, 1200],
            'TIMEFRAME_M30': [mt5.TIMEFRAME_M30, 1800],
            'TIMEFRAME_H1': [mt5.TIMEFRAME_H1, 3600],
            'TIMEFRAME_H2': [mt5.TIMEFRAME_H2, 7200],
            'TIMEFRAME_H3': [mt5.TIMEFRAME_H3, 10800],
            'TIMEFRAME_H4': [mt5.TIMEFRAME_H4, 14400],
            'TIMEFRAME_H6': [mt5.TIMEFRAME_H6, 21600],
            'TIMEFRAME_H8': [mt5.TIMEFRAME_H8, 28800],
            'TIMEFRAME_H12': [mt5.TIMEFRAME_H12, 43200],
            'TIMEFRAME_D1': [mt5.TIMEFRAME_D1, 86400],
            'TIMEFRAME_W1': [mt5.TIMEFRAME_W1, 604800],
            'TIMEFRAME_MN1' : [mt5.TIMEFRAME_MN1, 2592000],
        }
def calcular_feriados(ano):
    # Lista de feriados nacionais fixos
    feriados = [
        datetime(date(ano, 1, 1)),   # Confraternização Universal
        datetime(date(ano, 4, 21)),  # Tiradentes
        datetime(date(ano, 5, 1)),   # Dia do Trabalho
        datetime(date(ano, 9, 7)),   # Independência do Brasil
        datetime(date(ano, 10, 12)), # Nossa Senhora Aparecida
        datetime(date(ano, 11, 2)),  # Finados
        datetime(date(ano, 11, 15)), # Proclamação da República
        datetime(date(ano, 12, 25)), # Natal
    ]

    # Algoritmo de Meeus para calcular a data da Páscoa
    a = ano % 19
    b = ano // 100
    c = ano % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    mes = (h + l - 7 * m + 114) // 31
    dia = (h + l - 7 * m + 114) % 31 + 1
    pascoa = date(ano, mes, dia)

    # Cálculo do Carnaval e Sexta-feira Santa
    carnaval = pascoa.replace(day=pascoa.day - 47)
    sexta_santa = pascoa.replace(day=pascoa.day - 2)
    corpus_christi = pascoa.replace(day=pascoa.day + 60)

    # Adicionar feriados móveis
    feriados.extend([carnaval, sexta_santa, corpus_christi])

    # Dias de folga especiais (véspera de Natal e Ano Novo, se forem dias úteis)
    feriados.append(date(ano, 12, 31))  # Véspera de Ano Novo

    # Ordenar por data
    feriados.sort()

    return carnaval, sexta_santa, feriados