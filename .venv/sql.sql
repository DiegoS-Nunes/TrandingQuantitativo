CREATE TABLE dim_companhia (
    companhia_id        SERIAL PRIMARY KEY,
    cnpj                VARCHAR(18) UNIQUE,
    data_constituicao   DATE,
    pais_origem         VARCHAR(100),
    setor_atividade_id  INTEGER,
    descricao_atividade VARCHAR(255),
    pagina_web          VARCHAR(255)
);

CREATE TABLE setor_atividade (
    id_setor_atividade SERIAL PRIMARY KEY,
    setor_atividade    VARCHAR(100) UNIQUE
);

CREATE TABLE categoria_registro_cvm (
    id_categoria_registro SERIAL PRIMARY KEY,
    categoria             VARCHAR(10) UNIQUE
);

CREATE TABLE situacao_registro_cvm (
    id_situacao_registro SERIAL PRIMARY KEY,
    situacao             VARCHAR(20) UNIQUE
);

CREATE TABLE situacao_emissor (
    id_situacao_emissor SERIAL PRIMARY KEY,
    situacao            VARCHAR(20) UNIQUE
);

CREATE TABLE especie_controle_acionario (
    id_especie_controle SERIAL PRIMARY KEY,
    especie             VARCHAR(50) UNIQUE
);

CREATE TABLE dim_tempo (
    data_id           SERIAL PRIMARY KEY,
    data_completa     DATE UNIQUE,
    dia               INTEGER,
    mes               INTEGER,
    trimestre         INTEGER,
    ano               INTEGER,
    semestre          INTEGER,
    nome_mes          VARCHAR(20),
    nome_trimestre    VARCHAR(20),
    nome_semestre     VARCHAR(20),
    dia_semana        VARCHAR(20),
    flag_fim_semana   BOOLEAN,
    flag_feriado      BOOLEAN
);

CREATE TABLE dim_acao (
    acao_id               SERIAL PRIMARY KEY,
    ticker                VARCHAR(20) UNIQUE,
    tipo_acao             VARCHAR(50),
    data_inicio_negociacao DATE,
    data_fim_negociacao    DATE,
    companhia_id          INTEGER REFERENCES dim_companhia(companhia_id)
);

CREATE TABLE fato_cadastro (
    cadastro_id                  BIGSERIAL PRIMARY KEY,
    companhia_id                 INTEGER REFERENCES dim_companhia(companhia_id),
    data_referencia_id           INTEGER REFERENCES dim_tempo(data_id),
    categoria_registro_id        INTEGER REFERENCES categoria_registro_cvm(id_categoria_registro),
    situacao_registro_id         INTEGER REFERENCES situacao_registro_cvm(id_situacao_registro),
    setor_atividade_id           INTEGER REFERENCES setor_atividade(id_setor_atividade),
    situacao_emissor_id          INTEGER REFERENCES situacao_emissor(id_situacao_emissor),
    especie_controle_id          INTEGER REFERENCES especie_controle_acionario(id_especie_controle),
    encerramento_exercicio_id    INTEGER,
    nome_empresarial             VARCHAR(255),
    codigo_cvm                   INTEGER,
    data_registro_cvm            DATE,
    numero_documento             INTEGER,
    data_documento               DATE,
    data_categoria_registro      DATE,
    data_situacao_registro       DATE,
    data_situacao_emissor        DATE,
    data_especie_controle        DATE,
    data_alteracao_exercicio     DATE,
    versao_documento             INTEGER
);

CREATE TABLE fato_acao (
    acao_id            INTEGER REFERENCES dim_acao(acao_id),
    data_referencia_id INTEGER REFERENCES dim_tempo(data_id),
    quantidade_acoes   INTEGER,
    PRIMARY KEY (acao_id, data_referencia_id)
);

CREATE TABLE fato_indicador (
    indicador_id       BIGSERIAL PRIMARY KEY,
    companhia_id       INTEGER REFERENCES dim_companhia(companhia_id),
    data_referencia_id INTEGER REFERENCES dim_tempo(data_id),
    nome_indicador     VARCHAR(100),
    valor_indicador    DOUBLE PRECISION
);

CREATE TABLE fato_financas (
    bpp_id                BIGSERIAL PRIMARY KEY,
    companhia_id          INTEGER REFERENCES dim_companhia(companhia_id),
    data_fim_exercicio_id INTEGER REFERENCES dim_tempo(data_id),
    data_apuracao_id      INTEGER REFERENCES dim_tempo(data_id),
    codigo_conta          VARCHAR(50),
    descricao_conta       VARCHAR(255),
    valor_conta           DOUBLE PRECISION,
    conta_fixa            BOOLEAN
);

-- fato_cadastro
ALTER TABLE fato_cadastro
  ADD CONSTRAINT fk_fc_companhia FOREIGN KEY (companhia_id) REFERENCES dim_companhia(companhia_id),
  ADD CONSTRAINT fk_fc_data_ref FOREIGN KEY (data_referencia_id) REFERENCES dim_tempo(data_id),
  ADD CONSTRAINT fk_fc_categoria_registro FOREIGN KEY (categoria_registro_id) REFERENCES categoria_registro_cvm(id_categoria_registro),
  ADD CONSTRAINT fk_fc_situacao_registro FOREIGN KEY (situacao_registro_id) REFERENCES situacao_registro_cvm(id_situacao_registro),
  ADD CONSTRAINT fk_fc_setor FOREIGN KEY (setor_atividade_id) REFERENCES setor_atividade(id_setor_atividade),
  ADD CONSTRAINT fk_fc_situacao_emissor FOREIGN KEY (situacao_emissor_id) REFERENCES situacao_emissor(id_situacao_emissor),
  ADD CONSTRAINT fk_fc_especie_controle FOREIGN KEY (especie_controle_id) REFERENCES especie_controle_acionario(id_especie_controle);

-- fato_indicador
ALTER TABLE fato_indicador
  ADD CONSTRAINT fk_fi_companhia FOREIGN KEY (companhia_id) REFERENCES dim_companhia(companhia_id),
  ADD CONSTRAINT fk_fi_data_ref FOREIGN KEY (data_referencia_id) REFERENCES dim_tempo(data_id);

-- fato_financas
ALTER TABLE fato_financas
  ADD CONSTRAINT fk_ff_companhia FOREIGN KEY (companhia_id) REFERENCES dim_companhia(companhia_id),
  ADD CONSTRAINT fk_ff_fim_exercicio FOREIGN KEY (data_fim_exercicio_id) REFERENCES dim_tempo(data_id),
  ADD CONSTRAINT fk_ff_apuracao FOREIGN KEY (data_apuracao_id) REFERENCES dim_tempo(data_id);

-- fato_acao
ALTER TABLE fato_acao
  ADD CONSTRAINT fk_fa_acao FOREIGN KEY (acao_id) REFERENCES dim_acao(acao_id),
  ADD CONSTRAINT fk_fa_data_ref FOREIGN KEY (data_referencia_id) REFERENCES dim_tempo(data_id);

-- dim_acao
ALTER TABLE dim_acao
  ADD CONSTRAINT fk_da_companhia FOREIGN KEY (companhia_id) REFERENCES dim_companhia(companhia_id);

-- dim_companhia
ALTER TABLE dim_companhia
  ADD CONSTRAINT fk_dc_setor FOREIGN KEY (setor_atividade_id) REFERENCES setor_atividade(id_setor_atividade);
