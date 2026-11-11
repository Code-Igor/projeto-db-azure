# Projeto Banco de Dados no Azure
Repositório para a disciplina de Tópicos Avançados em Programação.

## Integrantes da equipe
- Igor dos Santos Lopes
- André Manoel de Santana
- Denis Sebastian Medina Crusado
  
## Desenho do Projeto

<img width="3078" height="1306" alt="image" src="https://github.com/user-attachments/assets/b9000005-0681-4d4a-a8fc-5906d855d283" />

## SQL Criação Tabelas

```sql
-- analista
CREATE TABLE [dbo].[analista]
(
    id_analista INT NOT NULL PRIMARY KEY,
    cd_analista NVARCHAR(50) NOT NULL,
    nm_analista NVARCHAR(200) NOT NULL,
    ds_email NVARCHAR(200) NULL,
    ds_nivel NVARCHAR(10) NOT NULL,
    id_fila_atual INT NULL,
    fl_ativo BIT NOT NULL,
    dt_inclusao DATETIME2 NOT NULL,
    dt_atualizacao DATETIME2 NULL,
    nm_sistema_origem NVARCHAR(100) NULL,
    cd_registro_origem NVARCHAR(100) NULL
)
 
-- categoria
CREATE TABLE [dbo].[categoria]
(
    id_categoria INT NOT NULL PRIMARY KEY,
    cd_categoria NVARCHAR(50) NOT NULL,
    nm_categoria NVARCHAR(150) NOT NULL,
    ds_descricao NVARCHAR(300) NULL,
    fl_ativo BIT NOT NULL,
    dt_inclusao DATETIME2 NOT NULL,
    dt_atualizacao DATETIME2 NULL,
    nm_sistema_origem NVARCHAR(100) NULL,
    cd_registro_origem NVARCHAR(100) NULL
)
 
-- chamado
CREATE TABLE [dbo].[chamado]
(
    id_chamado BIGINT NOT NULL PRIMARY KEY,
    nr_chamado NVARCHAR(50) NOT NULL,
    ds_tipo_chamado NVARCHAR(50) NOT NULL,
    ds_status_chamado NVARCHAR(30) NOT NULL,
    ds_prioridade NVARCHAR(20) NOT NULL,
    dt_criacao DATETIME2 NOT NULL,
    dt_resolucao DATETIME2 NULL,
    dt_ultima_atualizacao DATETIME2 NOT NULL,
    id_analista_atual INT NULL,
    id_reporter INT NOT NULL,
    id_categoria INT NULL,
    id_cliente_organizacao INT NOT NULL,
    id_fila_atual INT NULL,
    ds_titulo NVARCHAR(250) NULL,
    ds_descricao NVARCHAR(MAX) NULL,
    dt_inclusao DATETIME2 NOT NULL,
    dt_atualizacao DATETIME2 NULL,
    nm_sistema_origem NVARCHAR(100) NULL,
    cd_registro_origem NVARCHAR(100) NULL
)
 
-- chamado_sla
CREATE TABLE [dbo].[chamado_sla]
(
    id_chamado_sla BIGINT NOT NULL PRIMARY KEY,
    id_chamado BIGINT NOT NULL,
    id_sla INT NOT NULL,
    fl_breach BIT NOT NULL,
    qt_tempo_restante_minutos INT NULL,
    qt_tempo_decorrido_minutos INT NULL,
    qt_meta_minutos INT NOT NULL,
    dt_referencia DATETIME2 NOT NULL,
    dt_inclusao DATETIME2 NOT NULL,
    dt_atualizacao DATETIME2 NULL,
    nm_sistema_origem NVARCHAR(100) NULL,
    cd_registro_origem NVARCHAR(100) NULL
)
 
-- chamado_status_historico
CREATE TABLE [dbo].[chamado_status_historico]
(
    id_chamado_status_historico BIGINT NOT NULL PRIMARY KEY,
    id_chamado BIGINT NOT NULL,
    ds_status_chamado NVARCHAR(30) NOT NULL,
    dt_inicio_status DATETIME2 NOT NULL,
    dt_fim_status DATETIME2 NULL,
    qt_tempo_status_minutos INT NULL,
    id_analista_responsavel INT NULL,
    id_fila INT NULL,
    dt_inclusao DATETIME2 NOT NULL,
    dt_atualizacao DATETIME2 NULL,
    nm_sistema_origem NVARCHAR(100) NULL,
    cd_registro_origem NVARCHAR(100) NULL
)
 
-- cliente_organizacao
CREATE TABLE [dbo].[cliente_organizacao]
(
    id_cliente_organizacao INT NOT NULL PRIMARY KEY,
    cd_cliente_organizacao NVARCHAR(50) NOT NULL,
    nm_cliente_organizacao NVARCHAR(200) NOT NULL,
    nr_cnpj NVARCHAR(20) NULL,
    fl_ativo BIT NOT NULL,
    dt_inclusao DATETIME2 NOT NULL,
    dt_atualizacao DATETIME2 NULL,
    nm_sistema_origem NVARCHAR(100) NULL,
    cd_registro_origem NVARCHAR(100) NULL
)
 
-- csat_avaliacao
CREATE TABLE [dbo].[csat_avaliacao]
(
    id_csat_avaliacao BIGINT NOT NULL PRIMARY KEY,
    id_chamado BIGINT NOT NULL,
    id_analista INT NULL,
    nr_score INT NOT NULL,
    ds_comentario NVARCHAR(1000) NULL,
    dt_avaliacao DATETIME2 NOT NULL,
    dt_inclusao DATETIME2 NOT NULL,
    dt_atualizacao DATETIME2 NULL,
    nm_sistema_origem NVARCHAR(100) NULL,
    cd_registro_origem NVARCHAR(100) NULL
)
 
-- fila
CREATE TABLE [dbo].[fila]
(
    id_fila INT NOT NULL PRIMARY KEY,
    cd_fila NVARCHAR(50) NOT NULL,
    nm_fila NVARCHAR(150) NOT NULL,
    ds_descricao NVARCHAR(300) NULL,
    fl_ativo BIT NOT NULL,
    dt_inclusao DATETIME2 NOT NULL,
    dt_atualizacao DATETIME2 NULL,
    nm_sistema_origem NVARCHAR(100) NULL,
    cd_registro_origem NVARCHAR(100) NULL
)
 
-- sla
CREATE TABLE [dbo].[sla]
(
    id_sla INT NOT NULL PRIMARY KEY,
    cd_sla NVARCHAR(50) NOT NULL,
    nm_sla NVARCHAR(150) NOT NULL,
    qt_meta_minutos INT NOT NULL,
    ds_descricao NVARCHAR(300) NULL,
    fl_ativo BIT NOT NULL,
    dt_inclusao DATETIME2 NOT NULL,
    dt_atualizacao DATETIME2 NULL,
    nm_sistema_origem NVARCHAR(100) NULL,
    cd_registro_origem NVARCHAR(100) NULL
)
 
-- solicitante
CREATE TABLE [dbo].[solicitante]
(
    id_solicitante INT NOT NULL PRIMARY KEY,
    cd_solicitante NVARCHAR(50) NOT NULL,
    id_cliente_organizacao INT NOT NULL,
    nm_solicitante NVARCHAR(200) NOT NULL,
    ds_email NVARCHAR(200) NULL,
    ds_telefone NVARCHAR(50) NULL,
    fl_ativo BIT NOT NULL,
    dt_inclusao DATETIME2 NOT NULL,
    dt_atualizacao DATETIME2 NULL,
    nm_sistema_origem NVARCHAR(100) NULL,
    cd_registro_origem NVARCHAR(100) NULL
)
```
## Listagem Indicadores

Iremos utilizar os seguintes indicadores:
- Total Chamados
- Chamados por Status
- Chamado x Analista 
- Chamado x Avaliação 
- Chamados × SLA 
