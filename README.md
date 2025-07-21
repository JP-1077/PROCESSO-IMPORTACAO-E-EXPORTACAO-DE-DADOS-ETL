# Processo de Exportação e Importação de dados ETL

## Objetivo 🎯

Automatizar o fluxo completo de extração, exportação, importação e atualização da base de dados do KPI WFM, garantindo integridade, rastreabilidade e padronização dos dados.

## Tecnologias e Ferramentas 🛠

* Linguagem: Python 
* Bibliotecas: pandas, pyodbc, os, dotenv, datetime
* Banco de Dados: SQL Server
* Importação: BULK INSERT

## System Design ✍🏼

![Pipeline](Pipeline(6).png)

1. Extração de dados: Script Python conecta ao banco WFM, executa query SQL armazenada em arquivo .sql e carrega os dados em um DataFrame.

2. Exportação: Salva os dados extraídos em um arquivo CSV (WFM_SUBIR.csv)

3. Importação: Importação do CSV via BULK INSERT.

4. Log de execução: Ambos os processos (Python e SQL) registram logs em TB_PROCS_LOG.

## Detalhes Técnicos ⚙

### FONTE DE DADOS

* Banco de dados: SQL Server


### TRANSFORMAÇÕES

* Conversão de tipos no SQL (ex: VARCHAR → FLOAT, DATETIME, NVARCHAR).
* Filtro por SITE
* Remoção de duplicidades com DISTINCT.

### BASES ENVOLVIDAS

* Temporárias: #TEMP, #TEMPWFM
* STAGE: tmp_wfm_completo
* Final: tb_wmf_completo

## Monitoramento ✅

Ambos os processo (Python e SQL) armazenam registro de controle na base de monitoramento de processo:

* Nome do Processo de Exportação: EXPORT_BASE_KPI_WFM
* Nome do Processo de Importação: RCCM_ETL_IMPORT_WFM
