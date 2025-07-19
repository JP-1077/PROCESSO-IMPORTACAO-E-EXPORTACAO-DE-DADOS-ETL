

CREATE PROCEDURE PR_ETL_IMPORT_WFM AS

/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
																/* ETAPA 1: LIMPEZA DE STAGE E #TEMP */
/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
	TRUNCATE TABLE [dbo].[tmp_wfm_completo]

	-- Remove a tabela temporária #TEMP se ela existir para garantir que a execução seja limpa
	IF OBJECT_ID('tempdb..#TEMP', 'U') IS NOT NULL    DROP TABLE #TEMP;


/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
																/* ETAPA 2: CONFIGURAÇÕES DE LOG */
/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
	-- Armazena o horário de início do processo.
	DECLARE @START DATETIME = CAST(GETDATE() AS DATETIME)

	-- Define o nome do processo para rastreamento de log.
	DECLARE @PROCESS_NAME VARCHAR(MAX) = 'RCCM_ETL_IMPORT_WFM'


/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
																/* ETAPA 3: BLOCO DE CARGA */
/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
	
	-- Criação da tabela temporária #TEMP que receberá os dados brutos do CSV.
	CREATE TABLE #TEMP
	(
	[DATA] VARCHAR (500),	
	MES	 VARCHAR (500),
	ORGANIZACAO	VARCHAR (500),
	MATRICULA_WFM VARCHAR (500),
	[SITE] VARCHAR (500),
	Tempo_Escalado_WFM VARCHAR (500),
	Tempo_Escalado_Neutro VARCHAR (500), 	
	Tempo_Escalado_Real	VARCHAR (500),
	Tempo_Logado_CMS VARCHAR (500),
	tempo_aderente	VARCHAR (500),
	tempo_abs VARCHAR (500),
	tempo_aus VARCHAR (500),
	tempo_abs_aus VARCHAR (500),
	tempo_conectacao_adapt VARCHAR (500),
	tempo_Aderencia_neutra	VARCHAR (500),
	tempo_indisp_legal VARCHAR (500),
	tempo_investimento VARCHAR (500),
	tempo_pausa_bo VARCHAR (500),
	tempo_pausa_particular	VARCHAR (500),
	tempo_pausa_Retorno	VARCHAR (500),
	tempo_Perdas VARCHAR (500),
	tempo_treinamento VARCHAR (500),
	tempo_Idle VARCHAR (500),
	Tempo_Improdutivo VARCHAR (500),
	Tempo_Inativo VARCHAR (500),
	Tempo_Logado_BO VARCHAR (500),	
	Tempo_Produtivo_BO VARCHAR (500),
	Tdd	VARCHAR (500),
	Ausencia_Compensacao VARCHAR (500),	
	Minutos_Nao_Autorizados	VARCHAR (500),
	Minutos_Autorizados	VARCHAR (500),
	[agente em férias] VARCHAR (500)

	);

 
/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
																/* ETAPA 3: VARIAVEIS DE CONTROLE DE ARQUIVOS */
/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
	
	-- Armazena o caminho onde o arquivo CSV está localizado.
	DECLARE @PATH NVARCHAR(MAX) = '\\SNEPDB56C01\Repositorio\BDS\0047 - IMPORTACAO_KPI_WFM\0003 - SAIDAS\'

	-- Nome do arquivo a ser importado.
	DECLARE @FILE NVARCHAR(MAX) = 'WFM_SUBIR.CSV'

	-- Concatena as duas variaveis acima para termos o caminho completo do arquivo.
	DECLARE @FULLPATH NVARCHAR(MAX) = @PATH + @FILE
	
	-- Comando de importação usando BULK INSERT com modificação UTF - 8 e delimitador ";".
	DECLARE @SQL NVARCHAR(MAX) = ''
	SET @SQL = N'

	BULK INSERT #TEMP
	FROM ''' + @FullPath + '''
	WITH (
	FIELDTERMINATOR = '','',
	ROWTERMINATOR = ''0x0a'',
	FIRSTROW = 2,
	CODEPAGE = ''65001''
     		)';
	
	-- Executa o comando de importação 
	EXEC SP_EXECUTESQL @SQL;

	
/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
																	/* ETAPA 4: TRATAMENTO E INSERÇÃO DE DADOS NA STAGE */
/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/

	INSERT INTO [dbo].[tmp_wfm_completo] (
	DATA,	
	MES	,
	ORGANIZACAO	,
	MATRICULA_WFM,
	SITE,
	Tempo_Escalado_WFM	,
	Tempo_Escalado_Neutro,
	Tempo_Escalado_Real	,
	Tempo_Logado_CMS	,
	tempo_aderente,
	tempo_abs,
	tempo_aus,
	tempo_abs_aus,
	tempo_conectacao_adapt,
	tempo_Aderencia_neutra,
	tempo_indisp_legal,
	tempo_investimento,
	tempo_pausa_bo,
	tempo_pausa_particular,
	tempo_pausa_Retorno	,
	tempo_Perdas,
	tempo_treinamento,
	tempo_Idle,
	Tempo_Improdutivo,
	Tempo_Inativo,
	Tempo_Logado_BO	,
	Tempo_Produtivo_BO,
	Tdd	,
	Ausencia_Compensacao,
	Minutos_Nao_Autorizados,
	Minutos_Autorizados	,
	[agente em férias]
)
SELECT
    CAST(DATA AS datetime),
    CAST(MES AS float),
    CAST(ORGANIZACAO AS NVARCHAR(MAX)), 
    CAST(MATRICULA_WFM AS float),
    CAST(SITE AS NVARCHAR(510)),
    CAST(Tempo_Escalado_WFM AS FLOAT),
    CAST(Tempo_Escalado_Neutro AS FLOAT),
    CAST(Tempo_Escalado_Real AS FLOAT),
    CAST(Tempo_Logado_CMS AS FLOAT),
    CAST(tempo_aderente AS NVARCHAR(510)),
    CAST(tempo_abs AS NVARCHAR(510)),
    CAST(tempo_aus AS NVARCHAR(510)),
    CAST(tempo_abs_aus AS FLOAT),
    CAST(tempo_conectacao_adapt AS NVARCHAR(510)),
    CAST(tempo_Aderencia_neutra AS NVARCHAR(510)),
    CAST(tempo_indisp_legal AS NVARCHAR(510)),
    CAST(tempo_investimento AS NVARCHAR(510)),
    CAST(tempo_pausa_bo AS NVARCHAR(510)),
    CAST(tempo_pausa_particular AS NVARCHAR(510)),
    CAST(tempo_pausa_Retorno AS NVARCHAR(510)),
    CAST(tempo_Perdas AS NVARCHAR(510)),
    CAST(tempo_treinamento AS NVARCHAR(510)),
    CAST(tempo_Idle AS NVARCHAR(510)),
    CAST(Tempo_Improdutivo AS NVARCHAR(510)),
    CAST(Tempo_Inativo AS NVARCHAR(510)),
    CAST(Tempo_Logado_BO AS NVARCHAR(510)),
    CAST(Tempo_Produtivo_BO AS NVARCHAR(510)),
    CAST(Tdd AS NVARCHAR(510)),
    CAST(Ausencia_Compensacao AS FLOAT),
    CAST(Minutos_Nao_Autorizados AS FLOAT),
    CAST(Minutos_Autorizados AS FLOAT),
    CAST([agente em férias] AS NVARCHAR(510)) -- agora como texto
FROM #TEMP;




/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
																	/* ETAPA 6: REGRA DE NEGOCIO: EXECUÇÃO DE PROCEDURE */
/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/

	-- REGRA DE NEGOCIO: Executar Procedure PR_BD_WFM_COMPLETO. Pois, nela já está alocada todo o cruzamento e tratativa que a base final necessita para ser atualizada.

DECLARE @START DATETIME = CAST(GETDATE() AS datetime)

Select * into #TEMPWFM
From (

Select Distinct
						DATA
					,	MES
					,	ORGANIZACAO
					,	MATRICULA_WFM
					,	T4.SITE		
					,	Tempo_Escalado_WFM
					,	Tempo_Escalado_Neutro
					,	Tempo_Escalado_Real
					,	Tempo_Logado_CMS
					,	tempo_aderente
					,	tempo_abs
					,	tempo_aus
					,	tempo_abs_aus
					,	tempo_conectacao_adapt
					,	tempo_Aderencia_neutra
					,	tempo_indisp_legal
					,	tempo_investimento
					,	tempo_pausa_bo
					,	tempo_pausa_particular
					,	tempo_pausa_Retorno
					,	tempo_Perdas
					,	tempo_treinamento
					,	tempo_Idle
					,	Tempo_Improdutivo
					,	Tempo_Inativo
					,	Tempo_Logado_BO
					,	Tempo_Produtivo_BO
					,	Tdd
					,	Ausencia_Compensacao
					,	Minutos_Nao_Autorizados
					,	Minutos_Autorizados
					,	[agente em férias]	as agente_ferias
				from tmp_wfm_completo T4
					inner join TB_STAFF_MENSAL_NEW T3
						On T4.MATRICULA_WFM = t3.MATRICULA_TIM
						and MONTH(data)		= MES_REFERENCIA
						and YEAR(data)		= ANO_REFERENCIA
				Where T3.SITE = 'PISA'

)JJ

declare @dia as date
	set @dia = (select MIN(data) from #TEMPWFM)

delete from tb_wmf_completo
where DATA >= @dia


Insert tb_wmf_completo
Select *
From #TEMPWFM

insert into TB_PROCS_LOG
values(
	'ETL_WFM', --processo
	@START, --horario start
	cast(getdate() as datetime), -- horario end
	'OK', --status
	' Quantidade de linhas processada: '+cast(@@rowcount as varchar) -- frase descrição
)


/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
																	/* ETAPA 7: INSERÇÃO DE LOG */
/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
	

-- Inserir um log na tabela de monitoramento de processos (TB_PROCS_LOG). Para termos um registro quando o processo for executado com sucesso.
	insert into TB_PROCS_LOG
	values (
	@PROCESS_NAME, --processo
	@START, --horario start
	cast(getdate() as datetime), -- horario end
	'OK', --status
	NULL
)


/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/
																	/* ETAPA 8: LIMPEZA DE STAGE, #TEMP E DELETE DO ARQUIVO */
/*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-*/

	-- Remove a tabela temporária utilizadas e realiza uma limpeza na tabela de STAGE.
	IF OBJECT_ID('tempdb..#TEMP', 'U') IS NOT NULL    DROP TABLE #TEMP;
	TRUNCATE TABLE [dbo].[tmp_wfm_completo]