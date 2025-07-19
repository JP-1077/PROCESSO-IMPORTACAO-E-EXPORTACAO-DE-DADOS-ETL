# ================================================================================
"""
Projeto: Exportação da base de dados do KPI WFM.
Data da Criação: 15/07/2025
Desenvolvedor: João Pedro
Finalidade: 
    - Extrair dados sobre o WFM diretamente no banco do próprio indicador.
    - Transformar a base de dados extraída em um arquivo CSV.
    - Salvar o arquivo no diretorio do próprio processo localizado na pasta do servidor. 

"""
# ================================================================================



# ==============================================================================
#                       ETAPA 1: Importação de bibliotecas */
# ==============================================================================

import pandas as pd # Manipulação de dados
import pyodbc       # Conexão com Banco de dados SQL Server
import os           # Interação com o sistema operacional 
from dotenv import load_dotenv
from datetime import datetime

# ==============================================================================
#              ETAPA 2: Configuração do ambiente e conexão com banco */
# ==============================================================================

load_dotenv()

# Dados que fazem referência ao banco de dados do WFM
dados_conexao_banco_wfm = (
    f"Driver={{SQL Server}};"      
    f"server= {os.getenv('DB_SERVER')};"       
    f"database= {os.getenv("DB_NAME")};"          
    f"user= {os.getenv("DB_USER")};"
    f"password= {os.getenv("DB_PASS")};"
)

user = os.path.basename(os.environ['USERPROFILE'])
print(f'Usuário: {user} - Iniciando conexão com o banco WFM')

try:
    conexao = pyodbc.connect(dados_conexao_banco_wfm)
    cursor = conexao.cursor()
    print('✅ Conexão bem-sucedida')
except Exception as e:
    print(f'❌ Erro ao conectar ao banco de dados: {e}')
    raise

# ==============================================================================
#       ETAPA 3: Definição dos caminhos para entradas e saídas dos dados */
# ==============================================================================

caminho_origem_wfm = r'\\SNEPDB56C01\Repositorio\BDS\0047 - IMPORTACAO_KPI_WFM\0001 - ENTRADAS\query_origem_wfm.sql'
caminho_saida_wfm = r'\\SNEPDB56C01\Repositorio\BDS\0047 - IMPORTACAO_KPI_WFM\0003 - SAIDAS\WFM_SUBIR.csv'


# ==============================================================================
#                      ETAPA 4: Criação das Funções */
# ==============================================================================

def carregar_query(caminho):
    try:
        with open(caminho, 'r', encoding='utf-8') as f:
            return f.read()
    except UnicodeDecodeError:
        with open(caminho, 'r', encoding='latin-1') as f:
            return f.read()
    
def executar_query(conexao, query):
    return pd.read_sql(query, conexao)

def salvar_csv(base_wfm, caminho_saida_wfm):
    os.makedirs(os.path.dirname(caminho_saida_wfm), exist_ok=True)
    base_wfm.to_csv(caminho_saida_wfm, index=False, encoding='utf-8-sig')
    print(f"✅ Arquivo salvo com sucesso em: {caminho_saida_wfm}")

def conectar_banco_log_sqlserver():
    return pyodbc.connect(
    'Driver={SQL Server};'      # Drive de conexão
    'Server=Snepdb56c01;'       # Nome do servidor
    'Database=BDS;'             # Nome do banco de dados
    'Trusted_Connection=yes;'   # Autenticação integrada do Windows
)

def registrar_log_sqlserver(horario_inicio, status='OK', erro=None):
    try:
        conn = conectar_banco_log_sqlserver()
        cursor = conn.cursor()
        query = """
        INSERT INTO TB_PROCS_LOG
        VALUES (
            'EXPORT_BASE_KPI_WFM',
            ?, -- horário início
            CAST(GETDATE() AS DATETIME), -- horário fim
            ?, -- status
            ?  -- erro (nullable) 
        )
        """

        cursor.execute(query, horario_inicio, status, erro)
        conn.commit()
        cursor.close()
        conn.close()
        print("Log registrado com sucesso.")
    except Exception as e:
        print(f"Erro ao registrar log: {e}")

# ==============================================================================
#                     ETAPA 5: Execução da Aplicação */
# ==============================================================================
def main():
    print("🔄 Iniciando extração de dados WFM...")
    horario_inicio = datetime.now()

    try:    
        query = carregar_query(caminho_origem_wfm)
        df = executar_query(conexao, query)
        salvar_csv(df, caminho_saida_wfm)
        conexao.close()
        registrar_log_sqlserver(horario_inicio, status='OK')

    except Exception as e:
        registrar_log_sqlserver(horario_inicio, status='FALHA', erro=str(e))
        raise

    print("🏁 Processo finalizado com sucesso.")

if __name__ == '__main__':
    main()











