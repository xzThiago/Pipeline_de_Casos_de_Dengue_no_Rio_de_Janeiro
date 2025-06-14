# %% [markdown]
# # PROJETO DE ENGENHARIA DE DADOS - PIPELINE DE DENGUE RJ

# %%
# Importando bibliotecas
import os
import logging
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import requests
import io

# %%
# Configuração de Logs
# Configura o sistema de logging para registrar eventos importantes,
# ajudando o monitoramento e depuração do pipeline.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


# Carregamento de variáveis de ambiente
load_dotenv()

DB_HOST = os.getenv('DB_HOST')
DB_DATABASE = os.getenv('DB_DATABASE')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# Validação das variáveis de ambiente
if not all([DB_HOST, DB_DATABASE, DB_USER, DB_PASSWORD]):
    logging.error("Erro: Variáveis de ambiente do banco de dados não configuradas corretamente.")
    exit()
logging.info("Ambiente configurado com sucesso")

# %% [markdown]
# ## ETAPA 1: EXTRAÇÃO

# %%
def extrair_dados():
    """
    Extrai os dados de dengue do repositório online, simulando um navegador
    para evitar o erro 403 Forbidden.
    Fonte: Brasil.IO (https://brasil.io/dataset/dengue/caso/)
    """
    # URL pública do dataset de dengue para todos os municípios do Brasil.
    url = "https://data.brasil.io/dataset/dengue/caso.csv.gz"
    logging.info(f"Iniciando a extração de dados da URL: {url}")

   # Cabeçalho User-Agent para simular um navegador e evitar bloqueio (403 Forbidden)
    headers = {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    try:
        # usar a biblioteca 'requests' para fazer a requisição com o cabeçalho
        response = requests.get(url, headers=headers)

        # Levanta um erro HTTP para status ruins (4xx ou 5xx)
        response.raise_for_status()

        logging.info("Download dos dados realizados com sucesso. Realizando a leitura")

        # Usar io.StringIO para ler o conteúdo de texto da resposta como se fosse um arquivo
        # isso permite que o pandas leita os dados que já estão na memória
        df_bruto = pd.read_csv(io.StringIO(response.text), compression='infer', sep=',')

        logging.info("Dados extraídos e lidos em DataFrame com sucesso")
        logging.info(f"Total de {len(df_bruto)} registro brutos extraídos.")
        return df_bruto
    
    except requests.exceptions.RequestException as e:
        logging.error(f"Falha nan requisição HTTP: {e}")
        return None
    except Exception as e:
        logging.error(f"Falha ao processar os dados extraídoso: {e}")
        return None

# %% [markdown]
# ## ETAPA 2: TRANSFORMAÇÃO

# %%
def transformar_dados(df):
    """
    Aplica as transformações, limpeza e validações nos dados
    """
    if df is None or df.empty:
        logging.warning("Dataframe de entrada está vazio ou nulo. Pulando transformação")
        return None
    
    logging.info("Iniciando processo de transformação dos dados")

    # Filtragem por Estado
    # O código de estado (IBGE) para o Rio de Janeiro é 33
    # A coluna 'state' armazena a sigla 'RJ'
    df_rj = df[df['state'] == 'RJ'].copy()
    if df_rj.empty:
        logging.warning("Nenhum dados encontrado para o estado do Rio de Janeiro (RJ)")
        return None
    logging.info(f"Dados filtrados para o Rio de Janeiro. {len(df_rj)} registros encontrados")

    # Seleção e Renomeação de Colunas
    # Selecionamos as colunas relevantes e renomeamos para um padrão mais claro
    colunas_relevantes = {
        'date': 'data_notificacao',
        'city': 'cidade',
        'city_ibge_code': 'codigo_ibge',
        'cases': 'casos_confirmados'
    }
    df_transformado = df_rj[list(colunas_relevantes.keys())].copy()
    df_transformado.rename(columns=colunas_relevantes, inplace=True)
    logging.info("Colunas selecionadas e renomeadas.")

    # Limpeza e validação de Dados
    # Corrigir tipos de dados
    df_transformado['data_notificacao'] = pd.to_datetime(df_transformado['data_notificacao'], errors='coerce')
    df_transformado['codigo_ibge'] = df_transformado['codigo_ibge'].astype(int)
    df_transformado['casos_confirmados'] = df_transformado['casos_confirmados'].astype(int)
    logging.info("Tipos de dados corrigidos: data, ibge_code, casos.")

    #Tratar valores nulos (se houver)
    df_transformado.dropna(subset=['data_notificacao'], inplace=True)

    # Validar se não há casos negativos
    if (df_transformado['casos_confirmados'] < 0).any():
        logging.warning("Atenção: existem registros com casos confirmados negativos. Serão removidos.")
        df_transformado = df_transformado[df_transformado['casos_confirmados'] >= 0]

    # Criação de novas colunas (Features)
    df_transformado['ano'] = df_transformado['data_notificacao'].dt.year
    df_transformado['mes'] = df_transformado['data_notificacao'].dt.month
    df_transformado['semana_epidemiologica'] = df_transformado['data_notificacao'].dt.isocalendar().weeh.astype(int)
    logging.info("Novas colunas (ano, mes, semana_epidemiologica) criadas")

    # Remoção de duplicatas
    # Garante que cada combinação de cidade e data seja única.
    num_duplicados_antes = df_transformado.duplicated().sum()
    df_transformado.drop_duplicates(inplace=True)
    logging.info(f"{num_duplicados_antes} registros duplicados foram removidos")
    
    logging.info("Processo de transformação concluído")
    return df_transformado

# %% [markdown]
# ## ETAPA 3: CARGA

# %%
def carregar_dados(df, nome_tabela):
    """Carrega o DataFrame tratado para o banco de dados MariaDB"""
    if df is None or df.empty:
        logging.warning("DataFrame transformado está vazio. Nenhum dado para carregar")
        return
    
    logging.info(f"Iniciando a carga de dados para a tabela '{nome_tabela}'.")

    try:
        # Conexão com o banco de dados
        # String de conexão para o MariaDB
        connection_string = f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_DATABASE}"
        engine = create_engine(connection_string)

        with engine.connect() as connection:
            logging.info("Conexão com o banco de dados MariaDB estabelecida")

            # Inserção dos Dados
            df.to_sql(
                nome=nome_tabela,
                con=engine,
                if_exists='replace',
                index=False,
                chunksize=1000 # Insere dados em lotes para melhor performance
            )
            # Verificação e criação de chave primária
            query_pk = f'ALTER TABLE {nome_tabela} ADD PRIMARY KEY (codigo_ibge, data_notificacao);'
            connection.execute(text(query_pk))
            logging.info(f"Chave primária adicionada a tabela '{nome_tabela}")

        logging.info(f"Carga de {len(df)} registros na tabela '{nome_tabela} concluída com sucesso.")

    except Exception as e:
        logging.error(f"Falha ao carregar os dados no banco de dados: {e}")
        


# %% [markdown]
# ## EXECUÇÃO DO PIPELINE

# %%
if __name__ == "__main__":
    logging.info("INICIANDO PIPELINE DE DADOS DE DENGUE - RIO DE JANEIRO")

    #1. Extração 
    dados_brutos = extrair_dados()

    #2. Transformação
    dados_tratados = transformar_dados(dados_brutos)
    
    # 3. Carga
    if dados_tratados is not None:
        carregar_dados(dados_tratados, nome_tabela="casos_dengue_rj")
    else:
        logging.warning("Pipeline encerrado pois não há dados tratados para carregar.")

    logging.info("PIPELINE DE DADOS FINALIZADO!")


