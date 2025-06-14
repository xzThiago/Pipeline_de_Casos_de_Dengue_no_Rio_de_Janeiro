🚀 Pipeline de Dados de Dengue
Este projeto implementa um pipeline de ETL (Extração, Transformação, Carga) que coleta dados públicos de dengue, realiza a limpeza e transformação dos dados com Pandas, e os carrega em um banco de dados MariaDB. O objetivo é criar uma base de dados limpa e estruturada, pronta para análise.

🧱 Fluxo do Pipeline
Extração: Baixa o dataset histórico de dengue (.csv.gz) do repositório do InfoDengue.
Transformação:
Filtra os dados para manter apenas os registros do Rio de Janeiro.
Corrige tipos de dados, renomeia colunas e remove registros inválidos.
Cria colunas de ano, mês e semana_epidemiologica para facilitar análises.
Carga: Insere os dados tratados em uma tabela chamada casos_dengue_rj no banco MariaDB, substituindo os dados antigos.
🔧 Tecnologias
Python 3.9+
Pandas
SQLAlchemy & mysql-connector-python
Requests
Python-dotenv
MariaDB (ou MySQL)
🛠️ Como Configurar e Rodar
1. Pré-requisitos:

Ter o Python 3.9+ e um servidor MariaDB/MySQL rodando.
2. Instale as dependências:

Bash

pip install pandas requests sqlalchemy mysql-connector-python python-dotenv
3. Crie o banco de dados:

Execute este comando no seu cliente SQL:
<!-- end list -->

SQL

CREATE DATABASE IF NOT EXISTS engenharia_dados;
4. Configure as credenciais:

Crie um arquivo chamado .env na raiz do projeto com o seguinte conteúdo, preenchendo com seus dados:
<!-- end list -->

Snippet de código

DB_HOST=localhost
DB_DATABASE=engenharia_dados
DB_USER=seu_usuario_aqui
DB_PASSWORD=sua_senha_aqui
5. Execute o pipeline:

Bash

python pipeline_dengue.py
📊 Resultado
Ao final da execução, a tabela casos_dengue_rj será criada (ou substituída) no banco engenharia_dados, contendo todos os dados de dengue tratados e prontos para consulta.
