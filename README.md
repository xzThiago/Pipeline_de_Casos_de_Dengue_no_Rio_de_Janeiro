# 🚀 Pipeline de Dados de Dengue

Este projeto implementa um pipeline de **ETL** (Extração, Transformação, Carga) que coleta dados públicos de dengue, realiza a limpeza e transformação com **Pandas**, e carrega os dados tratados em um banco **MariaDB**.

O objetivo é criar uma base de dados limpa e estruturada, pronta para análises.

---

## 🧱 Fluxo do Pipeline

**1. Extração**  
- Baixa o dataset histórico de dengue (`.csv.gz`)

**2. Transformação**  
- Filtra os dados para manter apenas os registros do **Rio de Janeiro**  
- Corrige tipos de dados, renomeia colunas e remove registros inválidos  
- Cria colunas auxiliares: `ano`, `mes` e `semana_epidemiologica` para facilitar análises

**3. Carga**  
- Insere os dados tratados na tabela `casos_dengue_rj` do banco **MariaDB**, substituindo os dados antigos

---

## 🔧 Tecnologias Utilizadas

- Python 3.9+
- Pandas
- SQLAlchemy
- mysql-connector-python
- Requests
- Python-dotenv
- MariaDB (ou MySQL)

---

## 🛠️ Como Configurar e Rodar

### 1. Pré-requisitos

- Ter o **Python 3.9+** instalado  
- Ter um servidor **MariaDB/MySQL** rodando localmente ou na nuvem

### 2. Instale as dependências

```bash
pip install pandas requests sqlalchemy mysql-connector-python python-dotenv

3. Crie o banco de dados
Acesse seu cliente SQL e execute:
CREATE DATABASE IF NOT EXISTS engenharia_dados;

4. Configure as credenciais
Crie um arquivo chamado .env na raiz do projeto com o seguinte conteúdo:
DB_HOST=localhost
DB_DATABASE=engenharia_dados
DB_USER=seu_usuario_aqui
DB_PASSWORD=sua_senha_aqui

5. Execute o pipeline
python pipeline_dengue.py
