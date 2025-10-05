# 📊 pipeline_iptu_recife

Projeto que monta um fluxo de ingestão, processamento e disponibilização dos dados do IPTU da cidade do Recife.

---

## 🔍 Objetivos da Análise

Este pipeline tem como objetivo responder às seguintes perguntas, a partir dos dados históricos e anuais do IPTU do Recife:

---

### 1. **Volume de Imóveis**

- **Perguntas:**
  - Qual é o total de imóveis cadastrados?
  - Como o inventário de imóveis está distribuído fisicamente (por tipo, bairro, etc)?

- **Apresentação dos dados:**
  - Histórico consolidado.
  - Distribuição por ano.

---

### 2. **Idade dos Imóveis**

- **Pergunta:**
  - Como o inventário está distribuído em termos de idade de construção?

- **Apresentação dos dados:**
  - Histórico consolidado.
  - Distribuição por ano.

---

### 3. **Valor dos Imóveis**

- **Perguntas:**
  - Quais bairros apresentam maior crescimento no número de imóveis?
  - Quais bairros apresentam maior crescimento em valor venal?

- **Apresentação dos dados:**
  - Histórico consolidado.
  - Evolução por ano.

---

### 4. **Evolução do Inventário**

- **Perguntas:**
  - Quais bairros apresentam a maior evolução no número de imóveis ao longo do tempo?
  - Quais bairros apresentam a maior evolução em termos de valor ao longo do tempo?

- **Apresentação dos dados:**
  - Histórico consolidado.
  - Evolução ano a ano.

---

## 🛠️ Tecnologias Utilizadas

Este projeto é construído em **Python** e utiliza bibliotecas modernas para ingestão, transformação, análise e visualização de dados.

---

### 🔹 Linguagem Principal

| Tecnologia | Descrição |
|------------|-----------|
| **Python** | Linguagem principal para construção do pipeline de ETL, scripts de processamento e análises. |

---

### 📦 Bibliotecas de Análise de Dados

| Biblioteca        | Descrição |
|-------------------|-----------|
| **Pandas**        | Manipulação e análise de dados estruturados. |
| **NumPy**         | Suporte a operações numéricas vetorizadas. |
| **GeoPandas** *(opcional)* | Análise espacial e geográfica dos imóveis. |
| **Scikit-learn** *(opcional)* | Modelagem e previsão, se necessário. |

---

### 📊 Visualização e Documentação

| Ferramenta              | Descrição |
|-------------------------|-----------|
| **Jupyter Notebooks**   | EDA (Análise Exploratória de Dados) e documentação interativa. |
| **Matplotlib / Seaborn / Plotly** *(opcionais)* | Visualizações gráficas e estatísticas. |

---

### ⚙️ Infraestrutura e Versionamento

| Tecnologia     | Descrição |
|----------------|-----------|
| **Git & GitHub** | Controle de versão e colaboração. |
| **venv**         | Ambiente virtual isolado para dependências do projeto. |
| **.env**         | Armazenamento seguro de credenciais e variáveis sensíveis. |

---

## 📁 Estrutura do Projeto

```text
iptu_recife/
├── main.py             # Ponto de entrada principal para execução do pipeline de dados
├── requirements.txt    # Lista de bibliotecas Python necessárias
├── setup.py            # Configuração para empacotar o projeto (opcional)
├── config/             # Configurações de conexão (banco de dados, APIs)
├── logs/               # Armazenamento de logs de execução do projeto
├── modules/            # Módulos Python reutilizáveis (limpeza, transformação, modelagem)
├── notebooks/          # Notebooks Jupyter/Colab com análises e visualizações
├── test/               # Scripts de testes unitários
├── README.md           # Este arquivo
