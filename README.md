# 🛒 Olist Lakehouse — Databricks FREE

![PySpark](https://img.shields.io/badge/PySpark-3.x-E25A1C?logo=apachespark&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-Free%20Edition-FF3621?logo=databricks&logoColor=white)
![Terraform](https://img.shields.io/badge/Terraform-IaC-7B42BC?logo=terraform&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Medallion-003366?logo=delta&logoColor=white)

Pipeline de dados implementando a arquitetura Medallion no dataset [Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) utilizando Databricks Free Edition.

---

## 📌 Visão Geral

O projeto demonstra um pipeline de dados real de ponta a ponta: ingestão de CSVs brutos, limpeza e padronização na camada Silver e modelagem dimensional Star Schema na camada Gold, orquestrado via Databricks Workflows provisionado com Terraform.

* Arquitetura Lakehouse com Delta Lake
* Transformação de dados em larga escala com PySpark
* Modelagem dimensional (Star Schema, SCD Tipo 1 e Tipo 2)
* Infraestrutura como Código com Terraform
* Orquestração de pipelines com Databricks Workflows

---

## 🏗️ Arquitetura

### Pipeline

<img width="669" height="413" alt="image" src="https://github.com/user-attachments/assets/5673b7d7-d2aa-4ba9-8f82-fe48e7e4ac2d" />

### Star Schema — Camada Gold

<img width="2254" height="1548" alt="mermaid-diagram-2026-03-19-160301" src="https://github.com/user-attachments/assets/10c16b31-b12d-4361-804a-4ef01218cdc8" />

### DAG (Orquestração)

<img width="661" height="815" alt="Captura de tela de 2026-03-19 17-42-45" src="https://github.com/user-attachments/assets/e2edd7eb-0c55-4057-94f5-128cebe15acb" />

---

## 🛠️ Stack Tecnológica

| Seção | Tecnologia |
|---|---|
| Plataforma | Databricks Free Edition |
| Armazenamento | Delta Lake + Unity Catalog |
| Processamento | PySpark (Serverless) |
| Formato de dados | Delta (Bronze, Silver, Gold) |
| Orquestração | Databricks Workflows |
| IaC | Terraform + provider 'databricks/databricks' |
| Dataset | Olist Brazilian E-Commerce (Kaggle) |

---

## 📁 Estrutura do Projeto

```
olist-lakehouse-databricks/
├── notebooks/
│   ├── 00_setup.py
│   ├── silver/
│   │   ├── 01_silver_orders.py
│   │   ├── 02_silver_products.py
│   │   ├── 03_silver_order_items.py
│   │   ├── 04_silver_reviews.py
│   │   ├── 05_silver_customers.py
│   │   ├── 06_silver_sellers.py
│   │   ├── 07_silver_payments.py
│   │   ├── 08_silver_geolocation.py
│   │   └── 09_silver_product_category_name_translation.py
│   └── gold/
│       ├── 01_gold_dim_tempo.py
│       ├── 02_gold_dim_cliente.py
│       ├── 03_gold_dim_produto.py
│       ├── 04_gold_dim_seller.py
│       ├── 05_gold_fato_itens_pedido.py
│       ├── 06_gold_fato_pagamentos.py
│       └── 07_gold_fato_avaliacoes.py
├── terraform/
│   ├── main.tf
│   ├── variables.tf
│   └── terraform.tfvars       ← não versionado (.gitignore)
└── .gitignore
```

---

## 📄 Apontamento Extra

| Arquivo | Descrição |
|---|---|
| 'silver/04_silver_reviews.py' | Sanitização de CSV malformado via Python/regex antes da leitura com Spark |
| 'silver/08_silver_geolocation.py' | Normalização Unicode de cidades + agregação por CEP |
| 'gold/01_gold_dim_tempo.py' | Date spine via 'sequence()' com atributos em português |
| 'gold/02_gold_dim_cliente.py' | SCD Tipo 2 com 'lag()'/'lead()' para rastreamento de endereço |
| 'gold/05_gold_fato_itens_pedido.py' | Fato principal com 4 chaves de tempo e join SCD Tipo 2 |
| 'terraform/main.tf' | Provisionamento do Databricks Job com 16 tasks e dependências |

---

## 🔍 Detalhamento das Etapas

### Bronze

A camada Bronze preserva os dados brutos exatamente como foram disponibilizados, em CSVs originais do Olist armazenados em '/Volumes/workspace/olist-storage/bronze/'. Nenhuma transformação é aplicada nessa camada. A única exceção documentada é o arquivo 'olist_order_reviews_sanitized.csv', gerado como pré-processamento necessário antes da leitura com Spark.

---

### Silver

A camada Silver é responsável pelo refinamento da qualidade dos dados, como preparação para o que será demandado na etapa analítica:

1. Schema explícito na leitura (sem 'inferSchema')
2. Verificação de nulos pré e pós-tratamento
3. Cast de tipos corretos
4. Tratamento de valores inválidos e padronização de strings
5. Verificação de duplicatas e integridade de PKs
6. Escrita em Delta com 'mode("overwrite")'

**Decisões relevantes por tabela:**

**'01_silver_orders'**
Quatro timestamps com nullability diferenciada: 'order_purchase_timestamp' definido como 'NOT NULL' (evento obrigatório que define a existência do pedido); os demais 'order_approved_at', 'order_delivered_customer_date', 'order_estimated_delivery_date' são nullable, pois pedidos cancelados ou em trânsito podem não ter esses valores preenchidos.

**'02_silver_products'**
Cast de colunas dimensionais físicas ('weight_g', 'length_cm', 'height_cm', 'width_cm') com 'regexp_replace' para tratar separadores decimais inconsistentes antes do cast para 'DoubleType'. Produtos sem categoria recebem valor sentinela "sem_categoria" em vez de nulo, o que sinaliza explicitamente um problema de input da área de negócio e preserva a rastreabilidade.

**'03_silver_order_items'**
PK composta por '(order_id, order_item_id)'. Colunas financeiras 'price' e 'freight_value' tratadas com 'regexp_replace' para remoção de caracteres não numéricos antes do cast para 'DoubleType'.

**'04_silver_reviews'**
O dataset de reviews é o mais crítico do projeto. Aproximadamente 4.938 registros continham quebras de linha ('\n') dentro de campos de texto livre, corrompendo a leitura CSV padrão do Spark. A solução adotada foi um pré-processamento em Python puro via 're.sub()' que substitui '\n' por espaço dentro de campos entre aspas, gerando um arquivo sanitizado antes da leitura com Spark. O arquivo original é preservado. Adicionalmente, 'review_creation_date' e 'review_answer_timestamp' são convertidos com 'to_timestamp()' para que valores inválidos sejam convertidos para 'null' e registros com 'review_creation_date' nulo são descartados.

**'05_silver_customers' e '06_silver_sellers'**
Tabelas sem problemas de qualidade, tratamento seguiu o padrão de tipos e deduplicação.

**'07_silver_payments'**
PK composta por '(order_id, payment_sequential)'. Cast de 'payment_sequential' e 'payment_installments' para 'IntegerType'.

**'08_silver_geolocation'**
Normalização Unicode de nomes de cidades via 'translate()' que remove acentos e caracteres especiais para padronização. Agregação por 'geolocation_zip_code_prefix': coordenadas calculadas como média ('avg'), cidade e estado preservados via 'max()' após normalização. Limitação conhecida: CEPs com múltiplos bairros preservam um nome de cidade arbitrário, o que é aceitável pois é atributo descritivo de seller.

**'09_silver_product_category_name_translation'**
Tabela simples de mapeamento sem problemas de qualidade.

---

### Gold

A camada Gold implementa um Star Schema com 4 dimensões e 3 tabelas fato. As Surrogate Keys (SKs) são geradas via 'SHA2-256' com prefixo de entidade, o que garante idempotência no reprocessamento e reduz o risco a colisão de gerar chaves iguais:

'''
sha2(concat(lit("dim_cliente||"), col("customer_unique_id"), ...), 256)
'''

**Decisões relevantes:**

**'01_gold_dim_tempo'**
Date spine gerado via 'spark.sql' com função 'sequence()', cobrindo do primeiro dia do ano mínimo do dataset até 31/12 do ano mínimo + 10 anos. Atributos derivados em português ('nome_mes', 'nome_dia') via 'create_map()', pois 'date_format()' no PySpark não suporta locale 'pt_BR'. Linha sentinela com 'sk_tempo = "0"' e 'nome_mes = "data desconhecida"' para absorver joins com timestamps nulos nas fatos

**'02_gold_dim_cliente'**
Implementada como SCD Tipo 2 que rastreia histórico de mudança de endereço por cliente. A chave natural é 'customer_unique_id' (e não 'customer_id', que é transacional e se repete por pedido). A detecção de mudança de endereço usa 'lag()' sobre 'customer_zip_code_prefix' ordenado por 'order_purchase_timestamp'. O início de cada versão ('data_inicio') é o timestamp do pedido que introduziu o novo endereço; o fim ('data_fim') é calculado via 'lead(data_inicio) - INTERVAL 1 DAY'. Versão atual identificada por 'data_fim IS NULL' e 'is_current = True'

**'03_gold_dim_produto'**
SCD Tipo 1 em que produtos têm identidade própria por 'product_id'. Qualquer variação de produto no Olist gera um novo 'product_id'

**'04_gold_dim_seller'**
SCD Tipo 1, pois o histórico de mudança de endereço de seller não tem valor analítico relevante no contexto do Olis

**'05_gold_fato_itens_pedido'**
Granularidade: uma linha por item de pedido ('order_id' + 'order_item_id'). Quatro chaves de tempo para representar os diferentes momentos do ciclo de vida do pedido: compra, aprovação, entrega estimada e entrega real. Join com 'dim_cliente' usa condição SCD Tipo 2 que associa cada pedido ao endereço que o cliente tinha na época da compra. SKs de tempo nulas substituídas pelo valor sentinela '"0"'.

**'06_gold_fato_pagamentos'**
Granularidade: uma linha por método de pagamento por pedido ('order_id' + 'payment_sequential'). Separada da 'fato_itens_pedido' para preservar pedidos com múltiplos métodos de pagamento sem multiplicar linhas de itens.

**'07_gold_fato_avaliacoes'**
Granularidade: uma linha por review ('order_id'). Três chaves de tempo: compra, criação da review e resposta da review. 'sk_seller' foi excluída pois um pedido pode ter itens de múltiplos sellers e associar uma review a um seller específico exigiria uma suposição arbitrária não suportada pelo modelo do Olist.

---

## ⚙️ Infraestrutura e Orquestração

O pipeline é orquestrado via Databricks Workflows com um Job de 16 tasks provisionado como código com Terraform.

### Estrutura do Job

O pipeline executa em três estágios paralelos:

1. **Silver** — todas as 9 tabelas rodam em paralelo
2. **Dimensões Gold** — rodam em paralelo após Silver completa, respeitando dependências individuais (ex: 'dim_cliente' depende de 'silver_orders' + 'silver_customers')
3. **Fatos Gold** — rodam em paralelo após todas as dimensões estarem prontas

### Setup do Terraform

**Pré-requisitos:**
- Terraform >= 1.0 instalado localmente
- Personal Access Token gerado no Databricks (**Settings → Developer → Access Tokens**)

**Configuração:**

```bash
# Clone o repositório
git clone https://github.com/seu-usuario/olist-lakehouse-databricks
cd olist-lakehouse-databricks/terraform
```

Crie o arquivo `terraform.tfvars` com suas credenciais (não versionado):

```hcl
databricks_host  = "https://seu-workspace.cloud.databricks.com"
databricks_token = "seu_token_aqui"
```

**Execução:**

```bash
# Autenticação por variáveis de ambiente (recomendado)
export DATABRICKS_HOST="https://seu-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="seu_token_aqui"

terraform init
terraform plan
terraform apply
```

---

## ⚠️ Limitações

**Range da 'dim_tempo'**
O cálculo do 'max_date' usava 'greatest()' com valor sentinela '9999-12-31' no 'coalesce', o que inflava o range para milhares de anos. Corrigido: o range é calculado como 'ano_mínimo_do_dataset + 10 anos', tornando o date spine determinístico e sem datas irrelevantes.

**Reviews com dados malformados**
O CSV original de reviews contém 4.938 registros com quebras de linha dentro de campos de texto livre, incompatíveis com o parser multiLine do Spark. Resolvido utilizando um script Python antes da leitura. Registros com 'review_creation_date' inválido após conversão foram descartados.

**Geolocation com CEPs duplicados**
CEPs com múltiplos bairros ou cidades na tabela de geolocation mantiveram um nome de cidade apenas após a agregação por 'zip_code_prefix'. Aceitável pois é atributo descritivo de seller sem impacto nas métricas.

**'dim_cliente' SCD Tipo 2 sem timestamp real de mudança**
O histórico de endereço utiliza como referência a 'order_purchase_timestamp', pois não existe um timestamp real de atualização cadastral no dataset. Pedidos do mesmo cliente com endereços diferentes são tratados como mudança de endereço, mas a data exata da mudança é aproximada pela data do pedido.

**Sellers com múltiplos itens por pedido**
A 'fato_avaliacoes' não possui 'sk_seller' pois um pedido pode conter itens de múltiplos sellers.

---

## 🚀 Como Executar
 
### Pré-requisitos
 
- Conta no [Databricks Free Edition](https://community.cloud.databricks.com/)
- Dataset Olist disponível em `/Volumes/workspace/olist-storage/bronze/`
- Terraform >= 1.0 (para provisionamento do Job)
 
### Execução manual (notebook a notebook)
 
1. Execute `00_setup.py` — cria o catalog `olist` e os schemas `bronze`, `silver`, `gold` no Unity Catalog
2. Execute os notebooks Silver em ordem (`01` → `09`)
3. Execute os notebooks Gold de dimensões (`01` → `04`)
4. Execute os notebooks Gold de fatos (`05` → `07`)
 
### Execução via Databricks Workflows
 
Após provisionamento com Terraform, acesse **Workflows → olist_pipeline → Run Now** no Databricks.

---

## 📚 Fonte

[Olist Brazilian E-Commerce — Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

Dataset público com ~100k pedidos reais do e-commerce brasileiro entre 2016 e 2018, contendo informações de pedidos, clientes, sellers, produtos, pagamentos e avaliações.
