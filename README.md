# E-comm Genie — Camada Semântica de Vendas

> **Núcleo Estruturante | E-Commerce Grupo Boticário**  
> Fundação de dados do assistente integrado ao Slack para resposta a perguntas de performance de vendas em linguagem natural.

---

## Índice

1. Contexto
2. Arquitetura
3. Estrutura do repositório
4. Objetos produzidos
5. Como executar
6. Decisões de modelagem e governança
7. Perguntas suportadas e fora de escopo
8. Evoluções futuras

---

## 1. Contexto

O canal`#fale-com-dados recebe requisições analíticas que consomem tempo de especialistas. O **E-comm Genie** é um assistente GenAI integrado ao Slack que responde perguntas de performance de vendas em linguagem natural, liberando o time de dados para trabalho de maior valor.

Este repositório entrega a fundação de dados do Genie, baseado em uma arquitetura dimensional completa sobre `raw.tb_vendas`, pronta para consumo por LLM.

```
raw.tb_vendas
      │
      ▼
trusted.tb_vendas                  ← limpeza, normalização, exclusão de dados sensíveis
      │
      ├── semantic.dim_tempo       ← calendário com flags LY/MoM/ontem
      ├── semantic.dim_produto     ← catálogo de SKUs
      ├── semantic.dim_cliente     ← clientes anonimizados (cpf_hash)
      ├── semantic.dim_canal       ← canais com categorias legíveis
      └── semantic.dim_geografia   ← hierarquia geográfica
                │
                ▼
      semantic.fato_vendas         ← tabela fato com surrogate keys
                │
      ┌─────────┼──────────┬────────────┬─────────────┐
      ▼         ▼          ▼            ▼             ▼
mart.vendas  mart.vendas  mart.vendas  mart.vendas   mart.vendas     ← tabelas tratadas para uso direto da LLM
_diarias     _mensais     _cliente     _produto      _cupom

```

---

## 2. Arquitetura

### 2.1 Camadas

| Camada | Dataset | Responsabilidade |
|---|---|---|
| Raw | `ecomm_genie.raw` | Dados brutos — não modificados |
| Trusted | `ecomm_genie.trusted` | Limpeza, normalização e exclusão de dados sensíveis |
| Semantic | `ecomm_genie.semantic` | Dimensões + Fato |
| Mart | `ecomm_genie.mart` | Tabelas pré-agregadas para consumo direto pela LLM |

### 2.2 Estratégia de carga

O projeto possui duas procedures com estratégias diferentes:

**Full load** (`ecomm_genie_semantic_full_load`) — recria todos os objetos do zero com `CREATE OR REPLACE`. Usar na primeira execução ou para reconstrução completa.

**Incremental** (`ecomm_genie_semantic_incremental`) — atualiza apenas os dados dos últimos 14 dias via `MERGE` na trusted e fato, e `DELETE + INSERT` no mart diário. Usar na execução diária para reduzir custo e tempo de processamento.

### 2.3 Otimizações BigQuery

- `trusted.tb_vendas` e `semantic.fato_vendas` particionadas por `dt_venda` e clusterizadas, para que filtros temporais leiam apenas os blocos relevantes
- `mart.vendas_diarias` particionada por `dt_venda` e clusterizada por `canal_semantico`, `marca` e `região`
- Trusted lida uma vez por execução: todas as dimensões e a fato leem dela, sem releitura do raw
- `SAFE_DIVIDE()` em todas as divisões, sem risco de erro por divisão por zero
- `MERGE` no incremental, evitando reprocessamento de histórico completo onde apenas campos de status/valores podem mudar

---

## 3. Estrutura do repositório

```
ecomm-genie-semantica/
│
├── sql/
│   ├── ecomm_genie_semantic_full_load.sql    ← Procedure de carga completa (first run)
│   └── ecomm_genie_semantic_incremental.sql  ← Procedure incremental (execução diária)
│
├── schema/
│   └── ecomm_genie_metadata_schema.yaml      ← Dicionário semântico para a LLM
│
└── README.md                                 ← Este arquivo que vos fala
```

---

## 4. Objetos

### Dimensões (`ecomm_genie.semantic`)

| Tabela | Grain | Descrição |
|---|---|---|
| `dim_tempo` | 1 linha = 1 data | Calendário com flags de período para filtros diretos pela LLM |
| `dim_produto` | 1 linha = 1 SKU | Catálogo de produtos com marca e categoria |
| `dim_cliente` | 1 linha = 1 cpf_hash | Clientes anonimizados para CRM |
| `dim_canal` | 1 linha = canal + fonte + mídia | Canais com `canal_semantico` criado em linguagem natural |
| `dim_geografia` | 1 linha = região + uf + cidade | Hierarquia geográfica de entrega |

### Fato (`ecomm_genie.semantic`)

| Tabela | Grain | Descrição |
|---|---|---|
| `fato_vendas` | 1 linha = 1 item de pedido | Tabela fato com surrogate keys e métricas |

### DataMart (`ecomm_genie.mart`)

| Tabela | Grain | Perguntas respondidas |
|---|---|---|
| `vendas_diarias` | dia + ciclo + canal + marca + categoria + região | Maioria das perguntas. Flags de período já inclusas. |
| `vendas_mensais` | mês + marca | Share de marca, variação MoM e YoY |
| `vendas_cliente` | cpf_hash | Top clientes, recência de compra, categoria e marca preferidas |
| `vendas_produto` | SKU + ciclo + apresentação | Individual x combo por ciclo comercial |
| `vendas_cupom` | cupom + canal + marca + mês | Receita atribuída por cupom. Desconto total por cupom.|

### Metadata schema (`schema/ecomm_genie_metadata_schema.yaml`)

Dicionário semântico consumido pelo orquestrador para montagem do system prompt da LLM. Define tabelas, campos, aliases em linguagem natural, guia de tabelas por tipo de pergunta, exemplos de perguntas suportadas e perguntas fora de escopo com alternativas.

---

## 5. Como executar

### Pré-requisitos

- Projeto GCP com os datasets `ecomm_genie.raw`, `ecomm_genie.trusted`, `ecomm_genie.semantic`, `ecomm_genie.mart` e `ecomm_genie.procedures` criados
- Tabela `ecomm_genie.raw.tb_vendas` populada

### Primeira execução (full load)

```sql
CALL ecomm_genie.procedures.ecomm_genie_semantic_full_load();
```

Cria todos os objetos do zero na ordem correta: trusted → dimensões → fato → marts.

### Execuções diárias (incremental)

```sql
CALL ecomm_genie.procedures.ecomm_genie_semantic_incremental();
```

Atualiza os dados dos últimos 14 dias, evitando reprocessar dados desnecessários e sem atualizações.

#### Por que janela de atualização?

Pedidos podem ter `status` ou valores financeiros atualizados após a criação, por conta de cancelamentos tardios e ajustes de faturamento que são comuns. A janela de 14 dias garante que essas atualizações sejam capturadas sem reprocessar o histórico completo. Podendo ser adaptada futuramente caso seja vista a necessidade

---

## 6. Decisões de modelagem e governança

### 6.1 Exclusão de `cpf_consumidor_full` — Dado Sensível

A `raw.tb_vendas` contém CPFs reais como inteiros na coluna `cpf_consumidor_full`. Este campo é excluído desde o step 1 e não existe em nenhum objeto. A camada expõe apenas `cpf_hash`, suficiente para análises de CRM. 

### 6.2 Métrica primária de receita — `vlr_venda_pago`

A raw layer possui três campos financeiros: `vlr_receita_bruta_omni` foi descartadopor conter uma quantidade considerável de valores nulos e negativos, confirmando ruído. `vlr_receita_faturada` é mantido como métrica secundária. `vlr_venda_pago` (sem nulos na base raw) é adotado como `receita_liquida`.

### 6.3 Ruídos tratados na trusted layer

| Campo | Problema | Tratamento |
|---|---|---|
| `apresentacao_combo` | Valores inconsistentes: `'INDIVIDUAL'`/`'individual'`, `'COMBO'`/`'combo'` | `UPPER(TRIM(...))` |
| `des_ciclo` | `'SEM INFO'` em ciclos sem campanha | Considerado como `'REGULAR'` |
| `des_midia_canal` | Erros de digitação  em `'salesfoce'` | Corrigido para `'salesforce'` |
| `flg_pedidos_cd` / `flg_pedidos_pickup` | Nulos ao invés de zeros | `COALESCE(campo, 0)` |

### 6.4 `dim_tempo` com flags de período

As flags booleanas (`flg_ontem`, `flg_ly_ontem`, `flg_mes_atual` etc.) são calculadas no momento da carga com base em `CURRENT_DATE()`. Elas permitem que a LLM filtre períodos com `WHERE flg_xxx = 1` sem calcular datas dinamicamente, eliminando uma possível fonte de erros, que seria a execução de JOINS.

As flags estão presentes no `mart.vendas_diarias` para que a LLM filtre diretamente aqui também. Nos marts com grain mensal, as flags são recalculadas com lógica de período.

### 6.5 `canal_semantico` em `dim_canal`

A coluna `canal_semantico` traduz combinações de canal e fonte de tráfego em rótulos naturais como `'Site - Google Orgânico'` e `'App - CRM'`. O valor `'IA'` está criado para captura futura de tráfego de buscadores de IA (Perplexity, ChatGPT), ausentes na base de 2022.

### 6.6 `quantidade_pedidos` na fato

O campo usa `ROW_NUMBER() OVER (PARTITION BY cod_pedido ORDER BY cod_material) = 1` para marcar o primeiro item de cada pedido. Isso permite contar pedidos únicos com `SUM(quantidade_pedidos)`, evitando `COUNT(DISTINCT cod_pedido)` em queries sobre a fato.

### 6.7 YoY via `LAG(12)` em `mart.vendas_mensais`

A variação ano a ano usa `LAG(faturamento, 12) OVER (PARTITION BY marca ORDER BY ano_mes)`, retornando o valor do mesmo mês 12 posições atrás na série. Retorna `NULL` quando não há 12 meses de histórico. Com dados de 2+ anos em produção, o YoY funcionará automaticamente sem mudança de código.

### 6.8 `share_marca_pct` em `mart.vendas_mensais`

O share é calculado com `PARTITION BY ano_mes` sobre um mart com grain `ano_mes + marca` (sem canal). Isso garante que o denominador seja o total real do mês, independente de canal.

---

## 7. Perguntas suportadas e fora de escopo

### Suportadas

| Pergunta | Tabela | Como é resolvida |
|---|---|---|
| "Faturamento de ontem no site x mesmo dia do ano passado?" | `mart.vendas_diarias` | `WHERE flg_ontem = 1` e `WHERE flg_ly_ontem = 1` |
| "SKU X vendeu mais como individual ou combo no Ciclo 01?" | `mart.vendas_produto` | `WHERE cod_ciclo = 202201`, `share_apresentacao_pct` já calculado |
| "Melhor ticket médio por categoria no Nordeste em dezembro?" | `mart.vendas_diarias` | `WHERE regiao = 'NORDESTE' AND mes = 12 GROUP BY categoria ORDER BY ticket_medio DESC` |
| "Share de faturamento de cada marca neste mês?" | `mart.vendas_mensais` | `WHERE flg_mes_atual = 1`, `share_marca_pct` já calculado |
| "Receita atribuída ao cupom X no canal Y?" | `mart.vendas_cupom` | Filtro por `des_cupom` e `canal_semantico` |
| "Top 10 clientes que mais gastaram em Perfumaria?" | `mart.vendas_cliente` | `ORDER BY receita_total DESC LIMIT 10` **(retorna `cpf_hash`, não CPF completo)** |

### Fora de escopo

| Pergunta | Motivo | Alternativa oferecida |
|---|---|---|
| "Top 10 CPFs que mais gastaram?" | **CPF real excluído por LGPD** | Top 10 por `cpf_hash` anonimizado |
| "Quanto vem de tráfego de IA (Perplexity, ChatGPT)?" | Dados de buscadores IA ausentes na base de 2022 | Performance por `canal_semantico` disponível |
| "Qual o ROI da campanha de cupons no Instagram?" | Dados de custo de mídia indisponíveis | Receita por `des_cupom` + `canal_semantico` |
| "Previsão de receita para o próximo mês?" | Apenas dados históricos realizados | Tendência MoM e variação do período |
| "Busca de produto por nome ('Malbec', 'Her Code')?" | Base de demonstração com produtos anonimizados | Solicitar código de SKU ou categoria |

---

## 8. Evoluções futuras

**Captura de Dados de Buscadores IA** — adicionar condições na `dim_canal` para classificar tráfego de Perplexity, ChatGPT e similares em `canal_semantico = 'IA'`, já criado no código atual.

**Integração com dados de custo de mídia** — criar `mart.valores_midia` com `investimento`, `receita_atribuida` e `roi` por canal e período, habilitando resposta completa à pergunta de ROI de campanhas.

**Enriquecimento de `dim_cliente`** — integrar com cadastro oficial de clientes para substituir a definição de dados geográficos baseados em "último pedido" por dados cadastrais reais, mantendo `cpf_hash` como chave de relacionamento.

**Feedback loop via Slack** — implementar reações positivas/negativas nas respostas do Genie para coleta de avaliações e priorização de melhorias no sistema.

---

*Victor Hugo Daroit — Núcleo Estruturante | Grupo Boticário — 2025*
