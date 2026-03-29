-- =============================================================================
-- PROJETO     : E-comm Genie — Camada Semântica
-- AUTOR       : Victor Hugo Daroit | Núcleo Estruturante
-- VERSÃO      : 1.0.0
--
-- DESCRIÇÃO:
--   Procedure de full load que cria toda a estrutura de transformação da tb_vendas (raw) 
--   em camada semantica completa (trusted -> semantic -> mart), pronta para consumo
--   pelo E-comm Genie via Slack.
--
-- GOVERNANÇA:
--   - cpf_consumidor_full removido em todos os objetos — CPF real presente
--     na raw layer. dim_cliente usa apenas cpf_hash (MD5 anonimizado).
--   - Pedidos cancelados excluídos de algumas das métricas de receita via flg_valido.
--
-- RUÍDOS:
--   - apresentacao_combo: case inconsistente → UPPER(TRIM())
--   - des_ciclo: 'SEM INFO' → 'REGULAR'
--   - des_midia_canal: typo 'salesfoce' -> 'salesforce'
--   - vlr_receita_bruta_omni desconsiderado: 670 nulos e valores negativos
--     Métrica primária: vlr_venda_pago.
--
-- OTIMIZAÇÕES:
--   - Particionamento por dt_venda em trusted, na fato e nos marts diários.
--   - Clustering de tabela trusted, fato_vendas e vendas_diárias.
--   - Trusted lida uma vez: todas as dimensões e fato lêem dela.
--   - SAFE_DIVIDE() em todas as divisões: sem risco de divisão por zero.
--
-- =============================================================================

CREATE OR REPLACE PROCEDURE ecomm_genie.procedures.ecomm_genie_semantic_full_load()
BEGIN

-- =============================================================================
-- STEP 1: Limpeza, normalização e exclusão de dados sensíveis.
--
-- cpf_consumidor_full não é selecionado — exclusão por design de governança.
-- vlr_receita_bruta_omni desconsiderado por conta de muito ruído: 670 nulos e valores negativos (média -0.42, max 0.0).
-- =============================================================================

CREATE OR REPLACE TABLE ecomm_genie.trusted.tb_vendas 
PARTITION BY 
    dt_venda
CLUSTER BY 
    cod_pedido, 
    cod_material
AS
SELECT
    -- Chaves
    cod_un_negocio                                                  AS cod_un_negocio,
    cod_pedido                                                      AS cod_pedido,
    cod_material                                                    AS cod_material,
    cod_material_pai                                                AS cod_material_pai,
    -- Individual x Combo
    UPPER(TRIM(apresentacao_combo))                                 AS apresentacao_combo,
    -- Temporal
    DATE(dt_venda)                                                  AS dt_venda,
    TIMESTAMP(dt_hora_venda)                                        AS dt_hora_venda,
    -- Ciclo
    cod_ciclo                                                       AS cod_ciclo,
    CASE 
        WHEN TRIM(des_ciclo) = 'SEM INFO' 
        THEN 'REGULAR'
        ELSE TRIM(des_ciclo)
    END                                                             AS des_ciclo, -- 'SEM INFO' considerado como 'REGULAR'
    -- Produto
    marca_ind                                                       AS marca_ind,
    categoria_final_nivel1                                          AS categoria_final_nivel1,
    -- Geografia
    regiao                                                          AS regiao,
    uf                                                              AS uf,
    des_cidade                                                      AS des_cidade,
    -- Canal e Tráfego
    des_canal_venda_final                                           AS des_canal_venda_final,
    fonte_de_trafego_nivel_1                                        AS fonte_de_trafego_nivel_1,
    CASE 
        WHEN LOWER(TRIM(des_midia_canal)) = 'salesfoce' 
        THEN 'salesforce'
        ELSE LOWER(TRIM(des_midia_canal))
    END                                                             AS des_midia_canal,
    des_cupom                                                       AS des_cupom,
    -- Entrega/Retirada
    COALESCE(flg_pedidos_cd, 0)                                     AS flg_pedidos_cd,
    COALESCE(flg_pedidos_pickup, 0)                                 AS flg_pedidos_pickup,
    -- Status
    status_oms                                                      AS status_oms,
    flg_faturada                                                    AS flg_faturada,
    flg_aprovada                                                    AS flg_aprovada,
    CASE 
        WHEN flg_aprovada = 1 AND flg_faturada = 1
        THEN 1 
        ELSE 0
    END                                                             AS flg_valido, -- pedido válido para métricas de receita
    -- Valores
    COALESCE(vlr_receita_faturada, 0)                               AS vlr_receita_faturada,
    COALESCE(vlr_venda_pago, 0)                                     AS vlr_venda_pago,
    COALESCE(vlr_venda_desconto, 0)                                 AS vlr_venda_desconto,
    -- Cliente
    cpf_hash                                                        AS cpf_hash,
    CURRENT_TIMESTAMP()                                             AS dt_atualizacao
FROM 
    ecomm_genie.raw.tb_vendas
WHERE 1=1 
    AND dt_venda IS NOT NULL
;

-- =============================================================================
-- STEP 2: Dimensão Temporal para filtros e comparação de data.
--
-- Calendário completo gerado para range da base + margem de 1 ano para pedidos ainda em aberto,
-- com campos criador para futura estruturação das tabelas do mart
--
-- Criadas flags em datas chave como mesmo mes no ano passado, mesmo dia no ano passado, ontem,
-- entre outras datas que podem ser utilizadas em perguntas normalmente
-- =============================================================================

CREATE OR REPLACE TABLE ecomm_genie.semantic.dim_tempo AS
WITH 
    datas AS (
        SELECT 
            d AS dt_venda
        FROM 
            UNNEST(
                GENERATE_DATE_ARRAY(
                    '2022-01-01', 
                    DATE_ADD(CURRENT_DATE(), INTERVAL 365 DAY)
                )
            ) AS d
    ),
    ciclos AS (
        SELECT
            dt_venda,
            MAX(cod_ciclo)  AS cod_ciclo,
            MAX(des_ciclo)  AS des_ciclo
        FROM 
            ecomm_genie.trusted.tb_vendas
		GROUP BY
			dt_venda
    )
SELECT
    CAST(FORMAT_DATE('%Y%m%d', d.dt_venda) as INT64)                  AS id_tempo,
    d.dt_venda                                                      AS dt_venda,
    EXTRACT(YEAR    FROM    d.dt_venda)                             AS ano,
    EXTRACT(MONTH   FROM    d.dt_venda)                             AS mes,
    EXTRACT(QUARTER FROM    d.dt_venda)                             AS trimestre,
    EXTRACT(DAY     FROM    d.dt_venda)                             AS dia,
    FORMAT_DATE('%Y-%m',    d.dt_venda)                             AS ano_mes,
    FORMAT_DATE('%G-W%V',   d.dt_venda)                             AS ano_semana,
    CASE EXTRACT(MONTH FROM d.dt_venda)
        WHEN 1  THEN 'Janeiro'   WHEN 2  THEN 'Fevereiro'
        WHEN 3  THEN 'Março'     WHEN 4  THEN 'Abril'
        WHEN 5  THEN 'Maio'      WHEN 6  THEN 'Junho'
        WHEN 7  THEN 'Julho'     WHEN 8  THEN 'Agosto'
        WHEN 9  THEN 'Setembro'  WHEN 10 THEN 'Outubro'
        WHEN 11 THEN 'Novembro'  WHEN 12 THEN 'Dezembro'
    END                                                             AS nome_mes,
    CASE EXTRACT(DAYOFWEEK FROM d.dt_venda)
        WHEN 1 THEN 'Domingo'  WHEN 2 THEN 'Segunda'
        WHEN 3 THEN 'Terça'    WHEN 4 THEN 'Quarta'
        WHEN 5 THEN 'Quinta'   WHEN 6 THEN 'Sexta'
        WHEN 7 THEN 'Sábado'
    END                                                             AS dia_semana,
    COALESCE(c.cod_ciclo, 'tbd')                                    AS cod_ciclo,
    COALESCE(c.des_ciclo, 'REGULAR')                                AS des_ciclo,
    CASE 
        WHEN d.dt_venda = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)  
        THEN 1 
        ELSE 0 
    END                                                             AS flg_ontem,
    CASE 
        WHEN d.dt_venda = CURRENT_DATE() 
        THEN 1 
        ELSE 0 
    END                                                             AS flg_hoje,
    -- Mesmo dia de hoje do ano passado
    CASE 
        WHEN d.dt_venda = DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
        THEN 1 
        ELSE 0 
    END                                                             AS flg_ly_hoje, 
    -- Mesmo dia de ontem do ano passado
    CASE 
        WHEN d.dt_venda = DATE_SUB(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), INTERVAL 1 YEAR)
        THEN 1 
        ELSE 0 
    END                                                             AS flg_ly_ontem,
    -- Mês atual completo flagado
    CASE 
        WHEN FORMAT_DATE('%Y-%m', d.dt_venda) = FORMAT_DATE('%Y-%m', CURRENT_DATE())
        THEN 1 
        ELSE 0 
    END                                                             AS flg_mes_atual,
    -- Mês anterior completo flagado
    CASE 
        WHEN FORMAT_DATE('%Y-%m', d.dt_venda) = FORMAT_DATE('%Y-%m', DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))
        THEN 1 
        ELSE 0 
    END                                                             AS flg_ultimo_mes,
    -- Mesmo mês do ano passado flagado
    CASE 
        WHEN 1=1 
            AND EXTRACT(MONTH FROM d.dt_venda) = EXTRACT(MONTH FROM CURRENT_DATE())
            AND EXTRACT(YEAR  FROM d.dt_venda) = EXTRACT(YEAR FROM CURRENT_DATE()) - 1
        THEN 1 
        ELSE 0 
    END                                                             AS flg_ly_mes,
    -- Ano atual completo flagado
    CASE 
        WHEN EXTRACT(YEAR FROM d.dt_venda) = EXTRACT(YEAR FROM CURRENT_DATE())
        THEN 1 
        ELSE 0 
    END                                                             AS flg_ano_atual,
    -- Ano passado completo flagado
    CASE 
        WHEN EXTRACT(YEAR FROM d.dt_venda) = EXTRACT(YEAR FROM CURRENT_DATE()) - 1
        THEN 1 
        ELSE 0 
    END                                                             AS flg_ly_ano
FROM
    datas d
    LEFT JOIN   ciclos  c   ON  d.dt_venda = c.dt_venda
;

-- =============================================================================
-- STEP 3: Dimensão de Produto
--
-- Catálogo de SKUs com a hierarquia do produto.
-- Grain: 1 linha = 1 cod_material.
-- Não tenho acesso a base de cadastro de produtos, sendo assim não temos como buscar
-- produtos por "nome", como "Malbec", "Her Code", "Desodorante" etc. Apenas buscas
-- por código de categoria ou SKU.
-- =============================================================================

CREATE OR REPLACE TABLE ecomm_genie.semantic.dim_produto AS
SELECT 
    DISTINCT
    TO_HEX(
        MD5(
	    	CONCAT(
	    		SUBSTR(cod_material, 5),
	    		SUBSTR(cod_material_pai, 5)
	    	)
	    )
    )																AS id_produto,
    cod_material                                                    AS cod_material,
    cod_material_pai                                                AS cod_material_pai,
    marca_ind                                                       AS marca,
    categoria_final_nivel1                                          AS categoria
FROM 
    ecomm_genie.trusted.tb_vendas
;

-- =============================================================================
-- STEP 4: Dimensão de Cliente (sem CPF)
--
-- Clientes anonimizados para análises de CRM.
-- cpf_consumidor_full foi excluído daqui, mative apenas cpf_hash.
-- Como não tenho acesso ao cadastro do cliente, as informações geográficas são
-- do pedido mais recente do cliente para evitar duplicidade. Poderia ser melhorada
-- caso fosse cedido acesso a base de cadastro
-- =============================================================================

CREATE OR REPLACE TABLE ecomm_genie.semantic.dim_cliente AS
WITH 
    ultimo_pedido_cliente AS (
        SELECT
            cpf_hash,
            des_cidade,
            uf,
            regiao,
            ROW_NUMBER() OVER (PARTITION BY cpf_hash ORDER BY dt_venda DESC) AS rn
        FROM 
            ecomm_genie.trusted.tb_vendas
        WHERE 1=1
            AND cpf_hash IS NOT NULL
    )
SELECT
    cpf_hash                                                        AS id_cliente,
    cpf_hash                                                        AS cpf_hash,
    regiao                                                          AS regiao,
    uf                                                              AS estado,
    des_cidade                                                      AS cidade
FROM 
    ultimo_pedido_cliente
WHERE 1=1
    AND rn = 1
;

-- =============================================================================
-- STEP 5: Dimensão de Canal de Venda
--
-- Canal de venda com categorias mais "didáticas" para a LLM (coluna canal_semantico 
-- faz a combinação canal+fonte) que o usuário final provavelmente vai usar mais
-- naturalmente ("Google Orgânico", "Pago", "CRM").
--
-- Na base não foi encontrado nenhum label relacionado a IA, mas deixei pronto
-- para classificação caso no futuro esses casos apareçam.
-- =============================================================================

CREATE OR REPLACE TABLE ecomm_genie.semantic.dim_canal AS
SELECT 
    DISTINCT
    TO_HEX(
        MD5(
            CONCAT(
                COALESCE(des_canal_venda_final,''),
                COALESCE(fonte_de_trafego_nivel_1,''),
                COALESCE(des_midia_canal,'')
            )
	    )
    )																AS id_canal,
    des_canal_venda_final                                           AS des_canal_venda_final,
    CASE
        WHEN des_canal_venda_final IN ('Site','App') 
        THEN des_canal_venda_final
        WHEN des_canal_venda_final LIKE 'Mktp%'      
        THEN 'Marketplace'
        ELSE 'Outros'
    END                                                             AS canal_agrupado,
    fonte_de_trafego_nivel_1                                        AS fonte_de_trafego_nivel_1,
    COALESCE(des_midia_canal, 'nao_identificado')                   AS des_midia_canal,
    CONCAT(
        CASE
            WHEN des_canal_venda_final IN ('Site','App') 
            THEN des_canal_venda_final
            WHEN des_canal_venda_final LIKE 'Mktp%'      
            THEN 'Marketplace'
            ELSE 'Outros'
        END,
        ' - ',
        CASE
            WHEN 1=1
                AND fonte_de_trafego_nivel_1 = '1. Pagos' 
                AND des_midia_canal LIKE '%google%'
            THEN 'Google Pago'
            WHEN 1=1 
                AND fonte_de_trafego_nivel_1 = '1. Pagos'
                AND des_midia_canal LIKE '%facebook%'
            THEN 'Facebook Pago'
            WHEN fonte_de_trafego_nivel_1 = '1. Pagos'
            THEN 'Outros Pago'
            WHEN 1=1 
                AND fonte_de_trafego_nivel_1 = '2. Não Pagos'
                AND des_midia_canal LIKE '%google%'
            THEN 'Google Orgânico'
            WHEN fonte_de_trafego_nivel_1 = '2. Não Pagos'
            THEN 'Orgânico'
            WHEN fonte_de_trafego_nivel_1 = '3. CRM'
            THEN 'CRM'
            WHEN fonte_de_trafego_nivel_1 = '4. Brand Awareness'
            THEN 'Brand Awareness'
            WHEN des_midia_canal IN ('perplexity','chatgpt') 
            THEN 'IA'
            ELSE 'Outros'
        END
    )                                                               AS canal_semantico
FROM 
    ecomm_genie.trusted.tb_vendas
;

-- =============================================================================
-- STEP 6: Dimensão Geográfica
--
-- Grain: 1 linha = 1 combinação de uf + cidade.
-- =============================================================================

CREATE OR REPLACE TABLE ecomm_genie.semantic.dim_geografia AS
SELECT 
    DISTINCT
    TO_HEX(
        MD5(
            CONCAT(
                COALESCE(uf,''), 
                COALESCE(des_cidade,'')
            )
        )
    )                                                           AS id_geografia,
    regiao                                                      AS regiao,
    uf                                                          AS estado,
    des_cidade                                                  AS cidade
FROM ecomm_genie.trusted.tb_vendas
;

-- =============================================================================
-- STEP 7: Fato Vendas
--
-- Tabela fato central. 
-- Grain: 1 linha = 1 item de pedido.
-- Contém os IDs de todas as dimensões e também métricas financeiras.
-- Particionada por dt_venda, clusterizada por des_canal_venda_final e marca.
-- =============================================================================
CREATE OR REPLACE TABLE ecomm_genie.semantic.fato_vendas
PARTITION BY 
    dt_venda
CLUSTER BY 
    cod_pedido, 
    cod_material, 
    des_canal_venda_final, 
    marca_ind
AS
SELECT
    CURRENT_TIMESTAMP()                                         AS timestamp_atualizacao,
    -- Chaves das dimensões
    CAST(FORMAT_DATE('%Y%m%d', v.dt_venda) AS INT64)            AS id_tempo,
    TO_HEX(
        MD5(
	    	CONCAT(
	    		SUBSTR(cod_material, 5),
	    		SUBSTR(cod_material_pai, 5)
	    	)
	    )
    )															AS id_produto,
    v.cpf_hash                                                  AS id_cliente,
    TO_HEX(
        MD5(
            CONCAT(
                COALESCE(v.des_canal_venda_final,''),
                COALESCE(v.fonte_de_trafego_nivel_1,''),
                COALESCE(v.des_midia_canal,'')
            )
        )
    )                                                           AS id_canal,
    TO_HEX(
        MD5(
            CONCAT(
                COALESCE(v.uf,''), 
                COALESCE(v.des_cidade,'')
            )
        )
    )                                                           AS id_geografia,
    v.cod_pedido                                                AS cod_pedido,
    v.cod_material                                              AS cod_material,
    v.cod_material_pai                                          AS cod_material_pai,
    v.cpf_hash                                                  AS cpf_hash,
    v.dt_venda                                                  AS dt_venda, 
    v.cod_ciclo                                                 AS cod_ciclo,
    v.des_ciclo                                                 AS des_ciclo,
    v.status_oms                                                AS status_oms,
    v.des_canal_venda_final                                     AS des_canal_venda_final,
    v.marca_ind                                                 AS marca_ind,
    v.des_cupom                                                 AS des_cupom,
    CASE 
        WHEN v.flg_valido = 1 
        THEN v.vlr_venda_pago -- Usado valor pago pois outras colunas de valores possuem diversos valores nulos e negativos       
        ELSE 0 
    END                                                         AS receita_liquida,
    CASE 
        WHEN v.flg_valido = 1
        THEN v.vlr_venda_pago + v.vlr_venda_desconto -- Valor original do produto sem o desconto
        ELSE 0 
    END                                                         AS receita_bruta,
    CASE 
        WHEN v.flg_valido = 1 
        THEN v.vlr_venda_desconto    
        ELSE 0 
    END                                                         AS desconto,
    CASE 
        WHEN v.flg_valido = 1 
        THEN v.vlr_receita_faturada  
        ELSE 0 
    END                                                         AS receita_faturada,
    1                                                           AS quantidade_itens,
    CASE 
        WHEN ROW_NUMBER() OVER (PARTITION BY v.cod_pedido ORDER BY v.cod_material) = 1
        THEN 1
        ELSE 0
    END                                                         AS quantidade_pedidos,
    CASE 
        WHEN UPPER(v.apresentacao_combo) = 'COMBO'  
        THEN 1 
        ELSE 0 
    END                                                         AS flg_combo,
    v.flg_faturada                                              AS flg_faturada,
    v.flg_aprovada                                              AS flg_aprovada,
    v.flg_valido                                                AS flg_valido,
    CASE 
        WHEN v.status_oms = '4. Cancelado'          
        THEN 1 
        ELSE 0 
    END                                                         AS flg_cancelado,
    v.flg_pedidos_cd                                            AS flg_pedidos_cd,
    v.flg_pedidos_pickup                                        AS flg_pedidos_pickup
FROM 
    ecomm_genie.trusted.tb_vendas v
;

-- =============================================================================
-- STEP 8: Tabela de Vendas Diárias
--
-- Grain: 1 linha = dia + ciclo + canal + marca + categoria + região.
-- Particionada por dt_venda para eficiência em filtros temporais.
-- =============================================================================

CREATE OR REPLACE TABLE ecomm_genie.mart.vendas_diarias
PARTITION BY 
    dt_venda
CLUSTER BY 
    canal_semantico, 
    marca,
    regiao
AS
SELECT
    CAST(t.dt_venda as DATE)                                    AS dt_venda,
    t.ano                                                       AS ano,
    t.mes                                                       AS mes,
    t.nome_mes                                                  AS nome_mes,
    t.dia_semana                                                AS dia_semana,
    t.trimestre                                                 AS trimestre,
    t.ano_mes                                                   AS ano_mes,
    t.ano_semana                                                AS ano_semana,
    t.des_ciclo                                                 AS des_ciclo,
    t.cod_ciclo                                                 AS cod_ciclo,
    t.flg_ontem                                                 AS flg_ontem,
    t.flg_hoje                                                  AS flg_hoje,
    t.flg_ly_hoje                                               AS flg_ly_hoje,
    t.flg_ly_ontem                                              AS flg_ly_ontem,
    t.flg_mes_atual                                             AS flg_mes_atual,
    t.flg_ultimo_mes                                            AS flg_ultimo_mes,
    t.flg_ly_mes                                                AS flg_ly_mes,
    t.flg_ano_atual                                             AS flg_ano_atual,
    t.flg_ly_ano                                                AS flg_ly_ano,
    c.canal_semantico                                           AS canal_semantico,
    c.canal_agrupado                                            AS canal_agrupado,
    p.marca                                                     AS marca,
    p.categoria                                                 AS categoria,
    g.regiao                                                    AS regiao,
    g.estado                                                    AS estado,
    -- Métricas de receita (apenas flg_valido = 1)
    ROUND(SUM(f.receita_liquida), 2)                            AS faturamento,
    ROUND(SUM(f.receita_bruta), 2)                              AS receita_bruta,
    ROUND(SUM(f.desconto), 2)                                   AS desconto_total,
    -- Volume
    COUNT(DISTINCT f.cod_pedido)                                AS pedidos,
    COUNT(DISTINCT f.id_cliente)                                AS clientes_distintos,
    SUM(f.quantidade_itens)                                     AS itens_vendidos,
    -- Cancelamentos
    COUNT(
        DISTINCT  
        CASE 
            WHEN f.flg_cancelado = 1 
            THEN f.cod_pedido 
        END
    )                                                           AS pedidos_cancelados,
    -- Ticket médio por pedido válido
        ROUND(
            SAFE_DIVIDE(
                SUM(f.receita_liquida),
                NULLIF(
                    COUNT(
                        DISTINCT   
                            CASE 
                                WHEN f.flg_valido = 1
                                THEN f.cod_pedido 
                            END
                    )
                , 0)
            )
        , 2)                                                    AS ticket_medio,
        ROUND(
            SAFE_DIVIDE(
                SUM(f.desconto), 
                SUM(f.receita_bruta)
            ) 
            * 100
        , 2)                                                    AS desconto_medio_pct,
        ROUND(
            SAFE_DIVIDE(
                COUNT(DISTINCT CASE WHEN f.flg_cancelado = 1 THEN f.cod_pedido END),
                COUNT(DISTINCT f.cod_pedido)
            ) 
            * 100
        , 2)                                                    AS taxa_cancelamento_pct
FROM 
    ecomm_genie.semantic.fato_vendas                 f
    LEFT JOIN ecomm_genie.semantic.dim_tempo         t          ON  t.id_tempo      = f.id_tempo
    LEFT JOIN ecomm_genie.semantic.dim_canal         c          ON  c.id_canal      = f.id_canal
    LEFT JOIN ecomm_genie.semantic.dim_produto       p          ON  p.id_produto    = f.id_produto
    LEFT JOIN ecomm_genie.semantic.dim_geografia     g          ON  g.id_geografia  = f.id_geografia
GROUP BY 
    t.dt_venda, t.ano, t.mes, t.nome_mes, t.dia_semana, t.trimestre, t.ano_mes,
    t.ano_semana, t.des_ciclo, t.cod_ciclo, t.flg_ontem, t.flg_hoje, t.flg_ly_hoje,
    t.flg_ly_ontem, t.flg_mes_atual, t.flg_ultimo_mes, t.flg_ly_mes, t.flg_ano_atual,
    t.flg_ly_ano, c.canal_semantico, c.canal_agrupado, p.marca, p.categoria, g.regiao, g.estado
;

-- =============================================================================
-- STEP 9: Tabela de Vendas Mensais
--
-- Grain: 1 linha = mês +  marca.
-- Lê de mart.vendas_diarias já agregado (não da fato).
-- =============================================================================

CREATE OR REPLACE TABLE ecomm_genie.mart.vendas_mensais AS
WITH 
    base AS (
        SELECT
            ano_mes,
            ano,
            mes,
            nome_mes,
            trimestre,
            des_ciclo,
            marca,
            CASE
                WHEN ano_mes = FORMAT_DATE('%Y-%m', CURRENT_DATE())
                THEN 1
                ELSE 0
            END                                                 AS flg_mes_atual,
            CASE
                WHEN ano_mes = FORMAT_DATE('%Y-%m', DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))
                THEN 1
                ELSE 0
            END                                                 AS flg_ultimo_mes,
            CASE
                WHEN 1=1
                    AND mes = EXTRACT(MONTH FROM CURRENT_DATE())
                    AND ano = EXTRACT(YEAR  FROM CURRENT_DATE()) - 1
                THEN 1
                ELSE 0
            END                                                 AS flg_ly_mes,
            CASE
                WHEN ano = EXTRACT(YEAR FROM CURRENT_DATE())
                THEN 1
                ELSE 0
            END                                                 AS flg_ano_atual,
            CASE
                WHEN ano = EXTRACT(YEAR FROM CURRENT_DATE()) - 1
                THEN 1
                ELSE 0
            END                                                 AS flg_ly_ano,
            SUM(faturamento)                                    AS faturamento,
            SUM(receita_bruta)                                  AS receita_bruta,
            SUM(desconto_total)                                 AS desconto_total,
            SUM(pedidos)                                        AS pedidos,
            SUM(clientes_distintos)                             AS clientes_distintos,
            SUM(pedidos_cancelados)                             AS pedidos_cancelados,
            ROUND(
                SAFE_DIVIDE(
                    SUM(faturamento),
                    NULLIF(SUM(pedidos), 0)
                )
            , 2)                                                AS ticket_medio,
            ROUND(
                SAFE_DIVIDE(
                    SUM(desconto_total),
                    NULLIF(SUM(receita_bruta), 0)
                ) 
                * 100
            , 2)                                                AS desconto_medio_pct
        FROM 
            ecomm_genie.mart.vendas_diarias
        GROUP BY 
            ano_mes, ano, mes, nome_mes, trimestre, 
            des_ciclo, marca
    )
SELECT
    *,
    LAG(faturamento) OVER (
        PARTITION BY marca ORDER BY ano_mes
    )                                                           AS faturamento_mes_anterior,
    ROUND(
        SAFE_DIVIDE(
            faturamento - LAG(faturamento) OVER (
                PARTITION BY marca ORDER BY ano_mes),
            NULLIF(LAG(faturamento) OVER (
                PARTITION BY marca ORDER BY ano_mes), 0)
        ) * 100
    , 2)                                                        AS variacao_mom_pct,
    LAG(pedidos) OVER (
        PARTITION BY marca ORDER BY ano_mes
    )                                                           AS pedidos_mes_anterior,
    ROUND(
        SAFE_DIVIDE(
            pedidos - LAG(pedidos) OVER (
                PARTITION BY marca ORDER BY ano_mes),
            NULLIF(LAG(pedidos) OVER (
                PARTITION BY marca ORDER BY ano_mes), 0)
        ) * 100
    , 2)                                                        AS variacao_pedidos_mom_pct,
    LAG(faturamento, 12) OVER (
        PARTITION BY marca ORDER BY ano_mes
    )                                                           AS faturamento_ano_anterior,
    ROUND(
        SAFE_DIVIDE(
            faturamento - LAG(faturamento, 12) OVER (
                PARTITION BY marca ORDER BY ano_mes),
            NULLIF(LAG(faturamento, 12) OVER (
                PARTITION BY marca ORDER BY ano_mes), 0)
        ) * 100
    , 2)                                                        AS variacao_yoy_pct,
    LAG(pedidos, 12) OVER (
        PARTITION BY marca ORDER BY ano_mes
    )                                                           AS pedidos_yoy_anterior,
    ROUND(
        SAFE_DIVIDE(
            pedidos - LAG(pedidos, 12) OVER (
                PARTITION BY marca ORDER BY ano_mes),
            NULLIF(LAG(pedidos, 12) OVER (
                PARTITION BY marca ORDER BY ano_mes), 0)
        ) * 100
    , 2)                                                        AS variacao_pedidos_yoy_pct,
    ROUND(
        SAFE_DIVIDE(
            faturamento,
            SUM(faturamento) OVER (PARTITION BY ano_mes)
        ) 
        * 100
    , 2)                                                        AS share_marca_pct
FROM 
    base
;

-- =============================================================================
-- STEP 10: Tabela de Vendas por Cliente (sem CPF)
--
-- Grain: 1 linha = cliente (cpf_hash).
-- CRM: top clientes, frequência, recência, preferências.
-- =============================================================================

CREATE OR REPLACE TABLE ecomm_genie.mart.vendas_cliente AS
WITH 
    base_cliente AS (
        SELECT
            f.cpf_hash                                          AS cpf_hash,
            f.id_cliente                                        AS id_cliente,
            p.categoria                                         AS categoria,
            p.marca                                             AS marca,
            SUM(f.receita_liquida)                              AS receita_categoria,
            COUNT(DISTINCT f.cod_pedido)                        AS pedidos_categoria,
            MAX(f.dt_venda)                                     AS ultima_compra_categoria
        FROM 
            ecomm_genie.semantic.fato_vendas            f
            LEFT JOIN ecomm_genie.semantic.dim_produto  p       ON p.id_produto = f.id_produto
        WHERE 1=1
            AND f.flg_valido = 1
        GROUP BY
            f.cpf_hash, f.id_cliente, p.categoria, p.marca
    ),
    totais AS (
        SELECT
            cpf_hash                                            AS cpf_hash,
            id_cliente                                          AS id_cliente,
            ROUND(SUM(receita_categoria), 2)                    AS receita_total,
            SUM(pedidos_categoria)                              AS pedidos_total,
            ROUND(
                SAFE_DIVIDE(
                    SUM(receita_categoria),
                    NULLIF(SUM(pedidos_categoria), 0)
                )
            , 2)                                                AS ticket_medio,
            MAX(ultima_compra_categoria)                        AS ultima_compra,
            ARRAY_AGG(categoria ORDER BY receita_categoria DESC
                      LIMIT 1)[OFFSET(0)]                       AS categoria_preferida, -- Categoria preferida (maior receita)
            ARRAY_AGG(marca ORDER BY receita_categoria DESC
                      LIMIT 1)[OFFSET(0)]                       AS marca_preferida -- Marca preferida (maior receita)
        FROM 
            base_cliente
        GROUP BY 
            cpf_hash, id_cliente
    )
SELECT
    t.*,
    DATE_DIFF(CURRENT_DATE(), t.ultima_compra, DAY)             AS dias_desde_ultima_compra,
    CASE
        WHEN DATE_DIFF(CURRENT_DATE(), t.ultima_compra, DAY) <= 30  
        THEN 'Ativo'
        WHEN DATE_DIFF(CURRENT_DATE(), t.ultima_compra, DAY) <= 90  
        THEN 'Em risco'
        ELSE 'Inativo'
    END                                                         AS segmento_recencia
FROM totais t
;

-- =============================================================================
-- STEP 11: Tabela de Vendas por Produto
--
-- Grain: 1 linha = cod_material + cod_ciclo + apresentacao_combo.
-- =============================================================================

CREATE OR REPLACE TABLE ecomm_genie.mart.vendas_produto AS
SELECT
    f.cod_material                                              AS cod_material,
    f.cod_material_pai                                          AS cod_material_pai,
    p.marca                                                     AS marca,
    p.categoria                                                 AS categoria,
    f.cod_ciclo                                                 AS cod_ciclo,
    t.des_ciclo                                                 AS des_ciclo,
    t.ano_mes                                                   AS ano_mes,
    CASE 
        WHEN f.flg_combo = 1 
        THEN 'COMBO' 
        ELSE 'INDIVIDUAL' 
    END                                                         AS apresentacao_combo,
    COUNT(DISTINCT f.cod_pedido)                                AS pedidos,
    SUM(f.quantidade_itens)                                     AS itens_vendidos,
    ROUND(SUM(f.receita_liquida), 2)                            AS receita_liquida,
    ROUND(SUM(f.desconto), 2)                                   AS desconto_total,
    ROUND(
        SAFE_DIVIDE(SUM(f.receita_liquida),
                    NULLIF(COUNT(DISTINCT f.cod_pedido), 0))
    , 2)                                                        AS ticket_medio,
    ROUND(
        SAFE_DIVIDE(
            SUM(f.receita_liquida),
            SUM(SUM(f.receita_liquida)) OVER (
                PARTITION BY f.cod_material, f.cod_ciclo
            )
        ) * 100
    , 2)                                                        AS share_apresentacao_pct
FROM 
    ecomm_genie.semantic.fato_vendas                f
    LEFT JOIN ecomm_genie.semantic.dim_tempo        t           ON t.id_tempo    = f.id_tempo
    LEFT JOIN ecomm_genie.semantic.dim_produto      p           ON p.id_produto  = f.id_produto
    LEFT JOIN ecomm_genie.semantic.dim_canal        c           ON c.id_canal    = f.id_canal
WHERE 1=1 
    AND f.flg_valido = 1
GROUP BY 
    f.cod_material, f.cod_material_pai, p.marca, p.categoria, f.cod_ciclo, t.des_ciclo,
    t.ano_mes, CASE WHEN f.flg_combo = 1 THEN 'COMBO' ELSE 'INDIVIDUAL' END
;

-- =============================================================================
-- STEP 12: Tabela de Vendas por Cupom
--
-- Grain: 1 linha = cupom + canal_semantico + marca + mês.
-- =============================================================================

CREATE OR REPLACE TABLE ecomm_genie.mart.vendas_cupom AS
SELECT
    t.ano_mes                                                   AS ano_mes,
    t.ano                                                       AS ano,
    t.mes                                                       AS mes,
    t.nome_mes                                                  AS nome_mes,
    CASE
        WHEN t.ano_mes = FORMAT_DATE('%Y-%m', CURRENT_DATE())
        THEN 1
        ELSE 0
    END                                                         AS flg_mes_atual,
    CASE 
        WHEN t.ano_mes = FORMAT_DATE('%Y-%m', DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))
        THEN 1
        ELSE 0
    END                                                         AS flg_ultimo_mes,
    CASE
        WHEN 1=1
            AND mes = EXTRACT(MONTH FROM CURRENT_DATE())
            AND ano = EXTRACT(YEAR  FROM CURRENT_DATE()) - 1
        THEN 1
        ELSE 0
    END                                                         AS flg_ly_mes,
    f.des_cupom                                                 AS des_cupom,
    c.canal_semantico                                           AS canal_semantico,
    p.marca                                                     AS marca,
    COUNT(DISTINCT f.cod_pedido)                                AS pedidos,
    ROUND(SUM(f.receita_liquida), 2)                            AS receita_liquida,
    ROUND(SUM(f.desconto), 2)                                   AS desconto_total,
    ROUND(
        SAFE_DIVIDE(
            SUM(f.receita_liquida),
            NULLIF(COUNT(DISTINCT f.cod_pedido), 0))
    , 2)                                                        AS ticket_medio
FROM 
    ecomm_genie.semantic.fato_vendas                f
    LEFT JOIN ecomm_genie.semantic.dim_tempo        t           ON t.id_tempo   = f.id_tempo
    LEFT JOIN ecomm_genie.semantic.dim_canal        c           ON c.id_canal   = f.id_canal
    LEFT JOIN ecomm_genie.semantic.dim_produto      p           ON p.id_produto = f.id_produto
WHERE 1=1 
    AND f.flg_valido = 1
GROUP BY 
    t.ano_mes, t.ano, t.mes, t.nome_mes, CASE WHEN t.ano_mes = FORMAT_DATE('%Y-%m', CURRENT_DATE()) THEN 1 ELSE 0 END,
    CASE WHEN t.ano_mes = FORMAT_DATE('%Y-%m', DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)) THEN 1 ELSE 0 END,
    CASE WHEN 1=1 AND mes = EXTRACT(MONTH FROM CURRENT_DATE()) AND ano = EXTRACT(YEAR  FROM CURRENT_DATE()) - 1 THEN 1 ELSE 0 END, 
    f.des_cupom, c.canal_semantico, p.marca
;

END
;
