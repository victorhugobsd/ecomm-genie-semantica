## Comunicado de Release — E-comm Genie
#### Autor: Victor Hugo Daroit | Núcleo Estruturante
---

[Lançamento | E-comm Genie — Fundação Semântica para GenAI]

Olá pessoal! Tudo bem por aí?

Já pensaram em mandar uma mensagem no Slack e receber em segundos uma resposta como *"Qual foi o faturamento do Site ontem e o crescimento em número de vendas vs o mesmo dia do ano passado"*?

Pois é, a partir de agora isso vai ser possível! É com muito orgulho que apresento a fundação de dados do **E-comm Genie**, a camada semântica que vai alimentar nosso assistente de IA no `#fale-com-dados`!

*O que foi construído:*

- **Arquitetura dimensional completa** sobre a `tb_vendas`: camadas trusted -> semantic -> mart, tudo em BigQuery
- **Dimensão de tempo inteligente** com flags de período prontas (`flg_ontem`, `flg_ly_ontem`, `flg_mes_atual`...), permitindo que a LLM responda perguntas temporais sem risco de calcular datas erradas
- **Canal semântico**: traduzimos combinações técnicas de canal em linguagem natural (`"Site - Google Orgânico"`, `"App - CRM"`) para o Genie falar o mesmo idioma que vocês
- **5 marts pré-agregados** que respondem a grande maioria das perguntas recorrentes do canal sem consultar a base de dados inteira
- **Dados sensíveis eliminados**: CPF real excluído em todos os objetos desde a camada trusted, substituído por identificador anonimizado

*Exemplos do que o Genie vai conseguir responder:*

- Faturamento de ontem vs mesmo dia do ano passado, por canal, marca ou categoria
- Share de faturamento por marca no mês e variação MoM/YoY
- SKU X vendeu mais como individual ou dentro de combo em determinado ciclo?
- Qual categoria teve melhor ticket médio no Nordeste em dezembro?
- Top clientes por receita (identificados por hash anonimizado — LGPD)

*O que ainda não está no escopo desta versão:*

- Tráfego de buscadores IA (como Perplexity, ChatGPT), pois os dados não foram capturados na base atual (campo já reservado para quando o dado existir)
- ROI de campanhas requer dados de custo de mídia que não integram esta base. Receita atribuída por cupom está disponível como alternativa
- Busca de produto por nome, visto que a base tem apenas códigos de produto. Em produção com catálogo integrado isso será possível

---

**Quer entender como tudo isso funciona na prática?**
Vamos apresentar a solução ao vivo no dia 01/04 às 15h. 
Na sessão vamos abordar:
-  Visão geral da arquitetura e decisões tomadas
-  Que tipo de perguntas o Genie responde (e quais ele não responde, e por quê)
-  Demonstração da camada semântica no BigQuery
-  Como o nosso Núcleo de Insights vai conectar essa fundação ao assistente no Slack

Se você não está no invite, é só entrar em contato comigo!

---

O código está no repositório público do GitHub: https://github.com/victorhugobsd/ecomm-genie-semantica

Qualquer dúvida, sugestão ou pedido de acesso, podem acionar nosso time!
