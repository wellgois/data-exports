# 📊 JogandoLógica Data Platform

> Pipeline de engenharia de dados **cloud-native** que ingere eventos de
> aprendizagem de uma plataforma educacional real, em arquitetura
> **medallion (bronze → silver → gold)** sobre Azure Databricks + Delta
> Lake, orquestrada por Azure Data Factory.

**Stack:** Azure Data Factory · Azure Databricks · PySpark · Delta Lake · ADLS Gen2 · FastAPI · Python · Git/GitHub Actions

---

## 🎯 Contexto

A **JogandoLógica** é uma plataforma de jogos educacionais que uso em
sala de aula com meus próprios alunos do Ensino Fundamental II. Ela
gera milhares de eventos de atividade — sessões de jogo, missões,
acertos e erros — que ficam invisíveis num banco transacional.

Este projeto expõe esses dados via API REST e os processa por **dois
pipelines independentes**, cada um com seu propósito:

- **Pipeline analítico (Azure Databricks + ADF):** lakehouse medallion
  pra responder perguntas como *"qual habilidade da BNCC a turma 701
  menos domina?"* ou *"qual o pico de engajamento por dia da semana?"*
- **Pipeline operacional (GitHub Actions):** export diário em CSV
  versionado neste mesmo repo — backup auditável, leve, sem depender
  de infra Azure.

> **Nota de honestidade técnica:** este é um pipeline em produção,
> não um exemplo de sandbox. Como qualquer pipeline real, tem ciclos
> de manutenção e incidentes documentados em
> [TROUBLESHOOTING.md](./TROUBLESHOOTING.md). No momento da publicação
> deste README, ele está em remediação após uma combinação de stockout
> regional de SKU + ajuste de cota de assinatura Azure — também
> documentada.

## 🏗️ Arquitetura

```
                  FastAPI (JogandoLógica API)
                  REST + OAuth2 + OpenAPI/Swagger
                            │
            ┌───────────────┴───────────────┐
            ▼                               ▼
  ════════════════════════      ════════════════════════
   PIPELINE OPERACIONAL          PIPELINE ANALÍTICO
   (GitHub Actions)              (Azure Data Factory)
  ════════════════════════      ════════════════════════
            │                               │
   cron diário (03:00 BRT)         TumblingWindowTrigger
   OAuth2 client_credentials       pl_ingest_bronze
   paginação por cursor                └─→ pl_transform_silver_gold
   jq: JSON → CSV genérico                     │
            │                                  ▼
            ▼                       Azure Databricks + PySpark
   CSV versionado (Git)                        │
   exports/2026/                    ┌──────────┼──────────┐
   activities_YYYY-MM-DD.csv        ▼          ▼          ▼
                                  BRONZE     SILVER      GOLD
                                  (raw)     (limpo)    (marts)
                                  Delta      Delta      Delta
                                            │
                                            ▼
                                  ADLS Gen2 (abfss://)
```

**Por que dois pipelines?** O operacional é **simples, barato e
auditável** — basta o GitHub Actions, sem cloud. Serve como backup
versionado de CSVs (úteis pra inspeção rápida e replay). O analítico
é **escalável e dimensional** — Spark distribuído sobre Delta Lake
pra responder perguntas analíticas em volumes maiores. Caem em
contextos de uso diferentes.

## 🔁 Pipeline Operacional — `.github/workflows/export.yml`

Export diário em CSV via **GitHub Actions** com cron `0 6 * * *`
(03:00 BRT). Características técnicas:

- **OAuth2 client_credentials** contra a API JogandoLógica (token
  obtido em runtime via `/auth/token`, masked nos logs).
- **Paginação por cursor** com proteção contra loop infinito (limite
  de 1000 páginas, detecção de cursor repetido).
- **Janela temporal** parametrizável via `workflow_dispatch` ou
  inferida como "ontem", com tratamento explícito do `to_date`
  exclusivo (incluindo o dia final via +1d).
- **JSON → CSV genérico via `jq`**: união de chaves como cabeçalho,
  valores não-escalares serializados como JSON aninhado.
- **Defensivo**: `set -euo pipefail`, retry com backoff em todos os
  curls, timeouts conservadores.
- **Secrets do GitHub** pra credenciais — nada hardcoded.

Os CSVs ficam em `exports/YYYY/activities_YYYY-MM-DD.csv`, commitados
pelo bot `github-actions[bot]`. Vantagem desse pipeline: **funciona
sem nenhuma infra Azure** — útil pra auditoria e como fallback se o
pipeline analítico estiver em manutenção.

## 🧱 Pipeline Analítico — Camadas

### Bronze — `pipeline_bronze_ingest.py`

Extrai eventos da API JogandoLógica em janelas incrementais
(`from_date`/`to_date` passados pelo TumblingWindowTrigger do ADF via
`dbutils.widgets`), aplica um schema explícito (`StructType`) e
persiste em Delta Lake no ADLS Gen2. Credenciais lidas de **Databricks
Secrets** — nada hardcoded.

### Silver — `pipeline_silver_transform.py`

Limpeza, deduplicação e conformação dos eventos para um modelo
consistente. Aplica regras de qualidade (descarte de eventos órfãos,
normalização de identificadores de aluno e turma) e prepara a base
para agregação.

### Gold — `pipeline_gold_aggregations.py`

Agregações analíticas por aluno e turma. Modelo dimensional simples
com fato de atividade (grão = uma atividade do aluno) e dimensões de
aluno, turma e tempo.

## 🔑 Decisões técnicas

- **Ingestão incremental** por watermark (`from_date`/`to_date`),
  evitando reprocessar histórico a cada execução.
- **Databricks Secrets** para credenciais — segredo nunca entra no
  código nem no Git.
- **Delta Lake** (ACID, time travel) como formato de tabela em todas
  as camadas.
- **Storage parametrizado** (`abfss://bronze@<storage_account>...`) —
  o repo é público sem expor a conta de armazenamento real.
- **Identidade de execução:** as atividades do ADF recebem
  `@pipeline().RunId` como `pipeline_run_id`, permitindo rastrear no
  Databricks qual execução do ADF originou cada job.
- **Encadeamento de pipelines:** `pl_ingest_bronze` chama
  `pl_transform_silver_gold` via `ExecutePipeline`, permitindo ciclo
  ponta a ponta com único trigger, mas cada pipeline pode ser
  executado individualmente para debug.

## ⏱️ Orquestração

Execução agendada via **Azure Data Factory** com um
`TumblingWindowTrigger`, que dispara em janelas de tempo contíguas e
permite **rerun automático** de janelas que falham — característica
fundamental para um pipeline incremental.

## 🔧 Operações & Confiabilidade

Pipeline em produção; este projeto **não é um tutorial em sandbox**.
Algumas situações reais que apareceram e como foram tratadas estão
documentadas em [TROUBLESHOOTING.md](./TROUBLESHOOTING.md) —
incluindo um caso de *capacity stockout* do Azure que exigiu mudança
de família de SKU, e um bug de configuração de parâmetros que só era
visível no input runtime das atividades (`az datafactory
activity-run`), não no editor do ADF Studio.

## 📦 Versionamento

Todos os artefatos (notebooks Databricks, workflow GitHub Actions,
docs) versionados aqui. O pipeline operacional usa **GitHub Actions
nativo** pra deploy e execução agendada — sem servidor próprio.

## 🗺️ Roadmap

- [x] Pipeline operacional (GitHub Actions: API → CSV → Git)
- [x] Pipeline analítico bronze (ingestão incremental da API)
- [x] Pipeline analítico silver/gold (transformação e agregação)
- [x] Orquestração com TumblingWindowTrigger
- [x] Encadeamento bronze → silver → gold
- [ ] Aumento de cota Azure (em aprovação) → multi-node cluster
- [ ] Dashboard de learning analytics sobre a camada gold
- [ ] Enriquecimento das questões por habilidade BNCC via LLM
- [ ] Testes de qualidade de dados (Great Expectations / dbt tests)
- [ ] Alerting de runs falhas (fim do ponto cego)

## 👤 Sobre o autor

Sou professor de Matemática do Ensino Fundamental II em transição
para Engenharia de Dados. Construí e mantenho em produção a
plataforma educacional **JogandoLógica** (jogandologica.site) e a
**APIA** (euprofessor.site) — usadas pelos meus próprios alunos.
Este repositório transforma os dados reais delas numa plataforma
analítica cloud-native.

GitHub: [@wellgois](https://github.com/wellgois)
