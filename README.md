# 📊 JogandoLógica Data Platform

> Pipeline de engenharia de dados **cloud-native** que ingere eventos de
> aprendizagem de uma plataforma educacional real, em arquitetura
> **medallion (bronze → silver → gold)** sobre Azure Databricks + Delta
> Lake, orquestrada por Azure Data Factory — acompanhado de um pipeline
> operacional leve que exporta um feed diário **pseudonimizado** em CSV.

**Stack:** Azure Data Factory · Azure Databricks · PySpark · Delta Lake · ADLS Gen2 · PostgreSQL · FastAPI · Python · Bash/cron · HMAC-SHA256 · Git

---

## 🎯 Contexto

A **JogandoLógica** é uma plataforma de jogos educacionais que uso em
sala de aula com meus próprios alunos do Ensino Fundamental II. Ela
gera milhares de eventos de atividade — sessões de jogo, missões,
acertos e erros — que ficam invisíveis num banco transacional.

Este projeto processa esses dados por **dois pipelines independentes**,
cada um com sua fonte e propósito:

- **Pipeline analítico (Azure Databricks + ADF):** lakehouse medallion
  que ingere da API REST pra responder perguntas como *"qual habilidade
  da BNCC a turma 701 menos domina?"* ou *"qual o pico de engajamento
  por dia da semana?"*
- **Pipeline operacional (cron no VPS + PostgreSQL):** export diário em
  CSV lendo direto o banco de produção, com o identificador do
  participante **pseudonimizado (HMAC-SHA256)**. Versionado neste mesmo
  repo — backup auditável, leve, sem depender de infra Azure.

> **Nota de honestidade técnica:** este é um pipeline em produção,
> não um exemplo de sandbox. Como qualquer pipeline real, tem ciclos
> de manutenção e incidentes documentados em
> [TROUBLESHOOTING.md](./TROUBLESHOOTING.md). O pipeline operacional
> passou por uma migração de execução (GitHub Actions → cron no VPS,
> ver abaixo). O pipeline analítico Azure esteve em remediação após uma
> combinação de stockout regional de SKU + ajuste de cota de assinatura
> — também documentada.

## 🏗️ Arquitetura

```
        FastAPI (REST + OAuth2)        PostgreSQL (ap_saas)
        eventos de aprendizagem         banco de produção
                  │                            │
                  ▼                            ▼
        ════════════════════════      ════════════════════════
         PIPELINE ANALÍTICO            PIPELINE OPERACIONAL
         (Azure Data Factory)          (cron no VPS)
        ════════════════════════      ════════════════════════
                  │                            │
         TumblingWindowTrigger          cron diário (03:00 BRT)
         pl_ingest_bronze               psql: UNION de 4 fontes
             └─→ pl_transform_silver_gold   HMAC-SHA256 (pseudonimização)
                  │                          CSV pseudonimizado → git push
                  ▼                            │
        Azure Databricks + PySpark             ▼
                  │                    exports/2026/
        ┌─────────┼─────────┐         activities_YYYY-MM-DD.csv
        ▼         ▼         ▼
      BRONZE    SILVER     GOLD
      (raw)    (limpo)   (marts)
      Delta     Delta     Delta
                  │
                  ▼
        ADLS Gen2 (abfss://)
```

**Por que dois pipelines?** O operacional é **simples, barato e
auditável** — cron + `psql` + Git, sem cloud. Serve como backup
versionado de CSVs (úteis pra inspeção rápida e replay). O analítico
é **escalável e dimensional** — Spark distribuído sobre Delta Lake
pra responder perguntas analíticas em volumes maiores. Caem em
contextos de uso diferentes.

## 🔁 Pipeline Operacional — `export_activities.sh` (cron no VPS)

Export diário em CSV agendado por **cron no VPS** (`/etc/cron.d`) com
`0 6 * * *` (03:00 BRT). Características técnicas:

- **Fonte: PostgreSQL de produção** (`ap_saas`). O script consulta o
  banco direto via `psql \copy`, unindo as **4 tabelas de atividade**
  (jogos, dever, lógika, trabalhos) num feed único — o mesmo conjunto
  que alimenta o ranking público (Hall da Fama).
- **Pseudonimização na origem (HMAC-SHA256):** o identificador do
  participante — aluno **ou** equipe — é hasheado com um *pepper*
  secreto que vive **só no servidor** (`export.env`, fora do Git),
  tornando os hashes irreversíveis a partir deste repo público. PII
  direta (nomes de aluno/professor, `senha_aluno`, `token`) é
  **descartada na origem** e nunca chega ao CSV.
- **Grão heterogêneo tratado explicitamente:** fontes de grão de aluno
  e de grão de equipe são conformadas num schema comum
  (`source, ref_id, escola, turma, participante_hash, participante_tipo,
  pontos, acertos, total, tempo_seg, status, occurred_at`).
- **Janela temporal** inferida como "ontem" ou parametrizável via
  `FROM_DATE`/`TO_DATE`, com `to_date` exclusivo (fronteiras em UTC).
- **Defensivo e idempotente**: `set -euo pipefail`, e só faz commit se
  o CSV do dia mudou de fato (`git diff --cached --quiet`).

Os CSVs ficam em `exports/YYYY/activities_YYYY-MM-DD.csv`, commitados
pelo bot `jogandologica-bot`.

> **Migração GitHub Actions → cron no VPS.** A versão original deste
> pipeline rodava em GitHub Actions. O runner sofria timeouts de
> conectividade recorrentes contra a infra Hostinger, então a execução
> foi migrada pra um cron local no VPS. O workflow antigo permanece no
> repo como `.github/workflows/export.yml.disabled` para referência
> histórica.

## 🔒 Privacidade & Governança

Como os dados envolvem **menores de idade**, o pipeline operacional
aplica pseudonimização antes de qualquer dado sair do servidor:

- **HMAC-SHA256 com pepper secreto** no identificador do participante —
  consistente ao longo do tempo (permite análise longitudinal) e
  irreversível a partir do repositório (o pepper nunca é commitado).
- **PII direta descartada na origem:** nomes, senhas e tokens não são
  exportados em hipótese alguma.
- `escola` e `turma` são mantidos em claro por valor analítico; como o
  indivíduo é hasheado, não permitem reidentificação de uma criança.

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

Este repositório versiona o **script de export operacional**
(`export_activities.sh`), a **documentação** e os **CSVs de saída**
(`exports/YYYY/`). O pipeline operacional roda via **cron em VPS
próprio** (Hostinger), após a migração do GitHub Actions descrita
acima. Os notebooks do pipeline analítico vivem no **workspace
Databricks**; espelhá-los neste repo é item de roadmap.

## 🗺️ Roadmap

- [x] Pipeline operacional (cron no VPS: PostgreSQL → pseudonimização HMAC → CSV → Git)
- [x] Pseudonimização (HMAC-SHA256) do identificador de aluno/equipe
- [x] Pipeline analítico bronze (ingestão incremental da API)
- [x] Pipeline analítico silver/gold (transformação e agregação)
- [x] Orquestração com TumblingWindowTrigger
- [x] Encadeamento bronze → silver → gold
- [ ] Espelhar os notebooks Databricks neste repo
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
