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

Este pipeline expõe esses dados via API REST, ingere em janelas
incrementais e transforma num **lakehouse analítico**, permitindo
responder perguntas como *"qual habilidade da BNCC a turma 701 menos
domina?"* ou *"qual o pico de engajamento por dia da semana?"*.

> **Nota de honestidade técnica:** este é um pipeline em produção,
> não um exemplo de sandbox. Como qualquer pipeline real, tem ciclos
> de manutenção e incidentes documentados em
> [TROUBLESHOOTING.md](./TROUBLESHOOTING.md). No momento da publicação
> deste README, ele está em remediação após uma combinação de stockout
> regional de SKU + ajuste de cota de assinatura Azure — também
> documentada.

## 🏗️ Arquitetura

```
  FastAPI (JogandoLógica API)             ← fonte: REST + OpenAPI/Swagger
     │   GET /v1/events  (janela incremental)
     ▼
  Azure Data Factory                      ← orquestração
     │   TumblingWindowTrigger (diário)
     │   pl_ingest_bronze
     │      └─→ pl_transform_silver_gold (ExecutePipeline)
     ▼
  Azure Databricks  +  PySpark            ← processamento distribuído
     │
     ├── BRONZE  (raw)      Delta  ── eventos crus, schema validado
     ├── SILVER  (limpo)    Delta  ── deduplicação, tipagem, conformação
     └── GOLD    (marts)    Delta  ── agregações analíticas
     │
     ▼
  ADLS Gen2 (abfss://)  +  export versionado no GitHub
```

## 🧱 Camadas

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

Artefatos do pipeline versionados aqui no GitHub. Deploys via
**GitHub Actions** (workflow `github_export.yml`).

## 🗺️ Roadmap

- [x] Pipeline bronze (ingestão incremental da API)
- [x] Pipeline silver/gold (transformação e agregação)
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
