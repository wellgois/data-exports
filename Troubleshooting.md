# 🔧 Troubleshooting & operações

Este documento registra incidentes reais do pipeline em produção,
o raciocínio de diagnóstico e a correção aplicada. Vai ficando vivo
conforme novas situações aparecem — pipeline em produção sempre tem
história pra contar.

---

## Incidente 1 — Pipeline silenciosamente falhando há 8 dias

**Data:** Jun/2026
**Sintoma:** trigger `Started` no ADF, mas dashboards do projeto sem
dados novos desde 06/06.

### Diagnóstico

Olhando o **Monitor** do ADF (e via `az datafactory pipeline-run
query-by-factory`), as últimas ~24 execuções de `pl_ingest_bronze`
estavam todas com status **Failed**. O trigger `trigger_bronze_daily`
disparava certinho — mas toda janela falhava na mesma atividade
(`BronzeIngest`).

A mensagem da última falha apontou a causa real:

```
Databricks execution failed with error state: InternalError.
Reason: CLOUD_PROVIDER_RESOURCE_STOCKOUT (CLOUD_FAILURE).
SkuNotAvailable: The requested VM size for resource
'Standard_DS3_v2' is currently not available in location 'brazilsouth'.
```

**Causa raiz:** *capacity stockout* no Azure. O cluster do Databricks
estava configurado com `Standard_DS3_v2`, mas a região `brazilsouth`
não tinha capacidade desse SKU — e como o trigger é diário, todas as
janelas desde o início do stockout falharam em loop.

> Aprendizado: pipeline com trigger automático sem alerting é um ponto
> cego. Saber por canal de monitoramento, e não por "alguém notou que
> o dashboard parou", é prioridade do próximo ciclo.

### Correção

Editado o **Linked Service do Databricks no ADF** trocando
`newClusterNodeType` para uma família mais nova com capacidade
disponível (`Standard_D4ds_v5`). Mantida a versão de runtime
`14.3.x-scala2.12` e o autoscale `1:4`.

### Lições

1. Stockout de SKU em region específica é **comum** no Azure, e
   nenhum código corrige isso — é decisão de infra. A própria
   mensagem de erro da Microsoft sugere `enable flexible node types`
   pra deixar o Databricks negociar SKU em runtime.
2. Antes de qualquer ação, **ler a mensagem completa**: a
   notificação genérica do ADF ("falhou devido a falha de atividade")
   esconde a causa real, que está em `error.message` da activity-run.
3. Pra debug remoto e em mobilidade, o `az datafactory` CLI dá
   *exatamente* o que a UI mostra, sem depender do navegador:

   ```bash
   az datafactory pipeline-run query-by-factory \
     --factory-name $FACTORY --resource-group $RG \
     --last-updated-after $AFTER --last-updated-before $BEFORE \
     --query "value[].{status:status, msg:message, runId:runId}" -o yaml
   ```

---

## Incidente 2 — Erro 3202 `base_parameters` após corrigir o SKU

**Data:** Jun/2026
**Sintoma:** depois de trocar o SKU e republicar, novas execuções
passaram a falhar em segundos (5s, contra os ~30s do stockout),
com erro diferente:

```
Could not parse request object: Expected both 'key' and 'value'
to be set on elements in the field 'base_parameters'
```

### Diagnóstico

Erro **3202** do ADF, categoria *"Problema de configuração do
usuário"*. A atividade Notebook do Databricks recebe parâmetros via
`base_parameters` (lidos no notebook por `dbutils.widgets.get(...)`).
O ARM se recusa a executar quando algum item dessa lista tem **chave
ou valor vazio**.

Investigando o `pl_transform_silver_gold` no editor (atividade
`SilverTransform` → Configurações → Parâmetros de base), apareceu
uma linha em vermelho:

```
Nome:  pipeline_run_id
Valor: @pipeline().parameters.pipeline_run_id
Mensagem: "O parâmetro pipeline_run_id não foi encontrado em
           pl_transform_silver_gold"
```

**Causa raiz:** a expressão referenciava um parâmetro de pipeline
chamado `pipeline_run_id` que não existia (provavelmente removido em
algum refactor). Em runtime, a expressão resolvia pra string vazia, o
`value` ia em branco, e o ARM bloqueava a execução.

### Correção

Substituído o valor por **`@pipeline().RunId`** — expressão nativa do
ADF que retorna o ID de execução do pipeline sem depender de
parâmetro declarado. Aplicado tanto em `SilverTransform` quanto em
`BronzeIngest` (que tinha o mesmo padrão duplicado).

### Lições

1. **`Validate all` antes de `Publish all`.** O ARM faz a validação
   completa só no publish, então um erro como este só aparece quando
   você tenta gravar. Se já tiver alterações pendentes em outros
   pipelines, eles seguram a publicação inteira.
2. Quando a atividade falha em <10s, raramente é problema de cluster
   ou de código — quase sempre é validação do **request body** antes
   da chamada chegar no Databricks.
3. Padrões duplicados entre pipelines (mesmo `pipeline_run_id` no
   bronze e no silver) costumam quebrar juntos. Vale grep pra
   garantir.

---

## Incidente 3 — Cota Azure: combinação stockout + quota exceeded

**Data:** Jun/2026
**Sintoma:** após resolver os incidentes 1 e 2, o pipeline mudou de
erro mais uma vez:

```
AZURE_QUOTA_EXCEEDED_EXCEPTION (CLIENT_ERROR)
QuotaExceeded: Total Regional Cores quota.
Current Limit: 4, Current Usage: 0, Additional Required: 8.
```

### Diagnóstico

Diferente do incidente 1 (stockout — Azure não tinha capacidade),
agora a Azure **tinha** capacidade, mas a **assinatura** estava
limitada a 4 vCPUs de `Total Regional Cores` em `brazilsouth`. O
cluster configurado (driver + worker em `Standard_DS4_v2`) precisa
de 16 vCPUs — 4x mais que o permitido.

### Correção (em duas frentes)

**Imediata** — reconfigurar o linked service para **cluster
single-node** (`Standard_DS3_v2`, 0 workers), consumindo apenas
4 vCPUs. Permite o pipeline rodar dentro da cota atual.

**Permanente** — solicitação formal de aumento de cota (`Standard
DSv2 Family vCPUs` → 20) submetida via portal Azure. Resposta típica
em 24-48h. Após aprovação, voltar pro cluster multi-node padrão.

### Lições

1. Em conta gratuita ou Pay-As-You-Go nova, o limite de vCPUs por
   região é **muito baixo** (frequentemente 4). Pipelines de produção
   precisam de aumento de cota **antes** de qualquer otimização de
   código.
2. `SkuNotAvailable` (stockout) e `QuotaExceeded` (cota) têm
   mensagens parecidas mas causas opostas. Stockout depende do Azure;
   cota depende da sua assinatura. Saber distinguir economiza dias.
3. Cluster single-node Databricks (`spark.databricks.cluster.profile
   = singleNode`, `spark.master = local[*]`) é uma alternativa
   subestimada pra workloads pequenos: roda Spark de verdade, sem
   gastar cores em rede inter-worker. Bom pro bootstrap e pra debug.

---

## Padrões de operação

### Pegar a mensagem real de uma run que falhou

```bash
FACTORY="adf-jogandologica"; RG="rg-jogandologica"
LATEST=$(az datafactory pipeline-run query-by-factory \
  --factory-name "$FACTORY" --resource-group "$RG" \
  --last-updated-after "$(date -u -d '1 day ago' +%Y-%m-%dT%H:%M:%SZ)" \
  --last-updated-before "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --query "reverse(sort_by(value[?status=='Failed'], &runStart))[0].runId" -o tsv)

az datafactory activity-run query-by-pipeline-run \
  --factory-name "$FACTORY" --resource-group "$RG" --run-id "$LATEST" \
  --last-updated-after "$(date -u -d '1 day ago' +%Y-%m-%dT%H:%M:%SZ)" \
  --last-updated-before "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --query "value[].{atividade:activityName, status:status, erro:error.message}" \
  -o yaml
```

### Ver os parâmetros reais que o ADF mandou pro Databricks

```bash
az datafactory activity-run query-by-pipeline-run \
  --factory-name $FACTORY --resource-group $RG --run-id $RUNID \
  --last-updated-after ... --last-updated-before ... \
  --query "value[0].input.baseParameters" -o yaml
```

Útil quando o erro fala em `base_parameters` mas a UI mostra
parâmetros aparentemente corretos. O `input.baseParameters` é o que
**de fato** foi enviado.

---

## Roadmap de confiabilidade

- [ ] Alerta no ADF (email/Teams) quando uma run falha — fim do ponto cego
- [ ] Cluster policy com flexible node types (fallback automático de SKU)
- [ ] Quality gates entre camadas (silver não roda se bronze veio com 0 linhas)
- [ ] SLO de frescor de dados (gold ≤ 1h após a janela do bronze)
- [ ] Backfill automatizado quando o trigger detecta intervalo Failed
