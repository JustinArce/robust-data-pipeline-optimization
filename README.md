# Robust Data Pipeline Optimization

**A reproducible research project for batch and streaming systems**  
**Status:** protocol drafted; implementation and benchmark results pending.

This project investigates how to choose data-pipeline architectures and configurations when runtime, cost, freshness, and reliability compete. It grows from the original [Spark join optimization study](https://github.com/JustinArce/robust-spark-optimization) into a multi-workload experimental platform. Its intended contribution is a documented benchmark, a controlled comparison of Kappa, Lambda, and hybrid execution, and a reproducible optimization study. **No speedup, cost saving, or architectural winner is claimed yet.**

The complete, source-backed methods and analysis plan are in [the study design](protocol/study-design.md). That document is a protocol for prospective experiments, not a completed paper.

## Research questions

1. After equal tuning budgets, how do Kappa, Lambda, and hybrid pipelines compare on held-out workloads?
2. Can sequential, constrained multi-objective search find better feasible trade-offs per unit of experiment cost than simpler search methods?
3. Which choices remain reliable under scale, skew, event disorder, concurrent load, and failure?
4. When do inexpensive, small-scale trials predict full-scale results well enough to guide search?

The study will report contrary and null findings alongside improvements. A future paper depends on real implementations, independently reproducible measurements, and an empirical contribution.

## System boundary

```text
Sources → Ingestion → Storage → Transformation → Serving → Consumption
                         │              ├─ bounded batch jobs
                         │              └─ unbounded stream jobs
                         └─ versioned, replayable data
```

The five data stages are **sources, ingestion, storage, transformation, and serving**. Consumption is downstream: queries, reports, and an optional API. Batch and stream processing are execution modes inside this architecture. A Kappa design replays history through its stream path; a Lambda design maintains batch and speed paths; the hybrid uses versioned shared storage with bounded or continuous execution as appropriate. These are treatments to measure, not labels assigned to particular engines. [Spark's shared structured APIs](https://spark.apache.org/streaming/) and [Flink's account of bounded versus unbounded execution](https://flink.apache.org/2019/02/13/batch-as-a-special-case-of-streaming-and-alibabas-contribution-of-blink/) motivate the comparison.

| Workload | Batch case | Streaming case | Main stressors |
| --- | --- | --- | --- |
| Join and enrichment | Historical fact joins | Incoming events joined to changing reference data | Scale, key skew, spills |
| Aggregation | Historical rollups | Event-time windows | Bursts, late events, state |
| Feature generation | Historical training features | Incremental feature updates | Freshness, replay, point-in-time correctness |

The **first executable milestone is the original Spark join**. Other workloads and architecture variants follow after its measurement harness and correctness oracle work.

## How experiments will work

Architecture is a costly, hard-to-change **whole-plot factor**. Configurations are tuned within each architecture. Deployment order is balanced across blocks; configuration order is randomized within deployments. The selected configurations are evaluated on the **same held-out datasets and event traces**, with fresh deployment replicates. This split-plot and paired design avoids treating many tuning trials within one deployment as independent evidence about the architecture. The rationale and analysis are in [Sections 4–6 of the protocol](protocol/study-design.md#4-experimental-units-and-split-plot-design).

The search begins with a balanced space-filling design and known defaults. A candidate research-grade search policy uses [BoTorch's qLogNEHVI](https://botorch.org/docs/v0.16.1/multi_objective) for noisy multi-objective evaluations, subject to explicit constraints. Cost-aware scheduling and multi-fidelity promotion are **additional policies to specify and test**; qLogNEHVI alone does not make an experiment cost-aware. Cost-matched Sobol, random, and simpler multi-objective search baselines will be included. The full methodology and references are in the protocol.

Correct output and recovery are gates. Runs that crash, time out, violate memory limits, or fail reconciliation remain in the trial record as failures or censored observations. Final claims will use held-out scenarios and uncertainty intervals, not optimizer predictions alone.

## Planned repository layout

```text
configs/                      versioned factors, scenarios, budgets
src/robust_pipeline_opt/
  data/                       generators, ingestion, schemas, oracles
  workloads/                  joins, aggregation, features
  architectures/              Kappa, Lambda, hybrid
  harness/                    execution, metrics, immutable trial records
  design/                     split plots, pairing, initial designs
  optimization/               baselines, surrogate search, fidelity policy
  analysis/                   Pareto and statistical analysis
tests/                        correctness, replay, recovery, harness
infra/                        reproducible local deployment
protocol/study-design.md      full research protocol and sources
experiments/                  frozen experiment specifications
results/                      derived artifacts and raw-data manifests
paper/                        manuscript material once evidence exists
```

These directories describe the intended implementation; they are not presented as completed software.

## Reproduction status and milestones

There is **no runnable benchmark command yet**. Commands, exact dependencies, dataset manifests, and a tested software compatibility matrix will be published with the first executable milestone. Until then, this repository is a research design.

1. Implement one small Spark join workload, output oracle, baseline, and complete trial record.
2. Validate run-to-run noise, measurement units, ordering effects, and independent replication.
3. Add the five-stage data path and streaming replay/correctness checks.
4. Run architecture-specific tuning and matched, held-out comparisons.
5. Extend to aggregation and feature workloads; release raw data manifests, analysis code, figures, and limitations.

Progress and deviations from the protocol should be documented before confirmatory evaluation. The study design retains the original Taguchi and response-surface ideas as comparison methods while correcting its run-count, peak-memory, tail-estimation, and cost-accounting issues.
