# Robust Optimization of Batch and Streaming Data Pipelines

**Status: research protocol and implementation plan; no empirical results are claimed yet.**

This project studies a practical systems question: **under variable workloads and operational disturbances, which data-pipeline architecture and configuration offers the best feasible trade-off among performance, resource cost, data freshness, and reliability?** It extends the repository's original Spark-join proposal—screening, robust design, and sequential tuning—into a reproducible, multi-workload experimental platform. The intended outputs are open-source software, a benchmark dataset and harness, an auditable experiment record, and a manuscript-ready evaluation. A future paper is contingent on implementation, valid measurements, and a novel or useful empirical finding.

This document is the living research protocol linked from the [repository README](../README.md). Decisions marked **proposed** must be frozen in a dated protocol release before confirmatory data are collected. Results and claims will be added only after experiments run.

## Abstract (prospective)

Data-pipeline performance tuning is an expensive, noisy, constrained computer experiment. Batch and streaming systems also differ in their treatment of history, state, late events, and recovery. We propose a benchmark and optimization framework spanning join, aggregation, and feature-generation workloads. Architecture is assigned as a hard-to-change whole-plot factor; configurations are tuned within architecture; optimized candidates are then compared on paired, held-out workloads. Initial space-filling designs, replication, constrained multi-objective Bayesian optimization, and multi-fidelity evaluation are combined with explicit correctness gates and hierarchical uncertainty analysis. The study will report feasible Pareto sets, optimization cost, architecture interactions, and limits of generalization. **No performance result is asserted in this abstract.**

## 1. Research questions and contribution boundary

| ID | Question | Primary evidence |
| --- | --- | --- |
| RQ1 | How do Kappa, Lambda, and a hybrid architecture compare after each receives the same tuning budget? | Paired held-out differences by workload, with whole-plot uncertainty |
| RQ2 | Does sequential constrained multi-objective search find better feasible trade-offs per experimental cost than space-filling or simpler search? | Feasible hypervolume versus cumulative evaluation cost over independent optimizer seeds |
| RQ3 | Which configurations remain feasible and useful under scale, skew, burstiness, lateness, concurrency, and failure? | Scenario-specific outcomes, constraint violation rates, and robust fronts |
| RQ4 | How well do low-fidelity evaluations predict full-scale rankings and feasibility? | Rank agreement, calibration, promotion mistakes, and cost saved |
| RQ5 | When does shared transformation logic produce equivalent batch and stream outputs? | Reconciliation and replay tests with declared event-time semantics |

The main contribution is the **combination and evaluation** of rigorous design, architecture comparison, and multi-objective optimization in an executable data-engineering setting. Using an existing acquisition function is not, by itself, an algorithmic novelty claim. A method paper would require a clearly specified new method and independent algorithm benchmarks; an empirical systems paper could instead contribute the benchmark, measurements, and architecture findings.

### Falsifiable expectations, not findings

- **H1:** Architecture-by-workload interactions are material: no architecture dominates every workload and scenario.
- **H2:** A sequential search improves feasible hypervolume per unit of evaluation cost against a cost-matched space-filling baseline on at least one workload, but need not win universally.
- **H3:** Rankings obtained at small scale can reverse at larger scale or higher skew.
- **H4:** Paired workload traces and explicit whole-plot replication give more precise comparisons than unpaired, configuration-level analysis.

These are research hypotheses. The protocol will also report null and contrary results.

## 2. Scope and system under test

### Five stages plus consumption

```text
Sources → Ingestion → Storage → Transformation → Serving → Consumption
           │             │              │              │
           │             │              ├─ bounded batch jobs
           │             │              └─ unbounded stream jobs
           │             └─ versioned, replayable tables and event log
           └─ schema validation and source metadata
```

1. **Sources:** reproducibly generated operational records, reference data, and event traces; selected public data may be added with provenance and licenses.
2. **Ingestion:** batch imports and event capture, including schema and identifier validation.
3. **Storage:** durable event log, immutable raw snapshots, and versioned analytical tables; document retention and replay limits.
4. **Transformation:** cleaning, joins, aggregations, and feature computation in bounded and unbounded execution modes.
5. **Serving:** workload-specific materializations with explicit freshness and consistency contracts.
6. **Consumption:** queries, benchmark reports, experiment dashboard, and optional API. Consumption is downstream of the five stages, not a sixth processing stage.

### Workloads

| Workload | Bounded task | Unbounded task | Principal stressors |
| --- | --- | --- | --- |
| Join/enrichment | Large fact-to-dimension and fact-to-fact joins | Event enrichment against changing reference data | Size, skew, reference updates, spills |
| Aggregation | Historical window/rollup recomputation | Event-time windows with late events | Bursts, state growth, watermark policy |
| Feature generation | Historical training features | Incremental online/offline materialization | Point-in-time correctness, freshness, replay |

Each workload needs a deterministic **logical output specification** and oracle. Source generators must expose independent seeds for keys, payloads, timestamps, arrival disorder, and faults. Production traces, if used later, need separate provenance and privacy review.

### Architecture treatments

- **Kappa:** one stream-oriented transformation path, including replay of historical events through that path.
- **Lambda:** distinct batch recomputation and low-latency paths, merged according to a documented serving rule.
- **Hybrid:** versioned shared storage and common transformation contracts, with bounded batch and streaming execution selected per task.

These are *architectural treatments*, not synonyms for processing engines. Engine choice is held constant in the first comparison wherever technically feasible; a Spark-batch/Flink-stream implementation is a separate follow-up factor. Spark Structured Streaming shares structured APIs with batch Spark [S1]; Flink documents meaningful optimization differences between bounded and continuous execution [S2]. The hybrid is a hypothesis to test, not a presumed winner. A Kappa replay is valid only within the event log's retained history or from a demonstrably equivalent archival source.

## 3. Estimands, outcomes, and constraints

Let $w$ denote workload, $a$ architecture, $x$ its valid configuration, $z$ a workload/environment scenario, $b$ a deployment block, and $r$ an independent repetition. An observation is $Y(w,a,x,z,b,r)$, including failures and timeouts.

The architecture estimand is the paired difference between **policies selected after equal search budgets**, evaluated on the same held-out scenario distribution. It is conditional on the declared hardware, software versions, workload generator, tuning budget, and selection rule. It is not a universal architecture ranking. For an optimizer-method claim, repeat the *entire search* under independent seeds; repeating only the final configuration does not measure search variability.

| Outcome | Operational definition to freeze before confirmation |
| --- | --- |
| Batch duration | Wall time from submission to committed, queryable output; separately report startup and steady processing |
| Streaming freshness | Event-time-to-queryable-output distribution, with clock synchronization and treatment of late data stated |
| Throughput | Correctly committed records per second at a specified offered load; queue growth reported |
| Cost | Metered resource-seconds × published unit rates, plus storage and checkpoint/write costs when in scope; report raw resource use independently of assumed prices |
| Peak memory/state | Maximum observed per worker/executor and total state size; specify measurement interval and overhead |
| Recovery | Time to resume correct output after an injected fault, plus duplicates/missing records |
| Correctness | Record-level and aggregate reconciliation to an oracle, including event-time, update, and deduplication semantics |

**Primary objective vector, proposed:** minimize batch duration or stream freshness, and minimize normalized resource cost. Report throughput and recovery as secondary outcomes. Use at most two or three primary objectives per analysis because high-dimensional Pareto sets and hypervolume become difficult to interpret and compute [O4]. A cross-workload score requires a predeclared workload mixture and normalization; otherwise show workload-specific fronts.

**Hard gates, proposed:** correct output, zero unexplained loss/duplication, successful recovery, peak memory below the stated physical limit, and workload-specific freshness or completion service level. A crashed or timed-out run is recorded as infeasible/censored, never silently discarded or imputed as a fast run. “No excessive spills” must be replaced by a numeric threshold if used as a gate. Executor memory setting is a control; observed peak process/container memory is an outcome. The original proposal's 16 GB memory ceiling remains a candidate limit, subject to the actual testbed specification.

**Robustness:** report performance by scenario and a predeclared risk summary, such as worst-scenario performance or conditional value at risk. Do not use a P95 computed from four or five independent batch runs. Streaming event-level tail latency needs multiple independent traces/restarts; millions of correlated events in one run do not constitute millions of independent architecture replicates. A chance constraint such as $P(M \leq 16\,\mathrm{GB}) \geq 0.99$ requires enough independent evidence and an uncertainty bound; it cannot be certified from a few runs.

## 4. Experimental units and split-plot design

Architecture deployment is hard to change. Configuration settings are easier to change **within** a deployed architecture. The comparison therefore uses restricted randomization and a split-plot structure [D1–D3].

| Stratum | Experimental unit | Assigned factors | Independent replication |
| --- | --- | --- | --- |
| Whole plot | Fresh architecture deployment/reset in a time/hardware block | Architecture; engine only in a separate engine study | Multiple deployments per architecture across blocks |
| Subplot | Configuration run inside that deployment | Valid tuning controls, randomized order | Runs/configurations within each deployment |
| Scenario/repeat | Matched trace or dataset replay | Noise scenario, seed, fault schedule | New traces and repeated deployments, as feasible |

**Design rule:** cycle architecture order across blocks using restricted randomization; randomize configuration order within each whole plot. Before switching architecture, reset the declared system state or record carryover. Include a repeated baseline or sentinel run at intervals to detect temporal drift. A/B versus B/A order is balanced where deployment count permits. The analysis must use a whole-plot variance term for architecture and a subplot term for configurations; treating all configuration trials as independent architecture replicates is pseudoreplication.

The initial architecture study uses **matched, fixed configurations** to characterize systems. The final comparison uses configurations selected by equal-budget tuning within each architecture. The same held-out traces are then run under each selected configuration, with architecture deployments replicated. Pairing uses common dataset/event seeds [D4], but order is randomized to guard against cache and time effects [D5]. Pairing may reduce variance; it does not remove whole-plot uncertainty or make runs independent.

### Proposed confirmatory analysis

For each workload and metric, estimate paired architecture differences with a hierarchical model or a randomization-respecting analysis that includes deployment block, whole plot, trace, and run-order effects. Inspect residuals and heteroscedasticity. Use cluster/bootstrap resampling at the **highest independent level** (deployment/block and trace as appropriate), not event-level bootstrap over correlated stream records. Report point estimates, uncertainty intervals, and raw paired plots. For predeclared primary comparisons, state the multiplicity strategy; secondary comparisons are exploratory. Performance ratios may be easier to interpret on a log scale [D6].

## 5. Scenario design and data splits

| Dimension | Candidate levels / distribution | Why it matters |
| --- | --- | --- |
| Data volume | Pilot, medium, target scale | Scale-dependent reversals |
| Key frequency | Uniform, moderate Zipf-like skew, severe skew | Join and partition imbalance |
| Arrival process | Steady, burst, backlog/replay | Queueing and state pressure |
| Event disorder | On-time, moderate lateness, extreme lateness | Watermarks and corrections |
| Concurrent load | Isolated and controlled background load | Noisy neighbors |
| Failures | None, worker restart, checkpoint interruption | Recovery semantics |
| Reference updates | Static and changing dimension data | Temporal join correctness |

The exact distributions and target rates will be released with generator code. Deliberately extreme scenarios are identified separately from the estimated operating distribution; do not average them as though they were equally probable production conditions.

Partition independent seeds/traces into **design**, **model-check**, and **locked confirmatory hold-out** sets before tuning. The optimizer sees only design data. Model-check data may guide a documented protocol amendment; it cannot subsequently be called untouched hold-out. Confirmatory data are opened only after configurations, reference point, analysis plan, and comparison rules are frozen. Report all amendments.

## 6. Sequential experiment design

The research combines established DOE controls with modern model-based search. There is no universal best optimizer across unknown landscapes, conditional search spaces, and evaluation budgets. The algorithmic comparison is therefore part of the study.

### Phase 0 — measurement-system and feasibility pilot

Run baselines repeatedly across fresh deployments and traces. Estimate variation at machine/deployment, trace, and repeat levels, and test cold/warm cache and order effects. Instrument source-to-sink correctness, clock offsets, resource usage, Spark/Flink plans, and storage snapshots. Use pilot variance and cost to choose repetition counts and search budget rather than claiming that an arbitrary fixed count is statistically adequate [D5–D7]. Pilot data can inform the protocol but do not enter confirmatory inference.

### Phase 1 — initial design and screening

Within each architecture/workload, define a **typed, conditional, valid** configuration space. Use scrambled Sobol or another space-filling design for continuous controls, balanced categorical coverage, known default/anchor configurations, and purposeful replication. Sobol sequences require care with sample sizes and transformations [O1]. Reject invalid combinations before execution; log their reason. Screening models inspect important effects and interactions, but selection is driven by predictive utility and feasibility rather than a naive ANOVA p-value ranking from a mixed-level, unbalanced, adaptive design.

Preserve the original Taguchi outer-array and signal-to-noise concept as a **transparent historical baseline** on a compatible fixed subset. Do not collapse categorical levels merely to force an L16 array. The former README's 3 × 3 categorical grid crossed with approximately 13 response-surface points would yield approximately **117**, not 30, control combinations before outer noise scenarios. Fixed factorial/CCD designs may still be useful in a local, low-dimensional follow-up, with split-plot restrictions respected.

### Phase 2 — constrained, noisy multi-objective search

The research-grade candidate is **qLogNEHVI** in BoTorch for noisy, parallel, multi-objective evaluations, with explicit constraint modeling. BoTorch warns that legacy qNEHVI has numerical issues and recommends its log counterpart; the underlying log-improvement reformulation has a published methodological basis [O2–O4, O9]. Its suitability for our mixed/conditional spaces and measured noise is an empirical question, not a promise. A simpler Ax-managed study may be used where its model and constraints fit; exact library versions and acquisition settings are recorded.

At each iteration: (1) fit or update objective and constraint models; (2) validate calibration on available design data; (3) propose a small batch of valid candidates; (4) run them in randomized, blocked order with matched traces where possible; (5) record failures and resource cost; (6) decide whether to replicate, promote fidelity, or explore. Do **not** label qLogNEHVI itself “cost-aware”: cost-aware candidate/fidelity selection needs an additional specified acquisition policy or scheduling rule [O5].

**Equal-budget optimizer baselines:** scrambled Sobol, random search, a transparent Optuna multi-objective configuration, and optionally a response-surface/Taguchi-derived policy. Compare feasible hypervolume at the **same cumulative resource cost**, not merely the same trial count. Repeat complete search trajectories under independent seeds and, where practical, randomized deployment order. Freeze the hypervolume reference point and metric scaling before hold-out; show sensitivity to reasonable reference points. Report front coverage/diversity and constraint failures as well as hypervolume. Optuna documents current constrained multi-objective samplers [O6].

### Phase 3 — multi-fidelity promotion

Candidate fidelities may vary dataset size, event-trace duration, replication count, or resource scale. These dimensions are not interchangeable: short traces can miss state growth and failures, while small data can reverse join rankings. Validate low-to-high fidelity correlation and feasibility prediction first. A promotion policy should reserve a portion of budget for full-fidelity exploration to avoid permanently excluding slow-starting but strong configurations. BoTorch documents multi-objective multi-fidelity optimization [O7]; the specific policy implemented here must be declared and benchmarked against full-fidelity-only search. Simulation can debug the harness but cannot substantiate system performance claims.

### Phase 4 — robust candidate selection and locked confirmation

Select a small, predeclared number of feasible candidates per architecture from the design-data Pareto set. A single “knee” is not a uniquely objective business decision; publish the full feasible front and apply any preference/utility rule only if declared beforehand. Re-run candidates and defaults on held-out scenarios with fresh deployments and paired traces. Report scenario-specific failures and uncertainty. Robust multi-objective optimization under input uncertainty is an active method area [O8], but its assumptions should not be conflated with environmental disturbances or checkpoint failures.

### Stopping and budget rules

The final protocol will state maximum resource budget, maximum wall time, minimum independent whole-plot replications, minimum full-fidelity checks, failure/time-out policy, and stopping criteria. A plateau in estimated hypervolume alone is insufficient if feasibility uncertainty remains high. Interim plots are descriptive; optional stopping rules and confirmatory analysis are fixed in advance.

## 7. Factor-space governance

Configuration factors are workload- and engine-specific. Examples include join strategy, adaptive-query-execution settings, shuffle partitions, executor memory, file target size, stream parallelism, trigger/checkpoint interval, and state backend. Each factor has units, allowed values, default, dependencies, restart cost, and expected effect. Architecture and engine are not casually mixed into a single flat optimizer.

- Watermark and deduplication policy are part of the **correctness contract**. They may be varied only within a preapproved semantic envelope; otherwise the experiment changes the task being solved.
- Cloud/spot interruption policy is an environmental or deployment treatment, not simply “noise” if chosen by the operator.
- Spark version is a software-version block or separate treatment; it is not silently randomized across configuration trials.
- Invalid combinations and hardware limits are excluded by a machine-readable validator before model fitting and execution.
- Search spaces, defaults, and baseline configurations are versioned; changing them creates a new study epoch.

## 8. Engineering and reproducibility contract

Every run stores: `study_id`, `trial_id`, `whole_plot_id`, `block_id`, `architecture`, `engine`, `workload`, `configuration`, `search_policy`, `optimizer_seed`, `trace_id`, `data_snapshot`, `fidelity`, `run_order`, `code_commit`, `container_digest`, dependency versions, hardware profile, start/end times, raw metrics, failure status, logs, query plan, checkpoint identifier, and output snapshot. Raw records are immutable; aggregates and plots are derived.

The initial open-source implementation target is Spark batch and Structured Streaming, Kafka-compatible event capture, Iceberg tables, an object-store-compatible local testbed, PostgreSQL experiment metadata, and BoTorch/Optuna analysis. Additional engines are added only after the Spark baseline and harness are working. Iceberg documents Spark writes and Flink streaming writes, including relevant commit and maintenance semantics [S3–S4]. Pin and publish a tested compatibility matrix instead of claiming that arbitrary latest versions interoperate.

Minimum executable artifacts:

```text
protocol/              frozen design, hypotheses, analysis plan, amendments
workloads/             generators, traces, logical oracles, validity rules
architectures/         Kappa, Lambda, hybrid implementations
experiments/           design generation, scheduler, trial schema, analysis
tests/                 correctness, replay, recovery, metric checks
results/               raw-data manifests, derived tables, figures, limitations
paper/                 manuscript sources after evidence exists
```

The first public release should run one small Spark join benchmark end-to-end with a baseline and audit trail. Later releases add the other workloads and architecture treatments. The README will link reproducible commands when they exist; commands are intentionally not invented here.

## 9. Analysis and reporting checklist

1. Publish dataset and scenario definitions, testbed bill of materials, versions, and architecture diagrams.
2. Report every candidate's execution status; quantify missingness, crashes, and censored timeouts.
3. Show raw paired observations, per-scenario results, and uncertainty—not only means or an attractive Pareto plot.
4. Separate optimizer performance (entire repeated search trajectories) from final configuration performance (held-out evaluation).
5. Account for split-plot randomization and hierarchical variance; avoid treating events or subplot trials as whole-plot replicates.
6. Show baselines, total search cost, sensitivity to price model and hypervolume reference point, and negative findings.
7. Provide replay/correctness evidence for each architecture and state clearly where semantics differ.
8. Distinguish measured outcomes, modeled estimates, and simulated debugging results in every figure and table.
9. State external-validity limits: synthetic workloads, local hardware, chosen engines, finite scenario catalog, and implementation maturity.

## 10. Relationship to the original proposal

The original repository centered on a Spark join with Taguchi screening, response-surface optimization, and Optuna tuning. This protocol **retains the Spark join as workload one**, retains classical DOE and robust-design baselines, and retains sequential optimization. It corrects four methodological weaknesses: (i) a miscounted factorial × CCD budget, (ii) estimating P95 from too few batch runs, (iii) testing a peak-memory limit using mean memory, and (iv) treating trial counts rather than actual resource cost as the experiment budget. It adds architecture-level split plots, paired hold-out evaluation, explicit correctness, multi-fidelity validation, and modern noisy multi-objective search.

## References and methodological rationale

**Systems and storage**

- **[S1]** Apache Spark, [Structured Streaming](https://spark.apache.org/streaming/) — shared structured batch/stream APIs.
- **[S2]** Apache Flink, [Batch as a Special Case of Streaming](https://flink.apache.org/2019/02/13/batch-as-a-special-case-of-streaming-and-alibabas-contribution-of-blink/) — bounded and unbounded execution can favor different join and scheduling strategies.
- **[S3]** Apache Iceberg, [Spark Writes](https://iceberg.apache.org/docs/latest/spark-writes/) — versioned table writes and merge behavior.
- **[S4]** Apache Iceberg, [Flink Writes](https://iceberg.apache.org/docs/latest/flink-writes/) — streaming sink semantics and maintenance caveats.

**Design, pairing, and benchmarking**

- **[D1]** Robinson, Brenneman, and Myers, [An Intuitive Graphical Approach to Understanding the Split-Plot Experiment](https://jse.amstat.org/v17n1/robinson.html) — restricted randomization and distinct whole-plot/subplot errors.
- **[D2]** [Response Surface Designs within a Split-Plot Structure](https://www.tandfonline.com/doi/abs/10.1080/00224065.2005.11980310) — response-surface designs with hard-to-change factors.
- **[D3]** [A candidate-set-free algorithm for generating D-optimal split-plot designs](https://pmc.ncbi.nlm.nih.gov/articles/PMC3001117/) — flexible split-plot design construction.
- **[D4]** Heikes, Montgomery, and Rardin, [Using common random numbers in simulation experiments](https://journals.sagepub.com/doi/pdf/10.1177/003754977602700301) — matched random streams for comparing systems.
- **[D5]** Duplyakin et al., [Avoiding the Ordering Trap in Systems Performance Measurement](https://www.usenix.org/system/files/atc23-duplyakin.pdf) — order and carryover effects in systems experiments.
- **[D6]** [Quantifying Performance Changes with Effect Size Confidence Intervals](https://arxiv.org/abs/2007.10899) — uncertainty in performance ratios and multiple variation sources.
- **[D7]** Kalibera and Jones, [Rigorous Benchmarking in Reasonable Time](https://kar.kent.ac.uk/33611/45/p63-kaliber.pdf) — pilot-based allocation of repetitions across experimental levels.

**Sequential and multi-objective optimization**

- **[O1]** SciPy, [Quasi-Monte Carlo guide](https://docs.scipy.org/doc/scipy/tutorial/stats/quasi_monte_carlo.html) — Sobol designs and their sampling cautions.
- **[O2]** BoTorch, [Multi-objective Bayesian optimization](https://botorch.org/docs/v0.16.1/multi_objective) — qLogNEHVI and related acquisition functions.
- **[O3]** Daulton et al., [Parallel Bayesian Optimization of Multiple Noisy Objectives with Expected Hypervolume Improvement](https://arxiv.org/abs/2105.08195) — noisy parallel hypervolume search.
- **[O4]** BoTorch, [Noisy multi-objective tutorial](https://botorch.org/docs/v0.17.2/tutorials/multi_objective_bo) — implementation and current numerical warning about legacy qNEHVI.
- **[O5]** BoTorch, [Cost-aware Bayesian optimization](https://botorch.org/docs/v0.14.0/tutorials/cost_aware_bayesian_optimization) — cost-aware acquisition is an extra design choice.
- **[O6]** Optuna, [Multi-objective optimization](https://optuna.readthedocs.io/en/stable/tutorial/20_recipes/002_multi_objective.html) — contemporary constrained multi-objective baselines.
- **[O7]** BoTorch, [Multi-objective multi-fidelity optimization](https://botorch.org/docs/tutorials/Multi_objective_multi_fidelity_BO) — joint information-source and objective design.
- **[O8]** BoTorch, [Robust multi-objective optimization under input noise](https://botorch.org/docs/v0.17.0/tutorials/robust_multi_objective_bo) — robust methods and their documented scope.
- **[O9]** Ament et al., [Unexpected Improvements to Expected Improvement for Bayesian Optimization](https://arxiv.org/abs/2310.20708) — numerical pathologies and log-domain improvement reformulations.

References justify design choices; they do not establish this project's empirical results.
