# Design Pattern v0 Discussion

## Context

The project goal is not to build a generic Spark application template. The goal is to create an agentic system for Spark code optimization: a local platform where humans and agents can generate Spark jobs, run them, inspect physical plans and runtime behavior, compare alternatives, and learn which implementation choices actually improve execution.

That goal changes the architectural pitch. The creator of the program does not want the project to get stuck in architecture. That instinct is correct. If the platform becomes a large framework before we have enough optimization experiments, the project will start optimizing its own abstractions instead of optimizing Spark code.

The counter-risk is also real. Spark work has enough operational detail that a fully unstructured feature-oriented project can drift quickly. One agent may create sessions one way, another may hardcode paths, another may trigger actions casually, another may ignore event logs, and another may write a second local convention for the same IO problem. After a few iterations, the platform becomes harder for agents to reason about because there are too many competing patterns in the repository.

So the v0 pattern goal is modest: keep the workbench organized while the empirical work grows. The patterns are not here to freeze architecture. They are here to reduce cognitive load, avoid avoidable rework, make agent-generated code easier to review, and preserve comparability between optimization runs.

## External Framing

The references that matter most here are about software engineering practice and repository-level guidance for coding agents. The point is not that agents need a special architecture. The point is that the same practices that help humans understand a codebase also reduce drift when agents generate or modify code.

GitHub's Copilot documentation explicitly treats repository custom instructions as a place for project-specific coding standards, frameworks, tools, style preferences, test generation rules, and code-review guidance. That is directly related to this project: when an agent sees the same local rules for Spark sessions, IO, logging, tests, and execution flow, it is less likely to invent a parallel pattern. Reference: [GitHub Docs - About customizing GitHub Copilot responses](https://docs.github.com/en/copilot/concepts/prompting/response-customization).

VS Code's Copilot documentation makes the same point from the IDE side: custom instructions define common guidelines that influence generated code and can encode project-wide coding standards, architecture decisions, conventions, documentation standards, and file-specific rules. This supports keeping `agent_readme.md`, templates, Make targets, and Spark platform contracts concise and explicit. Reference: [VS Code - Use custom instructions](https://code.visualstudio.com/docs/copilot/customization/custom-instructions).

Google's engineering practices are not agent-specific, but they explain why the underlying habits matter. Their code review guide calls out design, functionality, complexity, tests, naming, comments, style, and documentation as review concerns. Those are exactly the areas where agent-generated code can drift if the repository does not make expectations visible. Reference: [Google Engineering Practices - Code Review](https://google.github.io/eng-practices/review/).

Google's guidance on small changes is also relevant. It argues that small, self-contained changes are easier to review thoroughly, easier to reason about, less likely to introduce bugs, easier to merge, and easier to roll back. For this project, that translates into keeping platform contracts, sample scripts, loaders, and observability changes small enough that agents and humans can reason about them independently. Reference: [Google Engineering Practices - Small CLs](https://google.github.io/eng-practices/review/developer/small-cls.html).

Martin Fowler's Continuous Integration article reinforces the validation side: each integration should be verified by an automated build and tests to detect integration errors quickly. In an agentic workflow, `make tests`, `make smoke`, and `make spark-logs` play that role locally. They prevent style and structure from becoming only documentation; they make the expected behavior executable. Reference: [Continuous Integration](https://martinfowler.com/articles/continuousIntegration.html).

The CodePlan paper is a useful research reference because it frames repository-level coding as a harder problem than localized code generation. It argues that repo-level tasks require planning, dependency awareness, previous-change context, and task-specific instructions. That supports this project's small platform layer: shared contracts and predictable structure make repository-level Spark changes easier for agents to plan. Reference: [CodePlan: Repository-level Coding using LLMs and Planning](https://arxiv.org/abs/2309.12499).

The AGENTS.md evaluation paper adds an important caution. It found that context files can make agents explore and test more, and agents tend to respect instructions, but excessive or unnecessary requirements can also reduce task success and increase cost. This is the strongest external argument for keeping our v0 rules minimal: enough guidance to reduce drift, not so much guidance that every task carries irrelevant constraints. Reference: [Evaluating AGENTS.md: Are Repository-Level Context Files Helpful for Coding Agents?](https://arxiv.org/abs/2602.11988).

Spark's own documentation remains relevant because this project is about Spark optimization, not generic Python scripting. Spark History can reconstruct UI state after an application ends when event logs are persisted, and Spark SQL exposes many tuning and execution controls. Centralizing defaults and event-log observability makes optimization comparisons more trustworthy. References: [Spark Monitoring and Instrumentation](https://spark.apache.org/docs/latest/monitoring.html) and [Spark SQL Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html).

## My Evaluation Of The v0 Choice (CODEX GPT 5.5)

My valuation is positive, with one important constraint: these patterns should stay as guardrails, not become the main product.

The strongest decision is separating reusable platform mechanics from optimization code. `SparkSessionFactory`, config loading, IO specs, dataset helpers, logging, and `SparkPlatJob` remove repeated setup decisions without hiding the Spark transformations. That is the correct level of abstraction for now. It gives agents a stable path without making every Spark experiment look identical.

The second strong decision is making IO config-driven but keeping logic code-driven. Paths, formats, write modes, and options belong in `lakehouse.yaml`; transformations remain in Python. This matters for agentic optimization because we want generated code to focus on the transformation and execution behavior, not on rediscovering where data lives.

The sanity-check split is not a major architectural idea; it is basic Spark hygiene. It is still worth writing down in v0 because agents often add counts, ordering, `show()`, or other actions while trying to prove that a job worked. Transformation jobs should stay focused on the transformation. Sanity scripts can run bounded checks when we intentionally want validation and event-log evidence.

Another useful decision is keeping script headers and templates explicit. This may look verbose, but it is practical for an agent-heavy repository. A script should tell the next reader what it does, how to run it, which steps it follows, and how it participates in the local platform. That reduces prompt-to-prompt drift.

### Why Guardrails Matter Here

Feature-oriented development is still the right direction, but "feature" in this project should mean a Spark optimization capability, experiment, sample flow, loader improvement, observability slice, or agent workflow. It should not mean every feature invents its own local runtime conventions.

The guardrails help in four ways:

- They reduce cognitive load for humans reviewing generated code.
- They reduce ambiguity for agents deciding where to add or modify behavior.
- They make Spark runs more comparable by standardizing session, config, IO, logs, and validation flow.
- They prevent early experiments from leaving behind incompatible mini-patterns that later become migration work.

This is the real value of the v0 design. It is not architectural elegance. It is keeping the project legible while the optimization system is still learning what it needs to become.

### Opinion Boundary

The evaluation from this point forward is my opinion as Codex, a GPT-5-based coding agent in this session. Treat it as technical judgment, not as a project rule. I am not labeling this as GPT-5.5 because this environment identifies me as Codex based on GPT-5.

### Where The Current Patterns Are Strong

The current structure is strong for a v0 platform because it creates a stable path without making the codebase rigid:

- `src/apps/...` can remain feature-oriented and experiment-friendly.
- `src/spark_platform/...` owns reusable platform mechanics.
- `src/config/lakehouse.yaml` owns entity/layer IO contracts.
- `make tests` protects reusable Python behavior without a Spark cluster.
- `make smoke` validates Spark, MinIO, Delta, History, and event logs.
- `make spark-logs` validates ClickHouse observability ingestion.

This is enough structure for consistent collaboration. It also leaves room for empirical changes. If a better ingestion contract appears after real repeated examples, we can extract it then. If `SparkPlatJob` becomes too restrictive, we can change it with evidence instead of preference.

### Where The Current Patterns Could Hurt Us

The biggest risk is premature framework growth. If every new case creates a new base class, factory, registry, or generic hook, the project will become abstract before it has enough real optimization workloads to justify those abstractions. That would slow down the exact learning loop this platform exists to support.

The second risk is false consistency. A script can technically use `SparkPlatJob` and still be poorly structured if it hides actions in transformations, hardcodes paths, or creates side effects outside the intended flow. The contract helps, but review still matters.

The third risk is documentation drift. We now have enough docs that stale docs can become a problem. The important docs should stay close to behavior: README for the entry path, operations for commands, this discussion for design intent, and sample script docs for coding shape.

The fourth risk is overfitting to the first customer sample. It is only a working example. The real evaluation starts when we add more Spark optimization scenarios and see which conventions still hold.

### What I Would Keep Stable

I would keep these as v0 defaults:

- Spark app names live in runner scripts.
- Spark session defaults live in `SparkSessionFactory`.
- Layer and dataset paths live in `lakehouse.yaml`.
- Normal transformation jobs use `SparkPlatJob`.
- Ingestion edges may stay simpler until repeated cases justify an ingestion contract.
- Validation actions live in sanity/check scripts, not transformation jobs.
- DataFrame API is preferred for samples, but SQL is allowed when the workload is naturally SQL-first.
- Event logs and ClickHouse are the normal path for plan/runtime inspection.
- Avoid broad `collect()`, `toPandas()`, `show()`, casual `take()`, and unnecessary `orderBy()`.

These are not arbitrary style rules. They exist because they preserve comparability across Spark runs and make event-log observability more useful for optimization analysis.

### What I Would Not Add Yet

I would not add a broad plugin framework, registry system, or many ABCs yet. We do not have enough real optimization use cases.

I would not create a generic ingestion abstraction from the first fake landing script. I would wait until there are at least two or three real ingestion styles and then extract only what repeats.

I would not force all jobs to be DataFrame-only forever. The project should prefer DataFrame APIs for samples, but Spark SQL is a valid interface. The rule should be: use the interface that makes the transformation clearer and easier to observe.

I would not normalize every Spark event type immediately. Keeping raw event JSON gives us replayability. Normalize the next event types when queries prove they are useful.

### Real Valuation

For the current goal, I would rate the design direction around 8/10.

Why not lower: the project already has a working local platform, pinned images, cached dependencies, Spark/Delta/MinIO/History/ClickHouse integration, repeatable smoke flow, fast tests, sample scripts, and an event-log path into ClickHouse. That is a strong base for a Spark code optimization system.

Why not higher: the design has only been tested with a small sample flow. The Go loader is still monolithic. The ingestion side is intentionally informal. We do not yet have enough optimization workloads to prove the abstractions will hold. The docs are useful now, but they will need pruning as the platform matures.

The right next move is not more architecture. The right next move is more evidence: add another optimization scenario, run it through the same platform path, inspect the event logs and physical plans, and see what repeats. Patterns should be promoted after repetition, not after taste.

### Practical Rule For Future Contributors And Agents

Feature-oriented does not mean pattern-free. It means features own their experiment or capability while shared platform code owns repetitive mechanics.

Before adding a new Spark feature, ask:

1. Does this feature need a new pattern, or can it use the existing session/config/IO/job shape?
2. Is this an ingestion edge, a transformation job, a validation script, an observability loader, or an agent workflow?
3. Are paths and write behavior in config instead of buried in code?
4. Are Spark actions intentional and bounded?
5. Will Spark History and ClickHouse explain what happened after the job finishes?
6. Is any new abstraction backed by at least two real use cases?

If a new prompt produces code that ignores these questions, the project will drift. If every prompt follows them blindly, the project may become rigid. The useful middle is to treat these patterns as defaults with explicit exceptions.

### Final thoughs

As an agent, I think your instinct is correct. The project should not become architecture-first, but agentic Spark optimization needs a clean operating surface. If every generated feature uses a different shape, future agents will spend more context understanding local variation than reasoning about Spark performance.

The v0 patterns are valuable because they reduce cognitive congestion. They give humans and agents shared expectations for session creation, IO, logging, lifecycle, smoke validation, and event-log observability. That makes generated code easier to review and makes optimization runs easier to compare.

The discipline now is restraint. Keep the workbench organized, but do not turn it into a framework ceremony. Let real Spark optimization cases tell us which abstractions deserve to become permanent.
