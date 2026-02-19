# DBT Coding Assistant Guidelines (dbt Fusion)

## Purpose
Use this document as the default operating guide for AI/code assistants working in this dbt project.

## Scope
- Applies to files in `models/`, `tests/`, `macros/`, `seeds/`, and `snapshots/`.
- Do not edit generated artifacts in `target/`.
- Do not edit third-party package code in `dbt_packages/` unless explicitly requested.

## Project Conventions
- Keep model naming consistent:
  - Staging: `stg_<source>__<entity>`
  - Marts: `fct_<entity>`, `dim_<entity>`
- Keep SQL style consistent with existing project files:
  - lowercase SQL keywords
  - clear aliasing (`id as order_id`)
  - explicit `source()` / `ref()` usage
- In staging models, prefer light cleanup + renaming + type normalization only.
- In mart models, implement business logic and metrics.

## Assistant Workflow For Changes
1. Read impacted SQL + YAML files before editing.
2. Make the smallest safe change that satisfies the request.
3. Add/update tests with the change.
4. Run targeted validation first, then broader validation if needed.
5. Summarize exactly what changed and what was validated.

## Testing Expectations
- Every model should have YAML properties with `version: 2`.
- Add column-level tests where relevant:
  - `not_null`
  - `unique`
  - `relationships`
  - `accepted_values`
- Use singular tests in `tests/` for multi-row business assertions.
- This project has `require_generic_test_arguments_property: true` enabled.
  - Always put generic test args under `arguments:`.
  - Example:
    - `relationships: { arguments: { to: ref('...'), field: ... } }`

## dbt Fusion Best Practices
- Prefer deterministic, explicit SQL and Jinja.
- Keep lineage explicit with `ref()` and `source()`; avoid hard-coded relation names.
- Avoid unnecessary dynamic SQL generation in models when static SQL is sufficient.
- Keep macros predictable and side-effect free.
- Favor compile-safe patterns so parse/compile feedback stays fast and reliable.

## Performance And Reliability
- Select only required columns (avoid `select *` in production models).
- Push filters and type casting as early as practical.
- Use incremental logic only when data volume justifies it and keys are well-defined.
- Keep models modular; one transformation concern per model.

## Documentation Expectations
- Add model/test/column descriptions whenever the purpose is clear from business logic, naming, or surrounding context.
- If purpose is not clear, still add documentation using a generic placeholder and keep it explicit that follow-up is needed.
- Placeholder templates:
  - Model: `TODO: describe the business purpose of this model (grain, key transformations, and downstream use).`
  - Test: `TODO: describe the business rule validated by this test and why failures matter.`
- Keep YAML docs and tests in sync with SQL changes.
- Document non-obvious logic with short comments in SQL.

## Validation Commands (Typical)
- Targeted model:
  - `dbt build --select <model_name>`
- Model + parents/children:
  - `dbt build --select +<model_name>+`
- Test-only pass:
  - `dbt test --select <model_name>`
- Whole project (when requested):
  - `dbt build`

## Safety Rules
- Never remove or weaken tests without explicit approval.
- Never make broad refactors when the request is narrow.
- Call out assumptions when requirements are ambiguous.
- If validation cannot be run, explicitly state what was not verified.
