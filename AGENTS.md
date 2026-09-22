# Agent rules (data-quality-forge)

## Git authorship

Commits and pull requests must be authored **only** as GitHub user **`jaime-sql`** (Jaime Garcia).

- Never set Cursor, codeagent, CopilotBot, or any other bot as author or co-author.
- Do not add `Co-authored-by:` trailers for bots or assistants.
- Configure git locally as `jaime-sql` / the matching GitHub noreply email before committing.

## Secrets

- Do not invent, hard-code, or commit Databricks tokens, hosts-with-tokens, or `.databrickscfg`.
- Use GitHub Actions repository secrets `DATABRICKS_HOST` and `DATABRICKS_TOKEN`.
- Scripts and workflows must never print the token.

## Notebooks vs CI Job

- Prefer teaching TODOs / `NotImplementedError` in
  `notebooks/Week09_Lab04_DataCleansing_and_Profiling.py` (student lab).
- Keep `notebooks/Pipeline_Bronze_Quarantine_Silver.py` **fully runnable** for CI and the
  Databricks Job `data-quality-forge-pipeline`. Never point that Job at the Lab 4 notebook.
