# Agent Guidelines

## Code Reuse

- Before adding new code, check `duckdb/` and `src/` for existing implementations of the same functionality.
- Whenever possible, reuse existing code — even if it requires refactoring.

## C++ Style

- Use tabs for indentation, spaces for alignment.
- Lines should not exceed 120 columns.
- Use `[u]int(8|16|32|64)_t` instead of `int`, `long`, etc.
- Use `idx_t` instead of `size_t` for offsets/indices/counts.
- Use const references for non-trivial objects.
- Use C++11 range-based for loops when possible.
- Always use braces for if statements and loops.
- Never use `const_cast`.

## Comment Conventions

- Try to keep comments short. In general, comments should be one short line. Only in exceptional situations should
  comments be more than one short line. Code should be mostly self-descriptive and too many large comments make code
  harder to read and understand.
- Avoid adding comments specific to how a change was made to the code that relates to a specific issue. For example, a
  comment like "add +1 to fix an off-by-one error" is not relevant to understanding the code. Such comments related to
  specific issues that were addressed belong in a PR description or commit message, not in the code itself.

## Naming Conventions

- Files: snake_case (e.g., `abstract_operator.cpp`)
- Types: PascalCase (e.g., `LogicalOperator`)
- Variables: snake_case (e.g., `chunk_size`)
- Functions: PascalCase (e.g., `GetChunk`)

## Formatting

- Always verify code is formatted by running `make format-fix`.

## Testing

- New code must always be tested with minimal sqltests.
