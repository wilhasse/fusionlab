# Repository Guidelines

## Project Structure & Module Organization

- `crates/fusionlab-cli/`: CLI binary (`fusionlab`) and subcommands (`mysql`, `df`).
- `crates/fusionlab-core/`: shared query runners (MySQL, DataFusion) and result/plan formatting.
- `docker/`: local MySQL 8 container (`docker-compose.yml`) and SSB schema (`init.sql`).
- `data/`: SSB query corpus in `data/queries/q*.sql` and data generator scripts in `data/generator/`.
- `results/`: generated benchmark artifacts (kept out of git except `results/.gitkeep`).
- `infra/pulumi/`: optional provisioning scripts for running MySQL on a VM (see `infra/pulumi/README.md`).

## Build, Test, and Development Commands

- Prereqs: Rust 1.70+ (via `rustup`) and Docker (for local MySQL).
- `cargo build` / `cargo build --release`: build the workspace (release binary at `target/release/fusionlab`).
- `cargo run -p fusionlab-cli -- --help`: CLI help and subcommand flags.
- `cargo run -p fusionlab-cli -- mysql "SELECT 1"`: run against MySQL (baseline).
- `cargo run -p fusionlab-cli -- df "SELECT COUNT(*) FROM lineorder"`: run locally via DataFusion.
- `cargo test` (or `cargo test -p fusionlab-core`): run unit + async tests.
- `cargo fmt --all` and `cargo clippy --workspace --all-targets --all-features -- -D warnings`: format and lint.

Local MySQL: `cd docker && docker compose up -d` (defaults: `root` / `root`, DB `ssb`).
SSB data: `./data/generator/setup-dbgen.sh` then `./data/generator/generate.sh -s 1` (outputs `*.tbl`, not committed).

## Coding Style & Naming Conventions

- Rust 2021 edition; 4-space indentation; keep modules small and focused.
- Prefer `rustfmt` defaults; avoid introducing new patterns without a clear reason.
- Naming: `snake_case` (functions/modules), `CamelCase` (types/traits), `SCREAMING_SNAKE_CASE` (consts).
- SQL files use `data/queries/q<group>.<idx>.sql` (e.g., `q1.1.sql`).

## Testing Guidelines

- Unit tests live in-module (`mod tests { … }`); use `#[tokio::test]` for async.
- Add integration tests under `crates/fusionlab-core/tests/` when exercising multiple modules.
- Avoid tests that require a real MySQL instance unless explicitly documented.

## Commit & Pull Request Guidelines

- Commit subjects in this repo are short and imperative (e.g., `Add …`, `Fix …`); keep changes scoped.
- PRs should include: what/why, how to run (`cargo test`, a sample `fusionlab …` command), and any benchmark/timing notes.
- Don’t commit generated artifacts (`target/`, `*.tbl`, `results/*.csv`, `results/*.jsonl`, `results/*.sqlite`); `.gitignore` already covers these.
