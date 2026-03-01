# replit.md

## Recent Changes

- **2026-02-24**: Fixed port configuration to 5000 for Replit compatibility.
- **2026-02-24**: Added missing client management API endpoints (`/api/get-clients`, `/api/add-client`, `/api/delete-client`).
- **2026-02-24**: Configured autoscale deployment.
- **2026-02-24**: Updated Python to 3.11 to fix `ImportError: cannot import name 'Literal' from 'typing'` in uvicorn.

## Overview

SPIZ (Strategic Intelligence) is an Italian-language media intelligence dashboard. It ingests CSV files containing press articles, stores them in Supabase, enriches them with AI-powered analysis (tone, topic, reputational risk) and OpenAI embeddings, and provides a conversational chat interface for querying and generating reports from the article data. The application serves a web-based dashboard with a chat panel and a client management system.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Backend: FastAPI (Python)

- **Framework**: FastAPI served via Uvicorn
- **Entry point**: `main.py` — defines routes for the web UI and API endpoints
- **Key modules**:
  - `api/ingestion.py` — Parses uploaded CSV files (using pandas), maps columns to the database schema, and bulk-inserts article records into Supabase
  - `api/analyzer.py` — Retroactive AI analysis: iterates over articles missing a `tone` field, sends article text to GPT-4o-mini, and updates Supabase with tone, dominant_topic, and reputational_risk
  - `api/chat.py` — Conversational AI interface with session memory, token budgeting (tiktoken), and report generation capabilities. Uses GPT-4o for complex analysis and GPT-4o-mini for lighter tasks
  - `generate_embeddings.py` — Batch script to generate OpenAI text-embedding-ada-002 vectors for articles and store them in Supabase (for semantic search)

### Frontend: Static HTML/JS/CSS

- Served from the `web/` directory via FastAPI's static file handling
- `web/index.html` — Main dashboard with a chat interface (left panel) and a data ingestion/client panel (right panel). Uses Bootstrap 5 and Inter font.
- `web/clienti.html` — Client management page for adding/removing monitored clients with keywords and semantic topics
- `web/app.js` and `web/style.css` — Currently empty, logic is inline in HTML files

### Database: Supabase (PostgreSQL)

- **Connection**: `services/database.py` — initializes the Supabase client using `SUPABASE_URL` and `SUPABASE_KEY` environment variables
- **Main table**: `articles` with columns including: `id`, `titolo`, `testata`, `data`, `giornalista`, `testo_completo`, `occhiello`, `sottotitolo`, `ave`, `tone`, `dominant_topic`, `reputational_risk`, `embedding`, `content_hash`, `macrosettori`
- **Clients table**: `clients` with columns: `id`, `name`, `keywords`, `semantic_topic`
- **Deduplication**: Uses `content_hash` field with upsert on conflict
- The embedding column stores OpenAI vector embeddings for semantic similarity search

### Data Flow

1. User uploads CSV files → `api/ingestion.py` parses and inserts into Supabase `articles` table
2. `run_analysis.py` (or `api/analyzer.py`) processes articles without tone analysis via GPT-4o-mini
3. `generate_embeddings.py` batch-generates embeddings for articles missing them
4. Chat interface queries articles from Supabase and uses OpenAI for conversational analysis
5. Reports can be generated as Word documents and saved to `static/reports/`

### Environment Variables Required

- `SUPABASE_URL` — Supabase project URL
- `SUPABASE_KEY` — Supabase anon/service key
- `OPENAI_API_KEY` — OpenAI API key
- `APP_BASE_URL` — Base URL for the deployed app (used for report download links)

### Key Design Decisions

- **Supabase instead of local PostgreSQL**: Chosen for managed hosting, built-in REST API, and vector storage support for embeddings. The tradeoff is external dependency but simplifies deployment.
- **In-memory session storage** (`_sessions` dict in `api/chat.py`): Simple but not persistent across restarts. Acceptable for a single-instance deployment.
- **CSV as ingestion format**: Flexible parsing with pandas `sep=None` auto-detection. Supports multiple file upload in a single request.
- **Two OpenAI models**: GPT-4o for complex chat analysis, GPT-4o-mini for batch article classification — balances quality vs cost.

## External Dependencies

### Services
- **Supabase** — PostgreSQL database with REST API, used for all data storage including vector embeddings. Requires `SUPABASE_URL` and `SUPABASE_KEY`.
- **OpenAI API** — Used for three purposes: (1) chat completions via GPT-4o/GPT-4o-mini, (2) article tone/topic analysis via GPT-4o-mini, (3) text-embedding-ada-002 for semantic embeddings. Requires `OPENAI_API_KEY`.

### Python Packages
- `fastapi` + `uvicorn` — Web framework and ASGI server
- `pandas` + `openpyxl` — CSV/Excel parsing
- `supabase` — Supabase Python client
- `openai` — OpenAI API client (v1.0+ syntax)
- `tiktoken` — Token counting for context window management
- `python-dotenv` — Environment variable loading
- `python-multipart` — File upload support for FastAPI
- `pydantic` — Request/response validation

### Frontend CDN
- Bootstrap 5.3.0 (CSS)
- Google Fonts (Inter)

prova giusto per vedere se funziona push/commit