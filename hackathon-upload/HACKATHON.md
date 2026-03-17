# Lime Fleet Ops: Real-Time Intelligent Scooter Fleet Management

## The Problem

When a large event (concert, sports game) happens in a city, thousands of scooter riders converge on one zone. The naive operational response is to generate **move tasks** — physically relocating scooters away from the crowded zone. But this is expensive and wasteful: after the event ends, riders will naturally redistribute the scooters themselves.

The **smart response**: detect the surge, understand *why* it happened, and dispatch **battery swap crews** instead. High-density usage means drained batteries that need swapping before riders leave.

## Architecture

```
ride_events → zone_anomalies → enriched_anomalies → ops_actions
(scooter        (ML_DETECT_       (Vector Search +     (Streaming Agent:
 pickups/        ANOMALIES on      RAG to explain       dispatch battery
 dropoffs)       dropoff count)    surge cause)         swap crews)
                                        ↑
                                   MongoDB Atlas
                                   (Vector DB with
                                    Seattle event docs)
```

## How It Works

### 1. Data Ingestion
- 12,948 scooter ride events across 8 Seattle zones streamed into Confluent Cloud Kafka
- Each event includes: zone, event type (pickup/dropoff), vehicle ID, battery level, timestamp
- Data published with **Avro serialization** and **Schema Registry**

### 2. Anomaly Detection (Flink SQL)
- `ML_DETECT_ANOMALIES` monitors dropoff counts per zone in **5-minute tumbling windows**
- Compares actual dropoff counts against historical baselines
- Flags zones where dropoffs significantly exceed the upper confidence bound
- Detects a surge of ~40-55 dropoffs/window in **Seattle Center** (vs steady-state 3-8)

### 3. Vector Database Creation
- 8 Seattle event knowledge base documents (Kraken games, Climate Pledge concerts, festivals, etc.)
- Embedded using **Azure OpenAI text-embedding-ada-002**
- Stored in **MongoDB Atlas** with vector search index
- Flink queries via `VECTOR_SEARCH_AGG` for semantic retrieval

### 4. RAG Enrichment (Flink SQL)
- Each detected anomaly triggers a natural language query
- Query is embedded and searched against the vector DB
- Top 3 matching documents retrieved
- `ML_PREDICT` with LLM summarizes the likely cause (e.g., "Kraken hockey game at Climate Pledge Arena, 17,151 capacity")

### 5. Autonomous Agent Action (Flink SQL)
- `CREATE AGENT` defines a Lime fleet ops agent with MCP tool calling via Zapier
- Agent receives enriched anomaly, reviews available crews, and dispatches battery swap teams
- Explicitly **suppresses move tasks** for event-driven surges (cost optimization)
- Results written to `ops_actions` table with structured output

## Confluent Cloud Features Used

| Feature | How We Use It |
|---------|--------------|
| **Kafka Topics** | `ride_events`, `documents`, `zone_anomalies`, `enriched_anomalies`, `ops_actions` |
| **Schema Registry** | Avro schemas for ride events with proper serialization |
| **Flink SQL** | Windowed aggregations, watermarks, event-time processing |
| **ML_DETECT_ANOMALIES** | Built-in ML function for real-time anomaly detection |
| **ML_PREDICT** | LLM embedding generation + text generation for RAG |
| **VECTOR_SEARCH_AGG** | Semantic search against MongoDB Atlas vector store |
| **CREATE AGENT / AI_RUN_AGENT** | Autonomous streaming agent with MCP tool calling |
| **Zapier MCP Connection** | External tool calling for crew dispatch API |
| **MongoDB Connector** | External vector database integration |

## Seattle Zones

Capitol Hill, Downtown, South Lake Union, University District, Pioneer Square, Fremont, Ballard, **Seattle Center** (surge zone — Climate Pledge Arena)

## Business Impact

- **Cost reduction**: Avoids unnecessary move tasks ($15-25 per move) during event-driven surges
- **Battery availability**: Ensures scooters are charged and ready when event-goers leave
- **Autonomous operations**: Zero human intervention from detection to dispatch
- **Real-time**: End-to-end pipeline runs in seconds, not minutes

## Tech Stack

- **Confluent Cloud** — Kafka, Flink SQL, Schema Registry, Streaming Agents
- **MongoDB Atlas** — Vector database with Atlas Vector Search
- **Azure OpenAI** — LLM for embeddings (text-embedding-ada-002) and text generation
- **Zapier MCP** — External tool calling for API integrations
- **Python** — Data generation and publishing scripts

## How to Run

```bash
# 1. Deploy infrastructure
uv run deploy  # Choose Lab3, Azure

# 2. Generate and publish ride events
python3 scripts/generate_hackathon_data.py
uv run publish_hackathon_data

# 3. Load vector DB
uv run python scripts/load_hackathon_vectors.py

# 4. Run Flink SQL steps (in Confluent Cloud SQL Workspace)
#    See assets/hackathon/flink-sql/ for all queries (steps 00-05)
```
