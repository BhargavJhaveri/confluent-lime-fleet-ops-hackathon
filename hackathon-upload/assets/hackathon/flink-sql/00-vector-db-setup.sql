-- Step 0: Build the Vector Database from scratch using Flink SQL
-- This demonstrates creating an end-to-end embedding pipeline:
--   raw docs (Kafka) -> embed with LLM -> store in MongoDB -> vector search
--
-- PREREQUISITES:
--   1. MongoDB Atlas free cluster (M0) with:
--      - Database: "lime_vector_search"
--      - Collection: "seattle_events"
--      - Atlas Search Index named "vector_index" on the collection with config:
--        {
--          "fields": [{
--            "type": "vector",
--            "path": "embedding",
--            "numDimensions": 1024,
--            "similarity": "cosine"
--          }]
--        }
--   2. Network access: Allow 0.0.0.0/0 (for Confluent Cloud connectivity)
--   3. Database user with readWrite permissions
--
-- NOTE: Replace <YOUR_MONGODB_*> placeholders below with your actual credentials.

-- 0a. Create MongoDB connection
CREATE CONNECTION IF NOT EXISTS `mongodb-connection-hackathon`
WITH (
    'type' = 'MONGODB',
    'endpoint' = 'mongodb+srv://cluster0.zgbnv2f.mongodb.net/',
    'username' = 'bhargav_hackathon',
    'password' = 'Bhargav@123'
);

-- 0b. Create the raw documents table (Kafka-backed)
-- Documents published here via `uv run publish_docs --docs-dir assets/hackathon/seattle_events_docs`
CREATE TABLE IF NOT EXISTS documents (
    document_id STRING,
    document_text STRING
);

-- 0c. Create the embeddings table (Kafka-backed, stores intermediate embeddings)
CREATE TABLE IF NOT EXISTS documents_embedded (
    document_id STRING,
    chunk STRING,
    embedding ARRAY<FLOAT>
);

-- 0d. Continuous Flink job: read raw docs -> embed with LLM -> write to embeddings table
-- This uses ML_PREDICT with the embedding model to vectorize each document
INSERT INTO documents_embedded
SELECT
    document_id,
    document_text AS chunk,
    embedding
FROM documents,
LATERAL TABLE(ML_PREDICT('llm_embedding_model', document_text));

-- 0e. Create the MongoDB vector search external table
-- This is the table that VECTOR_SEARCH_AGG queries against
CREATE TABLE IF NOT EXISTS documents_vectordb_hackathon (
    document_id STRING,
    chunk STRING,
    embedding ARRAY<FLOAT>
) WITH (
    'connector' = 'mongodb',
    'mongodb.connection' = 'mongodb-connection-hackathon',
    'mongodb.database' = 'lime_vector_search',
    'mongodb.collection' = 'seattle_events',
    'mongodb.index' = 'vector_index',
    'mongodb.embedding_column' = 'embedding',
    'mongodb.numCandidates' = '500'
);

-- 0f. Continuous Flink job: write embeddings from Kafka to MongoDB
-- This populates the vector database that powers RAG queries
INSERT INTO documents_vectordb_hackathon
SELECT document_id, chunk, embedding
FROM documents_embedded;
