-- Step 3: RAG Enrichment — explain WHY the dropoff surge happened
-- Takes detected anomalies, embeds a natural language query, performs vector search
-- against a knowledge base of Seattle events, and uses an LLM to summarize the cause.

CREATE TABLE enriched_anomalies
WITH ('changelog.mode' = 'append')
AS SELECT
    zone,
    window_time,
    dropoff_count,
    expected_dropoffs,
    avg_battery_level,
    unique_vehicles,
    anomaly_reason,
    top_chunk_1,
    top_chunk_2,
    top_chunk_3
FROM (
    SELECT
        rag_with_context.zone,
        rag_with_context.window_time,
        rag_with_context.dropoff_count,
        rag_with_context.expected_dropoffs,
        rag_with_context.avg_battery_level,
        rag_with_context.unique_vehicles,
        TRIM(llm_response.response) AS anomaly_reason,
        rag_with_context.top_chunk_1,
        rag_with_context.top_chunk_2,
        rag_with_context.top_chunk_3
    FROM (
        SELECT
            ad.zone,
            ad.window_time,
            ad.dropoff_count,
            ad.expected_dropoffs,
            ad.avg_battery_level,
            ad.unique_vehicles,
            ad.query,
            vs.search_results[1].document_id AS top_document_1,
            vs.search_results[1].chunk AS top_chunk_1,
            vs.search_results[1].score AS top_score_1,
            vs.search_results[2].document_id AS top_document_2,
            vs.search_results[2].chunk AS top_chunk_2,
            vs.search_results[2].score AS top_score_2,
            vs.search_results[3].document_id AS top_document_3,
            vs.search_results[3].chunk AS top_chunk_3,
            vs.search_results[3].score AS top_score_3
        FROM (
            SELECT
                zone,
                window_time,
                dropoff_count,
                expected_dropoffs,
                avg_battery_level,
                unique_vehicles,
                is_surge,
                CONCAT(
                    'Massive scooter dropoff surge detected in ',
                    zone,
                    ' Seattle at ',
                    DATE_FORMAT(window_time, 'h:mm a'),
                    ' (',
                    DATE_FORMAT(window_time, 'HH:mm'),
                    ') during ',
                    CASE
                        WHEN HOUR(window_time) >= 17 AND HOUR(window_time) < 20 THEN 'evening hours (5:00 PM - 8:00 PM)'
                        WHEN HOUR(window_time) >= 20 AND HOUR(window_time) < 23 THEN 'nightlife hours (8:00 PM - 11:00 PM)'
                        WHEN HOUR(window_time) >= 23 OR HOUR(window_time) < 2 THEN 'late night hours (11:00 PM - 2:00 AM)'
                        WHEN HOUR(window_time) >= 7 AND HOUR(window_time) < 10 THEN 'morning commute hours (7:00 AM - 10:00 AM)'
                        WHEN HOUR(window_time) >= 11 AND HOUR(window_time) < 14 THEN 'midday hours (11:00 AM - 2:00 PM)'
                        ELSE 'off-peak hours'
                    END,
                    '. Expected: ',
                    CAST(expected_dropoffs AS STRING),
                    ' dropoffs, Actual: ',
                    CAST(dropoff_count AS STRING),
                    ' dropoffs (+',
                    CAST(ROUND(((dropoff_count - expected_dropoffs) / expected_dropoffs) * 100, 1) AS STRING),
                    '%). Average battery level of parked scooters: ',
                    CAST(avg_battery_level AS STRING),
                    '%. What events, games, concerts, or gatherings are happening in ',
                    zone,
                    ' Seattle that would cause a mass arrival of people?'
                ) AS query,
                emb.embedding
            FROM zone_anomalies,
            LATERAL TABLE(ML_PREDICT('llm_embedding_model',
                CONCAT(
                    'Massive scooter dropoff surge detected in ',
                    zone,
                    ' Seattle at ',
                    DATE_FORMAT(window_time, 'h:mm a'),
                    '. What events, games, concerts are happening in ',
                    zone,
                    ' Seattle that would cause mass arrival of people?'
                )
            )) AS emb
            WHERE is_surge = true
        ) AS ad,
        LATERAL TABLE(
            VECTOR_SEARCH_AGG(
                documents_vectordb_hackathon,
                DESCRIPTOR(embedding),
                ad.embedding,
                3
            )
        ) AS vs
    ) AS rag_with_context,
    LATERAL TABLE(
        ML_PREDICT(
            'llm_textgen_model',
            CONCAT(
                'You are a Lime scooter fleet operations analyst. Analyze the retrieved event documents and identify which events are actively occurring during the detected scooter dropoff surge. Only consider events whose time ranges overlap with the surge time. Provide a one-two sentence explanation including specific event names, expected attendance, and time ranges. Also note that average battery level of scooters in the zone is ',
                CAST(rag_with_context.avg_battery_level AS STRING),
                '% which is critically low and will need battery swaps.\n\n',
                'USER QUERY: ', rag_with_context.query, '\n\n',
                'RETRIEVED DOCUMENTS:\n',
                'Document 1 (Score: ', CAST(rag_with_context.top_score_1 AS STRING), '):\n',
                'Source: ', rag_with_context.top_document_1, '\n',
                rag_with_context.top_chunk_1, '\n\n',
                'Document 2 (Score: ', CAST(rag_with_context.top_score_2 AS STRING), '):\n',
                'Source: ', rag_with_context.top_document_2, '\n',
                rag_with_context.top_chunk_2, '\n\n',
                'Document 3 (Score: ', CAST(rag_with_context.top_score_3 AS STRING), '):\n',
                'Source: ', rag_with_context.top_document_3, '\n',
                rag_with_context.top_chunk_3, '\n\n',
                'Provide only the reason, no additional text.'
            )
        )
    ) AS llm_response
);
