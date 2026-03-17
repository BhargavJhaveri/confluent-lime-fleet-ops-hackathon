-- Step 5: Invoke the agent on enriched anomalies
-- AI_RUN_AGENT triggers the Lime fleet ops agent for every detected surge.
-- Results are written to ops_actions with structured output.

CREATE TABLE ops_actions (
    PRIMARY KEY (zone) NOT ENFORCED
)
WITH ('changelog.mode' = 'append')
AS SELECT
    zone,
    window_time,
    dropoff_count,
    avg_battery_level,
    anomaly_reason,
    TRIM(REGEXP_EXTRACT(CAST(response AS STRING), 'Operations Summary:\s*\n(.+?)(?=\n\nDispatch JSON:)', 1)) AS ops_summary,
    TRIM(REGEXP_EXTRACT(CAST(response AS STRING), 'Dispatch JSON:\s*\n(?:```json\s*)?([\s\S]+?)(?:```)?(?=\n\nAPI Response:)', 1)) AS dispatch_json,
    TRIM(REGEXP_EXTRACT(CAST(response AS STRING), 'API Response:\s*\n(?:```json\s*)?([\s\S]+?)(?:```)?$', 1)) AS api_response
FROM enriched_anomalies,
LATERAL TABLE(AI_RUN_AGENT(
    `lime_fleet_ops_agent`,
    `anomaly_reason`,
    `zone`
));

-- View the results:
-- SELECT * FROM ops_actions;
