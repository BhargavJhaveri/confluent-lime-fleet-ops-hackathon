-- Step 2: Anomaly Detection — detect dropoff surges per zone
-- Uses ML_DETECT_ANOMALIES on dropoff counts in 5-minute tumbling windows.
-- Only flags zones where actual dropoffs exceed the upper bound (true surges).

-- First, visualize the anomaly detection (run this to see the graph):
WITH windowed_dropoffs AS (
    SELECT
        window_start,
        window_end,
        window_time,
        zone,
        COUNT(*) AS dropoff_count,
        AVG(battery_level) AS avg_battery_level,
        COUNT(DISTINCT vehicle_id) AS unique_vehicles
    FROM TABLE(
        TUMBLE(TABLE ride_events, DESCRIPTOR(event_ts), INTERVAL '5' MINUTE)
    )
    WHERE event_type = 'dropoff'
    GROUP BY window_start, window_end, window_time, zone
)
SELECT
    zone,
    window_time,
    dropoff_count,
    avg_battery_level,
    unique_vehicles,
    ML_DETECT_ANOMALIES(
        CAST(dropoff_count AS DOUBLE),
        window_time,
        JSON_OBJECT(
            'minTrainingSize' VALUE 286,
            'maxTrainingSize' VALUE 7000,
            'confidencePercentage' VALUE 99.999,
            'enableStl' VALUE FALSE
        )
    ) OVER (
        PARTITION BY zone
        ORDER BY window_time
        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS anomaly_result
FROM windowed_dropoffs;

-- Then, create the continuous anomaly detection table (run this and leave it running):
CREATE TABLE zone_anomalies AS
WITH windowed_dropoffs AS (
    SELECT
        window_start,
        window_end,
        window_time,
        zone,
        COUNT(*) AS dropoff_count,
        CAST(AVG(battery_level) AS INT) AS avg_battery_level,
        COUNT(DISTINCT vehicle_id) AS unique_vehicles
    FROM TABLE(
        TUMBLE(TABLE ride_events, DESCRIPTOR(event_ts), INTERVAL '5' MINUTE)
    )
    WHERE event_type = 'dropoff'
    GROUP BY window_start, window_end, window_time, zone
),
anomaly_detection AS (
    SELECT
        zone,
        window_time,
        dropoff_count,
        avg_battery_level,
        unique_vehicles,
        ML_DETECT_ANOMALIES(
            CAST(dropoff_count AS DOUBLE),
            window_time,
            JSON_OBJECT(
                'minTrainingSize' VALUE 286,
                'maxTrainingSize' VALUE 7000,
                'confidencePercentage' VALUE 99.9,
                'enableStl' VALUE FALSE
            )
        ) OVER (
            PARTITION BY zone
            ORDER BY window_time
            RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS anomaly_result
    FROM windowed_dropoffs
)
SELECT
    zone,
    window_time,
    dropoff_count,
    avg_battery_level,
    unique_vehicles,
    CAST(ROUND(anomaly_result.forecast_value) AS BIGINT) AS expected_dropoffs,
    anomaly_result.upper_bound AS upper_bound,
    anomaly_result.lower_bound AS lower_bound,
    anomaly_result.is_anomaly AS is_surge
FROM anomaly_detection
WHERE anomaly_result.is_anomaly = true
  AND dropoff_count > anomaly_result.upper_bound;
