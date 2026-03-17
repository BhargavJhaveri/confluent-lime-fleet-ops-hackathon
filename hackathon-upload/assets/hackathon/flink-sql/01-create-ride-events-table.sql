-- Step 1: Create the ride_events table
-- This table receives scooter ride events (pickups/dropoffs) from the data publisher.
-- The WATERMARK enables Flink's event-time processing for windowed aggregations.

CREATE TABLE IF NOT EXISTS ride_events (
    `event_id` STRING NOT NULL,
    `rider_email` STRING NOT NULL,
    `event_type` STRING NOT NULL,
    `zone` STRING NOT NULL,
    `origin_zone` STRING NOT NULL,
    `destination_zone` STRING NOT NULL,
    `vehicle_id` STRING NOT NULL,
    `battery_level` INT NOT NULL,
    `ride_duration_minutes` DOUBLE NOT NULL,
    `ride_cost` DOUBLE NOT NULL,
    `event_ts` TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL,
    WATERMARK FOR `event_ts` AS `event_ts` - INTERVAL '5' SECOND
);
