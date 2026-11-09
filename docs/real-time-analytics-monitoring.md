# Real-Time Analytics Monitoring

This page outlines a practical first implementation for Area 1: real-time analytics monitoring in Databricks.

The recommended first release is a near-real-time monitoring layer that ingests GA4 events into Databricks, standardizes them, aggregates them into short time windows, and triggers alerts when key metrics fall outside expected thresholds.

## What this should deliver

The initial solution should help teams answer a few operational questions quickly:

- Are we receiving data from GA4 as expected?
- Are key events firing at the expected volume?
- Are conversion events dropping unexpectedly?
- Are campaign sources and mediums still populated correctly?
- Can the team detect issues before stakeholders report a problem?

This is intentionally a monitoring and QA layer, not a full marketing attribution system.

## Recommended implementation pattern

The most practical MVP is:

1. Ingest GA4 events into Databricks
2. Clean and standardize the event structure in Silver
3. Aggregate event counts by 5 or 15 minute windows in Gold
4. Compare those counts against a baseline or threshold
5. Store alert outcomes in a monitoring table
6. Display them on a Databricks SQL dashboard

This approach gives an operationally useful monitoring layer without overbuilding the first version.

## 1. Choose the event source

There are two realistic options:

### Option A: GA4 BigQuery export (best first step)

Use GA4’s BigQuery export if you want the fastest path to a working Databricks QA solution.

Benefits:

- Easy to set up
- Good event-level data quality
- Works well with Databricks batch ingestion
- Suitable for operational monitoring and alerting

Important caveat:

- It is not a guaranteed seconds-level stream
- Intraday data can be updated over the course of the day
- Daily exports may arrive later than expected

For this reason, present the initial design as near-real-time or scheduled monitoring rather than true real-time monitoring.

### Option B: Stream data into Databricks directly

If you need lower-latency detection, send GA4-related events to Kafka, Event Hubs, or another streaming source and ingest them with Databricks Structured Streaming.

Benefits:

- Lower latency
- Better for operational alerts that need faster detection

Trade-offs:

- More infrastructure complexity
- More operational overhead
- Harder to debug without strong event contracts

### Recommended approach

For the first version, use GA4 BigQuery export with a batch or incremental ingestion process and target a freshness window of 15 minutes. Once the monitoring layer is stable, decide whether you need true streaming ingestion for higher urgency signals.

## 2. Use a medallion architecture

A simple medallion model keeps the solution maintainable.

### Bronze layer

Store raw event payloads as they arrive, including:

- source name
- ingestion timestamp
- event payload
- partition or batch metadata
- raw event identifiers

This layer should be append-only and immutable for reprocessing.

### Silver layer

Normalize key event fields, including:

- event timestamp
- event name
- user pseudo ID or anonymous identifier
- page URL or page location
- traffic source, medium, and campaign
- device type
- country or region if needed
- conversion flags

This is where you enforce a consistent structure so downstream reporting is predictable.

### Gold layer

Aggregate and store metrics for dashboards and alerts, for example:

- 5-minute or 15-minute traffic totals
- key event counts by source, medium, and campaign
- conversion totals and conversion rates
- freshness metrics
- monitoring results and alert statuses

## 3. Define the minimum viable metrics

Do not start with a broad dashboard. Start with a small set of metrics that support quick operational decisions.

Recommended MVP metrics:

- Total events by time window
- Active users by time window
- Key event counts, e.g. `page_view`, `session_start`, `generate_lead`, `purchase`
- Conversion counts and conversion rates
- Campaign traffic by `source`, `medium`, `campaign`
- Data freshness: latest event timestamp and latest ingestion timestamp

Also define the exact event names and conversion logic with the business owner before configuring alerts. Otherwise, a legitimate tracking change may appear to be an incident.

## 4. Build the first aggregation

The following example shows the Gold table pattern using a 5-minute aggregation. It assumes that a Silver table contains a normalized `event_ts` column and flattened event inputs.

### Example SQL batch pattern

```sql
CREATE OR REPLACE TABLE analytics.gold.event_metrics_5m AS
SELECT
  CAST(date_trunc('minute', event_ts) AS TIMESTAMP) AS window_start,
  event_name,
  source,
  medium,
  campaign,
  COUNT(*) AS event_count,
  COUNT(DISTINCT user_pseudo_id) AS users
FROM analytics.silver.ga4_events
WHERE event_ts >= current_date() - INTERVAL 7 DAYS
GROUP BY
  CAST(date_trunc('minute', event_ts) AS TIMESTAMP),
  event_name,
  source,
  medium,
  campaign;
```

This is a batch-friendly example that works well if you are processing GA4 export data in scheduled jobs.

### Example SQL streaming pattern

If you choose a streaming source, the same logic can be implemented in Databricks Structured Streaming using a tumbling window or watermark-based aggregation.

```python
from pyspark.sql import functions as F

stream_df = (
    spark.readStream
    .table("analytics.silver.ga4_events")
    .withWatermark("event_ts", "10 minutes")
    .groupBy(
        F.window("event_ts", "5 minutes").alias("event_window"),
        "event_name",
        "source",
        "medium",
        "campaign"
    )
    .agg(F.count("*").alias("event_count"))
)

query = (
    stream_df.writeStream
    .outputMode("append")
    .queryName("event_metrics_5m_stream")
    .table("analytics.gold.event_metrics_5m")
)
```

### Important note

For GA4 BigQuery exports, do not assume every batch read is final. Intraday data can be updated later, so your ingestion process should handle reruns and late-arriving data consistently.

## 5. Add operational alert checks

Keep the first set of checks simple and explainable. Start with a handful of high-value metrics rather than dozens of low-confidence rules.

Recommended alert types:

### Freshness checks

- No new data has arrived in the expected interval
- Latest event timestamp is older than expected
- Ingestion lag exceeds the accepted threshold

### Volume checks

- Total events are below a baseline threshold
- A key event count drops compared with the same time period last week
- A campaign or landing page shows a large traffic drop

### Conversion checks

- A key conversion event falls below a minimum count
- Conversion rate declines materially compared with a baseline
- A critical funnel step has no recent activity

### Data quality checks

- A high share of events has missing `source`
- A high share of events has missing `medium`
- A high share of events has unexpected empty campaign values

A good alert should answer three questions clearly:

- What metric is being monitored?
- What is the expected threshold or baseline?
- What should the operator do next?

For volume alerts, include a minimum-volume rule so small expected fluctuations do not create noise.

## 6. Store alert results in a table

Create a monitoring table that records alert outcomes by time window, for example:

```sql
CREATE TABLE IF NOT EXISTS analytics.gold.monitoring_alerts (
  alert_time TIMESTAMP,
  metric_name STRING,
  dimension STRING,
  observed_value DOUBLE,
  threshold_value DOUBLE,
  status STRING,
  notes STRING
);
```

Each row should record:

- metric name
- time window
- observed value
- threshold or baseline used
- status (ok, warning, critical)
- notes or explanation

This gives you an auditable history so the alerts can be reviewed and tuned over time.

## 7. Surface the metrics in Databricks

Once the Gold tables are built, create a Databricks SQL dashboard with these views:

- Event volume over time
- Key conversion count over time
- Campaign performance by source and medium
- Data freshness and ingestion lag
- Alert status and trend history

This dashboard should be operationally focused. It should answer: “Are things healthy right now?” not simply “what are the raw numbers?”

Set the refresh frequency to match the actual ingestion cadence. If the source is every 15 minutes, the dashboard does not need to refresh every second.

## 8. Roll out safely

Start with a limited use case, not a broad platform rollout.

### Safe rollout sequence

1. Pick one website or property
2. Define 3–5 critical events
3. Build Bronze and Silver tables
4. Create 15-minute Gold aggregations
5. Add a limited number of alerts
6. Run the alerts in observe-only mode for a period
7. Review false positives and tune thresholds
8. Move to active notifications when the alert quality is acceptable

### Good operational guardrails

- Restrict access to user-level data
- Do not ingest unnecessary identifiers
- Keep the cohort of monitored events small at first
- Document the owner for each alert
- Include a runbook or escalation path in each alert notification

## 9. Expected outcome of the first release

The first release should provide a useful operational layer instead of a complete analytics warehouse.

It should be able to:

- detect missing or delayed data
- monitor key events and conversions
- identify drops or spikes in traffic
- highlight source and campaign data quality issues
- support a Databricks dashboard and alert workflow

## 10. Recommended first milestone

A realistic milestone for this area is:

GA4 export → Bronze raw ingestion → Silver normalized events → 15-minute Gold metrics → freshness and conversion alerts → Databricks SQL dashboard

This is a strong starting point for a Databricks-based analytics QA capability.

Once that first version is stable, the next planning step is to decide whether the organization needs greater latency, additional data quality checks, or a move toward streaming ingestion.

## Summary

The best first implementation is not a highly complex real-time system. It is a disciplined monitoring layer built from GA4 event ingestion, a medallion architecture, small and explainable alerts, and a Databricks dashboard that operational teams can trust.

That makes it easy to implement, easy to debug, and easy to expand into more advanced QA capabilities later.
