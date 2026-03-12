

CREATE TABLE IF NOT EXISTS tr_events (
    timestamp DateTime64(3),
    event_type LowCardinality(String),
    task_channel LowCardinality(String),
    task_queue LowCardinality(String),
    task_sid String,
    worker_name LowCardinality(String),
    worker_sid String,
    workflow_name LowCardinality(String),
    conversation_sid String,
    customer_name String,
    direction LowCardinality(String)
) ENGINE = MergeTree()
ORDER BY (timestamp, task_queue, worker_name);



CREATE TABLE IF NOT EXISTS wds_events
(
    sid                                      String,
    eventtype                                LowCardinality(String),
    account_sid                              LowCardinality(String),
    timestamp                                DateTime64 CODEC(Delta, ZSTD(1)),
    workspace_sid                            LowCardinality(String),
    workspace_name                           String,

    operating_unit_sid                       LowCardinality(Nullable(String)),
    task_sid                                 Nullable(String),
    task_attributes                          Nullable(String) CODEC(ZSTD(3)),
    task_age                                 Nullable(UInt32),
    task_date_created                        Nullable(DateTime64) CODEC(Delta, ZSTD(1)),
    task_priority                            Nullable(Int16),
    task_assignment_status                   LowCardinality(Nullable(String)),
    task_completed_reason                    LowCardinality(Nullable(String)),
    task_canceled_reason                     LowCardinality(Nullable(String)),
    reason                                   Nullable(String),
    task_channel_sid                         LowCardinality(Nullable(String)),
    task_channel_unique_name                 LowCardinality(Nullable(String)),
    task_channel_name                        LowCardinality(Nullable(String)),
    task_channel_optimized_routing           Nullable(Bool),

    task_queue_sid                           LowCardinality(Nullable(String)),
    task_queue_name                          Nullable(String),
    task_queue_entered_date                  Nullable(DateTime64) CODEC(Delta, ZSTD(1)),
    task_queue_target_expression             Nullable(String) CODEC(ZSTD(3)),
    task_age_in_queue                        Nullable(UInt32),
    task_order                               Nullable(String),
    task_queue_reservation_activity_sid      Nullable(String),
    task_queue_assignment_activity_sid       Nullable(String),
    task_queue_max_reserved_workers          Nullable(UInt16),
    task_queue_created_date                  Nullable(DateTime64),
    task_queue_updated_date                  Nullable(DateTime64),

    task_version                             Nullable(UInt32),
    task_add_on_attributes                   Nullable(String) CODEC(ZSTD(3)),
    task_timeout                             Nullable(UInt16),
    task_routing_target                      Nullable(String),
    task_re_evaluated_reason                 Nullable(String),
    virtual_start_time                       Nullable(DateTime64),
    task_virtual_start_time                  Nullable(DateTime64),
    task_ignore_capacity                     Nullable(Bool),

    reservation_sid                          Nullable(String),
    reservation_timeout                      Nullable(UInt16),
    reservation_status                       LowCardinality(Nullable(String)),
    reservation_reason_code                  Nullable(Int16),
    reservation_version                      Nullable(UInt32),

    worker_sid                               Nullable(String),
    worker_name                              Nullable(String),
    worker_attributes                        Nullable(String) CODEC(ZSTD(3)),
    worker_activity_sid                      LowCardinality(Nullable(String)),
    worker_activity_name                     LowCardinality(Nullable(String)),
    worker_time_in_previous_activity         Nullable(Int64),
    worker_time_in_previous_activity_ms      Nullable(Int64),
    worker_previous_activity_sid             LowCardinality(Nullable(String)),
    worker_previous_activity_name            LowCardinality(Nullable(String)),
    worker_available                         Nullable(Bool),
    worker_version                           Nullable(UInt32),
    worker_date_created                      Nullable(DateTime64),
    worker_date_updated                      Nullable(DateTime64),
    worker_last_status_change                Nullable(DateTime64),
    worker_consumed_concurrency              Nullable(UInt16),
    worker_consumed_attention                Nullable(UInt16),

    worker_channel_available                 Nullable(UInt16),
    worker_channel_capacity                  Nullable(UInt16),
    worker_channel_available_capacity        Nullable(Float32),
    worker_channel_previous_capacity         Nullable(Float32),
    worker_channel_consumed_capacity         Nullable(UInt16),
    worker_channel_task_count                Nullable(UInt16),
    worker_channel_last_reserved_date        Nullable(DateTime64),

    workflow_sid                             LowCardinality(Nullable(String)),
    workflow_name                            Nullable(String),
    workflow_filter_name                     Nullable(String),
    workflow_filter_expression               Nullable(String) CODEC(ZSTD(3)),
    workflow_filter_target_expression        Nullable(String) CODEC(ZSTD(3)),
    workflow_filter_target_name              Nullable(String),
    workflow_rule_target_sid                 Nullable(String),

    previous_task_queue_sid                  LowCardinality(Nullable(String)),
    previous_task_queue_name                 Nullable(String),
    previous_task_priority                   Nullable(Int16),

    target_changed_reason                    Nullable(String),

    task_transfer_sid                        Nullable(String),
    transfer_type                            LowCardinality(Nullable(String)),
    transfer_initiating_worker_sid           Nullable(String),
    transfer_initiating_reservation_sid      Nullable(String),
    transfer_to                              Nullable(String),
    transfer_mode                            LowCardinality(Nullable(String)),
    transfer_started                         Nullable(DateTime64),
    transfer_failed_reason                   Nullable(String),
    transfer_status                          LowCardinality(Nullable(String)),

    event_description                        Nullable(String),
    resource_type                            LowCardinality(Nullable(String)),
    resource_sid                             Nullable(String),

    known_worker_sid                         Nullable(String),
    activity_sid                             LowCardinality(Nullable(String)),
    activity_name                            LowCardinality(Nullable(String)),
    activity_available                       Nullable(Bool),

    operating_unit_friendly_name             Nullable(String),
    operating_unit_date_created              Nullable(DateTime64),
    operating_unit_date_updated              Nullable(DateTime64),

    attention_profile_sid                    Nullable(String),
    attention_profile_friendly_name          Nullable(String),
    attention_profile_unique_name            Nullable(String),
    attention_profile_concurrency            Nullable(UInt16),
    attention_profile_date_created           Nullable(DateTime64),
    attention_profile_date_updated           Nullable(DateTime64),
    attention_profile_channel_sid            Nullable(String),
    attention                                Nullable(UInt16),
    attention_routing_enabled                Nullable(Bool),

    instance_sid                             Nullable(String),

    -- Skip indexes for MV filtering
    INDEX idx_task_sid task_sid TYPE bloom_filter GRANULARITY 4,
    INDEX idx_worker_sid worker_sid TYPE bloom_filter GRANULARITY 4,
    INDEX idx_activity_sid activity_sid TYPE bloom_filter GRANULARITY 4,
    INDEX idx_task_queue_sid task_queue_sid TYPE bloom_filter GRANULARITY 4
)
ENGINE = MergeTree()
PARTITION BY toMonday(timestamp)
ORDER BY (account_sid, workspace_sid, eventtype, timestamp, sid)
TTL timestamp + INTERVAL 2 YEAR DELETE
SETTINGS index_granularity = 4096;




CREATE TABLE tasks_current
(
    -- Primary key
    task_sid                String,

    -- Dimensions
    account_sid             String,
    workspace_sid           String,
    task_queue_sid          String,
    workflow_sid            String,
    operating_unit_sid      Nullable(String),
    task_channel_sid        Nullable(String),
    task_channel_name       Nullable(String),

    -- Task state (normalized: pending includes transferring)
    task_status             String,
    priority                Int32,

    -- Timestamps for age calculation
    task_created            DateTime64,
    task_queue_entered      DateTime64,

    -- For ReplacingMergeTree
    event_timestamp         DateTime64,
    is_deleted              UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(event_timestamp)
PARTITION BY toYYYYMM(task_created)
ORDER BY (account_sid, workspace_sid, task_queue_sid, task_sid)
SETTINGS index_granularity = 8192;


ALTER TABLE tasks_current MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';

-- Projection for Task Queue queries
ALTER TABLE tasks_current ADD PROJECTION proj_queue_stats 
(
    SELECT
        account_sid,
        workspace_sid,
        task_queue_sid,
        task_channel_sid,
        task_status,
        priority,
        is_deleted,
        count() AS task_count,
        min(task_created) AS oldest_created,
        min(task_queue_entered) AS oldest_queue_entered
    GROUP BY
        account_sid, workspace_sid, task_queue_sid,
        task_channel_sid, task_status, priority, is_deleted
);

-- Projection for Workspace queries
ALTER TABLE tasks_current ADD PROJECTION proj_workspace_stats
(
    SELECT
        account_sid,
        workspace_sid,
        task_channel_sid,
        task_status,
        priority,
        is_deleted,
        count() AS task_count,
        min(task_created) AS oldest_created
    GROUP BY
        account_sid, workspace_sid, task_channel_sid,
        task_status, priority, is_deleted
);

-- Projection for Workflow queries
ALTER TABLE tasks_current ADD PROJECTION proj_workflow_stats
(
    SELECT
        account_sid,
        workspace_sid,
        workflow_sid,
        task_channel_sid,
        task_status,
        priority,
        is_deleted,
        count() AS task_count,
        min(task_created) AS oldest_created
    GROUP BY
        account_sid, workspace_sid, workflow_sid,
        task_channel_sid, task_status, priority, is_deleted
);


-- Projection for Operating Unit queries
ALTER TABLE tasks_current ADD PROJECTION proj_ou_stats
(
    SELECT
        account_sid,
        workspace_sid,
        operating_unit_sid,
        task_channel_sid,
        task_status,
        priority,
        is_deleted,
        count() AS task_count,
        min(task_created) AS oldest_created
    GROUP BY
        account_sid, workspace_sid, operating_unit_sid,
        task_channel_sid, task_status, priority, is_deleted
);


-- Materialize projections
ALTER TABLE tasks_current MATERIALIZE PROJECTION proj_queue_stats;
ALTER TABLE tasks_current MATERIALIZE PROJECTION proj_workspace_stats;
ALTER TABLE tasks_current MATERIALIZE PROJECTION proj_workflow_stats;
ALTER TABLE tasks_current MATERIALIZE PROJECTION proj_ou_stats;

CREATE MATERIALIZED VIEW tasks_current_mv TO tasks_current AS
SELECT
    task_sid,
    account_sid,
    workspace_sid,
    task_queue_sid,
    workflow_sid,
    operating_unit_sid,
    task_channel_sid,
    task_channel_unique_name AS task_channel_name,
    -- Normalize status
    CASE
        WHEN task_assignment_status IN ('pending', 'transferring') THEN 'pending'
        ELSE task_assignment_status
    END AS task_status,
    coalesce(task_priority, 0) AS priority,
    coalesce(task_date_created, timestamp) AS task_created,
    coalesce(task_queue_entered_date, timestamp) AS task_queue_entered,
    timestamp AS event_timestamp,
    CASE
        WHEN eventtype IN ('task.deleted', 'task.canceled', 'task.completed',
                           'task.system-deleted', 'reservation.completed')
        THEN 1
        ELSE 0
    END AS is_deleted
FROM wds_events
WHERE timestamp >= now() - INTERVAL 30 DAY  -- MANDATORY time filter
  AND eventtype IN (
    'task.created', 'task-queue.entered', 'task.wrapup', 'task.updated',
    'reservation.created', 'reservation.rejected', 'reservation.rescinded',
    'reservation.timeout', 'reservation.accepted', 'reservation.canceled',
    'task.deleted', 'task.canceled', 'task.completed',
    'task.system-deleted', 'reservation.completed'
)
AND task_sid IS NOT NULL AND task_sid != '';


CREATE TABLE worker_activities_current
(
    worker_sid              String,
    account_sid             String,
    workspace_sid           String,
    operating_unit_sid      Nullable(String),
    activity_sid            String,
    event_timestamp         DateTime64,
    is_deleted              UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(event_timestamp)
ORDER BY (account_sid, workspace_sid, worker_sid)
SETTINGS index_granularity = 8192;

ALTER TABLE worker_activities_current MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';

-- Projection for workspace-level worker stats
ALTER TABLE worker_activities_current ADD PROJECTION proj_workspace_workers
(
    SELECT
        account_sid,
        workspace_sid,
        activity_sid,
        is_deleted,
        count() AS worker_count
    GROUP BY account_sid, workspace_sid, activity_sid, is_deleted
);


ALTER TABLE worker_activities_current ADD PROJECTION proj_ou_workers
(
    SELECT
        account_sid,
        workspace_sid,
        operating_unit_sid,
        activity_sid,
        is_deleted,
        count() AS worker_count
    GROUP BY account_sid, workspace_sid, operating_unit_sid, activity_sid, is_deleted
);

ALTER TABLE worker_activities_current MATERIALIZE PROJECTION proj_workspace_workers;
ALTER TABLE worker_activities_current MATERIALIZE PROJECTION proj_ou_workers;


CREATE MATERIALIZED VIEW worker_activities_current_mv TO worker_activities_current AS
SELECT
    worker_sid,
    account_sid,
    workspace_sid,
    operating_unit_sid,
    worker_activity_sid AS activity_sid,
    timestamp AS event_timestamp,
    CASE WHEN eventtype = 'worker.deleted' THEN 1 ELSE 0 END AS is_deleted
FROM wds_events
WHERE timestamp >= now() - INTERVAL 30 DAY  -- MANDATORY time filter
  AND eventtype IN (
    'worker.created', 'worker.channel.availability.update',
    'reservation.created', 'worker.activity.update',
    'worker.capacity.update', 'worker.deleted'
)
AND worker_sid IS NOT NULL AND worker_sid != ''
AND worker_activity_sid IS NOT NULL AND worker_activity_sid != '';


CREATE TABLE activities
(
    activity_sid            String,
    account_sid             String,
    workspace_sid           String,
    activity_friendly_name  String,
    available               UInt8,
    event_timestamp         DateTime64,
    is_deleted              UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(event_timestamp)
ORDER BY (account_sid, workspace_sid, activity_sid)
SETTINGS index_granularity = 8192;


CREATE MATERIALIZED VIEW activities_mv TO activities AS
SELECT
    activity_sid,
    account_sid,
    workspace_sid,
    coalesce(activity_name, '') AS activity_friendly_name,
    toUInt8(coalesce(activity_available, 0)) AS available,
    timestamp AS event_timestamp,
    CASE WHEN eventtype = 'activity.deleted' THEN 1 ELSE 0 END AS is_deleted
FROM wds_events
WHERE eventtype IN ('activity.created', 'activity.updated', 'activity.deleted')
AND activity_sid IS NOT NULL AND activity_sid != '';


CREATE DICTIONARY activities_dict
(
    activity_sid            String,
    account_sid             String,
    workspace_sid           String,
    activity_friendly_name  String,
    available               UInt8
)
PRIMARY KEY activity_sid
SOURCE(CLICKHOUSE(
    TABLE 'activities'
    /*WHERE is_deleted = 0*/
))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(MIN 30 MAX 60);


CREATE TABLE worker_queues
(
    worker_sid              String,
    task_queue_sid          String,
    account_sid             String,
    workspace_sid           String,
    updated_at              DateTime64,
    is_deleted              UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (account_sid, workspace_sid, task_queue_sid, worker_sid)
SETTINGS index_granularity = 8192;


-- Source table (fed by MV, used as Dictionary source)
CREATE TABLE queues_source
(
    task_queue_sid      String,
    account_sid         String,
    workspace_sid       String,
    task_queue_name     String,
    event_timestamp     DateTime
)
ENGINE = ReplacingMergeTree(event_timestamp)
ORDER BY (task_queue_sid)
SETTINGS index_granularity = 8192;

-- Materialized View to populate source table
CREATE MATERIALIZED VIEW queues_source_mv TO queues_source AS
SELECT
    task_queue_sid,
    account_sid,
    workspace_sid,
    task_queue_name,
    timestamp AS event_timestamp
FROM wds_events
WHERE eventtype IN ('task-queue.created', 'task-queue.updated')
  AND task_queue_sid IS NOT NULL AND task_queue_sid != ''
  AND task_queue_name IS NOT NULL;

-- Dictionary for O(1) lookups by SID
CREATE DICTIONARY queues_dict
(
    task_queue_sid      String,
    account_sid         String,
    workspace_sid       String,
    task_queue_name     String
)
PRIMARY KEY task_queue_sid
SOURCE(CLICKHOUSE(TABLE 'queues_source'))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(MIN 30 MAX 60);

-- Dictionary for O(1) lookups by name
CREATE DICTIONARY queues_by_name_dict
(
    workspace_sid       String,
    task_queue_name     String,
    task_queue_sid      String,
    account_sid         String
)
PRIMARY KEY workspace_sid, task_queue_name
SOURCE(CLICKHOUSE(TABLE 'queues_source'))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(MIN 30 MAX 60);



-- Source table
CREATE TABLE task_channels_source
(
    task_channel_sid    String,
    account_sid         String,
    workspace_sid       String,
    task_channel_name   String,
    event_timestamp     DateTime
)
ENGINE = ReplacingMergeTree(event_timestamp)
ORDER BY (task_channel_sid)
SETTINGS index_granularity = 8192;

-- Materialized View
CREATE MATERIALIZED VIEW task_channels_source_mv TO task_channels_source AS
SELECT
    task_channel_sid,
    account_sid,
    workspace_sid,
    task_channel_unique_name AS task_channel_name,
    timestamp AS event_timestamp
FROM wds_events
WHERE eventtype IN ('task-channel.created', 'task-channel.updated')
  AND task_channel_sid IS NOT NULL AND task_channel_sid != ''
  AND task_channel_unique_name IS NOT NULL;

-- Dictionary for O(1) lookups by SID
CREATE DICTIONARY task_channels_dict
(
    task_channel_sid    String,
    account_sid         String,
    workspace_sid       String,
    task_channel_name   String
)
PRIMARY KEY task_channel_sid
SOURCE(CLICKHOUSE(TABLE 'task_channels_source'))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(MIN 30 MAX 60);

-- Dictionary for O(1) lookups by name
CREATE DICTIONARY task_channels_by_name_dict
(
    workspace_sid       String,
    task_channel_name   String,
    task_channel_sid    String,
    account_sid         String
)
PRIMARY KEY workspace_sid, task_channel_name
SOURCE(CLICKHOUSE(TABLE 'task_channels_source'))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(MIN 30 MAX 60);