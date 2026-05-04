# OpenTelemetry Auto-Instrumentation Metrics

## HTTP Server

### Duration of inbound HTTP requests handled by the server (new, stable)

```
http_server_request_duration_seconds_bucket
http_server_request_duration_seconds_count
http_server_request_duration_seconds_sum
```

Attrs: `http_request_method`, `http_response_status_code`, `http_route`, `url_scheme`, `network_protocol_version`, `error_type`.

### Number of in-flight HTTP requests

```
http_server_active_requests
```

### Size of the body of inbound requests

```
http_server_request_body_size_bytes_bucket
http_server_request_body_size_bytes_count
http_server_request_body_size_bytes_sum
```

### Size of the body of outbound responses

```
http_server_response_body_size_bytes_bucket
http_server_response_body_size_bytes_count
http_server_response_body_size_bytes_sum
```

### Duration of inbound HTTP requests (legacy name, in milliseconds)

```
http_server_duration_milliseconds_bucket
http_server_duration_milliseconds_count
http_server_duration_milliseconds_sum
```

### Total bytes received/sent (legacy name)

```
http_server_request_size_bytes_total
http_server_response_size_bytes_total
```

---

## HTTP Client

### Duration of outbound HTTP requests made by the application (new, stable)

```
http_client_request_duration_seconds_bucket
http_client_request_duration_seconds_count
http_client_request_duration_seconds_sum
```

Attrs: `http_request_method`, `http_response_status_code`, `server_address`, `server_port`, `url_scheme`, `network_protocol_version`, `error_type`.

### Size of the client request body

```
http_client_request_body_size_bytes_bucket
http_client_request_body_size_bytes_count
http_client_request_body_size_bytes_sum
```

### Size of the client response body

```
http_client_response_body_size_bytes_bucket
http_client_response_body_size_bytes_count
http_client_response_body_size_bytes_sum
```

### HTTP connections opened by the client (active or idle)

```
http_client_open_connections
```

### Duration of outbound HTTP connections successfully established

```
http_client_connection_duration_seconds_bucket
http_client_connection_duration_seconds_count
http_client_connection_duration_seconds_sum
```

### In-flight HTTP requests on the client side

```
http_client_active_requests
```

### Duration of client HTTP requests (legacy name, in milliseconds)

```
http_client_duration_milliseconds_bucket
http_client_duration_milliseconds_count
http_client_duration_milliseconds_sum
```

### Total bytes sent/received by the client (legacy name)

```
http_client_request_size_bytes_total
http_client_response_size_bytes_total
```

---

## JVM Runtime (Java agent)

### Memory currently in use by the JVM (heap and non-heap)

```
jvm_memory_used_bytes
```

Attrs: `jvm_memory_pool_name` (Eden, Old Gen, Metaspace...), `jvm_memory_type` (heap/non_heap).

### Memory reserved and guaranteed to be available to the JVM

```
jvm_memory_committed_bytes
```

### Maximum amount of memory the JVM can allocate in the pool

```
jvm_memory_limit_bytes
```

### Memory in use measured right after the last garbage collection cycle

```
jvm_memory_used_after_last_gc_bytes
```

### Duration of garbage collection pauses

```
jvm_gc_duration_seconds_bucket
jvm_gc_duration_seconds_count
jvm_gc_duration_seconds_sum
```

Attrs: `jvm_gc_name` (G1 Young/Old, ZGC...), `jvm_gc_action`.

### Number of threads running in the JVM

```
jvm_thread_count
```

Attrs: `jvm_thread_daemon`, `jvm_thread_state` (runnable|blocked|waiting|timed_waiting).

### Total classes loaded since the process started

```
jvm_class_loaded_total
```

### Total classes unloaded since the process started

```
jvm_class_unloaded_total
```

### Current number of classes loaded in the JVM

```
jvm_class_count
```

### Total CPU time consumed by the JVM process

```
jvm_cpu_time_seconds_total
```

### Number of CPUs available to the JVM

```
jvm_cpu_count
```

### Process CPU utilization (0.0–1.0)

```
jvm_cpu_recent_utilization_ratio
```

### Memory used by direct/mapped byte buffers (experimental)

```
jvm_buffer_memory_used_bytes
jvm_buffer_memory_limit_bytes
jvm_buffer_count
```

### Open file descriptors and limit (experimental)

```
jvm_file_descriptor_count
jvm_file_descriptor_limit
```

### System-wide CPU utilization (experimental, opt-in)

```
jvm_system_cpu_utilization_ratio
jvm_system_cpu_load_1m
```

### Initial memory configured for the pool (experimental)

```
jvm_memory_init_bytes
```

### Legacy JVM metrics (`process.runtime.jvm.*`) — older agents

```
process_runtime_jvm_memory_usage_bytes
process_runtime_jvm_memory_committed_bytes
process_runtime_jvm_memory_limit_bytes
process_runtime_jvm_gc_duration_milliseconds_bucket
process_runtime_jvm_gc_duration_milliseconds_count
process_runtime_jvm_gc_duration_milliseconds_sum
process_runtime_jvm_threads_count
process_runtime_jvm_classes_loaded_total
process_runtime_jvm_classes_unloaded_total
process_runtime_jvm_classes_current_loaded
process_runtime_jvm_cpu_utilization_ratio
process_runtime_jvm_system_cpu_utilization_ratio
process_runtime_jvm_system_cpu_load_1m
```

---

## Database Client (JDBC, R2DBC, MongoDB, Cassandra, recent Redis)

### Duration of database operations (stable)

```
db_client_operation_duration_seconds_bucket
db_client_operation_duration_seconds_count
db_client_operation_duration_seconds_sum
```

Attrs: `db_system`, `db_operation_name`, `db_collection_name`, `db_namespace`, `db_response_status_code`, `server_address`, `server_port`, `error_type`.

### Number of rows returned per query

```
db_client_response_returned_rows_bucket
db_client_response_returned_rows_count
db_client_response_returned_rows_sum
```

---

## Database Connection Pool (HikariCP, c3p0, DBCP, UCP, Tomcat Pool, Vibur)

### Connections in use or idle in the pool (new)

```
db_client_connection_count
```

Attrs: `state=used|idle`, `pool_name`.

### Maximum number of connections allowed in the pool

```
db_client_connection_max
```

### Minimum and maximum idle connection limits

```
db_client_connection_idle_max
db_client_connection_idle_min
```

### Requests waiting for a connection from the pool

```
db_client_connection_pending_requests
```

### Total timeouts while trying to acquire a connection from the pool

```
db_client_connection_timeouts_total
```

### Time to create a new connection

```
db_client_connection_create_time_seconds_bucket
db_client_connection_create_time_seconds_count
db_client_connection_create_time_seconds_sum
```

### Time spent waiting for an available connection from the pool

```
db_client_connection_wait_time_seconds_bucket
db_client_connection_wait_time_seconds_count
db_client_connection_wait_time_seconds_sum
```

### Time between acquiring and returning the connection to the pool

```
db_client_connection_use_time_seconds_bucket
db_client_connection_use_time_seconds_count
db_client_connection_use_time_seconds_sum
```

### Legacy pool names (`db.client.connections.*`, plural)

```
db_client_connections_usage
db_client_connections_max
db_client_connections_idle_max
db_client_connections_idle_min
db_client_connections_pending_requests
db_client_connections_timeouts_total
db_client_connections_create_time_milliseconds_bucket
db_client_connections_create_time_milliseconds_count
db_client_connections_create_time_milliseconds_sum
db_client_connections_wait_time_milliseconds_bucket
db_client_connections_wait_time_milliseconds_count
db_client_connections_wait_time_milliseconds_sum
db_client_connections_use_time_milliseconds_bucket
db_client_connections_use_time_milliseconds_count
db_client_connections_use_time_milliseconds_sum
```

---

## Messaging (Kafka, RabbitMQ, JMS, Pulsar, SQS/SNS, PubSub)

### Duration of messaging operations (publish/receive/process)

```
messaging_client_operation_duration_seconds_bucket
messaging_client_operation_duration_seconds_count
messaging_client_operation_duration_seconds_sum
```

Attrs: `messaging_system` (kafka, rabbitmq...), `messaging_destination_name` (topic/queue), `messaging_operation_name`, `messaging_operation_type`, `messaging_client_id`, `messaging_consumer_group_name`, `error_type`.

### Total messages sent by the producer

```
messaging_client_sent_messages_total
```

### Total messages consumed/delivered to the application

```
messaging_client_consumed_messages_total
```

### Message processing duration (only `operation.type=process`)

```
messaging_process_duration_seconds_bucket
messaging_process_duration_seconds_count
messaging_process_duration_seconds_sum
```

---

## Kafka Client (via JMX, exposed by the Java agent)

### Rate and total of records sent by the producer

```
kafka_producer_record_send_rate
kafka_producer_record_send_total
```

### Producer byte throughput and compression

```
kafka_producer_byte_rate
kafka_producer_outgoing_byte_rate
kafka_producer_compression_rate_avg
```

### Producer errors and retries

```
kafka_producer_record_error_total
kafka_producer_record_retry_total
```

### Producer request latency (to the broker)

```
kafka_producer_request_latency_avg
kafka_producer_request_latency_max
```

### Rate and total of records consumed by the consumer

```
kafka_consumer_records_consumed_total
kafka_consumer_records_consumed_rate
kafka_consumer_bytes_consumed_total
```

### Consumer fetch latency and rate

```
kafka_consumer_fetch_latency_avg
kafka_consumer_fetch_latency_max
kafka_consumer_fetch_rate
```

### Consumer lag (distance to the end of the partition)

```
kafka_consumer_records_lag
kafka_consumer_records_lag_max
```

### Consumer lead (distance from the start of the partition)

```
kafka_consumer_records_lead
kafka_consumer_records_lead_min
```

### Offset commit latency

```
kafka_consumer_commit_latency_avg
kafka_consumer_commit_latency_max
```

### Number of partitions assigned to the consumer

```
kafka_consumer_assigned_partitions
```

---

## RPC / gRPC

### Duration of RPC calls on the server side

```
rpc_server_duration_milliseconds_bucket
rpc_server_duration_milliseconds_count
rpc_server_duration_milliseconds_sum
```

Attrs: `rpc_system=grpc`, `rpc_service`, `rpc_method`, `rpc_grpc_status_code`.

### Request/response sizes on the RPC server

```
rpc_server_request_size_bytes_bucket
rpc_server_request_size_bytes_count
rpc_server_request_size_bytes_sum
rpc_server_response_size_bytes_bucket
rpc_server_response_size_bytes_count
rpc_server_response_size_bytes_sum
```

### Messages per RPC (streaming) — server

```
rpc_server_requests_per_rpc_bucket
rpc_server_requests_per_rpc_count
rpc_server_requests_per_rpc_sum
rpc_server_responses_per_rpc_bucket
rpc_server_responses_per_rpc_count
rpc_server_responses_per_rpc_sum
```

### Duration of RPC calls on the client side

```
rpc_client_duration_milliseconds_bucket
rpc_client_duration_milliseconds_count
rpc_client_duration_milliseconds_sum
```

### Request/response sizes on the RPC client

```
rpc_client_request_size_bytes_bucket
rpc_client_request_size_bytes_count
rpc_client_request_size_bytes_sum
rpc_client_response_size_bytes_bucket
rpc_client_response_size_bytes_count
rpc_client_response_size_bytes_sum
```

### Messages per RPC (streaming) — client

```
rpc_client_requests_per_rpc_bucket
rpc_client_requests_per_rpc_count
rpc_client_requests_per_rpc_sum
rpc_client_responses_per_rpc_bucket
rpc_client_responses_per_rpc_count
rpc_client_responses_per_rpc_sum
```

---

## .NET Runtime (.NET 9+, meter `System.Runtime`)

### CPUs available and CPU time consumed by the process

```
dotnet_process_cpu_count
dotnet_process_cpu_time_seconds_total
```

### Process resident memory (working set)

```
dotnet_process_memory_working_set_bytes
```

### Garbage collector collections per generation

```
dotnet_gc_collections_total
```

Attrs: `generation=gen0|gen1|gen2`.

### Total bytes allocated on the managed heap

```
dotnet_gc_heap_total_allocated_bytes_total
```

### Memory committed at the end of the last GC collection

```
dotnet_gc_last_collection_memory_committed_size_bytes
```

### Heap size and fragmentation at the last collection

```
dotnet_gc_last_collection_heap_size_bytes
dotnet_gc_last_collection_heap_fragmentation_size_bytes
```

### Total pause time caused by the GC

```
dotnet_gc_pause_time_seconds_total
```

### JIT activity (compiled IL, methods, time)

```
dotnet_jit_compiled_il_size_bytes_total
dotnet_jit_compiled_methods_total
dotnet_jit_compilation_time_seconds_total
```

### Active threads in the .NET ThreadPool

```
dotnet_thread_pool_thread_count
```

### ThreadPool work items processed and queue

```
dotnet_thread_pool_work_item_count_total
dotnet_thread_pool_queue_length
```

### Lock (monitor) contention in the runtime

```
dotnet_monitor_lock_contentions_total
```

### Active timers in the runtime

```
dotnet_timer_count
```

### Loaded assemblies

```
dotnet_assembly_count
```

### Total exceptions thrown since the process started

```
dotnet_exceptions_total
```

---

## Legacy .NET Runtime (`process.runtime.dotnet.*`, .NET 6–8)

### GC collection, allocation and heap totals

```
process_runtime_dotnet_gc_collections_count_total
process_runtime_dotnet_gc_objects_size_bytes
process_runtime_dotnet_gc_allocations_size_bytes_total
process_runtime_dotnet_gc_committed_memory_size_bytes
process_runtime_dotnet_gc_heap_size_bytes
process_runtime_dotnet_gc_heap_fragmentation_size_bytes
process_runtime_dotnet_gc_pause_time_milliseconds_total
```

### Legacy JIT (IL, methods, time)

```
process_runtime_dotnet_jit_il_compiled_size_bytes_total
process_runtime_dotnet_jit_methods_compiled_count_total
process_runtime_dotnet_jit_compilation_time_milliseconds_total
```

### ThreadPool, monitor and miscellaneous (legacy)

```
process_runtime_dotnet_thread_pool_threads_count
process_runtime_dotnet_thread_pool_completed_items_count_total
process_runtime_dotnet_thread_pool_queue_length
process_runtime_dotnet_monitor_lock_contention_count_total
process_runtime_dotnet_timer_count
process_runtime_dotnet_assemblies_count
process_runtime_dotnet_exceptions_count_total
```

---

## ASP.NET Core (Kestrel + diagnostics, .NET 8+)

### Active connections and duration on the Kestrel server

```
kestrel_active_connections
kestrel_connection_duration_seconds_bucket
kestrel_connection_duration_seconds_count
kestrel_connection_duration_seconds_sum
```

### Rejected connections and Kestrel queues

```
kestrel_rejected_connections_total
kestrel_queued_connections
kestrel_queued_requests
kestrel_upgraded_connections
```

### TLS handshakes (duration and concurrency)

```
kestrel_tls_handshake_duration_seconds_bucket
kestrel_tls_handshake_duration_seconds_count
kestrel_tls_handshake_duration_seconds_sum
kestrel_active_tls_handshakes
```

### Route match attempts in the ASP.NET Core pipeline

```
aspnetcore_routing_match_attempts_total
```

### Exceptions captured by the diagnostics middleware

```
aspnetcore_diagnostics_exceptions_total
```

### Rate limiting (active leases, queue, duration, totals)

```
aspnetcore_rate_limiting_active_request_leases
aspnetcore_rate_limiting_queued_requests
aspnetcore_rate_limiting_request_lease_duration_seconds_bucket
aspnetcore_rate_limiting_request_lease_duration_seconds_count
aspnetcore_rate_limiting_request_lease_duration_seconds_sum
aspnetcore_rate_limiting_requests_total
```

---

## Node.js Runtime (`@opentelemetry/instrumentation-runtime-node`)

### Event loop delay (min/max/mean/stddev and percentiles)

```
nodejs_eventloop_delay_min_seconds
nodejs_eventloop_delay_max_seconds
nodejs_eventloop_delay_mean_seconds
nodejs_eventloop_delay_stddev_seconds
nodejs_eventloop_delay_p50_seconds
nodejs_eventloop_delay_p90_seconds
nodejs_eventloop_delay_p99_seconds
```

### Event loop utilization (active fraction between 0 and 1)

```
nodejs_eventloop_utilization
```

### Cumulative event loop time in active/idle state

```
nodejs_eventloop_time_seconds_total
```

Attrs: `state=active|idle`.

### Duration of V8 GC cycles

```
v8js_gc_duration_seconds_bucket
v8js_gc_duration_seconds_count
v8js_gc_duration_seconds_sum
```

### V8 heap space sizes (new_space, old_space, code_space, map_space...)

```
v8js_heap_space_size_total_bytes
v8js_heap_space_size_used_bytes
v8js_heap_space_size_available_bytes
v8js_heap_space_size_physical_bytes
```

### V8 heap memory used and limit

```
v8js_memory_heap_used_bytes
v8js_memory_heap_limit_bytes
```

---

## Process (via `system-metrics` instrumentation)

### CPU time consumed by the process

```
process_cpu_time_seconds_total
```

Attrs: `cpu_mode=user|system`.

### Process CPU utilization (0.0–1.0)

```
process_cpu_utilization
```

### Physical and virtual memory used by the process

```
process_memory_usage_bytes
process_memory_virtual_bytes
```

### Disk and network I/O by the process

```
process_disk_io_bytes_total
process_network_io_bytes_total
```

Attrs: `disk_io_direction=read|write`, `network_io_direction`.

### Threads and file descriptors opened by the process

```
process_thread_count
process_open_file_descriptor_count
```

---

## System (via `system-metrics` instrumentation)

### System CPU utilization and time

```
system_cpu_utilization
system_cpu_time_seconds_total
```

Attrs: `cpu`, `cpu_mode`.

### System physical memory usage by state

```
system_memory_usage_bytes
system_memory_utilization
```

Attrs: `state=used|free|cached|buffers`.

### Disk I/O (bytes, operations, busy time)

```
system_disk_io_bytes_total
system_disk_operations_total
system_disk_io_time_seconds_total
```

### Network I/O (bytes, packets, errors)

```
system_network_io_bytes_total
system_network_packets_total
system_network_errors_total
```

### Open network connections by protocol and state

```
system_network_connections
```

Attrs: `protocol`, `state`.

### Filesystem usage (space per mountpoint/type)

```
system_filesystem_usage_bytes
system_filesystem_utilization
```

Attrs: `state`, `type`, `mode`, `mountpoint`.

---

## FaaS (Lambda, Azure Functions, Cloud Functions)

### Duration and initialization of serverless invocations

```
faas_invoke_duration_seconds_bucket
faas_invoke_duration_seconds_count
faas_invoke_duration_seconds_sum
faas_init_duration_seconds_bucket
faas_init_duration_seconds_count
faas_init_duration_seconds_sum
```

### Totals of invocations, errors, timeouts and cold starts

```
faas_invocations_total
faas_errors_total
faas_timeouts_total
faas_coldstarts_total
```

### CPU, memory and network consumed by the function

```
faas_cpu_usage_seconds_bucket
faas_cpu_usage_seconds_count
faas_cpu_usage_seconds_sum
faas_mem_usage_bytes_bucket
faas_mem_usage_bytes_count
faas_mem_usage_bytes_sum
faas_net_io_bytes_bucket
faas_net_io_bytes_count
faas_net_io_bytes_sum
```

---

## Collector meta-metrics (not from the application, but vital)

### Spans/metrics/logs accepted and refused per receiver

```
otelcol_receiver_accepted_spans_total
otelcol_receiver_refused_spans_total
otelcol_receiver_accepted_metric_points_total
otelcol_receiver_refused_metric_points_total
otelcol_receiver_accepted_log_records_total
otelcol_receiver_refused_log_records_total
```

### Spans/metrics sent and failures per exporter

```
otelcol_exporter_sent_spans_total
otelcol_exporter_send_failed_spans_total
otelcol_exporter_sent_metric_points_total
otelcol_exporter_send_failed_metric_points_total
otelcol_exporter_sent_log_records_total
otelcol_exporter_send_failed_log_records_total
```

### Exporter queue (occupancy and capacity)

```
otelcol_exporter_queue_size
otelcol_exporter_queue_capacity
```

### Batch processor (batch size and send triggers)

```
otelcol_processor_batch_batch_send_size_bucket
otelcol_processor_batch_batch_send_size_count
otelcol_processor_batch_batch_send_size_sum
otelcol_processor_batch_timeout_trigger_send_total
otelcol_processor_batch_size_trigger_send_total
```

### Collector process resources

```
otelcol_process_cpu_seconds_total
otelcol_process_memory_rss_bytes
otelcol_process_uptime_seconds_total
```

### Kafka receiver of the L2 Collector (lag, offset, messages)

```
otelcol_kafka_receiver_messages_total
otelcol_kafka_receiver_current_offset
otelcol_kafka_receiver_offset_lag_ratio
```

---

## Sources

- [HTTP metrics semconv](https://opentelemetry.io/docs/specs/semconv/http/http-metrics/)
- [JVM metrics semconv](https://opentelemetry.io/docs/specs/semconv/runtime/jvm-metrics/)
- [Database client metrics semconv](https://opentelemetry.io/docs/specs/semconv/db/database-metrics/)
- [Messaging metrics semconv](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-metrics/)
- [.NET CLR metrics semconv](https://opentelemetry.io/docs/specs/semconv/runtime/dotnet-metrics/)
- [Java supported libraries](https://github.com/open-telemetry/opentelemetry-java-instrumentation/blob/main/docs/supported-libraries.md)
- [JMX Metric Insight (Java)](https://opentelemetry.io/blog/2023/jmx-metric-insight/)
- [Node runtime instrumentation](https://www.npmjs.com/package/@opentelemetry/instrumentation-runtime-node)
- [.NET auto-instrumentation config](https://github.com/open-telemetry/opentelemetry-dotnet-instrumentation/blob/main/docs/config.md)
