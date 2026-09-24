# 领域约定

用于统一表达媒体语料的来源、版本、用途申请和放行事实，支持跨机构交换时保持权利边界可追踪。

聚合对象包括`source_asset`、`corpus_slice`、`use_request`、`release_decision`。事件类型包括`ASSET_INGESTED`、`SLICE_REVIEWED`、`USE_REQUESTED`、`USE_APPROVED`、`RIGHT_WITHDRAWN`。所有时间都必须携带时区，版本号从 1 开始递增，校验层不会替调用方改写输入。

## 事件载荷

- `ASSET_INGESTED`：还需包含 `source_id`, `content_hash`。
- `USE_REQUESTED`：还需包含 `purpose`, `territory`。
- `RIGHT_WITHDRAWN`：还需包含 `right_id`, `effective_at`。

同一事件标识的幂等与冲突处理属于上层业务服务职责；交换层只负责稳定报告结构、枚举、时间、版本和必需载荷问题。
