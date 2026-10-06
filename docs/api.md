# HTTP API 手册

基础地址默认 `http://127.0.0.1:8080`。请求/响应均为 UTF-8 JSON。

- `X-Actor-Id: <人员编号>`：操作者身份；也可在请求体中用同名字段传递。
- `Idempotency-Key: <键>`：写请求安全重试；同键重复提交原样返回首次响应。
- 错误响应：`{"code": ..., "message": ..., "details": ...}`，状态码含
  `400/403/404/409/422/500`。

时间字段一律使用带时区的 ISO 8601；日期参数 `date=YYYY-MM-DD` 按当日 `00:00Z` 解释。

## 机构与资产

### `POST /v1/orgs`
```json
{"org_id": "hnr", "name": "海南日报"}
```

### `POST /v1/assets/ingest`
```json
{
  "batch_id": "round1", "collector_id": "hnr", "collector_org_id": "hnr",
  "asset_id": "story-2", "title": "本地补采：人物专访",
  "content_hash": "sha256…", "authorization_fingerprint": "sha256…"
}
```
响应 `status`：`ingested`（201）/ `retransmitted`（200，安全重传）/
`disputed`（200，编号相同但正文或授权指纹不同，已隔离）。

### `POST /v1/assets/{asset_id}/rights` 声明权利依据
```json
{
  "right_id": "person-zh",
  "territories": ["CN"], "languages": ["zh"],
  "products": ["train", "govqa"], "purpose_tags": ["news"],
  "slice_ids": ["story-2:s1"],
  "embargo_not_before": "2026-10-01T00:00:00+08:00",
  "not_after": "2027-12-31T23:59:59+08:00"
}
```
仅资产归集人可声明（`403`）；`"*"` 为维度通配。

### `POST /v1/assets/{asset_id}/slices`
```json
{"slices": [{"content_hash": "sha256…", "sensitivity": {"person_interview": true}}]}
```
未给 `slice_id/ordinal` 时自动生成（`{asset_id}:s{序号}`）。

## 审校与申请

### `POST /v1/slices/{slice_id}/reviews`
`review_kind` 仅允许 `desensitization`（脱敏复核）或 `fact`（事实审校）；
两类最新记录都必须 `passed=true` 且复核人不同。
```json
{"review_kind": "fact", "passed": true, "notes": "事实无误"}
```

### `POST /v1/requests`
```json
{
  "purpose": "news",
  "slice_ids": ["story-2:s1", "story-2:s2"],
  "territories": ["CN"], "languages": ["zh"], "products": ["train", "govqa"],
  "materials": {"proposal": "v1", "data_flow": "closed"}
}
```
响应含 `materials_fingerprint`。

### `POST /v1/requests/{request_id}/decision`
```json
{"approved": true}
```
批准时逐切片校验：资产非争议、切片未冻结、双审通过且复核人不同、授权在决定时点
覆盖全部地域/语言/产品。任一不满足返回 `422` 与每切片 `blockers`；驳回须给
`{"approved": false, "reason": "…"}`。重复决定返回 `409`。

### `POST /v1/requests/{request_id}/supersede`
申请人材料改变时作废旧申请并生成新申请；旧批准立即失效。
```json
{"materials": {"proposal": "v2", "data_flow": "open"}}
```

## 撤回与发布

### `POST /v1/rights/{right_id}/withdraw`
```json
{"effective_at": "2026-10-05T00:00:00+08:00", "reason": "被采访人撤回训练用途授权"}
```
返回 `202` 与传播作业编号；后台 worker 冻结失去全部有效授权的切片、
冻结包含它们的数据集（沿派生边传播），并为撤回生效前的已发布版本生成处置义务。

### `POST /v1/slices/{slice_id}/publications`
发布事实永久保留，载荷中的授权快照记录"当时依据"。
```json
{"published_at": "2026-10-03T09:00:00+08:00",
 "basis": {"channel": "政务终端", "decision_id": "dec-…"}}
```

### `POST /v1/dispositions/{disposition_id}/fulfill`
```json
{"note": "已下线发布版本并通知使用方"}
```

## 数据集

### `POST /v1/datasets`
```json
{"dataset_id": "ds-train", "name": "训练集", "product": "train",
 "slice_ids": ["story-2:s1"]}
```

### `POST /v1/datasets/{dataset_id}/derivations`
```json
{"child_dataset_id": "ds-govqa"}
```
父数据集已冻结时，子数据集一并冻结；不允许派生环。

## 查询

### `GET /v1/clearance`
参数：`slice_id, date, territory, language, product`（均必填），`purpose`（可选）。
```json
{
  "slice_id": "story-2:s1", "at": "2026-10-06T00:00:00+00:00",
  "territory": "CN", "language": "zh", "product": "train",
  "usable": true, "blockers": [],
  "active_right_ids": ["person-zh"],
  "effective_decision": {"decision_id": "dec-…", "decider_id": "officer-zhang",
                         "materials_fingerprint": "…", "valid_until": "…"},
  "frozen_datasets": [], "open_dispositions": [],
  "sensitivity": {"person_interview": true}
}
```
`blockers` 可能值：`asset_disputed`、`slice_frozen`、`review_incomplete`、
`review_not_passed`、`reviewer_must_differ`、`no_covering_right`、
`no_effective_decision`。

### `GET /v1/datasets/{dataset_id}/manifest`
从数据集反查每个切片的：原文资产（批次/归集人/内容指纹/授权指纹/状态）、
敏感标记、两类审校、权利依据（含撤回与到期状态）、全部批准记录（批准人、
是否仍有效及失效原因）、发布及当时依据、待履行处置义务，以及数据集的父母/子女。

### 其他只读端点
- `GET /v1/assets`、`GET /v1/requests`、`GET /v1/datasets`、`GET /v1/dispositions`
- `GET /v1/assets/{asset_id}/trace`：资产下切片、授权与事件流。
- `GET /v1/aggregates/{aggregate_type}/{aggregate_id}/events`：任一聚合的原始事件。
- `GET /v1/jobs`：作业计数（pending/running/done/dead）。
- `GET /health`。

## 运维

- `POST /v1/maintenance/run-jobs`：同步执行一批到期作业（测试/无 worker 部署时使用）。
- `POST /v1/maintenance/rebuild-projections`：由事件流重建全部读模型。
- 环境变量：`CORPUS_DB`、`CORPUS_HOST`、`CORPUS_PORT`、`CORPUS_WORKER_INTERVAL`
  （秒，默认 5）、`CORPUS_HTTP_LOG`（输出访问日志）。
