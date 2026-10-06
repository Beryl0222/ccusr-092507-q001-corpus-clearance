# HTTP API

所有接口返回 JSON；写接口要求请求头：

- `X-Org-Id`：执行机构编号（必须与请求体中声明的机构字段一致，否则 403 `org_mismatch`）。
- `X-User-Id`：执行人员（复核、放行接口必填）。
- `Idempotency-Key`：可选；同一方法+路径+键的重放返回首次结果（`idempotent_replay: true`），错误响应不缓存。

错误体形如：

```json
{"error": {"code": "single_active_decision", "message": "……", "details": {}}}
```

## 机构与归集

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/v1/orgs` | 登记机构及职责 `collector/reviewer/approver/applicant/platform` |
| POST | `/v1/assets/ingest` | 语料版本入库。同 `batch_id`+`content_hash` 安全重传；编号相同而正文或授权不同返回 409 `asset_disputed`，现行版本不动、争议版本隔离 |
| GET | `/v1/assets` / `/v1/assets/{id}` | 语料清单 / 版本明细（含各版指纹、授权指纹、隔离状态） |
| POST | `/v1/assets/{id}/disputes/resolve` | 平台裁决争议版本：`resolution=promote|reject`、`version`；promote 时补登记该版授权 |
| POST | `/v1/assets/{id}/slices` | 段落切片；默认挂接该现行版本全部有效授权，继承禁发期标记 |
| GET | `/v1/slices/{id}` | 切片状态、挂接授权、双职责复核、完整处理记录 |
| POST | `/v1/slices/{id}/reviews` | 复核：`duty=desensitize|fact_check`；同一人兼任两职返回 403 |

## 权利依据

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/v1/grants` | 登记授权（地区/语言/产品/`retain_until`）。归集方只能为自己归集的版本登记；相同 `grant_id` 安全重传 |
| GET | `/v1/grants/{id}` | 授权状态（active/expired/withdrawn）、撤回生效时刻 |
| POST | `/v1/grants/{id}/withdraw` | **仅授权方机构**可撤回；`effective_at` 可指向未来，传播进入持久作业队 |

## 申请与放行

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/v1/requests` | 提交申请，材料被规范化指纹化；同编号材料相同安全重传，材料改变则作废旧决定并重开 |
| POST | `/v1/requests/{id}/decisions` | `verdict=approved|denied`。并发跨机构放行只有一个成功，其余 409 `single_active_decision`；前置条件不满足时批准返回 409 `approval_precondition_failed` |
| GET | `/v1/requests` | 申请清单 |

## 派生数据集与处置义务

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/v1/datasets` | 创建派生数据集（训练集、问答集、国际传播包） |
| POST | `/v1/datasets/{id}/publish` | 凭当时有效的放行决定发布；发布时刻实时复核权利，撤回/到期/材料失配均拒绝 |
| GET | `/v1/datasets` / `/v1/datasets/{id}/lineage` | 清单 / 血缘反查（见下） |
| GET | `/v1/obligations?status=open` | 待履行处置义务全局清单 |
| POST | `/v1/obligations/{id}/fulfill` | 标记处置义务已履行 |

## 核心查询

### 资格判定

```
GET /v1/eligibility?slice_id=...&date=2026-12-02&territory=CN&language=zh&product=qa
```

`date` 为纯日期时按海南时区当日判定；也可传带时区的时间戳。返回：

```json
{
  "slice_id": "slice_...", "date": "2026-12-02",
  "territory": "CN", "language": "zh", "product": "qa",
  "usable": true, "blockers": [],
  "effective_grants": ["g1"],
  "rights": [{"grant_id": "g1", "status": "active", "territories": ["CN"], "...": "..."}]
}
```

阻断原因覆盖：切片冻结、禁发期未满、双职责复核未通过、无覆盖地区/语言/产品的有效授权、保留期届满。

### 数据集血缘反查

`GET /v1/datasets/{id}/lineage` 返回：

- `slices[]`：每个切片的**原文版本**（`asset_id`/`asset_version`/`content_hash`/`source_id`/归集机构/批次）、挂接权利、双职责复核人、完整处理事件流；
- `decisions[]`：发布所依据的批准（批准人、批准机构、有效期、材料指纹、当时依据快照、是否仍 active）；
- `published_basis`：发布瞬间固化的决定清单、材料指纹与切片版本（已发布版本永久保留当时依据）；
- `pending_obligations[]` / `obligations[]`：撤回后仍待履行与全部处置义务。

## 运维

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/healthz` | 健康检查 |
| GET | `/v1/jobs` | 持久作业队状态（pending/leased/done/dead、重试错误） |
| POST | `/v1/maintenance/run-jobs` | 手动执行一批到期作业（到期扫描、撤回传播） |

服务默认启动后台调度线程执行到期作业；`--no-scheduler` 时只靠手动触发。重启后自动重放事件投影、回收中断租约、续跑未完成作业。

## 启动

```bash
PYTHONPATH=src python3 -m corpus_clearance.serve --db corpus.db --port 8080
```
