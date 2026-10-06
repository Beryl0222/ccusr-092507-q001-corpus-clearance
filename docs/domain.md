# 领域约定

用于统一表达媒体语料的来源、版本、用途申请和放行事实，支持跨机构交换时保持权利边界可追踪。

聚合对象包括 `source_org`、`source_asset`、`corpus_slice`、`rights_grant`、`use_request`、`release_decision`（以申请聚合上的决定事件表达）、`derived_dataset`、`obligation`。

事件类型：

| 事件 | 聚合 | 含义 |
| --- | --- | --- |
| `ORG_REGISTERED` | source_org | 登记机构及其职责（collector/reviewer/approver/applicant/platform） |
| `ASSET_INGESTED` | source_asset | 语料版本入库，携带来源、批次、正文指纹、授权指纹、敏感标记（含禁发期） |
| `ASSET_QUARANTINED` | source_asset | 编号相同而正文或授权不同，争议版本被隔离，现行版本不动 |
| `ASSET_DISPUTE_RESOLVED` | source_asset | 平台对争议版本裁决 `promote`（升为现行并补登记授权）或 `reject`（作废） |
| `SLICE_CUT` | corpus_slice | 段落切片，挂接该语料版本的权利依据，继承禁发期等标记 |
| `SLICE_REVIEWED` | corpus_slice | 复核结论，职责为 `desensitize`（脱敏）或 `fact_check`（事实审校） |
| `GRANT_RECORDED` | rights_grant | 权利依据：地区、语言、产品、生效起点、保留期限 |
| `GRANT_EXPIRED` | rights_grant | 保留期届满 |
| `GRANT_WITHDRAWN` | rights_grant | 授权方撤回，携带生效时刻（可为未来时刻） |
| `SLICE_FROZEN` | corpus_slice | 撤回/到期波及的切片被冻结 |
| `USE_REQUESTED` | use_request | 使用申请，携带材料指纹；同编号材料变化会重开并作废旧决定 |
| `USE_APPROVED` / `USE_DENIED` | use_request | 放行/拒绝决定，携带决定编号、材料指纹、适用范围和当时依据 |
| `DECISION_SUPERSEDED` | use_request | 旧有效决定作废（材料改变），每个申请始终至多一个有效决定 |
| `DATASET_CREATED` | derived_dataset | 派生数据集（训练集、政务问答集、国际传播包等） |
| `DATASET_PUBLISHED` | derived_dataset | 发布，载荷固化当时的有效决定、材料指纹与切片版本 |
| `DATASET_FROZEN` | derived_dataset | 上游切片权利撤回/到期时，已发布数据集冻结 |
| `OBLIGATION_RAISED` / `OBLIGATION_FULFILLED` | obligation | 已发布版本的后续处置义务（清除或通知），按来源幂等 |
| `RIGHT_WITHDRAWN` | 对应聚合 | 对外交换形态的撤回事实（保留契约原始事件） |

所有时间都必须携带时区，版本号从 1 开始递增，校验层不会替调用方改写输入。

## 关键事件载荷

- `ASSET_INGESTED`：`source_id`, `content_hash`（另含 `batch_id`、`grant_print`、`declared_grants`、`flags`、`quarantined`）。
- `USE_REQUESTED`：`purpose`, `territory`（另含 `language`、`product`、`slice_ids`、`materials_hash`）。
- `RIGHT_WITHDRAWN`：`right_id`, `effective_at`。
- `USE_APPROVED`/`USE_DENIED`：`decision_id`, `request_materials_hash`, `territory`, `language`, `product`, `valid_from`, `valid_until`。

## 不变量

1. **归集范围**：只有具备 collector 职责的机构能归集；机构只能为自己归集的语料版本登记权利依据。
2. **职责分离**：同一切片的脱敏与事实审校必须由不同人员完成，两项均通过才进入可放行状态。
3. **材料定版**：决定携带申请材料指纹；同编号申请材料改变，旧决定立即作废（`DECISION_SUPERSEDED`），必须重新审查。
4. **唯一有效决定**：同一申请跨机构并发的放行在单个立即写事务内检查并写入，只产生一个有效决定。
5. **幂等摄取**：相同 `batch_id` + `content_hash` 安全重传；编号相同而正文指纹或授权指纹不同则隔离争议，不覆盖现行版本。
6. **最小冻结**：撤回只冻结挂接该权利依据的切片及其派生数据集；已发布版本保留发布时依据，并按（数据集, 切片, 授权）生成处置义务。
7. **到期与重启**：保留期扫描和撤回传播均为持久作业，处理器幂等；进程重启后重放投影并续跑未完成作业。

同一事件标识的幂等与冲突处理属于上层业务服务职责；交换层只负责稳定报告结构、枚举、时间、版本和必需载荷问题。
