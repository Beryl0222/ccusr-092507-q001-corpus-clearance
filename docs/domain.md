# 领域约定

用于统一表达媒体语料的来源、内容版本、段落切片、权利依据、地域语言、保留期限、
敏感标记、派生数据集、使用申请与放行决定，支持央地汇交与跨机构交换时保持权利边界
可追踪。所有状态变更都以领域事件落库，读模型可随时由事件流重建。

## 聚合

| 聚合 | 标识 | 含义 |
| --- | --- | --- |
| `organization` | org_id | 参与归集、审校、申请、放行的机构 |
| `source_asset` | asset_id | 一篇报道资产（含通讯社通稿、地方补采等来源） |
| `corpus_slice` | slice_id | 段落切片，携带敏感标记与双职责审校记录 |
| `right_basis` | right_id | 权利依据：地域、语言、产品、用途、禁发期与保留期限 |
| `use_request` | request_id | 使用申请，含材料指纹 |
| `release_decision` | decision_id | 放行/驳回决定，同一申请至多一个当前有效批准 |
| `derived_dataset` | dataset_id | 训练集、政务问答集、国际传播包等及其派生关系 |
| `disposition` | disposition_id | 撤回后对已发布版本产生的处置义务 |

## 事件

`ORG_REGISTERED`、`ASSET_INGESTED`、`ASSET_DISPUTED`、`RIGHT_DECLARED`、
`SLICE_CUT`、`SLICE_REVIEWED`、`USE_REQUESTED`、`REVIEW_OPENED`、
`USE_APPROVED`、`USE_REJECTED`、`REQUEST_SUPERSEDED`、`RIGHT_WITHDRAWN`、
`RIGHT_EXPIRED`、`SLICE_FROZEN`、`DATASET_REGISTERED`、
`DATASET_DERIVATION_RECORDED`、`DATASET_FROZEN`、`PUBLICATION_RECORDED`、
`DISPOSITION_RAISED`、`DISPOSITION_FULFILLED`、`DECISION_EFFECTIVENESS_CHANGED`。

所有时间必须携带时区；版本号对每个聚合从 1 开始递增；校验层不替调用方改写输入。

## 关键规则

1. **归集范围**：归集方只能对自己归集的资产声明权利依据；权利依据的
   地域/语言/产品/用途/期限构成下游使用的硬边界，`*` 表示通配。
2. **安全重传**：同一批次与内容指纹（以及授权指纹一致）的重复入库直接回放既有资产，
   不产生新事件；编号相同而正文指纹或授权指纹不同，资产进入 `quarantined` 争议隔离，
   隔离期间不得声明授权、切片或放行。
3. **双职责审校**：切片须同时具备最新的 `desensitization`（脱敏）与 `fact`（事实）
   通过记录，且两类复核人不得相同，方可进入批准。
4. **材料指纹**：申请对完整材料计算指纹；材料改变必须作废旧申请
   （`REQUEST_SUPERSEDED`），旧批准同步失效，禁止沿用旧批准使用新语料。
5. **唯一有效决定**：写事务串行化 + 部分唯一索引，保证同一申请在跨机构并发放行下
   只产生一个当前有效批准；后到的批准/驳回返回冲突。
6. **撤回传播**：`RIGHT_WITHDRAWN` 只冻结失去全部有效授权的切片；冻结沿
   数据集派生边向下传播。切片在撤回生效前的已发布版本不被改写，发布事件保留当时
   依据快照，并对每个受影响发布生成待履行处置义务；履行后义务关闭，事实仍保留。
7. **到期处理**：声明 `not_after` 时登记到期作业，另有周期扫描兜底；作业持久化、
   认领带租约，服务重启后继续未完成的扫描与传播。
8. **资格判定**：给定切片、日期、地区、语言、产品，系统综合资产状态、切片冻结、
   双审结论、授权维度与期限、当前有效决定后给出 `usable` 与阻塞原因。

## 可追踪关系

```
organization ──collects──▶ source_asset ──cuts──▶ corpus_slice
                                │                     │
                          declares│              reviews│（脱敏/事实，不同人）
                                ▼                     ▼
                           right_basis            use_request ◀── applicant
                                │                     │
                                └──── dimensions ◀────┤
                                                      ▼
                                              release_decision
corpus_slice ──membership──▶ derived_dataset ──derives──▶ derived_dataset
corpus_slice ──published──▶ PUBLICATION_RECORDED（当时依据快照）
right_basis ──withdrawn──▶ SLICE_FROZEN / DATASET_FROZEN / DISPOSITION_RAISED
```

同一事件标识的幂等与冲突处理由业务服务与存储层负责；交换层（`contracts.py` 与
`domain.schema.json`）只负责稳定报告结构、枚举、时间、版本和必需载荷问题。
