# 自贸港语料用途放行库

面向央地汇交流程的语料用途放行后端：把**来源机构、内容版本、段落切片、权利依据、
地域语言、保留期限、敏感标记、派生数据集和使用申请**串成可追踪关系，并通过 HTTP API
回答"一段语料在指定日期能否用于某个地区、语言和产品"。

仅依赖 Python 3.11+ 标准库；持久化使用 SQLite（事件溯源 + 可重建读模型）。

## 它保证什么

- **归集方只能声明自己拥有的范围**：只有资产归集人能声明权利依据；地域/语言/产品/
  用途/禁发期/保留期限构成硬边界。
- **安全重传**：相同批次 + 内容指纹 + 授权指纹的重复提交回放既有事实，不产生重复事件。
- **争议隔离**：同一编号但正文或授权依据不同，资产进入隔离，禁止下游使用，待人工裁决。
- **职责分离**：脱敏复核与事实审校是两种职责，最新记录都通过且复核人不同才能批准。
- **材料改变不沿用旧批准**：申请携带材料指纹；变更必须作废旧申请，旧批准随即失效。
- **并发放行唯一决定**：写事务串行化配合部分唯一索引，跨机构并发批准只产生一个有效决定。
- **撤回最小影响**：只冻结失去全部有效授权的切片及其派生数据集（沿派生边传播）；
  已发布版本保留当时依据快照，并生成待履行处置义务。
- **到期与重启续跑**：授权到期作业 + 周期扫描持久化、带租约认领，服务重启后继续未完成的
  到期扫描和撤回传播。
- **双向反查**：从任一数据集清单可反查原文版本、处理记录、批准人和仍待履行的限制。

## 目录

- `contracts/domain.schema.json`：事件信封、聚合/事件类型与载荷约定。
- `src/corpus_clearance/domain.py`：枚举、错误、时间与指纹工具。
- `src/corpus_clearance/store.py`：SQLite 事件库、事务、投影重建。
- `src/corpus_clearance/projections.py`：读模型投影（与事件同事务更新）。
- `src/corpus_clearance/jobs.py`：持久化作业队列（到期扫描、撤回传播）。
- `src/corpus_clearance/services.py`：全部业务规则。
- `src/corpus_clearance/httpapi.py`：HTTP API 与后台 worker。
- `tests/`：契约、服务规则（含并发、撤回、重启恢复）与 HTTP 端到端测试。
- `docs/domain.md`：领域对象、事件与规则；`docs/api.md`：HTTP 接口手册。

## 快速开始

```bash
python3 -m corpus_clearance.httpapi --host 127.0.0.1 --port 8080 --db data/clearance.db
# 后台 worker 默认随服务启动（每 5 秒认领到期作业），可用 --no-worker 关闭
```

资格判定（核心问题）：

```bash
curl -G http://127.0.0.1:8080/v1/clearance \
  --data-urlencode 'slice_id=story-2:s1' \
  --data-urlencode 'date=2026-10-06' \
  --data-urlencode 'territory=CN' \
  --data-urlencode 'language=zh' \
  --data-urlencode 'product=train'
```

返回 `usable: true/false`、阻塞原因、生效决定与批准人、有效授权、被冻结数据集、
待履行处置义务与敏感标记。

从数据集反查血缘与义务：

```bash
curl http://127.0.0.1:8080/v1/datasets/ds-govqa/manifest
```

写接口建议携带 `Idempotency-Key` 头，重复提交原样回放首次响应；操作者通过
`X-Actor-Id` 头（或请求体字段）传递。

## 典型流程

1. `POST /v1/orgs` 登记机构。
2. `POST /v1/assets/ingest` 汇交报道（批次、归集人、内容指纹、授权指纹）。
3. `POST /v1/assets/{id}/rights` 声明权利依据（地域/语言/产品/用途/禁发期/保留期限）。
4. `POST /v1/assets/{id}/slices` 切分段落（可带敏感标记）。
5. `POST /v1/slices/{id}/reviews` 分别完成脱敏复核与事实审校（不同复核人）。
6. `POST /v1/requests` 提交用途申请（完整材料 → 材料指纹）。
7. `POST /v1/requests/{id}/decision` 放行或驳回。
8. `POST /v1/datasets`、`.../derivations` 登记数据集与派生关系；
   `POST /v1/slices/{id}/publications` 记录发布及当时依据。
9. `POST /v1/rights/{id}/withdraw` 撤回；后台作业冻结切片/派生数据集并生成处置义务。
10. `POST /v1/dispositions/{id}/fulfill` 履行处置义务。

## 测试 / 编译检查

```bash
python3 -m unittest discover -s tests
python3 -m compileall -q src tests
```

## 事件契约样例校验

```bash
PYTHONPATH=src python3 -m corpus_clearance.cli contracts/domain.schema.json data/sample.json
```

命令成功输出 `valid`；失败逐行输出字段、代码和中文说明并以非零状态结束。
