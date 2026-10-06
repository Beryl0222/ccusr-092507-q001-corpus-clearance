# 自贸港语料用途放行库

在首轮央地汇交中，同一篇报道可能同时含有通讯社来源、地方补采、尚在禁发期的政策附件和只能用于中文传播的人物授权。本服务把**来源机构、内容版本、段落切片、权利依据、地域语言、保留期限、敏感标记、派生数据集和使用申请**串成可追踪关系，避免训练集、政务问答、国际传播包拿到彼此不相容的权利。

## 核心规则

- **归集范围限定**：只有 collector 机构能归集；机构只能为自己归集的语料版本登记权利依据；只有授权方本人能撤回。
- **幂等与争议隔离**：相同批次 + 内容指纹安全重传；编号相同而正文或授权指纹不同 → 争议版本隔离，现行版本不受影响。
- **职责分离**：脱敏（desensitize）与事实审校（fact_check）必须由不同人员完成，两项通过才可放行。
- **材料定版**：申请材料经规范化指纹化；材料改变后旧批准立即作废，必须重新申请与审查。
- **唯一有效决定**：跨机构并发放行在单个立即写事务内收敛，同一申请只产生一个有效决定。
- **最小撤回**：撤回只冻结挂接该授权的切片及其派生数据集；已发布版本永久保留发布时依据，并生成幂等的处置义务（清除或通知）。
- **到期与重启**：保留期扫描与撤回传播是持久作业，处理器幂等；服务重启后重放投影、回收中断租约、续跑未完成作业。

## 目录

- `contracts/domain.schema.json`：事件信封、聚合类型（source_org / source_asset / corpus_slice / rights_grant / use_request / release_decision / derived_dataset / obligation）与事件载荷约定。
- `docs/domain.md`：领域对象、事件语义与不变量。
- `docs/api.md`：HTTP API 说明。
- `src/corpus_clearance/`
  - `contracts.py`：交换契约校验；`store.py`：SQLite 事件库、投影、持久作业队与命令幂等；
  - `projector.py`：事件 → 读模型；`service.py`：领域服务；`jobs.py`：到期扫描/撤回传播处理器与调度线程；
  - `httpapi.py` / `serve.py`：HTTP 服务；`cli.py`：事件契约命令行校验。
- `tests/`：契约、领域规则（含并发）与真实 HTTP 端到端、跨进程重启续跑测试。
- `data/sample.json`：可直接校验的联调样例。

## 快速开始

```bash
# 启动服务（SQLite 持久化，自带后台到期扫描）
PYTHONPATH=src python3 -m corpus_clearance.serve --db corpus.db --port 8080
```

典型链路：

```bash
# 1. 登记机构 → 2. 汇交语料（携带来源与授权声明）
curl -s -X POST localhost:8080/v1/orgs -H 'X-Org-Id: news' -H 'Content-Type: application/json' \
  -d '{"org_id":"news","name":"通讯社","roles":["collector"]}'
curl -s -X POST localhost:8080/v1/assets/ingest -H 'X-Org-Id: news' -H 'Content-Type: application/json' \
  -d '{"asset_id":"art-1","contributor_org_id":"news","source_id":"x-1","batch_id":"b1","content_hash":"h1",
       "declared_grants":[{"grant_id":"g1","territories":["CN"],"languages":["zh"],
       "products":["qa","training"],"retain_until":"2027-06-01T00:00:00+08:00"}]}'
# 3. 切片 → 4. 两名不同人员分别脱敏/事实审校 → 5. 申请 → 6. 放行 → 7. 发布数据集
# 8. 授权方可撤回；授权人员随时可判定资格、反查血缘
curl -s 'localhost:8080/v1/eligibility?slice_id=slice_xxx&date=2026-12-02&territory=CN&language=zh&product=qa'
curl -s localhost:8080/v1/datasets/ds-qa/lineage
```

完整请求字段见 `docs/api.md`。

## 测试

```bash
python3 -m unittest discover -s tests
```

## 编译检查

```bash
python3 -m compileall -q src tests
```

## 样例校验

```bash
PYTHONPATH=src python3 -m corpus_clearance.cli contracts/domain.schema.json data/sample.json
```

命令成功时输出 `valid`；校验失败时逐行输出字段、代码和中文说明，并以非零状态结束。
