# 车路云事件可信合成

城市道路云控中心在施工占道等场景中会同时收到路侧单元（RSU）、联网车辆和人工巡检的
事件上报，车道编号、发生时间与解除结论可能互相矛盾。本服务把每一份原始报文作为证据
完整留存，按**来源能力 × 时间新鲜度 × 交叉印证**合成当前判断，并让每次面向下游发布的
路况快照绑定所使用的证据清单与规则版本。

## 设计原则

1. **只增不改**：报文日志（journal）与快照账本（ledger）均只追加。修订、撤回、重复送达、
   迟到报文都会留下记录，历史发布永远不可改写。
2. **证据可解释**：每条快照列出被采信证据（来源、观测时刻、内在权重、新鲜度衰减后权重）
   与被排除证据（重复 / 被修订 / 被撤回 / 旧消息重放 / 越权操作 / 同源立场更新）。
3. **规则版本化**：合成规则（`fusion-rules-v1`）带版本号并写入快照；新版本只能登记、
   不能覆盖旧版本，重演时可指定规则版本对照结论差异。
4. **封存冻结**：事件片段封存后，迟到证据只能进日志，不能改变或重新发布该片段；
   复发形成的新片段可独立封存。
5. **重演不落账**：按任意历史时点重演只读取日志，返回“当时判断 vs 事后新增证据”的差异，
   绝不写入快照账本。

## 合成规则（fusion-rules-v1）

| 维度 | 规则 |
| --- | --- |
| 来源能力 | RSU 0.90、人工巡检 0.85、车辆 0.65（未知来源 0.30） |
| 时间新鲜度 | 60 秒内系数 1.0，之后线性衰减，600 秒归零 |
| 证据权重 | 来源能力 × 上报置信 × 新鲜度系数 |
| 立场模型 | 同一来源只保留观测时刻最新的立场；来源翻证（先报占道后报解除）自动对冲旧立场 |
| 交叉印证 | 每多一个独立来源 +0.15，封顶 1.0 |
| 单源限速 | 仅一个来源时综合置信不超过 0.75，防止单车误报直接下发 |
| 活跃阈值 | 正侧综合置信 ≥ 0.5 判为 active |
| 解除裕度 | 反侧来源加权立场净胜正侧 ≥ 0.12 才关闭片段（cleared） |
| 复发窗口 | 解除后 30 秒内到达的占道观测视为**旧消息重放**；之后到达视为**复发**，开启 E2 |
| 车道冲突 | 按新鲜+能力加权在独立来源间投票，败选车道列入 `lane_conflicts` |

## 模块结构

```
src/event_fusion/
  contracts.py     数据契约：观测、报文（observe/amend/withdraw）、结论枚举
  rules.py         版本化规则集与登记处
  journal.py       只增报文日志：逐字留存，重复报文记 duplicate_of
  engine.py        修订/撤回链、立场状态机、片段切分、复发/重放、指纹
  snapshots.py     快照账本：证据+规则版本绑定，SHA-256 哈希链
  service.py       编排：摄入幂等、发布去重、封存、时点重演对比
  serialization.py JSON 序列化
  api.py           标准库 HTTP API（无第三方依赖）
```

## HTTP API

启动：`PYTHONPATH=src python -m event_fusion.api --host 127.0.0.1 --port 8080`

| 方法与路径 | 说明 |
| --- | --- |
| `POST /reports` | 摄入报文；重复 `report_id` 返回 `duplicated=true` 且不发布 |
| `POST /publish` | `{"as_of": "..."}` 手动触发合成，仅发布结论有变化的事件 |
| `POST /events/{key}/seal` | `{"at": "..."}` 封存当前片段并生成封存快照 |
| `GET /events/{key}/judgment?as_of=...` | 查询某时点判断（只读，不落账） |
| `GET /events/{key}/snapshots` | 事件的历史发布序列 |
| `GET /snapshots/{id}` | 单条快照（含证据编号、内容哈希、前序哈希） |
| `POST /replay` | `{"as_of": "...", "rules_version"?: "..."}` 重演并对比事后证据 |
| `GET /journal` | 只增报文日志（含重复与迟到报文） |
| `GET /seals` | 封存记录 |
| `GET /verify` | 重算快照哈希链，判断是否被篡改 |

报文示例：

```json
{
  "report_id": "rsu-1001",
  "action": "observe",
  "source": {"source_type": "rsu", "external_id": "rsu-7",
             "observed_at": "2026-10-21T02:00:00+00:00"},
  "received_at": "2026-10-21T02:00:30+00:00",
  "observation": {
    "event_key": "EV-CONSTR-7",
    "kind": "lane_closed",
    "location_code": "lane-3",
    "source": {"source_type": "rsu", "external_id": "rsu-7",
               "observed_at": "2026-10-21T02:00:00+00:00"},
    "confidence": 0.95
  }
}
```

撤回 / 修订使用 `"action": "withdraw" | "amend"` 与 `"target_report_id"`，
且只允许来源操作**自己**发出的报文。

## 离线演示

`python run_cli.py` 演示完整施工占道时间线：车辆单源先发 lane-2 → RSU 与巡检迟到后
交叉印证纠正为 lane-3 → 重复报文幂等 → 封存 → 封存后迟到报文不改判 → 解除后复发开启
E2 → 在 02:00:15 重演，对比当时误判与事后证据，且哈希链始终有效。

## 运行测试

```bash
python -m unittest discover -s tests -v   # 27 项：契约/引擎/服务/HTTP 端到端
python -m compileall -q src tests run_cli.py
```
