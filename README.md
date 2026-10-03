# 车路云事件可信合成服务

施工占道期间，云控中心会同时收到路侧单元（RSU）、车辆和人工巡检的上报，
车道编号、发生时间、解除结论可能互相矛盾。本服务不采信"最后一条消息"，
而是**完整保留每个来源的观测、修订与撤回**，按来源能力、时间新鲜度与
交叉印证形成当前判断，并让**每次面向下游发布的路况快照都绑定所使用的
证据与规则版本**。

## 设计原则

| 需求 | 机制 |
| --- | --- |
| 完整保存观测/修订/撤回 | 仅追加事件日志（JSONL，可无配置直接落盘） |
| 来源能力 + 新鲜度 + 交叉印证 | 规则化加权仲裁，见下节 |
| 发布绑定证据与规则版本 | `TrafficSnapshot` 内嵌全部证据项（含被排除/撤回原因）与规则版本+参数指纹 |
| 迟到消息只影响未封存判断 | 解除结论封存发生片段（episode），迟到报文在接入时即被拒绝并入档 |
| 重复报文不重复触发发布 | `事件+来源类型+来源外部编号` 去重，重复报文无任何副作用 |
| 复发与旧消息重放区分 | 按观测时刻与上次解除时刻的间隔判定（默认 60 秒容忍带） |
| 按指定时点重演、不改写历史 | 在隔离引擎中重放日志；历史发布快照从日志原样读取 |
| 重启不丢状态 | 进程重启由日志重建判断；发布记录原样恢复、绝不重新发布 |

## 仲裁规则（`rules.py`，版本 `1.0.0`）

对未封存片段内每条有效证据计算：

```
权重 = 来源能力 × 报文可信度 × 0.5 ** (证据年龄 / 新鲜度半衰期)
```

- 来源能力：`rsu=0.90`、`inspector=0.85`、`vehicle=0.60`，未知来源保守取 `0.30`；
- 占道阵营（`lane_closed`/`obstacle`）与解除阵营（`lane_open`/`cleared`）
  分别汇总，每增加一个不同来源类型，阵营得分按 `cross_confirm_bonus=0.15`
  加成（最多计 2 个额外来源）；
- 胜方需达到发布门槛 `0.35` 且相对负方领先至少 15%，否则判 `uncertain`，
  不向下游下发占道/解除（已有 active 基线后转 uncertain 会发布，以阻止下游继续按占道行驶）；
- **权威解除**：能力 ≥ 0.80 的来源（路侧、人工）作出解除时，严格早于该
  解除时刻的占道观测视为已被恢复通行的事实取代（仍在快照中留痕，权重置零、
  标注原因），车辆来源的解除不具有此效力；
- 结论中的事件类型与车道按加权多数决定；
- 规则可按生效时间注册多个版本，重演时自动取当时生效版本；每次发布记录
  规则版本号与参数 SHA256 指纹。

## 发生片段、封存与复发

1. 第一条有效占道/障碍报文开启 episode 1；
2. 解除阵营胜出时，episode 封存（`sealed=true`），封存时刻之后接入的报文：
   - 观测时刻早于解除，或距解除不超过复发间隔 → **迟到/重放，排除并留痕**，不改判；
   - 观测时刻晚于解除且超过复发间隔（默认 60 秒）→ **复发**，开启 episode 2 并发布；
3. episode 2 进行中，观测时刻落在 episode 1 解除容忍带内的迟到报文同样被拒绝，
   不能污染新片段。

接入响应的 `classification`：`accepted` / `recurrence` / `duplicate` /
`excluded` / `withdrawn`。

## 模块结构

```
src/event_fusion/
  contracts.py   # 既有数据契约（EventObservation/SourceRef）
  clock.py       # 可注入时钟（SystemClock/ManualClock）
  sources.py     # 来源能力登记
  rules.py       # 版本化融合规则与参数指纹
  models.py      # Judgment / EvidenceItem / TrafficSnapshot
  store.py       # 仅追加事件日志（内存 + JSONL）
  engine.py      # 接入分类、封存、复发/重放识别、加权仲裁
  service.py     # 发布策略、撤回/修订、日志重建、时点重演对比
  api.py         # 标准库 HTTP API（零第三方依赖）
tests/test_fusion.py   # 18 个场景测试
run_cli.py             # 离线冒烟演示
```

## HTTP API

启动：`PYTHONPATH=src python3 -m event_fusion.api --port 8080 --log data/events.jsonl`

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/v1/events/<key>/observations` | 上报观测（支持可选 `received_at` 网关可信时间戳） |
| POST | `/v1/events/<key>/revisions` | 来源修订：撤回旧报文 + 修订版重新接入，两者均留痕 |
| POST | `/v1/events/<key>/withdrawals` | 撤回（可先于观测到达，到达时自动生效） |
| GET | `/v1/events/<key>/judgment` | 当前判断 + 全量证据明细 |
| GET | `/v1/events/<key>/publications` | 该事件不可变发布链 |
| GET | `/v1/snapshots/<publication_id>` | 按编号取原始发布快照 |
| GET | `/v1/replay?event_key=<key>&as_of=<ISO8601>` | 时点重演，返回当时判断、当前判断、字段差异、事后新增证据 |
| GET | `/v1/events` / `/health` | 事件清单 / 健康检查 |

上报示例：

```json
POST /v1/events/ROAD-A7/observations
{
  "kind": "lane_closed",
  "location_code": "lane-3",
  "confidence": 0.95,
  "received_at": "2026-10-21T02:01:00+00:00",
  "source": {
    "source_type": "rsu",
    "external_id": "rsu-3",
    "observed_at": "2026-10-21T02:01:00+00:00"
  }
}
```

发布策略：只有判断发生**实质变化**（状态、事件类型、车道、片段变化，
或置信度变化 ≥ 0.10）才发布新快照；快照通过 `supersedes` 串成链，
封存发布带 `sealed=true`。重复报文、被排除报文绝不触发发布。

重演响应中的 `publications_at_then` 直接取自截至该时点的不可变发布日志；
重演不产生任何新记录（`history_unchanged=true`）。

## 运行

```bash
python3 -m unittest discover -s tests -v      # 18 个场景测试
python3 -m compileall -q src tests run_cli.py # 编译检查
python3 run_cli.py                            # 离线端到端冒烟演示
```
