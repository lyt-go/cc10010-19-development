# 数据仓库ETL任务编排平台

面向数据仓库ETL任务开发、作业编排调度、任务依赖解析、源端数据校验、异常任务重跑、血缘分板与运行全景监控的ETL工程管理平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm install
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| ETL作业 | `etljob` | 作业定义 | 作业编号、作业名称、源端数据表 |
| 任务编排 | `dag` | 编排图 | 图编号、图名称、拓扑结构 |
| 依赖解析 | `dependency` | 依赖关系 | 关系编号、上游作业、下游作业 |
| 源端校验 | `sourcecheck` | 校验配置 | 配置编号、源端数据表、行数下限 |
| 运行实例 | `runtime` | 运行记录 | 记录编号、作业名称、开始时间 |
| 异常重跑 | `rerun` | 重跑任务 | 任务编号、原始运行编号、重跑区间开始 |
| 基线管理 | `baseline` | 基线定义 | 基线编号、基线名称、承诺完成时间 |
| 分区检测 | `partitioncheck` | 检测规则 | 规则编号、数据表名、期望分区数 |
| 分区补齐 | `partitionbackfill` | 检测批次/补齐任务/处理记录/重跑清单 | 检测批次号、补齐任务编号、分区标识、优先级、完整性结论 |
| 数据比对 | `comparator` | 比对任务 | 任务编号、源表名、目标表名 |
| 元数据存储 | `metastore` | 元数据表 | 表编号、表名、所属数仓层 |
| 资源配置 | `resource` | 资源池 | 池编号、池名称、最大并发数 |
| SLA监控 | `slavalidate` | SLA规则 | 规则编号、适用作业、完成截止时间 |
| SQL模板 | `sqltpl` | 模板定义 | 模板编号、模板名称、模板类型 |
| 数据备份 | `backuprestore` | 备份策略 | 策略编号、数据表名、备份周期 |
| 日志归档 | `logarchive` | 归档策略 | 策略编号、日志类型、保留天数 |
| 通知渠道 | `notifychan` | 渠道配置 | 渠道编号、渠道类型、接收地址 |
| 发布计划 | `publishplan` | 发布单 | 发布编号、发布内容、变更说明 |

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 分区补齐受控交接

分区检测只负责产出缺失分区清单，补齐执行由 `partitionbackfill` 模块承接：

- **检测批次与快照**：每次检测生成带校验和的缺失分区清单快照；交接前重算校验和，
  快照被篡改直接拒绝（`POST /api/partitionbackfill/detections`、`.../detections/{id}/handoff`）。
- **一份任务**：同一数据表分区在未补齐期间只存在一份补齐任务，重复检测复用并挂接新批次；
  补齐后再次缺失则重开原任务，不产生第二份。
- **冲突仲裁**：自动补齐与人工处理结果都留痕；人工优先，同来源较晚到达的记录覆盖较早记录，
  被采纳的记录决定分区完整性结论（`POST .../tasks/{id}/results`，处理记录见 `.../results`）。
- **原子提交**：补齐完成与完整性结论在同一事务内提交，中途异常整体回滚
  （`services/partitionbackfill.transaction`）。
- **状态分列**：补齐任务状态（待交接/待补齐/补齐中/已补齐/补齐失败）与运行实例分区状态
  （未下发/运行实例处理中/已完成/失败）分字段存储，互不覆盖。
- **任务文件**：补齐任务可下载 CSV（含分区标识与优先级），重新下载与查询结果保持完整一致
  （`GET .../tasks/{id}/download`、`.../tasks-file/bundle`）。
- **异常重跑对接**：重跑读取补齐任务数据快照且只消费仍未补齐的分区，已补齐分区自动跳过；
  业务结果落到重跑清单并随分区检测重算，成功分区原子提交完整性结论
  （`/api/rerun/partition-snapshot/consume`、`.../partition-rerun/items/{编号}/results`）。
- 端到端场景校验：`cd backend && .venv/bin/python scenario_check.py`。

领域：数据工程
子方向：数仓与数据湖
