# Build Cancellation Service

单实例 FastAPI 服务：以**进程组**方式运行样例“构建”，提供启动、取消、
状态查询和日志（stdout/stderr 分开留尾，每路最多 32 KiB）接口。

## 设计要点

- **单实例**：构建状态保存在进程内存中，每个构建是一个独立进程组
  （`start_new_session=True`，组 ID 等于主进程 PID）。因此 uvicorn 固定
  单 worker，不能横向扩容（多实例间看不到彼此的子进程）。
- **统一回收**：服务启动时通过 `PR_SET_CHILD_SUBREAPER` 把自己设为 subreaper，
  主进程先退出的同组子进程会被 reparent 到本服务；一个全局 reaper 线程用
  `waitpid(-1, WNOHANG)` 轮询回收所有僵尸并记录退出状态，避免各构建互相
  “偷走”对方子进程的退出码。
- **取消流程（每个构建只执行一次，重复取消共享同一条收尾记录）**：
  1. 取消前等待整组 `READY` 握手：每个进程在装好 SIGTERM handler 后才打印
     `READY`，服务端按当前组成员数核对 READY 计数，消除“信号早于 handler
     安装、被默认处置直接打死”的启动竞态（主进程先退时也覆盖孤儿子进程）；
  2. `killpg(SIGTERM)` 通知**整组**收尾；
  3. 轮询 `/proc` 检查组内是否还有 PID（僵尸也算存活，直到被回收）；
  4. 宽限期到期仍有存活进程 → `killpg(SIGKILL)` 强制终止整组，并在状态里
     标记 `forced=true`；
  5. 整组退出 **且** stdout/stderr 两个管道都 EOF（日志泵线程结束）后，
     才把状态置为 `cancelled`，确保收尾输出（含报错）全部读入。
- **日志读取方式**：管道 fd 设为非阻塞，用 `select` + 原始 `os.read` 排空。
  不能用 Python 的 `BufferedReader.read()`——它对管道做内部预读，会请求比
  当前可用字节更多的数据并阻塞在 `pipe_read` 直到 EOF，表现为进程存活期间
  读线程一直拿不到数据。两个独立 `TailBuffer` 分别只保留各自最新 32 KiB，
  返回 `retained_bytes / total_bytes / truncated`，两路内容绝不串流；等待
  退出期间泵持续读取，“收尾时大量输出”也能完整留尾。
- **主进程先退**：监管器按进程组而非主进程 PID 判断存活，主进程退出后
  继续跟踪同组其余进程，取消信号仍发给整组。
- **其他构建不受影响**：信号按进程组隔离；全局 reaper 统一回收，互不干扰。
- **日志**：两个独立 `TailBuffer`，分别只保留各自最新 32 KiB，
  返回 `retained_bytes / total_bytes / truncated`，两路内容绝不串流。
  等待退出期间日志泵持续读取，因此“收尾时大量输出”也能完整留尾。

## API（交互文档见 `/docs`）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 健康检查、构建计数 |
| POST | `/builds` | 启动样例构建 |
| GET | `/builds` | 列出全部构建 |
| GET | `/builds/{id}` | 查询单个构建状态 |
| POST | `/builds/{id}/cancel` | 取消（幂等，可带 `grace_seconds`） |
| GET | `/builds/{id}/logs?stream=all|stdout|stderr` | 日志留尾 |
| POST | `/builds/{id}/wait?timeout=` | 等待构建结束 |

### 启动请求示例

```json
{
  "scenarios": ["main-exits-early", "ignore-term", "noisy-teardown"],
  "duration": 60.0,
  "grace_seconds": 5.0,
  "build_id": "demo-1"
}
```

`scenarios` 可任意组合（见下）。状态字段 `cancellation.forced` 标明本次取消
是否动用了 SIGKILL；`leader_exit` 给出主进程退出码或致死信号。

## 样例构建（`samples/worker.py`，仅用标准库）

- `--main-exits-early`：主进程派生同组长生命周期子进程后立即退出；
- `--ignore-term`：收到 SIGTERM 只记录不退出，逼服务在宽限后 SIGKILL；
- `--noisy-teardown`：收到 SIGTERM 后在 stdout、stderr 各打印约 250 KiB
  （远超 32 KiB），并在 stderr 输出收尾错误
  `teardown error: cleanup step reported failure code 7`。

## 运行

### Docker Compose（要求的交付方式）

```bash
docker compose up --build
# 打开 http://localhost:8000/docs
```

### 本地直接运行

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 1
```

## 测试

```bash
pip install -r requirements.txt httpx pytest
pytest -q
```

测试用 TestClient 在进程内启动真实样例子进程树，覆盖：正常成功、宽限内
优雅取消、宽限后强制 SIGKILL、主进程先退、主进程先退叠加拒绝收尾、
大量输出下双路 32 KiB 留尾、重复取消沿用首次进度、并行构建隔离、
404/409/422 错误路径、结束后再取消；另有留尾缓冲与 /proc 组检测的单元测试。

## 实现与自测记录

### 实际实现要点

- 单 FastAPI 进程、uvicorn 单 worker；每个构建一个独立进程组 + 一个监管线程
  和两个日志泵线程，状态全部在内存。
- `PR_SET_CHILD_SUBREAPER` + 全局 reaper 线程 `waitpid(-1, WNOHANG)` 统一回收，
  解决多构建间退出码归属与主进程先退后的孤儿回收。
- 存活判定走 `/proc/<pid>/stat` 的 pgrp 字段，按组而非主进程 PID 判断。
- 非阻塞 `select`/`os.read` 日志泵 + 32 KiB 环形留尾（按字节保留最新内容）。
- 整组 `READY` 握手保证取消信号不会落在未装好 handler 的进程上。

### 自测结果（本机 Linux 6.8 / Python 3.11）

- `pytest`：17 项全部通过，连续 5 轮稳定，单轮约 8 秒；每次结束后
  `pgrep` 确认无残留 worker 进程。
- 真实 uvicorn + curl 端到端：`/docs`、`/openapi.json` 返回 200；
  优雅取消 `forced=false` 且 stderr 留尾含收尾报错；`ignore-term` 宽限后
  `forced=true`、主进程退出原因为 SIGKILL；并行构建取消其一时另一构建保持
  running；重复取消第二次返回 `reused_existing_cancellation=true` 且沿用首次
  宽限与时间戳；`noisy-teardown` 两路各保留恰好 32768 字节、`truncated=true`
  且两路不串流；主进程先退后孤儿子进程的 heartbeat 仍被采集。
- 服务 SIGTERM 关停：lifespan 对在途构建执行收尾，ignore-term 构建在宽限后
  被强制终止，进程与服务均干净退出，无残留。

### 余项 / 边界

- 本环境未安装 Docker，`Dockerfile` 与 `docker-compose.yml` 仅做了 YAML 与
  语法静态校验，未实际 `docker compose up`；镜像基于 `python:3.11-slim`，
  依赖固定在 `requirements.txt`，逻辑依赖 Linux 的 subreaper/`/proc`/进程组。
- 状态在内存，服务重启后历史构建记录不保留（符合单实例定位；如需持久化要
  另加存储）。
- 就绪握手超时（默认 15s）后若进程仍存活，会带 note 继续执行取消，避免卡死。

