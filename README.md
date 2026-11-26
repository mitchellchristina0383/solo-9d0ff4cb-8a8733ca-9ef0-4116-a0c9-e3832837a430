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
| POST | `/builds/{id}/stacks` | 按构建号采样各成员**主线程** Python 调用栈 |

### 调用栈采样（`POST /builds/{id}/stacks`）

值班员排查“构建还在跑、进度不动”时按构建号抓调用栈。请求体
`{"timeout": 5.0}` 给出调用方可接受的**最长等待时间**（0–30 秒）。

- **仅内置样例可采**：信号处理器只装在本服务启动的 `samples/worker.py`
  进程里，靠环境变量约定私有栈目录与信号号，不面向任意外部进程。
- **成员名单以受理时刻为准**：受理时快照 `/proc` 中该进程组的存活成员
  （`group_pids`，含僵尸判定）。主进程先退时它已不在名单里，仍在跑的
  同组子进程照常被采；受理后新出现的进程一律不采。
- **按 PID 返回各成员主线程栈**，每个成员的 `status` 取值：
  - `ok`：已拿到，`stack` 为主线程调用栈文本；
  - `exited`：采样窗口内中途退出（或变僵尸被回收），`detail` 说明情形；
  - `timeout`：到期仍存活但未回应，`stack` 为 `null`；
  - `error`：信号/文件异常的兜底（`detail` 附原因）。
  已拿到的栈无论其他成员结局如何都照常返回。
- **并发采样互不串单**：通知用**可排队的实时信号** `SIGRTMIN+2`（不合并、
  按序投递）。每次采样为每个 PID 预留一个递增序号，worker 把第 N 次转储
  原子写到 `stack-p<PID>-s<N>.txt`；第 N 次请求只认第 N 号文件。某次请求
  超时后才落盘的迟到回应仍带旧序号，后续请求预留的是新序号，因此**迟到
  回应不可能计入任何其他请求**；每份回应带独立 `sample_id`，只归触发它的
  那次采样。
- **采样不影响其他功能**：采样在独立请求线程内轮询等待，日志泵照常采集、
  取消流程照常进行；构建进入终态后采样返回 409，未知构建号返回 404。
- 可延迟回应的样例：`POST /builds` 带 `stack_delay`（秒），worker 的栈
  处理器会先睡这么久再写转储，用于在 `/docs` 复现迟到回应、超时与并发隔离。

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
  `teardown error: cleanup step reported failure code 7`；
- `--stack-delay N`：栈采样处理器先睡 N 秒再写转储，制造迟到/超时回应，
  供 `/docs` 上演练并发采样隔离（由启动请求的 `stack_delay` 字段透传，
  主进程派生的子进程会继承该参数）。

worker 启动时若收到栈目录环境变量 `BUILDSVC_STACK_DIR`，会安装实时信号
`SIGRTMIN+2`（信号号可用 `BUILDSVC_STACK_SIGNAL` 覆盖）的处理器：信号在
**主线程**执行，用 `traceback.format_stack` 抓被中断的主线程调用栈，按
PID 内递增序号原子替换写文件（先写临时文件再 `os.replace`，读者不会看到
半截内容）。

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
栈采样另有 `tests/test_stacks.py`：采样器对真实 RT 信号子进程的
ok/timeout/中途退出/序号独立性/迟到转储不串单单元测试，以及 API 级的
正常抓栈、主进程先退后只采存活子进程、可延迟样例下并发采样隔离、
采样期间取消、404/409/422 路径。

## 实现与自测记录

### 实际实现要点

- 单 FastAPI 进程、uvicorn 单 worker；每个构建一个独立进程组 + 一个监管线程
  和两个日志泵线程，状态全部在内存。
- `PR_SET_CHILD_SUBREAPER` + 全局 reaper 线程 `waitpid(-1, WNOHANG)` 统一回收，
  解决多构建间退出码归属与主进程先退后的孤儿回收。
- 存活判定走 `/proc/<pid>/stat` 的 pgrp 字段，按组而非主进程 PID 判断。
- 非阻塞 `select`/`os.read` 日志泵 + 32 KiB 环形留尾（按字节保留最新内容）。
- 整组 `READY` 握手保证取消信号不会落在未装好 handler 的进程上。
- 调用栈采样（`app/stacks.py` + worker 信号处理器）：
  - 通知走可排队实时信号 `SIGRTMIN+2`，并发采样不合并、按序投递；
  - 每构建私有栈目录（服务启动生成临时根目录，构建终态后删子目录，
    服务关停删根目录），worker 经环境变量获知目录与信号号；
  - 采样器持有 per-PID 递增序号，转储文件名带序号且原子替换；请求只认
    自己预留的序号文件，迟到转储带旧序号，天然无法计入后续请求；
  - 受理时快照组成员名单，轮询区分 `ok/exited/timeout/error`，
    僵尸（`/proc` 状态 `Z`）按中途退出处理；
  - 采样在请求线程内进行，不触碰日志泵与取消状态机。

### 自测结果（本机 Linux 6.8 / Python 3.11）

- `pytest`：28 项全部通过（原 17 项 + 栈采样 11 项），多轮稳定；每次结束后
  `pgrep` 确认无残留 worker 进程。
- 真实 uvicorn + curl 端到端（`/docs` 与 `/openapi.json` 均 200，新端点与
  `StackSampleRequest` schema 已注册）：
  - 可延迟样例（`stack_delay=1.0`）并发采样：0.3s 短请求返回
    `timeout` 且 `stack=null`（seq=1），并发的 5s 长请求返回 `ok`（seq=2）；
    迟到的 seq=1 转储落盘后，第三次采样只认 seq=3 并成功，证明迟到回应
    不串任何其他请求；
  - 主进程先退：受理时名单只剩孤儿子进程（PID ≠ 组 ID），返回其主线程栈
    且含 `run_long_lived`，构建保持 running；
  - 采样期间取消：采样请求正常返回（`ok`），构建走到 `cancelled`，stdout 的
    收信号行与 stderr 的 `teardown error` 均照常采集；
  - 终态构建采样 409、未知构建号 404、`timeout` 超上限 422；
  - 构建终态后其栈子目录被清理、无 worker 残留；服务 SIGTERM 关停时栈根目录
    一并删除，进程干净退出。

### 余项 / 边界

- 本环境未安装 Docker，`Dockerfile` 与 `docker-compose.yml` 未实际
  `docker compose up`（沿用既有配置，无需改动：栈文件位于容器内 `/tmp`，
  信号/进程组均在同一容器命名空间）；镜像基于 `python:3.11-slim`，
  依赖固定在 `requirements.txt`，逻辑依赖 Linux 的 subreaper/`/proc`/进程组/
  实时信号。
- 状态在内存，服务重启后历史构建记录不保留（符合单实例定位；如需持久化要
  另加存储）。
- 就绪握手超时（默认 15s）后若进程仍存活，会带 note 继续执行取消，避免卡死。
- 采样仅对内置样例 worker 生效：处理器与栈目录均由本服务经环境变量装配，
  未装配的进程不会响应采样信号；服务端信号失败按成员级 `error` 兜底。
- 实时信号为 Linux 特性（`SIGRTMIN+2`，避开 glibc 保留的低编号 RT 信号）。

