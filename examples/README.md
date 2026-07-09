# Examples

## 1) 启动服务端

```bash
./examples/start_server.sh
```

可选环境变量：

- `HOST` (默认 `127.0.0.1`)
- `PORT` (默认 `8000`)
- `DB_URL` (默认 `sqlite:///<项目绝对路径>/alocals3.db`)
- `STORAGE_ROOT` (默认 `<项目绝对路径>/data`)
- `SERVER_BIN` (默认 `<项目绝对路径>/target/release/alocals3-server`，不存在时自动 release 构建)
- `LOG_LEVEL` (默认 `info`)

## 2) 客户端基础流程（插入、查询、下载、删除）

```bash
./examples/client_flow.sh
```

可选环境变量：

- `ENDPOINT` (默认 `http://127.0.0.1:8000`)
- `BUCKET` (默认 `demo-bucket`)
- `KEY` (默认 `docs/hello.txt`)
- `WORK_DIR` (默认 `./examples/.tmp`)

## 3) 缓存 304 演示

```bash
./examples/cache_304_demo.sh
```

该脚本会自动：

- 上传对象
- 读取 `ETag` 和 `Last-Modified`
- 通过 `If-None-Match` 和 `If-Modified-Since` 发起条件请求
- 打印返回状态码（预期 `304`）

## 4) 并发一致性 Benchmark

```bash
./examples/benchmark_consistency.sh
```

可选环境变量：

- `ENDPOINT` (默认 `http://127.0.0.1:8000`)
- `DURATION` (默认 `20`)
- `WRITERS` (默认 `4`)
- `READERS` (默认 `4`)
- `DELETERS` (默认 `2`)
- `PAYLOAD_SIZE` (默认 `2048`)

## 5) 压力测试 Benchmark

```bash
./examples/benchmark_stress.sh
```

可选环境变量：

- `ENDPOINT` (默认 `http://127.0.0.1:8000`)
- `DURATION` (默认 `30`)
- `CONCURRENCY` (默认 `50`)
- `KEY_SPACE` (默认 `1000`)
- `OBJECT_SIZE` (默认 `4096`)
- `WRITE_RATIO` (默认 `0.5`)

## 6) GC 正确性测试

```bash
./examples/gc_correctness.sh
```

该脚本会自动构建/使用 `target/debug/alocals3-server`，启动多个临时本机服务端实例，并在 `/tmp` 下创建隔离的 SQLite 数据库和对象目录。脚本结束后默认清理临时目录。

覆盖场景：

- 后台 GC 清理 DB 无引用的 orphan 文件
- 后台 GC 清理 `.tmp` 临时文件
- 后台 GC 清理 DB 有记录但文件已不存在的记录
- 活跃对象不会被误删，且 `GET` 内容正确
- 多个 key 共享同一 content-addressed 文件时，删除其中一个 key 不会误删仍被引用的文件
- `--gc-grace-secs` 会保留年轻 orphan 文件和年轻 missing-file 记录
- `--disable-gc` 会禁用运行时自动 GC
- `ALOCALS3_GC_INTERVAL_SECS=0` 与禁用自动 GC 等效
- 旧 orphan content path 被新 PUT 复用时，GC 不会基于旧快照误删新对象文件

可选环境变量：

- `SERVER_BIN` (默认 `target/debug/alocals3-server`)
- `START_PORT` (默认 `18111`，脚本会使用连续 5 个端口)
- `BASE_DIR` (默认 `/tmp/alocals3-gc-correctness.XXXXXX`)
- `KEEP_BASE_DIR=1` 保留临时目录用于排查

最近一次本机验证结果：

```text
GC correctness and race scenarios passed
orphan_files_deleted=1 missing_records_deleted=1 stale_tmp_files_deleted=1
remaining_objects: grace=1 disable_flag=1 interval_zero=1 race=30
```

## 7) Python file-like API 大文件 Benchmark

```bash
./examples/benchmark_file_like.sh
```

该脚本使用 `LocalS3Client.open(url, "wb")` 写入大文件，再用 `LocalS3Client.open(url, "rb")` 流式读取并校验 SHA-256。它用于验证 Python file-like API 的吞吐和大文件低内存路径。

运行前需要：

- 已启动本机服务端
- 当前 Python 环境已安装本包及 native 扩展（例如先在你的 Python 3.12 环境里执行 `python -m pip install -e .`）

可选环境变量：

- `ENDPOINT` (默认 `http://127.0.0.1:8000`)
- `BUCKET` (默认 `bench-file-like`)
- `KEY` (默认 `large/file-like.bin`)
- `SIZE_MIB` (默认 `1024`)
- `CHUNK_MIB` (默认 `8`)
- `REPEATS` (默认 `1`)
- `TIMEOUT` (默认 `300`)
- `DISABLE_PROXY=1` 传递 `--disable-proxy`
- `KEEP_OBJECT=1` 保留 benchmark 对象

也可以直接运行 Python 脚本：

```bash
python examples/bench_file_like.py --endpoint http://127.0.0.1:8000 --size-mib 1024 --chunk-mib 8
```

最近一次本机验证使用 Python 3.12 conda 环境和 release server，测试 512 MiB 对象：

```text
write: 512 MiB in 4.428074s, 115.63 MiB/s
read:  512 MiB in 0.845939s, 605.24 MiB/s
sha256 verification: ok
```

## 8) Python file-like API 多路复用和断点续读验证

```bash
./examples/benchmark_stream_multiplex.sh
```

该脚本使用 `LocalS3ClientAsync` 和 file-like `open()` 验证：

- 一个 reader 打开并长时间 idle 后仍可继续读取
- reader idle 期间，同一个客户端仍可持续执行 `health()`
- 一个 writer 打开并长时间 idle 时不会占用服务端 TCP 会话
- writer `close()` 上传期间，服务端仍可响应其他请求
- 通过 `range_header="bytes=N-"` 重新打开 reader 可以完成断点续读，并用 SHA-256 校验结果

可选环境变量：

- `ENDPOINT` (默认 `http://127.0.0.1:8000`)
- `BUCKET` (默认 `bench-stream-multiplex`)
- `SIZE_MIB` (默认 `256`)
- `CHUNK_MIB` (默认 `8`)
- `RESUME_AFTER_MIB` (默认 `64`)
- `IDLE_SECS` (默认 `12`)
- `TIMEOUT` (默认 `2`)
- `DISABLE_PROXY=1` 传递 `--disable-proxy`
- `KEEP_OBJECTS=1` 保留测试对象

最近一次本机验证使用 Python 3.12 conda 环境和 release server，测试 128 MiB 对象，`timeout=2s`、`idle=5s`：

```text
idle_reader: health failures=0, p95=5.400 ms, reader continued after idle
range_resume: content-range=bytes 33554432-134217727/134217728, sha256 ok
idle_writer: health failures=0, p95=5.050 ms
writer_close_upload: health failures=0, p95=3.956 ms
```

当前支持断点续读（Range GET）。写入侧 file-like API 当前是 Rust 临时文件 spool 后单次 PUT；如果上传 TCP 会话在 `close()` 期间中断，需要重新 PUT。断点续写/断点续传上传需要额外的 multipart 或 upload-session 协议。
