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
