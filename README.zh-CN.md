# alocals3

`alocals3` 是一个面向本地开发和内部场景的 S3 风格对象存储。

当前 `main` 分支是 Rust 优先版本：

- Server：纯 Rust 二进制，不需要 Python 运行时。
- 元数据后端：SQLite 或 PostgreSQL。
- 对象内容：本地文件系统，按 SHA-256 分片落盘。
- Python client：wheel 包，底层 HTTP 网络由 Rust `reqwest` 实现。
- Python 目标版本：Python 3.12+，PyO3 使用 `abi3-py312` limited API。

项目实现的是 S3 兼容子集，不是完整 AWS S3 API。

## 快速启动

构建并运行 Rust server：

```bash
PYO3_NO_PYTHON=1 cargo build --release --no-default-features --features server,server-binary --bin alocals3-server

target/release/alocals3-server \
  --host 127.0.0.1 \
  --port 8000 \
  --database-url "sqlite:///./alocals3.db" \
  --storage-root ./data
```

后台垃圾回收默认开启，会定期清理数据库不再引用的对象文件，以及文件已经不存在的数据库记录。候选项必须超过 GC grace period 才会被删除。

```bash
target/release/alocals3-server \
  --gc-interval-secs 300 \
  --gc-grace-secs 300 \
  --gc-start-delay-secs 30
```

设置 `--disable-gc` 可以关闭后台 GC。`--gc-interval-secs 0` 和 `ALOCALS3_GC_INTERVAL_SECS=0` 与它等效。

运行可复现的 GC 正确性测试：

```bash
./examples/gc_correctness.sh
```

该测试覆盖 orphan 文件、missing-file 记录、过期临时文件、活跃/共享对象保留、grace period、GC 禁用方式，以及 orphan content path 被新 PUT 复用时的竞态。最近一次本机验证结果：

```text
GC correctness and race scenarios passed
orphan_files_deleted=1 missing_records_deleted=1 stale_tmp_files_deleted=1
remaining_objects: grace=1 disable_flag=1 interval_zero=1 race=30
```

使用 PostgreSQL：

```bash
target/release/alocals3-server \
  --host 127.0.0.1 \
  --port 8000 \
  --database-url "postgresql://user:password@127.0.0.1:5432/alocals3" \
  --storage-root ./data
```

从源码安装 Python client：

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip maturin
python -m pip install -e .
```

## 构建产物

发布构建脚本在 [scripts](scripts/README.md)：

```bash
# Linux 静态 server + Python 3.12+ ABI3 wheel
scripts/build-linux-release.sh

# macOS arm64，macOS 11 ABI 基线
scripts/build-macos-release.sh

# Windows 10+ PowerShell
.\scripts\build-windows-release.ps1
```

Linux server 默认目标是 `x86_64-unknown-linux-musl`；Linux wheel 默认目标 tag 是 `manylinux_2_28`。macOS 默认目标是 `aarch64-apple-darwin`，并设置 `MACOSX_DEPLOYMENT_TARGET=11.0`。

wheel 现在配置为 Python 3.12+ ABI3，也就是 PyO3 `abi3-py312`。它通常不是 `cp312-cp312` wheel；如果要构建严格绑定 CPython 3.12 的 wheel，需要移除 `abi3-py312`。

平台 wheel 会内置 Rust server 可执行文件，并安装 `alocals3-server` 命令。独立 server 构建产物在类 Unix 平台也命名为 `alocals3-server`，Windows 平台命名为 `alocals3-server.exe`。

## 配置

Server CLI 参数：

- `--host`：监听地址，默认 `127.0.0.1`
- `--port`：监听端口，默认 `8000`
- `--database-url`：SQLite 或 PostgreSQL 连接串
- `--storage-root`：对象内容落盘目录
- `--log-level`：日志级别或 `tracing_subscriber` 过滤 directive，默认 `info`
- `--max-upload-size`：上传 body 最大字节数，默认 `0` 表示禁用 axum body 限制
- `--version`：输出 server 版本号和作者信息

环境变量：

- `ALOCALS3_DATABASE_URL`：默认 `sqlite:///./alocals3.db`
- `ALOCALS3_STORAGE_ROOT`：默认 `./data`
- `ALOCALS3_LOG_LEVEL`：默认 `info`
- `ALOCALS3_MAX_UPLOAD_SIZE`：默认 `0`

连接串示例：

- SQLite：`sqlite:///./alocals3.db`
- PostgreSQL：`postgresql://user:password@127.0.0.1:5432/alocals3`

脚本和服务里建议给 SQLite 使用绝对路径，避免因为工作目录不同写到不同数据库。持续并发写入场景建议使用 PostgreSQL。

日志输出到 stderr。常用级别包括 `debug`、`info`、`warn`、`error`；也支持完整 `EnvFilter` directive，例如 `warn,alocals3_server=debug`。

默认情况下，server 不再使用 axum 的 body extractor 上传大小限制。当前实现会先把每个上传对象整体读入内存再写入磁盘，所以实际可上传大小仍受进程内存和可用磁盘限制。如果需要显式限制上传大小，可以设置 `--max-upload-size`。

## 存储布局

- Bucket 和对象元数据存储在 SQLite 或 PostgreSQL。
- 对象字节内容存储在本地磁盘。
- Blob 路径按内容寻址并分片：
  - `sha256(<object bytes>) = <digest>`
  - `{storage_root}/objects/{digest[:2]}/{digest[2:4]}/{digest}`

Bucket 名称、对象 key、prefix、delimiter、continuation token 都按 UTF-8 文本处理。客户端会自动对路径参数做 UTF-8 percent-encoding；调用时传 `logs/data.txt` 或 `logs/数据.txt` 这样的原始字符串，不要传已经 URL 编码过的片段。

## HTTP API

- `GET /healthz`：健康检查
- `GET /s3`：列出 buckets
- `PUT /s3/{bucket}`：创建 bucket
- `DELETE /s3/{bucket}`：删除空 bucket
- `GET /s3/{bucket}/objects`：列出对象
- `GET /s3/{bucket}?list-type=2`：S3 风格 ListObjectsV2
- `PUT /s3/{bucket}/{key}`：上传对象
- `GET /s3/{bucket}/{key}`：下载对象
- `HEAD /s3/{bucket}/{key}`：获取对象元信息
- `DELETE /s3/{bucket}/{key}`：删除对象

支持的对象能力：

- `ETag` 是对象内容的 MD5 hex 摘要。
- `Range` 请求返回 `206` 或 `416`。
- `PUT` 支持 `If-None-Match` 和 `If-Match`。
- `PUT` 支持 `Content-MD5` 校验。
- `GET` 和 `HEAD` 支持 `If-None-Match`。

`PUT /s3/{bucket}/{key}` 返回：

- `201`：新建对象
- `200`：覆盖已有对象
- `400`：`Content-MD5` 无效或不匹配
- `412`：条件请求失败

## Client 用法

Python runtime 依赖列表刻意保持为空。HTTP 网络功能在 Rust 里实现，不依赖 `httpx`。

```python
import asyncio
from pathlib import Path

from alocals3.client import ALocalS3Client, ALocalS3ClientAsync

with ALocalS3Client("http://127.0.0.1:8000", disable_proxy=True) as client:
    client.create_bucket("demo")
    info = client.put_object("demo", "logs/数据.txt", Path("data.txt"))
    print(info["etag"])
    copied = client.copy_object(
        "demo", "logs/数据.txt", "demo", "logs/copied.txt",
        metadata={"foo": "bar"},
    )

    data, headers = client.get_object_range("demo", "logs/数据.txt", "bytes=0-99")
    print(len(data), headers.get("content-range"))

    with client.open("s3://demo/logs/数据.txt", "r") as f:
        print(f.read())

    with client.open("s3://demo/logs/from-open.txt", "wb") as f:
        f.write(b"hello from file-like API\n")

    client.get_object_to_file("demo", "logs/数据.txt", Path("copy.txt"))


async def main() -> None:
    async with ALocalS3ClientAsync("http://127.0.0.1:8000", disable_proxy=True) as client:
        print(await client.list_buckets())
        async with client.open("s3://demo/logs/from-async-open.txt", "wb") as f:
            await f.write(b"hello from async file-like API\n")
        async with client.open("s3://demo/logs/from-async-open.txt", "rb") as f:
            print(await f.read())


asyncio.run(main())
```

CLI：

```bash
python -m alocals3.client --endpoint http://127.0.0.1:8000 CREATE_BUCKET demo
python -m alocals3.client --endpoint http://127.0.0.1:8000 PUT demo file.bin ./file.bin
python -m alocals3.client --endpoint http://127.0.0.1:8000 COPY demo file.bin demo copy.bin --metadata foo=bar
python -m alocals3.client --endpoint http://127.0.0.1:8000 GET demo file.bin ./copy.bin
python -m alocals3.client --endpoint http://127.0.0.1:8000 LIST_OBJECTS_V2 demo --prefix logs/ --delimiter /
```

也可以通过兼容 S3 的 `PUT` + `x-amz-copy-source` 调用 `CopyObject`。复制只会新增一条指向源内容寻址 blob 的数据库引用，不复制对象字节，因此相对于对象大小是 O(1)。服务端会持久化 `x-amz-meta-*` 用户元数据；复制时默认使用 `COPY`，传 `x-amz-metadata-directive: REPLACE` 可替换元数据。

## 从 SQLite 迁移到 PostgreSQL

先停止写入或制作 SQLite 快照，然后执行：

```bash
alocals3-migrate2pg \
  --source sqlite:///./alocals3.db \
  --target postgresql://user:password@127.0.0.1:5432/alocals3
```

该命令只迁移数据库元数据。请继续使用相同的 `--storage-root`（或单独迁移存储目录），因为对象 blob 由数据库中的相对内容寻址路径引用。迁移可重复执行，所有 bucket/object 数据行会在一个 PostgreSQL 事务中提交。

设置 `disable_proxy=True` 或传 `--disable-proxy` 可以忽略 `HTTP_PROXY`、`HTTPS_PROXY`、`ALL_PROXY`、`NO_PROXY` 等代理环境变量。

`client.open()` 是 file-like API，但不是简单的内存对象包装：

- 读模式（`"rb"` / `"r"`）会创建 Rust 持有的流式 HTTP reader。`open()` 会发起请求并读取响应头，但对象 body 会在返回文件对象的 `read()` 路径中按需从网络读取。
- 写模式（`"wb"` / `"w"`）会把 `write()` 数据写入 Rust 持有的临时文件；正常 `close()` 或正常退出 `with` 时才发送 HTTP `PUT`。如果 `with` 块内抛异常，则丢弃上传。
- `cache_path=` 是 best-effort，会在数据经过 file-like 对象时同步写入；cache 写失败不会导致网络读写失败。
- asyncio 代码里使用 `ALocalS3ClientAsync.open()` 和 `async with`。返回的是 async file-like 对象，提供可 `await` 的 `read()`、`readline()`、`readinto()`、`write()`、`flush()`、`close()`、`discard()` 方法，底层复用同一套 Rust 实现。

可以用大对象 benchmark 验证 Python file-like API：

```bash
./examples/benchmark_file_like.sh
```

可以用下面的脚本验证 stream 多路复用和基于 Range 的断点续读：

```bash
./examples/benchmark_stream_multiplex.sh
```

当前 file-like API 支持通过 `range_header="bytes=N-"` 断点续读。上传断点续传尚未实现；写模式会先写入 Rust 持有的临时文件，并在 `close()` 时用一次 HTTP `PUT` 上传。

## Curl 示例

```bash
curl -i -X PUT http://127.0.0.1:8000/s3/demo
curl -i -X PUT --data-binary @file.bin http://127.0.0.1:8000/s3/demo/file.bin
curl -i http://127.0.0.1:8000/s3/demo/file.bin
curl -i -H "Range: bytes=0-99" http://127.0.0.1:8000/s3/demo/file.bin
curl -sS "http://127.0.0.1:8000/s3/demo?list-type=2&prefix=logs/&delimiter=/&max-keys=100"
```

条件 PUT：

```bash
curl -i -X PUT -H "If-None-Match: *" --data-binary @file.bin \
  http://127.0.0.1:8000/s3/demo/file.bin

curl -i -X PUT -H 'If-Match: "d41d8cd98f00b204e9800998ecf8427e"' --data-binary @file.bin \
  http://127.0.0.1:8000/s3/demo/file.bin

MD5_B64=$(openssl md5 -binary file.bin | openssl base64)
curl -i -X PUT -H "Content-MD5: ${MD5_B64}" --data-binary @file.bin \
  http://127.0.0.1:8000/s3/demo/file.bin
```

## 一致性说明

- 对象字节内容通过临时文件和原子 rename 写入。
- 元数据通过所选数据库后端提交。
- 数据库和文件系统之间不是一个跨存储的全局事务。
- 进程或机器异常退出时，可能产生孤儿 blob 文件。Python 包不再提供备用 storage backend 或 Python server 路径；运维清理应在请求路径之外处理。

## 更新记录

[updates.md](updates.md)

## License

[The MIT License](LICENSE)
