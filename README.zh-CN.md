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
- `--max-upload-size`：上传 body 最大字节数，在流式接收过程中执行；默认 `0` 表示不限制
- `--max-concurrent-uploads`：同时流式写盘的上传请求数上限，默认 `16`
- `--auth-enabled <true|false>`：显式开启或关闭鉴权，默认 `false`
- `--service-token`：内部 `/s3` API 使用的 Bearer Token，默认为空
- `--user-auth-secret`：只读 `/api/object` API 使用的 HMAC 密钥，默认为空
- `--object-auth-cookie`：浏览器访问令牌的 Cookie 名称，默认 `alocals3-auth-cookie`
- `--version`：输出 server 版本号和作者信息

环境变量：

- `ALOCALS3_DATABASE_URL`：默认 `sqlite:///./alocals3.db`
- `ALOCALS3_STORAGE_ROOT`：默认 `./data`
- `ALOCALS3_LOG_LEVEL`：默认 `info`
- `ALOCALS3_MAX_UPLOAD_SIZE`：默认 `0`
- `ALOCALS3_MAX_CONCURRENT_UPLOADS`：默认 `16`
- `ALOCALS3_AUTH_ENABLED`：鉴权开关，默认 `false`；命令行参数优先
- `ALOCALS3_SERVICE_TOKEN`：开启鉴权后 `/s3` 所需的 Bearer Token
- `ALOCALS3_USER_AUTH_SECRET`：用于校验 `/api/object` Lauraycs 访问令牌的 HMAC 密钥
- `ALOCALS3_OBJECT_AUTH_COOKIE`：访问令牌 Cookie 名称，默认 `alocals3-auth-cookie`

### 鉴权机制

鉴权状态由 `--auth-enabled <true|false>`（或 `ALOCALS3_AUTH_ENABLED`）显式控制，
不再通过密钥是否为空隐式开启：

- 设置 `--auth-enabled true` 后，`/s3` 要求
  `Authorization: Bearer <service-token>`，`/api/object/{bucket}/{key}` 要求有效的
  HMAC 用户令牌。此时 `--service-token` 和 `--user-auth-secret` 都是必填项，任一
  为空时服务端会拒绝启动。
- 设置 `--auth-enabled false`（默认值）后，两类 API 均可匿名访问。即使已经配置
  Token 或密钥，也会在重新开启鉴权前保持忽略状态。
- `/healthz` 不需要鉴权。

开关与密钥都可以直接通过命令行参数设置：

```bash
SERVICE_TOKEN="$(openssl rand -hex 32)"
USER_AUTH_SECRET="$(openssl rand -hex 32)"

target/release/alocals3-server \
  --auth-enabled true \
  --service-token "$SERVICE_TOKEN" \
  --user-auth-secret "$USER_AUTH_SECRET" \
  --object-auth-cookie "alocals3-auth-cookie" \
  --database-url "sqlite:///./alocals3.db" \
  --storage-root ./data
```

也可以继续使用环境变量。后续示例假定启动服务和客户端前已导出相同的值：

```bash
export ALOCALS3_AUTH_ENABLED=true
export ALOCALS3_SERVICE_TOKEN="$SERVICE_TOKEN"
export ALOCALS3_USER_AUTH_SECRET="$USER_AUTH_SECRET"
export ALOCALS3_OBJECT_AUTH_COOKIE="alocals3-auth-cookie" # 可选
```

开启后，可通过 curl 或 Python client 调用 `/s3`：

```bash
curl -H "Authorization: Bearer ${ALOCALS3_SERVICE_TOKEN}" \
  http://127.0.0.1:8000/s3

# CLI 和 Python client 也会自动读取 ALOCALS3_SERVICE_TOKEN。
python -m alocals3.client --endpoint http://127.0.0.1:8000 LIST_BUCKETS
```

```python
from alocals3.client import ALocalS3Client

with ALocalS3Client(
    "http://127.0.0.1:8000",
    service_token="与服务端相同的-service-token",
) as client:
    print(client.list_buckets())
```

用户访问令牌格式为 `<payload>.<signature>`。`payload` 是
`<username>|<access>|<expires-unix-seconds>|<nonce>` 的无填充 URL-safe Base64
编码；`signature` 是使用 `ALOCALS3_USER_AUTH_SECRET` 对编码后 payload 计算的
HMAC-SHA256 小写十六进制摘要。服务端要求 username 和 nonce 非空，并校验过期
时间与签名；当前仅透传 `access` 字段，不会把它解释为授权规则。该令牌通常由
Lauraycs 签发；下面的独立示例展示了准确的生成和调用格式：

```bash
USER_TOKEN="$(python - <<'PY'
import base64, hashlib, hmac, os, secrets, time

raw = f"demo-user|read|{int(time.time()) + 3600}|{secrets.token_hex(16)}"
payload = base64.urlsafe_b64encode(raw.encode()).rstrip(b"=").decode()
signature = hmac.new(
    os.environ["ALOCALS3_USER_AUTH_SECRET"].encode(),
    payload.encode(),
    hashlib.sha256,
).hexdigest()
print(f"{payload}.{signature}")
PY
)"

curl -H "Authorization: Bearer ${USER_TOKEN}" \
  http://127.0.0.1:8000/api/object/demo/file.bin
```

浏览器也可以把同一个令牌放入 `ALOCALS3_OBJECT_AUTH_COOKIE` 指定的 Cookie，
例如 `Cookie: alocals3-auth-cookie=<user-token>`。Bearer Header 与 Cookie 同时
存在时，优先使用 Bearer Header。Amazon S3 没有定义鉴权 Cookie 名；SigV4 使用
`Authorization` Header、查询参数或浏览器 POST 字段。因此
`alocals3-auth-cookie` 是 alocals3 自定义名称，不属于 `x-amz-*` 协议字段。
CloudFront 的签名 Cookie 需要三枚配套 Cookie，属于另一套协议，因此不复用其名称。

如需关闭鉴权，用 `false` 重启服务即可；即使环境变量设置了
`ALOCALS3_AUTH_ENABLED=true`，命令行参数也会覆盖它：

```bash
target/release/alocals3-server \
  --auth-enabled false \
  --database-url "sqlite:///./alocals3.db" \
  --storage-root ./data
```

无鉴权模式只应在可信的本地开发环境中使用。

连接串示例：

- SQLite：`sqlite:///./alocals3.db`
- PostgreSQL：`postgresql://user:password@127.0.0.1:5432/alocals3`

脚本和服务里建议给 SQLite 使用绝对路径，避免因为工作目录不同写到不同数据库。持续并发写入场景建议使用 PostgreSQL。

日志输出到 stderr。常用级别包括 `debug`、`info`、`warn`、`error`；也支持完整 `EnvFilter` directive，例如 `warn,alocals3_server=debug`。

PUT 请求体会直接流式写入临时文件，同时增量计算 MD5 和 SHA-256，因此内存占用不会随对象大小增长。上传完成后先同步文件并原子移动到内容寻址目录，再提交数据库元数据。`--max-concurrent-uploads` 限制同时进行的上传流数量，`--max-upload-size` 对固定长度和 chunked 请求都会在流式接收过程中生效。

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
- `GET|HEAD /api/object/{bucket}/{key}`：需用户鉴权的只读浏览器对象访问

支持的对象能力：

- `ETag` 是对象内容的 MD5 hex 摘要。
- `GET` 以 1 MiB 分块从磁盘流式读取，不再把完整对象缓存在服务端内存中；
  客户端断开并丢弃响应时，对应文件流也会被释放。
- Range 请求会直接 seek 到指定偏移，只流式读取请求的字节数。
- `Range` 请求返回 `206` 或 `416`。
- `PUT` 支持 `If-None-Match` 和 `If-Match`。
- `PUT` 支持 `Content-MD5` 校验。
- `GET` 和 `HEAD` 支持 `If-None-Match`。
- 对象响应使用 `private, no-cache` 配合 ETag 条件请求，内容未变化时返回 `304`；
  这样既允许浏览器私有缓存，也不会把可覆盖的 bucket/key 路径误认为永久不变。
- `/api/object` 会用 `private, no-store` 把无版本路径重定向到
  `/api/object/{bucket}/{key}?version={etag}`。版本与当前对象一致时返回
  `private, max-age=31536000, immutable`；版本缺失或过期时重定向到当前版本。
  查询参数属于浏览器缓存键，因此覆盖同路径对象会生成新缓存项，不会复用旧内容。
  `private` 会阻止共享代理缓存已鉴权的对象，`Vary: Authorization, Cookie` 则按凭据
  隔离浏览器私有缓存项。

从曾把纯路径 URL 标记为 `immutable` 的版本升级时，客户端需要至少修改一次请求 URL
（例如追加 `?cache-version=2`），或者清理旧浏览器缓存。服务端的新响应头无法撤销已经
缓存的 immutable 响应；修改 URL 后请求会到达服务端，再被重定向到规范的
`?version={etag}` URL。

面向浏览器的接口通常复用同一流式路径。只有 `*/3dtiles/tileset.json` 和
`*/terrain/layer.json` 因为需要改写旧版兼容字段而进行内存缓冲，且缓冲上限为
16 MiB。

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
- `ALocalS3ClientAsync` 由 Tokio/异步 reqwest 的原生 Future 通过 PyO3 暴露，不再用 `asyncio.to_thread()` 调度同步客户端。它的 `open()` 返回可用于 `async with` 的 async file-like 对象，提供可 `await` 的 `read()`、`readline()`、`readinto()`、`write()`、`flush()`、`close()`、`discard()` 方法。

可以用大对象 benchmark 验证 Python file-like API：

```bash
./examples/benchmark_file_like.sh
```

可以用下面的脚本验证 stream 多路复用和基于 Range 的断点续读：

```bash
./examples/benchmark_stream_multiplex.sh
```

可以直接对原生异步客户端做并发压力测试：

```bash
python examples/bench_async_client.py --duration 30 --concurrency 50 --disable-proxy
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

### 异步对象元数据（HEAD）

```python
headers = await client.head_object("my-bucket", "folder/object.arrow")
size = int(headers["content-length"])
etag = headers["etag"]
```

`ALocalS3ClientAsync.head_object` 返回小写键名的 HTTP 响应头，包括文件大小、
ETag、最后修改时间、内容类型和 `x-amz-meta-*` 自定义元数据。
该接口复用原生异步连接池、认证、代理和超时设置，不下载正文、不列举 bucket。
空文件的 `content-length` 为 `0`；包括 404 在内的 HTTP 错误沿用
`RuntimeError` 和 `ALOCALS3_HTTP_STATUS:<状态码>` 标记。

### 分批上传单个对象（Multipart Upload）

同步 `ALocalS3Client` 与原生异步 `ALocalS3ClientAsync` 都提供以下接口；
同步调用去掉示例中的 `await` 即可。客户端和服务端都需要包含此补丁的构建。

```python
upload = await client.create_multipart_upload(
    "my-bucket", "results/data.arrow", "application/vnd.apache.arrow.file"
)
upload_id = upload["upload_id"]  # 持久化此 ID，进程重启后可继续上传
first = await client.upload_part(upload_id, 1, b"first batch")
second = await client.upload_part(upload_id, 2, b"second batch")
# 同一个编号重传会替换该分片；不同编号可以并发上传。
parts = (await client.list_parts(upload_id))["parts"]
result = await client.complete_multipart_upload(upload_id, parts)
# 确认提交结果已持久化后，可释放提交回执；不会删除已发布的对象。
await client.abort_multipart_upload(upload_id)
```

- `pending_uploads` 保存上传会话，`pending_upload_parts` 保存逐片记录；
  分片文件保存在存储根目录的 `multipart/`，不进入已发布对象的 GC 范围。
  主对象表 **不增加上传状态字段**，上传中的内容不可通过 GET/HEAD/List 读取。
- 完成时按编号流式合并，校验每片长度及 MD5。提交清单必须包含全部已上传分片、
  编号从 1 连续递增且 ETag 匹配，缺片、重复、乱序或旧清单均报错，绝不静默丢片。
- 文件安装成功后，在同一个短事务内写入主对象表、移除 pending 数据，并保存
  `completed_uploads` 回执。覆盖旧对象时，提交前仍可读取旧对象；事务失败可重试。
  重复提交同一清单返回原回执，不会覆盖之后写入的新版本。空清单可创建空对象。
- 上传接收和文件合并不持有 SQL 连接；SQLite 和 PostgreSQL 均支持。
  使用非阻塞 OS 文件锁协调同一存储目录上的多个服务进程，服务重启后可以续传。
  构建工具链要求 Rust 1.89 或更新版本。
- 不设最小分片大小、16 MiB 分片上限或业务分片数量上限。
  服务端已有的 `max_upload_size` 若明确设为非零，则同时检查总大小；默认 0 表示不限。
  服务端合并采用固定缓冲区，客户端 `upload_part` 只需要提供当前分片的字节数据。
- `abort_multipart_upload` 可取消未完成上传并删除分片，也可释放完成回执。
  未完成会话不会自动过期，以免误删仍需续传的数据；调用方应保存 ID 并在放弃时取消。
- 这是 alocals3 的 JSON 分片协议，不是 AWS S3 的 XML multipart 协议：
  `POST /s3-multipart/<bucket>/<key>` 创建；`PUT /s3-uploads/<id>/<part_number>` 上传；
  `GET /s3-uploads/<id>` 查询；`POST /s3-uploads/<id>` 提交；`DELETE /s3-uploads/<id>` 取消。
  所有路由复用服务凭据认证；数据库新表由服务启动时自动创建。

测试：先构建服务端并安装新客户端，再运行 `python tests/test_multipart.py`。
默认使用隔离的 SQLite 临时数据库；设置 `ALOCALS3_TEST_POSTGRES_URL` 后使用指定
PostgreSQL 数据库中的临时 schema，测试完成后删除，不使用业务表。
