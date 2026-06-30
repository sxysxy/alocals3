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

Linux server 默认目标是 `x86_64-unknown-linux-musl`。macOS 默认目标是 `aarch64-apple-darwin`，并设置 `MACOSX_DEPLOYMENT_TARGET=11.0`。

wheel 现在配置为 Python 3.12+ ABI3，也就是 PyO3 `abi3-py312`。它通常不是 `cp312-cp312` wheel；如果要构建严格绑定 CPython 3.12 的 wheel，需要移除 `abi3-py312`。

## 配置

Server CLI 参数：

- `--host`：监听地址，默认 `127.0.0.1`
- `--port`：监听端口，默认 `8000`
- `--database-url`：SQLite 或 PostgreSQL 连接串
- `--storage-root`：对象内容落盘目录

环境变量：

- `ALOCALS3_DATABASE_URL`：默认 `sqlite:///./alocals3.db`
- `ALOCALS3_STORAGE_ROOT`：默认 `./data`

连接串示例：

- SQLite：`sqlite:///./alocals3.db`
- PostgreSQL：`postgresql://user:password@127.0.0.1:5432/alocals3`

脚本和服务里建议给 SQLite 使用绝对路径，避免因为工作目录不同写到不同数据库。持续并发写入场景建议使用 PostgreSQL。

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

from alocals3.client import LocalS3Client, LocalS3ClientAsync

with LocalS3Client("http://127.0.0.1:8000", disable_proxy=True) as client:
    client.create_bucket("demo")
    info = client.put_object("demo", "logs/数据.txt", Path("data.txt"))
    print(info["etag"])

    data, headers = client.get_object_range("demo", "logs/数据.txt", "bytes=0-99")
    print(len(data), headers.get("content-range"))

    client.get_object_to_file("demo", "logs/数据.txt", Path("copy.txt"))


async def main() -> None:
    async with LocalS3ClientAsync("http://127.0.0.1:8000", disable_proxy=True) as client:
        print(await client.list_buckets())


asyncio.run(main())
```

CLI：

```bash
python -m alocals3.client --endpoint http://127.0.0.1:8000 CREATE_BUCKET demo
python -m alocals3.client --endpoint http://127.0.0.1:8000 PUT demo file.bin ./file.bin
python -m alocals3.client --endpoint http://127.0.0.1:8000 GET demo file.bin ./copy.bin
python -m alocals3.client --endpoint http://127.0.0.1:8000 LIST_OBJECTS_V2 demo --prefix logs/ --delimiter /
```

设置 `disable_proxy=True` 或传 `--disable-proxy` 可以忽略 `HTTP_PROXY`、`HTTPS_PROXY`、`ALL_PROXY`、`NO_PROXY` 等代理环境变量。

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
- 进程或机器异常退出时，可能产生孤儿 blob 文件，可通过离线 GC 清理。

## Legacy Python 代码

仓库里仍保留旧版 Python server/storage 模块，用于兼容和迁移。`main` 分支的推荐运行时是 Rust server。

旧版 Python 路径可能需要安装可选依赖：

```bash
python -m pip install ".[legacy-python-server]"
```

离线 GC 当前仍是 Python 工具：

```bash
python -m alocals3.gc
python -m alocals3.gc --apply
```

## 更新记录

[updates.md](updates.md)

## License

[The MIT License](LICENSE)
