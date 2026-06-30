# alocals3

本地文件系统 S3 风格服务：server 是纯 Rust 二进制，Python client 是 Rust 网络实现的 wheel 包。

## 快速启动

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
cargo build --release --bin alocals3-server
target/release/alocals3-server \
  --host 127.0.0.1 \
  --port 8000 \
  --database-url "sqlite:///./alocals3.db" \
  --storage-root ./data
```

## Rust Server 与 Python Client

server 是 `alocals3-server` Rust 二进制，不依赖 Python。元数据后端支持 SQLite 和 PostgreSQL，对象内容仍然存放在本地磁盘。

```bash
# SQLite
target/release/alocals3-server --database-url "sqlite:///./alocals3.db"

# PostgreSQL
target/release/alocals3-server \
  --database-url "postgresql://user:password@127.0.0.1:5432/alocals3"
```

Python 包通过 `maturin` 构建 wheel，提供 `LocalS3Client` 和 `LocalS3ClientAsync`。HTTP 网络功能在 Rust `reqwest` 中实现，不再依赖 `httpx`。

## 配置

- `ALOCALS3_STORAGE_ROOT`: 本地对象数据目录，默认 `./data`
- `ALOCALS3_DATABASE_URL`: 数据库连接串，默认 `sqlite:///./alocals3.db`

示例：

- SQLite: `sqlite:///./alocals3.db`
- PostgreSQL: `postgresql://user:password@127.0.0.1:5432/alocals3`

说明：SQLite 连接串建议使用绝对路径，避免因启动目录不同导致读写到不同数据库文件。
并发写场景下，SQLite 默认已开启 `WAL` + `busy_timeout`。
生产环境建议优先使用 PostgreSQL。

## 存储策略

- key/元数据索引由 Rust server 写入 SQLite 或 PostgreSQL
- 对象内容存放在本地磁盘
- 文件路径使用 hash 分片：
  - `sha256(<object bytes>) = <digest>`
  - 文件落盘路径为：`{storage_root}/objects/{digest[:2]}/{digest[2:4]}/{digest}`

## API 骨架

- `GET /healthz`: 健康检查
- `GET /s3`: 列出 buckets
- `PUT /s3/{bucket}`: 创建 bucket
- `DELETE /s3/{bucket}`: 删除空 bucket
- `GET /s3/{bucket}/objects`: 列出对象
- `GET /s3/{bucket}?list-type=2`: S3 风格 ListObjectsV2（支持 `prefix`、`delimiter`、`max-keys`、`continuation-token`）
- `PUT /s3/{bucket}/{key}`: 上传对象
- `GET /s3/{bucket}/{key}`: 下载对象（支持 `304`，支持 `Range` 返回 `206/416`）
- `HEAD /s3/{bucket}/{key}`: 只取元信息（支持 `304`，支持 `Range` 返回 `206/416`）
- `DELETE /s3/{bucket}/{key}`: 删除对象

Bucket 名称、对象 key、prefix、delimiter、continuation token 都按 UTF-8 文本处理。客户端会自动对路径参数做 UTF-8 percent-encoding；调用时传 `logs/数据.txt` 这样的原始字符串，不要传已经 URL 编码过的片段。

## 条件 PUT 与完整性校验

`PUT /s3/{bucket}/{key}` 支持：

- `If-None-Match`：当 ETag 匹配时拒绝覆盖（支持 `*`）
- `If-Match`：仅当 ETag 匹配时允许覆盖
- `Content-MD5`：服务端校验上传内容摘要，不匹配返回 `400 BadDigest`

返回码：

- `201`：新建对象
- `200`：覆盖已有对象
- `412`：前置条件失败

```bash
# 仅当对象不存在时创建
curl -i -X PUT -H "If-None-Match: *" --data-binary @file.bin \
  http://127.0.0.1:8000/s3/demo/file.bin

# 仅当 ETag 匹配时覆盖
curl -i -X PUT -H 'If-Match: "d41d8cd98f00b204e9800998ecf8427e"' --data-binary @file.bin \
  http://127.0.0.1:8000/s3/demo/file.bin

# 携带 Content-MD5 校验
MD5_B64=$(openssl md5 -binary file.bin | openssl base64)
curl -i -X PUT -H "Content-MD5: ${MD5_B64}" --data-binary @file.bin \
  http://127.0.0.1:8000/s3/demo/file.bin
```

## ListObjectsV2 示例

```bash
curl -sS "http://127.0.0.1:8000/s3/demo?list-type=2&prefix=logs/&delimiter=/&max-keys=100"

python -m alocals3.client --endpoint http://127.0.0.1:8000 \
  LIST_OBJECTS_V2 demo --prefix logs/ --delimiter / --max-keys 100
```

## Partial Content 用法示例

```bash
# 前 100 字节
curl -i -H "Range: bytes=0-99" http://127.0.0.1:8000/s3/demo/video.bin

# 末尾 512 字节
curl -i -H "Range: bytes=-512" http://127.0.0.1:8000/s3/demo/video.bin

# client CLI
python -m alocals3.client --endpoint http://127.0.0.1:8000 \
  GET demo video.bin ./part.bin --range "bytes=0-99"
```

```python
import asyncio
from pathlib import Path
from alocals3.client import LocalS3Client, LocalS3ClientAsync

client = LocalS3Client("http://127.0.0.1:8000")
data, headers = client.get_object_range("demo", "video.bin", "bytes=0-99")
print(len(data), headers.get("content-range"))

headers = client.get_object_to_file(
    "demo",
    "video.bin",
    Path("./part.bin"),
    range_header="bytes=100-199",
)
print(headers.get("content-range"))
client.close()

# 忽略环境中的 HTTP_PROXY/HTTPS_PROXY/ALL_PROXY/NO_PROXY。
client = LocalS3Client("http://127.0.0.1:8000", disable_proxy=True)
client.close()

async def main():
    async with LocalS3ClientAsync("http://127.0.0.1:8000", disable_proxy=True) as async_client:
        print(await async_client.list_buckets())

asyncio.run(main())
```

## 一致性与原子性

- `PUT`: 对象文件通过临时文件 + `os.replace` 原子替换，避免读到半写入内容；对象元数据映射通过数据库事务提交。
- `DELETE`: 对象元数据删除在数据库事务内完成（请求路径不做物理文件删除，以缩小临界区并避免并发竞态）。
- 以上不是“数据库与文件系统跨存储的单一全局事务”，极端故障下可能产生孤儿文件（可通过离线 GC 回收）。

## 离线 GC

```bash
# 仅扫描
python -m alocals3.gc

# 删除孤儿文件
python -m alocals3.gc --apply

# 安装后可直接执行
alocals3-gc --apply
```

## 更新记录

[updates.md](updates.md)

## LICENSE

[The MIT License](LICENSE)
