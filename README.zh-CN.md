# alocals3

基于 FastAPI 的本地文件系统 S3 风格服务（项目骨架）。

## 快速启动

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m alocals3.server --reload
# 或者通过命令行覆盖数据库连接串
python -m alocals3.server --database-url "sqlite:////absolute/path/alocals3.db" --reload
```

## 配置

- `ALOCALS3_APP_NAME`: 应用名，默认 `alocals3`
- `ALOCALS3_STORAGE_ROOT`: 本地对象数据目录，默认 `./data`
- `ALOCALS3_DATABASE_URL`: 数据库连接串，默认 `sqlite:///./alocals3.db`

示例：

- SQLite: `sqlite:///./alocals3.db`
- PostgreSQL: `postgresql+psycopg://user:password@127.0.0.1:5432/alocals3`

说明：SQLite 连接串建议使用绝对路径，避免因启动目录不同导致读写到不同数据库文件。

## 存储策略

- key/元数据索引存放在数据库（SQLAlchemy，支持 SQLite/PostgreSQL）
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
- `PUT /s3/{bucket}/{key}`: 上传对象
- `GET /s3/{bucket}/{key}`: 下载对象（支持 `304`（Not Modified 客户端缓存），支持 `Range` 返回 `206/416`（Partial Content 部分内容/Request Range Not Satisfiable 无法满足请求的范围））
- `HEAD /s3/{bucket}/{key}`: 只取元信息（支持 `304`（Not Modified 客户端缓存））
- `DELETE /s3/{bucket}/{key}`: 删除对象

## Partial Content 用法示例

```bash
# 前 100 字节
curl -i -H "Range: bytes=0-99" http://127.0.0.1:8000/s3/demo/video.bin

# 末尾 512 字节
curl -i -H "Range: bytes=-512" http://127.0.0.1:8000/s3/demo/video.bin

# client CLI
python -m alocals3.client --endpoint http://127.0.0.1:8000 \
  get demo video.bin ./part.bin --range "bytes=0-99"
```

```python
from pathlib import Path
from alocals3.client import LocalS3Client

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
```

## 一致性与原子性

- `PUT`: 对象文件通过临时文件 + `os.replace` 原子替换，避免读到半写入内容；对象元数据映射通过数据库事务提交。
- `DELETE`: 对象元数据删除在数据库事务内完成（请求路径不做物理文件删除，以缩小临界区并避免并发竞态）。
- 以上不是“数据库与文件系统跨存储的单一全局事务”，极端故障下可能产生孤儿文件（可通过离线 GC 回收）。

### LICENSE

[The MIT License](LICENSE)
