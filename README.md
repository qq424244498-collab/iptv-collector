# IPTV Collector

IPTV 自动采集器，支持：
- 从 M3U/M3U8/HTML 中采集直播源
- 自动去重、校验和排序
- 输出 `combined.m3u` 和 `sources.json`
- 可选 XMLTV/EPG 匹配
- Web UI（FastAPI）
- Docker / docker-compose 一键启动
- GitHub Actions 自动刷新

## 目录结构

- `main.py`：采集逻辑
- `app.py`：Web UI API 服务
- `epg.py`：XMLTV EPG 匹配辅助
- `config.yaml`：采集配置
- `combined.m3u`：最终导出 M3U
- `sources.json`：最终导出 JSON
- `Dockerfile`：Docker 构建文件
- `docker-compose.yml`：本地容器启动
- `.github/workflows/refresh.yml`：自动刷新任务

## 安装

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 运行

### 1) 采集源

```bash
python main.py --config config.yaml
```

### 2) 启动 Web UI

```bash
uvicorn app:app --host 0.0.0.0 --port 8000
```

访问：
- `http://localhost:8000/`
- `http://localhost:8000/api/sources`

### 3) Docker

```bash
docker build -t iptv-collector .
docker run --rm -p 8000:8000 -e MODE=server iptv-collector
```

或使用 docker-compose：

```bash
docker-compose up --build
```

## 配置说明

编辑 `config.yaml`：

```yaml
seeds:
  - "https://iptv.445569.xyz/live.m3u"
concurrency: 12
timeout_s: 12
max_fetch_size: 2097152
output_m3u: "combined.m3u"
output_json: "sources.json"
validate_with_ffprobe: false
follow_discovered_m3u: true
max_discovered_links: 500
epg_path: ""
```

## 注意

- 仅用于合法、公开、可授权的 IPTV 内容采集。
- 不要抓取或传播受版权限制的付费流、私有源或未授权内容。

## 许可证

MIT License。
