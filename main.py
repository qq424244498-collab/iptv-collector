#!/usr/bin/env python3
"""IPTV 自动采集器.

- 从种子 URL 抓取 M3U/M3U8/HTML 中的流地址
- 解析并验证流资源
- 去重/排序并输出 combined.m3u 和 sources.json
- 可选 XMLTV/EPG 匹配
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import re
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple
from urllib.parse import urljoin

import aiohttp
import yaml

from epg import match_epg

DEFAULT_CONFIG = {
    "seeds": [],
    "concurrency": 12,
    "timeout_s": 12,
    "max_fetch_size": 2 * 1024 * 1024,
    "user_agent": "iptv-collector/1.0",
    "output_m3u": "combined.m3u",
    "output_json": "sources.json",
    "validate_with_ffprobe": False,
    "follow_discovered_m3u": True,
    "max_discovered_links": 500,
    "epg_path": "",
    "title_prefix": "",
}

LINK_RE = re.compile(
    r"https?://[^\s\"'<>]+\.(?:m3u8?|mp4|ts|flv|mkv|avi)(?:\?[^\s\"'<>]*)?",
    flags=re.IGNORECASE,
)
HREF_RE = re.compile(r'href=["\'](?P<h>[^"\']+)["\']', flags=re.IGNORECASE)
SRC_RE = re.compile(r'src=["\'](?P<s>[^"\']+)["\']', flags=re.IGNORECASE)


def norm_url(url: str) -> str:
    return (url or "").strip()


def hash_url(url: str) -> str:
    return hashlib.sha1(norm_url(url).encode("utf-8")).hexdigest()


def parse_extinf(line: str) -> Tuple[Dict[str, str], str]:
    attrs: Dict[str, str] = {}
    title = ""
    if line.startswith("#EXTINF:"):
        payload = line[len("#EXTINF:") :].strip()
        if "," in payload:
            attrs_part, title = payload.rsplit(",", 1)
        else:
            attrs_part, title = payload, ""
        for key, value in re.findall(r'([A-Za-z0-9_\-]+)\s*=\s*"([^"]*)"', attrs_part):
            attrs[key] = value
    return attrs, title.strip()


def parse_m3u_text(text: str) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    lines = [ln.strip() for ln in text.splitlines()]
    i = 0
    while i < len(lines):
        line = lines[i]
        if line.startswith("#EXTINF:"):
            attrs, title = parse_extinf(line)
            j = i + 1
            uri = None
            while j < len(lines):
                candidate = lines[j]
                if candidate and not candidate.startswith("#"):
                    uri = candidate
                    break
                j += 1
            if uri:
                entries.append({"title": title, "attrs": attrs, "uri": uri})
                i = j + 1
                continue
        elif line and not line.startswith("#"):
            entries.append({"title": "", "attrs": {}, "uri": line})
        i += 1
    return entries


async def fetch_text(session: aiohttp.ClientSession, url: str, timeout_s: int, max_size: int) -> Dict[str, Any]:
    try:
        async with asyncio.timeout(timeout_s):
            async with session.get(url, allow_redirects=True) as resp:
                data = await resp.content.read(max_size)
                text = data.decode(resp.charset or "utf-8", errors="ignore")
                return {
                    "status": resp.status,
                    "text": text,
                    "content_type": resp.headers.get("Content-Type", ""),
                    "url": str(resp.url),
                }
    except Exception as exc:  # pragma: no cover
        return {"status": None, "text": "", "content_type": "", "url": url, "error": str(exc)}


async def head_check(session: aiohttp.ClientSession, url: str, timeout_s: int) -> Dict[str, Any]:
    try:
        async with asyncio.timeout(timeout_s):
            async with session.head(url, allow_redirects=True) as resp:
                return {
                    "status": resp.status,
                    "content_type": resp.headers.get("Content-Type", ""),
                    "url": str(resp.url),
                }
    except Exception as exc:  # pragma: no cover
        return {"status": None, "content_type": "", "url": url, "error": str(exc)}


def extract_links_from_html(html: str, base_url: str = "") -> List[str]:
    result: set[str] = set()
    for match in LINK_RE.finditer(html):
        result.add(match.group(0))
    for match in HREF_RE.finditer(html):
        href = match.group("h")
        if href.startswith("http"):
            if re.search(r"\.(?:m3u8?|mp4|ts|flv|mkv|avi)(?:\?.*)?$", href, flags=re.IGNORECASE):
                result.add(href)
        elif href.startswith("/") and base_url:
            result.add(urljoin(base_url, href))
    for match in SRC_RE.finditer(html):
        src = match.group("s")
        if src.startswith("http") and re.search(r"\.(?:m3u8?|mp4|ts|flv|mkv|avi)(?:\?.*)?$", src, flags=re.IGNORECASE):
            result.add(src)
    return sorted(result)


def ffprobe_check(url: str, timeout_s: int = 8) -> Dict[str, Any]:
    try:
        proc = subprocess.run(
            ["ffprobe", "-v", "error", "-timeout", str(timeout_s * 1000000), "-show_streams", url],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout_s + 2,
        )
        return {
            "ok": proc.returncode == 0 and bool(proc.stdout),
            "returncode": proc.returncode,
            "stderr": proc.stderr.decode("utf-8", errors="ignore"),
            "stdout": proc.stdout.decode("utf-8", errors="ignore"),
        }
    except Exception as exc:  # pragma: no cover
        return {"ok": False, "error": str(exc)}


class Collector:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.seen: Dict[str, Dict[str, Any]] = {}
        self.discovered_count = 0
        self.sem = asyncio.Semaphore(config["concurrency"])

    def add_entry(self, url: str, title: str = "", attrs: Dict[str, str] | None = None, source: str = "") -> None:
        clean_url = norm_url(url)
        if not clean_url:
            return
        key = hash_url(clean_url)
        if key in self.seen:
            self.seen[key]["sources"].add(source)
            if title and not self.seen[key]["title"]:
                self.seen[key]["title"] = title
            return
        self.seen[key] = {
            "url": clean_url,
            "title": title,
            "attrs": attrs or {},
            "sources": {source} if source else set(),
            "status": None,
            "score": 0,
            "last_checked": None,
            "extra": {},
        }

    async def process_seed(self, session: aiohttp.ClientSession, seed_url: str) -> None:
        url = norm_url(seed_url)
        if not url:
            return
        async with self.sem:
            data = await fetch_text(session, url, self.config["timeout_s"], self.config["max_fetch_size"])
        if data.get("status") and data.get("text"):
            text = data["text"]
            if "#EXTM3U" in text.upper():
                for entry in parse_m3u_text(text):
                    stream_url = norm_url(entry.get("uri", ""))
                    self.add_entry(stream_url, title=entry.get("title", ""), attrs=entry.get("attrs", {}), source=url)
                return

        base = data.get("url", url)
        for link in extract_links_from_html(data.get("text", "") or "", base_url=base):
            if self.discovered_count >= self.config["max_discovered_links"]:
                break
            self.discovered_count += 1
            if link.lower().endswith((".m3u", ".m3u8")) and self.config.get("follow_discovered_m3u"):
                await self.process_seed(session, link)
            else:
                self.add_entry(link, title="", attrs={}, source=url)

    async def validate_all(self) -> None:
        headers = {"User-Agent": self.config.get("user_agent", DEFAULT_CONFIG["user_agent"])}
        async with aiohttp.ClientSession(headers=headers) as session:
            tasks = [self.validate_one(session, item) for item in self.seen.values()]
            await asyncio.gather(*tasks)

    async def validate_one(self, session: aiohttp.ClientSession, entry: Dict[str, Any]) -> None:
        async with self.sem:
            head = await head_check(session, entry["url"], self.config["timeout_s"])
        entry["last_checked"] = int(time.time())
        if head.get("status") and head["status"] < 400:
            entry["status"] = head["status"]
            ct = head.get("content_type", "")
            score = 1
            if "application/vnd.apple.mpegurl" in ct.lower() or entry["url"].lower().endswith(".m3u8"):
                score += 3
            if ct.lower().startswith("video/"):
                score += 2
            if entry["url"].lower().endswith((".m3u8", ".m3u")):
                score += 2
            entry["score"] = score
            if self.config.get("validate_with_ffprobe"):
                probe = ffprobe_check(entry["url"], timeout_s=self.config["timeout_s"])
                if probe.get("ok"):
                    entry["score"] += 2
                    entry["extra"]["ffprobe"] = probe
        else:
            entry["status"] = head.get("status")
            entry["score"] = 0

    def export_m3u(self, path: str, entries: Iterable[Dict[str, Any]]) -> None:
        output = Path(path)
        lines = ["#EXTM3U"]
        for entry in entries:
            if entry.get("score", 0) <= 0:
                continue
            title = entry.get("title") or entry.get("attrs", {}).get("tvg-name") or "Unknown"
            if self.config.get("title_prefix"):
                title = f"{self.config['title_prefix']} {title}".strip()
            attrs = entry.get("attrs", {})
            attrs_string = "".join(f' {key}="{value}"' for key, value in attrs.items())
            lines.append(f'#EXTINF:-1{attrs_string},{title}')
            lines.append(entry["url"])
        output.write_text("\n".join(lines) + "\n", encoding="utf-8")

    def export_json(self, path: str) -> None:
        data = []
        for entry in sorted(self.seen.values(), key=lambda x: (-x.get("score", 0), x.get("title", ""))):
            data.append({
                "title": entry.get("title", ""),
                "url": entry["url"],
                "attrs": entry.get("attrs", {}),
                "score": entry.get("score", 0),
                "sources": sorted(entry.get("sources", set())),
                "status": entry.get("status"),
                "last_checked": entry.get("last_checked"),
                "extra": entry.get("extra", {}),
            })
        Path(path).write_text(json.dumps({"generated_at": int(time.time()), "count": len(data), "items": data}, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    async def run(self) -> None:
        headers = {"User-Agent": self.config.get("user_agent", DEFAULT_CONFIG["user_agent"])}
        async with aiohttp.ClientSession(headers=headers) as session:
            tasks = [self.process_seed(session, seed) for seed in self.config.get("seeds", [])]
            if tasks:
                await asyncio.gather(*tasks)
        await self.validate_all()

        epg_path = self.config.get("epg_path")
        if epg_path:
            payload = json.loads(Path(self.config["output_json"]).read_text(encoding="utf-8") if Path(self.config["output_json"]).exists() else '{"items": []}')
            matched = match_epg(payload, epg_path)
            Path(self.config["output_json"]).write_text(json.dumps(matched, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

        self.export_m3u(self.config["output_m3u"], self.seen.values())
        self.export_json(self.config["output_json"])


def load_config(path: str | None = None) -> Dict[str, Any]:
    cfg = DEFAULT_CONFIG.copy()
    if path and Path(path).exists():
        with open(path, "r", encoding="utf-8") as fh:
            loaded = yaml.safe_load(fh) or {}
        cfg.update(loaded)
    return cfg


async def main_async(config_path: str) -> None:
    cfg = load_config(config_path)
    collector = Collector(cfg)
    await collector.run()
    print(f"Generated: {cfg['output_m3u']} and {cfg['output_json']}")


def main() -> None:
    parser = argparse.ArgumentParser(description="IPTV 自动采集器")
    parser.add_argument("--config", "-c", default="config.yaml")
    args = parser.parse_args()
    asyncio.run(main_async(args.config))


if __name__ == "__main__":
    main()
