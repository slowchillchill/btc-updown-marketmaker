#!/usr/bin/env python3
"""只补公共CUDA扩展的锁定构建工具，不重装现有模型环境。"""
import json
from pathlib import Path
import os
import subprocess
import sys
from autodl_environment import download, download_candidates, on_data_disk, digest


def main():
    root = on_data_disk(Path(__file__).resolve().parents[1])
    lock = root / 'gpu-build-tools.lock.json'
    items = json.loads(lock.read_text())['artifacts']
    bundle = on_data_disk(Path('/root/autodl-tmp/dependencies/issue-181-gpu-build'))
    bundle.mkdir(parents=True, exist_ok=True)
    records = []
    for item in items:
        item['filename'] = item['url'].rsplit('/', 1)[-1]
        item['download_urls'] = download_candidates(item['url'], None, True)
        wheel = on_data_disk(bundle / item['filename'])
        download(item, wheel, 10, retry_sources=True)
        # 不解析或升级其他依赖。wheel内容只来自官方锁定SHA。
        subprocess.run(['uv', 'pip', 'install', '--python', sys.executable,
                        '--offline', '--no-deps', str(wheel)], check=True)
        records.append({'name': item['name'], 'version': item['version'],
                        'sha256': digest(wheel), 'path': str(wheel)})
    result = {'lock_sha256': digest(lock), 'installed': records,
              'ninja_version': subprocess.check_output(['ninja', '--version'], text=True).strip()}
    (bundle / 'installation.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result), flush=True)


if __name__ == '__main__':
    main()
