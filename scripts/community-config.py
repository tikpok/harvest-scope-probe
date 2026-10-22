#!/usr/bin/env python3
"""ローカル資源設定を検証し、Compose用の数値設定だけを出力する。"""
import argparse
import json
import math
from pathlib import Path


def render(config):
    cpu = config.get('cpuCores', 0.2)
    memory = config.get('maxMemoryBytes', 469762048)
    if isinstance(cpu, bool) or not isinstance(cpu, (int, float)) or not math.isfinite(cpu) or cpu <= 0:
        raise ValueError('cpuCores は正の有限数で指定してください')
    if isinstance(memory, bool) or not isinstance(memory, int) or memory < 33554432:
        raise ValueError('maxMemoryBytes は32MiB以上の整数で指定してください')
    limits = config.get('limits', {})
    for key, default in [('maxConcurrency', 4), ('burst', 4), ('dailyTrafficBytes', 5368709120), ('spoolBytes', 104857600)]:
        value = limits.get(key, default)
        ceiling = 65535 if key in ('maxConcurrency', 'burst') else 9223372036854775807
        if isinstance(value, bool) or not isinstance(value, int) or not 0 < value <= ceiling:
            raise ValueError(f'{key} の上限が不正です')
    rate = limits.get('requestsPerSecond', 3.0)
    if isinstance(rate, bool) or not isinstance(rate, (int, float)) or not math.isfinite(rate) or rate <= 0:
        raise ValueError('requestsPerSecond は正の有限数で指定してください')
    return f'COMMUNITY_CPUS={cpu:.6f}\nCOMMUNITY_MEMORY={memory}\n'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('config', type=Path)
    args = parser.parse_args()
    try:
        print(render(json.loads(args.config.read_text(encoding='utf-8'))), end='')
    except (ValueError, OSError, TypeError) as exc:
        parser.exit(2, f'設定エラー: {exc}\n')


if __name__ == '__main__':
    main()
