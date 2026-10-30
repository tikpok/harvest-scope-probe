#!/usr/bin/env python3
"""ローカル資源設定を検証し、Compose用の数値設定だけを出力する。"""
import argparse
import json
import math
from pathlib import Path


def render(config):
    cpu = config.get('cpuCores', 0.2)
    memory = config.get('maxMemoryBytes', 469762048)
    if isinstance(cpu, bool) or not isinstance(cpu, (int, float)) or not math.isfinite(cpu) or not 0 < cpu <= 1.0:
        raise ValueError('cpuCores は0より大きく1以下の有限数で指定してください')
    if isinstance(memory, bool) or not isinstance(memory, int) or not 33554432 <= memory <= 1073741824:
        raise ValueError('maxMemoryBytes は32MiB以上1GiB以下の整数で指定してください')
    cpu_percent = config.get('maxCpuPercent', 70)
    if isinstance(cpu_percent, bool) or not isinstance(cpu_percent, int) or not 1 <= cpu_percent <= 100:
        raise ValueError('maxCpuPercent は1以上100以下の整数で指定してください')
    limits = config.get('limits', {})
    for key, default in [('maxConcurrency', 4), ('burst', 4), ('dailyTrafficBytes', 21474836480), ('spoolBytes', 104857600)]:
        value = limits.get(key, default)
        ceiling = default
        if isinstance(value, bool) or not isinstance(value, int) or not 0 < value <= ceiling:
            raise ValueError(f'{key} は1以上{ceiling}以下の整数で指定してください')
    rate = limits.get('requestsPerSecond', 3.0)
    if isinstance(rate, bool) or not isinstance(rate, (int, float)) or not math.isfinite(rate) or not 0 < rate <= 3.0:
        raise ValueError('requestsPerSecond は0より大きく3以下の有限数で指定してください')
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
