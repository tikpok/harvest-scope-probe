#!/usr/bin/env python3
"""署名済み配布情報を検証する。Docker操作や更新は実行しない。"""

import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import re
import subprocess
import sys


def timestamp(value):
    if not isinstance(value, str):
        raise ValueError("配布日時が不正です")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.utcoffset() != timedelta(0):
        raise ValueError("配布日時はUTCで指定してください")
    return parsed


def verify(manifest, signature, public_key, platform, now=None):
    """指定した信頼済み鍵で検証し、検証済みの配布情報だけを返す。"""
    now = now or datetime.now(timezone.utc)
    if manifest.stat().st_size > 65536 or signature.stat().st_size != 64:
        raise ValueError("配布情報または署名のサイズが不正です")
    result = subprocess.run(
        ["openssl", "pkeyutl", "-verify", "-pubin", "-inkey", str(public_key),
         "-rawin", "-in", str(manifest), "-sigfile", str(signature)],
        capture_output=True, timeout=10, check=False,
    )
    if result.returncode:
        raise ValueError("署名を検証できません。取得元と信頼済み公開鍵を確認してください")
    data = json.loads(manifest.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or data.get("schemaVersion") != 1:
        raise ValueError("未対応の配布情報schemaです")
    if data.get("protocolVersion") != 2:
        raise ValueError("未対応のprotocolです")
    image = data.get("image")
    if not isinstance(image, str) or not re.fullmatch(
        r"[a-z0-9]+(?:[.-][a-z0-9]+)*(?::[0-9]{1,5})?"
        r"(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)+@sha256:[a-f0-9]{64}", image
    ):
        raise ValueError("imageはregistryとsha256 digestで固定してください")
    if not isinstance(data.get("version"), str) or not re.fullmatch(
        r"[0-9]+\.[0-9]+\.[0-9]+(?:-[A-Za-z0-9.-]+)?", data["version"]
    ):
        raise ValueError("版番号が不正です")
    platforms = data.get("platforms")
    if not isinstance(platforms, list) or platform not in platforms:
        raise ValueError("このplatform向けの配布ではありません")
    released = timestamp(data.get("releasedAt"))
    expires = timestamp(data.get("expiresAt"))
    if released > now + timedelta(minutes=5):
        raise ValueError("配布日時が未来です")
    if expires <= now or expires <= released:
        raise ValueError("配布情報の有効期限が切れています")
    if not isinstance(data.get("changes"), list) or not all(
        isinstance(item, str) for item in data["changes"]
    ):
        raise ValueError("変更内容が不正です")
    return data


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("signature", type=Path)
    parser.add_argument("public_key", type=Path, help="別途確認した信頼済み公開鍵")
    parser.add_argument("--platform", choices=["linux/amd64", "linux/arm64"], required=True)
    args = parser.parse_args()
    try:
        result = verify(args.manifest, args.signature, args.public_key, args.platform)
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        print(f"検証失敗: {error}", file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
