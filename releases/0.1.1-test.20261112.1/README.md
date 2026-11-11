# 試験配布版 0.1.1-test.20261112.1

導入・更新用の配布情報を再発行しました。プログラムの更新はありません。前回の`0.1.1-test.20261105.1`と同じイメージを使います。

- 配布情報の発行日時：2026年11月12日08:11:31 JST
- 有効期限：2026年11月19日08:11:31 JST（発行から7日間）
- 対応環境：Linux・x86_64（amd64）

既にこのイメージで稼働しているProbeは、再起動・再作成・再参加の操作は不要です。Probe ID、参加用の鍵、保存データ、資源設定、当日の通信使用量をそのまま使います。

## 配布情報を確認する

既存のProbeのフォルダーで、配布情報を取得して署名・有効期限を検証します。

```bash
git pull --ff-only
python3 scripts/community-release.py \
  releases/0.1.1-test.20261112.1/release.json \
  releases/0.1.1-test.20261112.1/release.sig \
  release-public.pem --platform linux/amd64
```

初めて導入する場合は、[参加者向け手順](../../docs/participant.md)に沿って進めてください。IDの申請や提出は不要です。通常起動時に署名登録が済むと、active・contributorとして参加します。

使用するイメージは次の固定値です。

```text
ghcr.io/tikpok/harvest-scope-probe@sha256:a4e2b6f30d9343c603f36d18cfff7fc783c20e777773eed5c64e56a8a1ef30d2
```

有効期限は導入・更新に使う配布情報の期限です。期限を過ぎても、稼働中のProbeが自動で停止することはありません。検証に失敗した場合は、導入・更新には進まないでください。
