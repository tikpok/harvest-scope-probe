# 実機試験で見つかった検査不能の修正：0.1.0-test.20261022.3

eMudhra対応済みの前版へ、NIS1 Supernodeの固定提供元許可と通信予算の見直しを重ねたamd64向け試験版です。ソースは `ea4478fce0f2c4c9e147b065ea10f461fa6111c7`。実機2台の継続受入は未完了です。

## 修正と検証

- SupernodeはNIS1の単独検査だけで固定提供元を最大4要求、貸出上限以内に許可します。任意の追加ホストは許可せず、公開IP確認と同一オリジン制限を維持します。
- Centralは登録済みの公開ws/wss接続先を使います。提供元への予約は全Community Probeで256単位、境界分を含む保守的な10分窓（最長約11分）へ制限します。旧A/Bの要求はこの枠に含みません。
- 通信予算の既定値は20GiB/日です。実機Aの約1時間の消費を換算すると約14GiB/日相当で、5GiBでは処理が止まるため見直しました。一時予約・保守的なオーバーヘッドを含む計算値で、実転送量や終日稼働の保証とは区別します。
- CPU0.2、448MiB、追加swapなし、同時4件、毎秒3要求、burst4、未送信100MiBは維持。既存の明示的な設定を自動変更しません。
- Rust248件、Python11件、専用DBのcommunity7件が成功しました。従来のignored 3件は除外しています。eMudhraの12チェーン拒否・正常チェーン受理、OS証明書更新を維持しています。
- 350対象・2台の模擬結果5,564件を約118秒で受領し、Supernode以外は2地点が揃いました。Supernodeは全体予約枠により後続へ回されます。実測と後続窓での収束の確認は未完了です。

固定イメージ：

```text
ghcr.io/tikpok/harvest-scope-probe@sha256:dc8d6ddec963167ea7ef6ab5e7aaf951ce7d4e09a86e54fc72903ea577966cf4
```

## 手動更新

専用Community Probeのディレクトリで、まずソースと配布情報を取得し、信頼済み公開鍵で署名・期限・amd64を検証します。Docker操作にsudoが必要な端末では付けてください。

```sh
git pull --ff-only
python3 scripts/community-release.py releases/0.1.0-test.20261022.3/release.json releases/0.1.0-test.20261022.3/release.sig release-public.pem --platform linux/amd64
docker pull ghcr.io/tikpok/harvest-scope-probe@sha256:dc8d6ddec963167ea7ef6ab5e7aaf951ce7d4e09a86e54fc72903ea577966cf4
```

現在の3設定ファイルを新しい専用ディレクトリへ控え、専用Probeだけを停止します。

```sh
backup_dir=$(mktemp -d "$PWD/.env.community.backup.XXXXXXXX")
cp .env.community .env.community.resources community-limits.json "$backup_dir/"
echo "$backup_dir"
docker compose --env-file .env.community --env-file .env.community.resources -f compose.community-probe.yaml stop probe
```

`.env.community` の `PROBE_IMAGE` を検証済みの固定イメージへ変更してください。通信予算も20GiBへ揃える場合は、`community-limits.json` の `limits.dailyTrafficBytes` だけを `21474836480` へ変更します。その他の個別設定は保持します。既存の5GiBを残すこともできますが、到達後は次のUTC日まで仕事を取得しません。

```sh
python3 scripts/community-config.py community-limits.json > .env.community.resources
docker compose --env-file .env.community --env-file .env.community.resources -f compose.community-probe.yaml config
docker compose --env-file .env.community --env-file .env.community.resources -f compose.community-probe.yaml up -d --no-deps --pull never --force-recreate probe
docker compose --env-file .env.community --env-file .env.community.resources -f compose.community-probe.yaml exec -T probe /usr/local/bin/monitor-probe --status
```

端末鍵・Probe ID・未送信結果のボリュームを保持します。起動状態だけでなく、休止理由・残量・未送信結果とCentralでの検査別到着を確認してください。旧A/Bは継続稼働、公開判定はshadowのまま比較します。

## 手動復旧

専用Probeを停止し、控えた3設定ファイルを元の名前へ戻して上記の再作成コマンドを実行します。復旧先の前版イメージを削除せず保持してください。直前の `0.1.0-test.20261022.2` はeMudhra対応済みですが、今回直した検査不能が再発します。対応前の `0.1.0-test.20261021` は通常の復旧先に使用しません。他のコンテナ・ノード・ホストswap設定は変更しません。
