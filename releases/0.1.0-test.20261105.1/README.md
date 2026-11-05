# NIS1の高さ保持を修正した試験版 0.1.0-test.20261105.1

NIS1の補助APIが失敗した際、取得済みのチェーン高さまで捨ててしまう問題を修正しました。判定不能を示すUnknownは維持し、高さをCentralへ渡します。通常の同期遅延の値や通信枠は変えません。

対象は既存Community Probeのamd64環境です。署名、Probe ID、登録鍵、保存データ、ローカルの通信・資源上限を引き継ぎます。[ソース](https://github.com/tikpok/harvest-scope-probe/commit/3c28feaab4d88bbfe63868f92343082cb2cd4504)の変更はengineと回帰テストの2ファイルです。関連Rustテスト60件と、通信なし・非root・読取専用・CPU0.2・448MiBでの状態表示を確認済みです。A/B実機への反映と継続観測は未確認です。

## 署名を検証してから取得する

専用Probeのディレクトリで実行します。既存の信頼済み公開鍵を使ってください。Dockerにsudoが必要な端末では付けてください。

```sh
git pull --ff-only
python3 scripts/community-release.py releases/0.1.0-test.20261105.1/release.json releases/0.1.0-test.20261105.1/release.sig release-public.pem --platform linux/amd64
python3 scripts/community-config.py community-limits.json
docker pull ghcr.io/tikpok/harvest-scope-probe@sha256:9f8561765bd25f6d3c29e908d71cba3f4a7afab77b2206c7b97396950d5a4e4d
```

署名検証が失敗した場合は更新を中止します。実行時の版表示は互換性のため0.1.0のままです。更新確認には固定イメージを使います。

## 設定を退避し、Probeだけを更新する

現在の設定を控え、表示された退避先を記録します。

```sh
release_backup=$(mktemp -d "$PWD/.env.community.backup.XXXXXXXX")
cp -p .env.community .env.community.resources community-limits.json "$release_backup/"
printf '%s\n' "$release_backup"
```

`.env.community` の `PROBE_IMAGE` だけを次の値へ変更します。

```text
ghcr.io/tikpok/harvest-scope-probe@sha256:9f8561765bd25f6d3c29e908d71cba3f4a7afab77b2206c7b97396950d5a4e4d
```

資源上限とデータボリュームを維持して、専用Probeを再作成します。

```sh
docker compose --env-file .env.community --env-file .env.community.resources -f compose.community-probe.yaml config --quiet
docker compose --env-file .env.community --env-file .env.community.resources -f compose.community-probe.yaml up -d --no-deps --pull never --force-recreate probe
docker compose --env-file .env.community --env-file .env.community.resources -f compose.community-probe.yaml exec -T probe /usr/local/bin/monitor-probe --status
```

更新前と同じProbe ID、イメージのdigest、休止理由、未送信量を確認します。Centralで更新後の観測が届いていることも照合してください。Aを確認してからBを更新します。

## 失敗した端末だけ旧版へ戻す

退避した設定3ファイルを戻し、同じComposeコマンドでProbeだけを再作成します。旧イメージとデータボリュームは保持してください。`--rejoin` や `down --volumes` は、既存のIDや保存データを維持する更新には使いません。

最終更新: 2026年11月5日
