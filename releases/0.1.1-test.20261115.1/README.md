# 最終化をブロック数で判定する試験版 0.1.1-test.20261115.1

既存Community Probeのlinux/amd64環境向けです。対象ノードの最終化高と参照ノード群の最終化高の差を測り、60ブロック以上でWarning、120ブロック以上でCriticalと判定します。ネットワーク全体の停止も、最終化が進まない間にチェーンが進んだブロック数で判定します。

[ソース](https://github.com/tikpok/harvest-scope-probe/commit/72029bf9e9c355cf0aa61dbbaaab677834594f2f)、[配布情報](release.json)、[署名](release.sig)を確認してください。実行時版は0.1.1、通信プロトコルはv2です。Probe ID、登録鍵、保存形式、通信・資源上限を引き継ぎます。

## 更新直後の判定には観測が必要です

対象ノードの遅れは、その時点の最終化高から求めます。ネットワーク全体の停止は履歴が必要なため、最初の観測だけでは判定できません。その後の観測で、最終化の停止とチェーンの進行を確認します。

観測間隔が30分を超えた場合や接続先が変わった場合は、停止の履歴を取り直します。Finalizationの証拠は日次対象も30分で期限切れとなり、確認に使う観測は15分以内にそろえる必要があります。日次対象の実行頻度は変わりません。更新直後の判定不能を正常判定として扱わないでください。

## 署名を検証してから取得します

専用Probeのディレクトリで実行します。既存の信頼済み公開鍵を使ってください。Dockerにsudoが必要な端末では、Dockerコマンドに付けてください。

```sh
git pull --ff-only
python3 scripts/community-release.py releases/0.1.1-test.20261115.1/release.json releases/0.1.1-test.20261115.1/release.sig release-public.pem --platform linux/amd64
python3 scripts/community-config.py community-limits.json
docker pull ghcr.io/tikpok/harvest-scope-probe@sha256:f46ba12ba3feced83a08f90d4deda8b16071cdff0ac26911b76f0b1f1ac92580
```

署名検証が失敗した場合は更新を中止します。配布情報の有効期限は発行から7日間です。

## 設定を退避し、Aを確認してからBへ進みます

現在の設定を退避し、表示された退避先を控えてください。

```sh
release_backup=$(mktemp -d "$PWD/.env.community.backup.XXXXXXXX")
cp -p .env.community .env.community.resources community-limits.json "$release_backup/"
printf '%s\n' "$release_backup"
```

`.env.community`の`PROBE_IMAGE`だけを次の値へ変更します。

```text
ghcr.io/tikpok/harvest-scope-probe@sha256:f46ba12ba3feced83a08f90d4deda8b16071cdff0ac26911b76f0b1f1ac92580
```

資源上限とデータボリュームを維持して、専用Probeを再作成します。

```sh
docker compose --env-file .env.community --env-file .env.community.resources -f compose.community-probe.yaml config --quiet
docker compose --env-file .env.community --env-file .env.community.resources -f compose.community-probe.yaml up -d --no-deps --pull never --force-recreate probe
docker compose --env-file .env.community --env-file .env.community.resources -f compose.community-probe.yaml exec -T probe /usr/local/bin/monitor-probe --status
```

更新前と同じProbe ID、イメージのdigest、休止理由、未送信量を確認します。Centralへ新しいブロック数の証拠が届き、観測が続くことも照合してください。Aの確認を終えてからBを更新します。署名・ビルドの検証だけでは、A/B実機の受入確認は完了しません。

## 失敗した端末だけ旧版へ戻します

退避した設定3ファイルを戻し、同じComposeコマンドでProbeだけを再作成してください。旧イメージとデータボリュームは保持します。`--rejoin`や`down --volumes`はIDや保存データを失うおそれがあるため、この更新には使いません。

不明点は[参加者向け手順](../../docs/participant.md)と[GitHub Issues](https://github.com/tikpok/harvest-scope-probe/issues)を参照してください。鍵や認証情報は投稿しないでください。
