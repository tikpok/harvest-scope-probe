# 結果署名と設定上限の試験配布：0.1.0-test.20261030.1

既存Community Probeを同じID・登録鍵・保存データのまま更新するamd64向け試験版です。[ビルド元ソース](https://github.com/tikpok/harvest-scope-probe/commit/0230769d7ed29e39d56aec0ac7faaaab32f11c05)から作成しました。

## 変更と確認

- 結果本文へ既存の登録鍵で署名します。署名は送信元と本文の対応を確認するもので、観測内容の正しさや参加資格の証明にはなりません。信頼の付与は運営側の別手続きです。
- 設定の最大値は同時4件、毎秒3要求、burst4、20GiB/日、未送信100MiB、CPU1コア、メモリ1GiBです。CPU0.2・448MiBの既定値と上限内の既存設定を維持します。通信予算は予約額を含み、OSの実転送量とは区別します。
- rustls0.23.46、Tokio1.54.1、quinn-proto0.11.18、chacha200.10.2へ更新。証明書対策の回帰試験とOS証明書更新を維持し、perl-base・libpcre2・tzdataも更新しています。
- Rust252件、Python13件が成功しました。従来のignored3件は除外しています。コンテナ内のテスト・ビルド、通信なし・非root・読取専用・CPU0.2・448MiBでの起動と状態表示を確認済みです。arm64と更新後の実機継続受入は未完了です。
- cargo-auditに残るRSAの指摘は任意のMySQL依存由来で、配布Probeの実際の依存経路に含まれないことを確認しました。

固定イメージは次のとおりです。実行時版は互換性のため `0.1.0` のままです。

```text
ghcr.io/tikpok/harvest-scope-probe@sha256:70c340e6a37ed069b631d5f1c272ececadc0352ab3d44ec438f1aca16e68da4f
```

## 手動更新

専用Community Probeのディレクトリで実行します。Dockerにsudoが必要な端末では付けてください。まず配布情報の署名・有効期限・amd64対応を、既存の信頼済み公開鍵で確認します。

```sh
git pull --ff-only
python3 scripts/community-release.py releases/0.1.0-test.20261030.1/release.json releases/0.1.0-test.20261030.1/release.sig release-public.pem --platform linux/amd64
python3 scripts/community-config.py community-limits.json
docker pull ghcr.io/tikpok/harvest-scope-probe@sha256:70c340e6a37ed069b631d5f1c272ececadc0352ab3d44ec438f1aca16e68da4f
```

現在の設定3ファイルを、権限700の新しいディレクトリへ控えます。控えの場所を記録してください。

```sh
release_backup=$(mktemp -d "$PWD/.env.community.backup.XXXXXXXX")
cp -p .env.community .env.community.resources community-limits.json "$release_backup/"
printf '%s\n' "$release_backup"
```

`.env.community` の `PROBE_IMAGE` だけを上記の固定イメージへ変更します。上限内の資源設定はそのまま引き継ぎます。設定を確認してから、ボリュームを保持して専用Probeだけを再作成します。

```sh
docker compose --env-file .env.community --env-file .env.community.resources -f compose.community-probe.yaml config --quiet
docker compose --env-file .env.community --env-file .env.community.resources -f compose.community-probe.yaml up -d --no-deps --pull never --force-recreate probe
docker compose --env-file .env.community --env-file .env.community.resources -f compose.community-probe.yaml exec -T probe /usr/local/bin/monitor-probe --status
```

更新前と同じProbe IDであること、コンテナのimage digest、休止理由・未送信量、Centralでの新しい仕事の発行と検査別の結果到着を確認します。既存A/Bを維持し、Aの信頼設定やCentralの切替は段階ごとに確認します。

## 手動復旧

更新に失敗した場合は、専用Probeを停止して控えた3設定ファイルを戻し、同じボリュームで再作成します。復旧用の直前イメージを保持してください。前版 `0.1.0-test.20261022.3` は証明書対策済みですが、今回の結果署名・設定上限は備えていません。Aへ信頼権限を付与した後は運営側の切戻しと合わせて対応します。
