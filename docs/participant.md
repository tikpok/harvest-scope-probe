# 任意参加Probeの使い方

Centralを運営する人と、観測を手伝う人を分けた方式です。参加者はいつでも停止・撤退できます。稼働保証、障害対応、ログ提出、撤退の事前連絡は不要です。監視先からの問い合わせは配布情報に記載された運営窓口が担当します。

対象はLinuxのDocker Compose、amd64／arm64です。配布者がその版・アーキテクチャを検証し、署名付きの配布情報を公開してから導入してください。リポジトリのコードがあるだけでは、一般配布の受入試験が済んだことにはなりません。

## 導入

専用の作業ディレクトリに、次を用意します。

- `compose.community-probe.yaml`
- `.env.community.example` と `community-limits.example.json`
- `scripts/community-config.py` と `scripts/community-release.py`
- 選んだ版の `release.json`、`release.sig`
- 別経路で確認した運営の署名公開鍵 `release-public.pem`

Python 3とOpenSSLは管理端末で使用します。Probe自体に管理端末の権限を渡す必要はありません。公開鍵を更新ファイルと一緒に無条件で差し替えないでください。

署名と対象アーキテクチャを検証します。arm64では最後の値を `linux/arm64` に変えます。

```sh
python3 scripts/community-release.py release.json release.sig release-public.pem --platform linux/amd64
cp .env.community.example .env.community
cp community-limits.example.json community-limits.json
python3 scripts/community-config.py community-limits.json > .env.community.resources
```

検証結果の `image` を `.env.community` の `PROBE_IMAGE` に記入します。値は `registry/path@sha256:...` です。`latest`などの移動するタグは使いません。CentralのURLも同じファイルで確認します。

以下の関数は、この専用Composeだけを操作するための省略記法です。

```sh
probe_compose() {
  docker compose --env-file .env.community --env-file .env.community.resources -f compose.community-probe.yaml "$@"
}
probe_compose config
```

検証したダイジェストを明示してイメージを手動取得し、起動します。次の例のプレースホルダーは配布情報の値へ置き換えます。

```sh
docker pull 'REGISTRY/PATH@sha256:VERIFIED_DIGEST'
probe_compose up -d --no-deps --pull never probe
```

初回起動時に端末鍵とProbe IDが登録されます。試験状態で始まり、運営が整合性と観測元を確認して採用するまでは公開判定の裏付けには使われません。受信用ポート、ウォレット、ノードのデータ、Dockerソケットは不要です。

## 状態確認

```sh
probe_compose exec -T probe /usr/local/bin/monitor-probe --status
probe_compose logs --since 15m --tail 100 probe
```

設定上限、通信使用量、残り予算、未送信件数、稼働中の仕事、休止理由、採用状態、版の互換性、配布情報のURLを表示します。Centralが一時停止している間も、ローカルの状態を確認できます。

`resource_pressure` は資源不足、`traffic_budget` は当日の通信予算到達、`spool_full` は保存枠不足を示します。自動的に仕事の取得を抑える状態です。増設をお願いする通知ではありません。資源と通信予算が戻れば、設定の範囲内で再開します。通信予算の日付はUTCで区切ります。

通信量は、測定先・Centralへの要求前に予約し、応答の大きさと保守的な通信オーバーヘッドで精算します。OSのインターフェースで測った厳密な総転送量とは区別してください。イメージ取得は管理端末の手動操作であり、この予算に含みません。

## 資源を変更する

資源設定は `community-limits.json` にまとめます。

| 設定 | 既定値 |
|---|---:|
| `cpuCores` | 0.1 |
| `maxMemoryBytes` | 268435456 |
| `limits.maxConcurrency` | 1 |
| `limits.requestsPerSecond` | 0.2 |
| `limits.burst` | 1 |
| `limits.dailyTrafficBytes` | 104857600 |
| `limits.spoolBytes` | 104857600 |

必要な項目だけ変更し、Composeの値を再生成して手動で再作成します。

```sh
python3 scripts/community-config.py community-limits.json > .env.community.resources
probe_compose up -d --no-deps --pull never --force-recreate probe
```

CPU・メモリの制限はコンテナに適用されます。メモリとswapの合計上限を同じ値にするため、Probeの追加swap使用は許可しません。ホストのswap設定は変えません。設定を下げて実行できなくなった仕事は期限切れで解放され、対象ノードの障害には数えません。ログは10MiBを3本まで保持します。

メモリ判定では回収可能なキャッシュと作業用メモリを分け、メモリ待ち時間も確認します。休止後は安定したサンプルが続いてから再開します。資源ファイルを読む操作を、再開のために繰り返す必要はありません。

## 一時停止・再開・撤退

停止はCentralが不通でも完了します。明示的に停止したコンテナは自動再開しません。

```sh
probe_compose stop probe
```

再開するときは手動で実行します。

```sh
probe_compose start probe
```

撤退は先に停止し、必要なら接続可能な範囲で資格の失効を通知します。`--leave` はローカルに撤退状態を保存し、鍵を削除した後で、最大3秒の通知を試みます。

```sh
probe_compose stop probe
probe_compose run --rm --no-deps --pull never probe --leave
probe_compose down --volumes
```

削除するのはこの専用Composeのコンテナと専用データだけです。他のノードやコンテナは操作しません。再参加では新しい端末鍵が作られ、再び試験状態から始まります。削除せず撤退したデータを使う場合は、停止後に `--rejoin` を明示してください。以前の端末鍵に属する未送信結果は引き継ぎません。

## 手動更新・手動復旧

1. 新しい版の変更内容・互換性・署名・ダイジェストを確認します。
2. 上記の検証コマンドで署名を検証します。
3. 現在の `.env.community` を前版用に控えます。前版イメージも保持します。
4. 新しいダイジェストを指定して `docker pull` を手動実行します。
5. `.env.community` の `PROBE_IMAGE` を検証した値へ変更します。
6. `probe_compose up -d --no-deps --pull never --force-recreate probe` を実行します。
7. `--status` とログで状態を確認します。

失敗した場合は `PROBE_IMAGE` を保持した前版のダイジェストへ戻し、同じ再作成コマンドを手動で実行します。復旧可能な版とローカルデータの互換性は配布情報で確認してください。自動更新、起動時のイメージ取得、Docker全体の設定変更は行いません。Centralが互換性のない版への仕事を停止しても、イメージを強制更新することはありません。
