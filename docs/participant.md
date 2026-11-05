# 参加者向け手順

ProbeはNEM・Symbolの公開ノードを検査し、結果をHarvest Scope Network Monitorへ送ります。いつでも停止・撤退できます。稼働保証、障害対応、ログ提出、撤退の事前連絡は不要です。

## 1. 導入に必要なもの

Linux・amd64（x86_64）の端末、Docker Compose、Python 3、OpenSSLを用意してください。以下ではGitを使ってファイルを取得します。arm64の実機受け入れ確認は済んでいません。

受信用ポートの開放、ウォレット、秘密鍵、既存ノードのデータは不要です。Probeは参加用の鍵を自分で作ります。他のコンテナを操作するためのDockerソケットも渡しません。VPS代や通信料は参加者の負担です。

## 2. ファイルと配布情報を確認する

[公式リポジトリ](https://github.com/tikpok/harvest-scope-probe)を取得し、そのフォルダーへ移動します。

```bash
git clone --depth 1 https://github.com/tikpok/harvest-scope-probe.git
cd harvest-scope-probe
```

2026年11月5日時点の試験配布版は`0.1.1-test.20261105.1`、Probeの実行時版は`0.1.1`です。通信プロトコルはv2のままです。NIS1の補助APIが失敗しても、取得済みの対象ノードの高さを保持する修正を含みます。

署名・有効期限・対応CPUを確認します。

```bash
python3 scripts/community-release.py \
  releases/0.1.1-test.20261105.1/release.json \
  releases/0.1.1-test.20261105.1/release.sig \
  release-public.pem --platform linux/amd64
```

成功すると、版番号、対応CPU、`image`などが表示されます。`image`は後で使うため控えてください。`@sha256:...`は、使用するイメージの内容を固定する識別値です。

この版の配布情報は **2026年11月12日12:09:41 JST** まで有効です。期限切れや署名エラーが出た場合は、端末の時計とGitHub上の新しい配布情報を確認し、検証に成功するまで導入・更新へ進まないでください。検証を省略したり、期限を書き換えたりはしません。

公開鍵と検証スクリプトは、公式リポジトリから取得したものを確認して使います。この確認は公式GitHubを信頼することが前提です。署名は配布情報の改変を検出しますが、取得元・公開鍵・検証ツールが一緒に差し替えられた場合まで検出するものではありません。公開鍵を更新するときは、運営の変更案内を確認してください。

署名とイメージの識別値だけでは、以前の正規な配布情報が提示されたことまでは見分けられません。有効期限を設けることで、古い署名済みの案内が長期間使われることを防ぎます。

配布情報の有効期限は、導入・更新に使える案内の期限です。期限を過ぎただけで、すでに動いているProbeが停止したり、自動更新されたりすることはありません。

## 3. 初回の設定を作って起動する

次のコピー操作は初回だけ行います。更新時は、手元の設定を上書きしないでください。

```bash
cp .env.community.example .env.community
cp community-limits.example.json community-limits.json
python3 scripts/community-config.py community-limits.json > .env.community.resources
```

`.env.community`を開き、`PROBE_IMAGE`へ検証結果の`image`をそのまま記入します。送信先の`CENTRAL_API_URL`は `https://monitor-api.7338.work` です。移動するタグの`latest`などは使いません。

以後のコマンドを短くするため、次をコピーして実行してください。このProbe専用のCompose設定で操作します。別のシェルを開いた場合は、もう一度実行します。

```bash
probe_compose() {
  sudo docker compose --env-file .env.community --env-file .env.community.resources -f compose.community-probe.yaml "$@"
}
probe_compose config
```

設定内容を確認し、検証したイメージを手動で取得して起動します。下の値は、検証結果の`image`へ置き換えてください。0.1.1版の検証結果は次の固定値です。

```bash
sudo docker pull 'ghcr.io/tikpok/harvest-scope-probe@sha256:a4e2b6f30d9343c603f36d18cfff7fc783c20e777773eed5c64e56a8a1ef30d2'
probe_compose up -d --no-deps --pull never probe
```

公開GHCRイメージの取得に、GitHubアカウントや`docker login`は不要です。起動時に勝手に別のイメージを取得することはありません。

## 4. 参加状態と役割を確認する

```bash
probe_compose exec -T probe /usr/local/bin/monitor-probe --status
probe_compose logs --since 15m --tail 100 probe
```

初回起動時に参加用の鍵とProbe IDが作られ、`trial`で登録されます。参加状態は次のように分かれます。

| 表示 | 意味 |
| --- | --- |
| `trial` | 試験参加。通常の仕事を受け取り、検査結果を送る段階 |
| `active` | 通常参加。自動承認の条件を満たすか、運営の採用操作で移行した状態 |
| `contributor` | 通常参加とは別に、希望者が運営の確認を受けて付く役割 |

運営側で自動承認を有効にすると、登録後10分以上、3対象以上・6件以上の有効な仕事、最近の稼働通知と観測結果などを確認して、`trial`から`active`へ移行します。**現在の試験運用では自動承認を保留しています。** `trial`のままでも異常ではなく、再登録は不要です。`active`になるためのProbe ID提出も不要です。

`active`になった後、`contributor`を希望する場合だけProbe IDを運営へお知らせください。運営者と端末・接続元の対応を確認します。通常時の補助集計や、運営の観測点が一時停止したときにCentralと組み合わせる判定を補う役割です。複数の独立した運営者の観測とCentralの確認など、運営側の条件がそろった場合に使います。役割は自動では付きません。

状態表示には、資源上限、通信使用量、残り予算、未送信件数、稼働中の仕事、休止理由、参加状態が含まれます。表示できる項目は配布版によって異なります。

| 休止理由 | 意味と対応 |
| --- | --- |
| `resource_pressure` | CPU・メモリなどの負荷が高いため待機。資源が戻れば設定の範囲で再開 |
| `traffic_budget` | 通信予算に到達。翌UTC日まで新しい検査を待機 |
| `spool_full` | 未送信結果の保存枠に到達。送信が進むまで待機 |
| `central_unavailable` | 送信先と通信できない状態。送信先の公開情報とログを確認 |

新しい版で `server.autoAdmissionReason` が表示される場合、`policy_not_enforced` は運営側の切替待ち、`warming_up` は登録後の待機、`waiting_for_results` は検査実績の待機を意味します。`server.admission`は参加状態、`server.trustRole`は任意の役割です。

コンテナの`healthy`は、全対象・全検査の完了を意味しません。休止や未送信が続く場合は、`--status`とログを確認してください。問い合わせ時に鍵・認証情報を公開する必要はありません。

## 5. 資源や通信の上限を変更する

`community-limits.json`を編集します。初期設定と設定できる最大値は次のとおりです。

| 項目 | 初期設定 | 最大値 |
| --- | ---: | ---: |
| `cpuCores` | 0.2 | 1.0 |
| `maxMemoryBytes` | 469762048（448MiB） | 1073741824（1GiB） |
| `maxCpuPercent` | 70 | 100 |
| `limits.maxConcurrency` | 4 | 4 |
| `limits.requestsPerSecond` | 3.0 | 3.0 |
| `limits.burst` | 4 | 4 |
| `limits.dailyTrafficBytes` | 21474836480（20GiB/日） | 同左 |
| `limits.spoolBytes` | 104857600（100MiB） | 同左 |

各項目は個別に下げられます。メモリは32MiB以上、CPU・要求速度は0より大きい数、`maxCpuPercent`は1〜100、それ以外は正の整数で指定します。上限を超える設定はエラーになります。

20GiB/日は通信予算の上限であり、毎日その量を使う指定ではありません。小さい予算も選べますが、到達すると翌UTC日まで新しい検査を止めます。UTCの日付は日本時間の午前9時に変わります。使用量は保守的な見積りを含み、OSで測る総転送量とは一致しません。イメージのダウンロードはこの予算に含まれません。

設定を変えたら、Compose用の値を作り直して再作成します。

```bash
python3 scripts/community-config.py community-limits.json > .env.community.resources
probe_compose up -d --no-deps --pull never --force-recreate probe
```

既存の設定は、配布版を更新しても自動では増量しません。少ない資源では実行できる検査が減り、観測不足になる場合がありますが、増設の義務はありません。メモリ上限はコンテナに適用し、Probeの追加swap使用は許可しません。ホストのswap設定は変更しません。

## 6. 一時停止・再開する

一時停止します。送信先に接続できないときも実行できます。

```bash
probe_compose stop probe
```

同じ鍵・Probe IDで再開します。

```bash
probe_compose start probe
```

一時停止だけなら、撤退や再参加の操作は不要です。

## 7. 撤退・再参加する

撤退するときは、先に停止してから次を実行します。

```bash
probe_compose stop probe
probe_compose run --rm --no-deps --pull never probe --leave
```

`--leave`は専用データに撤退状態を記録し、参加用の鍵を削除します。その後、最大3秒の範囲で送信先へ撤退を通知します。通知に失敗してもローカルの撤退処理は完了します。

専用データも削除する場合だけ、次を実行します。鍵、参加状態、未送信結果、通信予算の記録などが対象です。他のノードやコンテナのデータは削除しません。

```bash
probe_compose down --volumes
```

専用データを削除していない場合は、起動し直しても撤退状態が続きます。再参加するには、停止した状態で次を実行します。

```bash
probe_compose run --rm --no-deps --pull never probe --rejoin
```

新しい鍵とProbe IDが自動で作られ、`trial`から始まります。以前の参加中に取得した未送信の検査結果は削除されます。資源設定と、残っている当日の通信予算の記録はそのまま使います。以前の`contributor`の役割は引き継がず、希望する場合は新しいProbe IDで運営の確認を受け直します。

このコマンドは再参加後も端末上でProbeを動かし続けます。通常のバックグラウンド運転に戻す場合は、`Ctrl+C`で終了してから起動します。

```bash
probe_compose up -d --no-deps --pull never probe
```

専用データをすべて削除した場合は、この再参加コマンドは不要です。設定を確認して起動すると、新しい鍵・Probe IDで`trial`から始まります。

## 8. 手動で更新・復旧する

1. GitHubの新しい配布案内で、変更内容とローカルデータの互換性を確認します。
2. 新しい版の署名・有効期限・対応CPUを検証します。初回設定のコピーは繰り返しません。
3. 現在の設定と旧版のイメージを保持します。公開鍵や検証スクリプトを変更する場合は、変更内容も確認します。
4. 検証結果の`image`を指定して、`sudo docker pull`で手動取得します。
5. `.env.community`の`PROBE_IMAGE`を、その`image`へ変更します。
6. 次を実行して再作成し、`--status`とログを確認します。

```bash
probe_compose up -d --no-deps --pull never --force-recreate probe
probe_compose exec -T probe /usr/local/bin/monitor-probe --status
probe_compose logs --since 15m --tail 100 probe
```

失敗した場合は、配布案内でデータ互換性が確認できる旧版の`PROBE_IMAGE`へ戻し、同じ再作成コマンドを実行します。更新や復旧のために`down --volumes`を実行する必要はありません。

0.1.1版へ更新したら、`--status`の`version`が`0.1.1`であることを確認してください。配布版の`0.1.1-test.20261105.1`とは表記が異なります。古い版が表示される場合は、`PROBE_IMAGE`の指定とコンテナの再作成を確認してください。自動更新や、送信先からの強制的なイメージ更新は行いません。
