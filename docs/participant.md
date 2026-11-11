# 参加者向け手順

ProbeはNEM・Symbolの公開ノードを検査し、結果をHarvest Scope Network Monitorへ送るプログラムです。参加・停止・撤退は自由です。サーバー代や通信料は参加者の負担です。

初めて導入する場合は、1〜4の順に進めてください。設定変更や停止・更新は、5以降を参照してください。

## 1. 導入に必要なもの

- Linuxのx86_64環境（arm64は動作確認が済んでいません）
- Docker Compose、Python 3、OpenSSL、Git

受信用ポートの開放や、ウォレット・既存ノードの用意は不要です。

## 2. ファイルを取得して配布情報を確認する

[公式リポジトリ](https://github.com/tikpok/harvest-scope-probe)を取得し、そのフォルダーへ移動します。

```bash
git clone --depth 1 https://github.com/tikpok/harvest-scope-probe.git
cd harvest-scope-probe
```

試験配布版`0.1.1-test.20261112.1`の署名・有効期限・対応CPUを確認します。

```bash
python3 scripts/community-release.py \
  releases/0.1.1-test.20261112.1/release.json \
  releases/0.1.1-test.20261112.1/release.sig \
  release-public.pem --platform linux/amd64
```

成功すると、使用するイメージが`image`に表示されます。この値を次の設定で使います。

この版の配布情報は2026年11月19日08:11:31 JSTまで有効です。

期限切れや署名エラーが出た場合は、端末の時計とGitHub上の新しい配布情報を確認してください。検証に成功するまで導入・更新には進まないでください。

有効期限は導入・更新に使う配布情報の期限です。期限を過ぎても、稼働中のProbeは停止しません。

## 3. 初回の設定を作って起動する

次のコピー操作は初回だけ行います。更新時は設定を上書きしないでください。

```bash
cp .env.community.example .env.community
cp community-limits.example.json community-limits.json
python3 scripts/community-config.py community-limits.json > .env.community.resources
```

`.env.community`を開き、`PROBE_IMAGE`に検証結果の`image`をそのまま記入します。送信先の`CENTRAL_API_URL`は`https://monitor-api.7338.work`です。

続けて、操作用の関数を登録し、設定を表示します。以後のコマンドは、このフォルダーで実行してください。別のシェルを開いた場合は、関数をもう一度登録します。

```bash
probe_compose() {
  sudo docker compose --env-file .env.community --env-file .env.community.resources -f compose.community-probe.yaml "$@"
}
probe_compose config
```

設定を確認し、イメージを取得して起動します。次の取得先が、検証結果の`image`と一致することを確認してください。

```bash
sudo docker pull 'ghcr.io/tikpok/harvest-scope-probe@sha256:a4e2b6f30d9343c603f36d18cfff7fc783c20e777773eed5c64e56a8a1ef30d2'
probe_compose up -d --no-deps --pull never probe
```

ダウンロードにGitHubアカウントや`docker login`は不要です。

## 4. 稼働を確認する

```bash
probe_compose exec -T probe /usr/local/bin/monitor-probe --status
probe_compose logs --since 15m --tail 100 probe
```

初回起動時に参加用の鍵とProbe IDが自動で作られ、通常参加（`active`）として検査を始めます。`contributor`の役割も自動で付きます。IDの申請・提出や運営者の確認は不要です。

`server.admission`が`active`、`server.trustRole`が`contributor`なら通常参加です。送信先が移行・比較中の場合は`trial`（試験参加）で待機します。再登録する必要はありません。

状態表示では、通信使用量、未送信件数、休止理由などを確認できます。検査が止まっている場合は、次を確認してください。

| 休止理由 | 意味と対応 |
| --- | --- |
| `resource_pressure` | CPU・メモリの負荷が高いため待機。負荷が下がれば再開します |
| `traffic_budget` | 通信予算に到達。次の日本時間午前9時まで新しい検査を待ちます |
| `spool_full` | 未送信結果の保存枠に到達。送信が進むまで待機します |
| `central_unavailable` | 送信先と通信できません。ログと送信先の公開情報を確認してください |

## 5. 資源や通信の上限を変更する

`community-limits.json`を編集します。変更したい項目だけ調整してください。

| 項目（設定名） | 初期設定 | 最大値 |
| --- | ---: | ---: |
| CPU（`cpuCores`） | 0.2コア | 1コア |
| メモリ（`maxMemoryBytes`） | 469762048（448MiB） | 1073741824（1GiB） |
| CPU負荷で休止する目安（`maxCpuPercent`） | 70% | 100% |
| 同時検査数（`limits.maxConcurrency`） | 4 | 4 |
| 毎秒の要求数（`limits.requestsPerSecond`） | 3.0 | 3.0 |
| 一時的な連続要求数（`limits.burst`） | 4 | 4 |
| 1日の通信予算（`limits.dailyTrafficBytes`） | 21474836480（20GiB） | 同左 |
| 未送信結果の保存枠（`limits.spoolBytes`） | 104857600（100MiB） | 同左 |

メモリ・通信予算・保存枠はバイト数で指定します。メモリは32MiB以上、CPUと毎秒の要求数は0より大きい数、`maxCpuPercent`は1〜100、それ以外は正の整数で指定してください。

毎日20GiBを使うわけではありません。通信予算に達すると、次の日本時間午前9時まで新しい検査を止めます。通信使用量は見積りを含み、OSの総転送量とは一致しません。イメージのダウンロードも予算には含まれません。

変更後は、設定を反映してコンテナを作り直します。

```bash
python3 scripts/community-config.py community-limits.json > .env.community.resources
probe_compose up -d --no-deps --pull never --force-recreate probe
```

更新しても、手元の資源設定が自動で増えることはありません。

## 6. 一時停止・再開する

一時停止します。送信先と通信できない場合も実行できます。

```bash
probe_compose stop probe
```

同じProbe IDで再開します。

```bash
probe_compose start probe
```

一時停止だけなら、撤退や再参加の操作は不要です。

## 7. 撤退・再参加する

撤退するときは、停止してから次を実行します。参加用の鍵が削除され、撤退状態が記録されます。送信先への通知に失敗しても、端末側の撤退処理は完了します。

```bash
probe_compose stop probe
probe_compose run --rm --no-deps --pull never probe --leave
```

Probe専用の保存データも削除したい場合だけ、次を実行します。参加状態・未送信結果・通信使用量の記録が消えます。

```bash
probe_compose down --volumes
```

保存データを残した場合は、起動し直しても撤退状態が続きます。再参加するには、停止した状態で次を実行してください。新しい鍵とProbe IDが作られ、以前の未送信結果は削除されます。設定と当日の通信使用量の記録は引き継ぎます。

```bash
probe_compose run --rm --no-deps --pull never probe --rejoin
```

このコマンドは再参加後も動き続けます。`Ctrl+C`で終了してから、通常の起動に戻します。

```bash
probe_compose up -d --no-deps --pull never probe
```

保存データをすべて削除した場合は、再参加コマンドは不要です。設定を確認して起動すると、新しいProbe IDで参加します。

## 8. 手動で更新・復旧する

既存のProbeのフォルダーで、配布情報を取得します。

```bash
git pull --ff-only
```

配布情報だけの更新で、検証結果の`image`が現在の`PROBE_IMAGE`と同じなら、コンテナの再作成は不要です。イメージを変更するときは、次の順に進めます。

1. GitHubの新しい配布案内で、変更内容と保存データの互換性を確認します。
2. 「2. ファイルを取得して配布情報を確認する」と同じ方法で、新しい配布情報の署名・有効期限・対応CPUを検証します。
3. 現在の設定と旧版のイメージを残し、検証結果の`image`を指定して`sudo docker pull`で取得します。
4. `.env.community`の`PROBE_IMAGE`を新しい`image`に変更し、次を実行します。

```bash
probe_compose up -d --no-deps --pull never --force-recreate probe
probe_compose exec -T probe /usr/local/bin/monitor-probe --status
probe_compose logs --since 15m --tail 100 probe
```

初回設定のコピーは繰り返さないでください。更新は手動です。

失敗した場合は、配布案内でデータ互換性を確認した旧版の`PROBE_IMAGE`へ戻し、同じコマンドで再作成します。更新や復旧のために`down --volumes`を実行すると保存データが消えるため、使わないでください。

不具合や手順の質問は[GitHub Issues](https://github.com/tikpok/harvest-scope-probe/issues)へお寄せください。鍵や認証情報は投稿しないでください。
