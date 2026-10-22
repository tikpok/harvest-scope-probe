# 既存A/B相当の処理能力：0.1.0-test.20261022.2

eMudhra対応済みコミット `8a9c471` に処理能力変更 `8607451a2b4218069f3ea67de6f7c001679330c5` を重ねたamd64向け試験版です。CA更新・問題の12チェーンの拒否・正常チェーンの受理を維持しています。arm64と、2台の実機での継続運転の受入は未完了です。

## 変更と検証

- 旧A/Bと同じ同時4件、毎秒3件、burst 4。CPUは0.2コア、メモリ448MiB、追加swapなし。
- 通信予算は5GiB/日、未送信結果は100MiB。各項目はローカル設定で個別に増減できます。
- 検査中にCentralを待つ際も他の検査が進み、完了後は空き枠通知・結果送信・再貸出を即時に行います。資源監視の15秒周期は維持します。
- 配布用ソースとDockerfile内のRustテスト247件が成功。従来のignored 3件は除外したままです。Pythonテスト11件、非root・読み取り専用・外部通信禁止のイメージでの設定確認も成功しています。
- Centralの専用DBでは、異なるIPの採用済み2台・350対象・模擬観測5,700件を約104秒で永続受領し、設定した検査の2地点分が揃いました。これは実機のCPU・通信・長時間運転の保証とは区別します。
- Centralの対象別予約枠修正も反映しています。公開判定はshadowを維持しています。

固定イメージ：

```text
ghcr.io/tikpok/harvest-scope-probe@sha256:29037a6612c51a41607c7b5b755153b437a826005085580d74d0f7d83d8d7e81
```

プロトコルv2・実行時版0.1.0・端末鍵・Probe ID・状態形式は維持します。旧方式A/Bのコンテナへ直接上書きする版ではありません。専用のCommunity Probeだけを更新します。

## 手動更新

専用リポジトリの `main` を取得し、信頼済みの公開鍵で新しい配布情報を検証します。

```sh
git pull --ff-only
python3 scripts/community-release.py releases/0.1.0-test.20261022.2/release.json releases/0.1.0-test.20261022.2/release.sig release-public.pem --platform linux/amd64
docker pull ghcr.io/tikpok/harvest-scope-probe@sha256:29037a6612c51a41607c7b5b755153b437a826005085580d74d0f7d83d8d7e81
```

現在の設定を控えてから、専用Probeを停止します。Docker操作にsudoが必要な端末では付けて実行します。

```sh
cp .env.community .env.community.before-capacity
cp .env.community.resources .env.community.resources.before-capacity
cp community-limits.json .env.community.limits.before-capacity
docker compose --env-file .env.community --env-file .env.community.resources -f compose.community-probe.yaml stop probe
```

`.env.community` の `PROBE_IMAGE` を上記の検証済みdigestへ変更します。以前の `community-limits.json` は自動変更されません。新しい既定能力へ揃える場合は次を実行します。個別調整を残す場合はコピーせず、必要な項目だけ編集してください。

```sh
cp community-limits.example.json community-limits.json
python3 scripts/community-config.py community-limits.json > .env.community.resources
docker compose --env-file .env.community --env-file .env.community.resources -f compose.community-probe.yaml config
docker compose --env-file .env.community --env-file .env.community.resources -f compose.community-probe.yaml up -d --no-deps --pull never --force-recreate probe
docker compose --env-file .env.community --env-file .env.community.resources -f compose.community-probe.yaml exec -T probe /usr/local/bin/monitor-probe --status
```

端末鍵と未送信結果のボリュームは保持します。稼働状態に加え、設定値、通信残量、未送信件数、休止理由、Centralでの検査別の観測時刻を確認します。試験状態では検査項目が制限され、公開判定にも使われません。運営による採用と、2地点の送信元の確認後に処理能力の実機比較を行います。

## 手動復旧

問題時は専用Probeを停止し、控えた3ファイルを元の名前へ戻して同じ再作成コマンドを実行します。前版イメージは削除せず保持してください。eMudhra対応済みの `0.1.0-test.20261022.1` を復旧先にできます。対応前の `0.1.0-test.20261021` は通常の復旧先に使用しません。

5GiB/日は旧Aの短期間の概算を基にした試験値です。残量と観測鮮度を見ながら参加者が調整します。Centralが上限を引き上げたり、イメージを自動更新したりすることはありません。
