# 証明書修正版 0.1.0-test.20261022.1

amd64向けの署名付き試験配布版です。一般配布の実機受入試験とarm64の検証は未完了です。旧方式A/Bの稼働コンテナは、この公開作業では変更していません。

## 修正内容

- `rustls-webpki`: 0.103.15 → 0.103.16
- `webpki-roots`: 1.0.8 → 1.0.10
- OS証明書: Debian `ca-certificates 20250419~deb12u2`
- プロトコルv2・実行時版0.1.0・既存の状態形式を維持

イメージは次のダイジェストで固定しています。

```text
ghcr.io/tikpok/harvest-scope-probe@sha256:7a36dce48ef02bd72c5a9799e5e0748f1cc074d5e5ee277ddb8c138168282447
```

ビルド元はコミット `7431ba5e53f3f19ce32bd0b8459c584255d25cbe` です。公開先から資格情報なしで取得し、イメージのソースラベル・linux/amd64・UID/GID 10001:10001が一致することを確認しました。

## 実施した検証

- 更新前の依存では、問題の12チェーン中6件を受け入れることを再現
- 更新後は、rustlsで12件すべてを不信任または発行元不明として拒否し、正常なpython.orgのチェーンを受理
- 公開配布リポジトリの `cargo test --locked --release --workspace` が成功。既存の容量試験1件は従来どおりignoredで未実施
- 配布ツールのPythonテスト9件が成功
- Dockerfile内でもRustテストを実行してから実行ファイルをビルド
- コンテナ内のOS証明書束も12件を拒否し、正常チェーンの検証に成功
- 外部通信禁止・非root・読み取り専用・権限削除の条件で `--status` に成功

今回はローカルDockerでビルド・検証し、GHCRへ公開しました。タグを契機とするGitHub Actionsは実行が作成されていないため、Actionsによる公開経路の実行確認は未完了です。

## 既存Community Probeの手動更新

初回導入や資源設定は[参加者向け手順](../../docs/participant.md)を参照してください。旧方式A/Bへの上書き更新には使いません。

1. `release.json` と `release.sig` を取得し、すでに信頼を確認した公開鍵で検証します。公開鍵を更新ファイルと一緒に無条件で差し替えないでください。

```sh
python3 scripts/community-release.py releases/0.1.0-test.20261022.1/release.json releases/0.1.0-test.20261022.1/release.sig release-public.pem --platform linux/amd64
```

2. 検証結果が上記ダイジェストと一致したら、そのダイジェストを指定して取得します。

```sh
docker pull ghcr.io/tikpok/harvest-scope-probe@sha256:7a36dce48ef02bd72c5a9799e5e0748f1cc074d5e5ee277ddb8c138168282447
```

3. 現在の `.env.community` を控え、同ファイルの `PROBE_IMAGE` を検証済みの値へ変更します。端末鍵・状態・未送信データのボリュームは保持します。

```dotenv
PROBE_IMAGE=ghcr.io/tikpok/harvest-scope-probe@sha256:7a36dce48ef02bd72c5a9799e5e0748f1cc074d5e5ee277ddb8c138168282447
```

4. 専用の作業ディレクトリで、Probeだけを再作成します。

```sh
docker compose --env-file .env.community --env-file .env.community.resources -f compose.community-probe.yaml up -d --no-deps --pull never --force-recreate probe
```

5. `--status`、コンテナのhealth、未送信件数を確認します。運営側でも稼働通知と新しい観測の到着を確認してください。

```sh
docker compose --env-file .env.community --env-file .env.community.resources -f compose.community-probe.yaml exec -T probe /usr/local/bin/monitor-probe --status
```

問題があれば、控えた前版の `PROBE_IMAGE` に戻して同じ再作成操作を行います。旧試験版は今回の証明書対策前なので、緊急復旧後は修正版への更新を改めて行ってください。

配布情報の有効期限は `release.json` に記載しています。期限切れの場合は、有効な署名付き配布情報を確認してから更新してください。
