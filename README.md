# Harvest Scope Community Probe

Harvest Scope の観測に任意で参加する Probe の配布候補です。このリポジトリには Probe と共通ライブラリ、コンテナ定義、参加者向け設定・検証ツールだけを収めています。Central のサーバーコードや運用設定は含みません。

**現段階はプレリリースです。** 実機での受入試験、運営が採用する手続きは完了していません。一般参加者向けの導入案内として扱わないでください。A/B の実機試験にも、このソースから作成した同一候補を使います。 現在、作成・起動確認した候補は amd64 のみで、arm64 は未検証です。

対象は Linux 上の Docker Compose（amd64／arm64）です。署名済み配布情報とイメージ digest の検証、手動での導入・更新・撤退手順は [参加者向け手順](docs/participant.md) に記載しています。Probe は非 root ユーザーで動作し、受信用ポートや Docker ソケットを使いません。標準の資源上限は CPU 0.1 コア、メモリ 256 MiB（追加 swap なし）です。並列数 1、毎秒 0.2 要求・バースト 1、日次通信量と未送信データは各 100 MiB です。参加者が自分の端末で設定を増やす場合に限り、上限を変更できます。

利用条件は [参加用途限定ライセンス](LICENSE) です。Harvest Scopeへの参加に必要な複製・ビルド・実行と、ローカル設定の変更を許可します。参加・停止・撤退は任意で、稼働保証・障害対応・ログ提出は求めません。一般的なソースコード改変や第三者への再配布を許可するライセンスではありません。Centralの利用許諾には適用しません。

開発用の確認コマンドは次のとおりです。

```sh
cargo test --locked --workspace
python3 -m unittest discover -s scripts/tests -p 'test_community_*.py'
docker build -t harvest-scope-community-probe:local .
```

コンテナは `PROBE_MODE=community` で起動します。Compose は検証済み digest を `.env.community` の `PROBE_IMAGE` に指定する方式で、イメージの取得や更新を自動では行いません。

配布情報の署名は [release-public.pem](release-public.pem) で検証します。初回は、運営から別経路で受け取った公開鍵の指紋と照合してください。公開鍵のDER表現のSHA-256は次の値です。

```text
b0792a1a7712145c21ece1756131993a4b2a690df381b06c81d02956891cf12b
```

**証明書更新の注意:** `0.1.0-test.20261021` はeMudhraの不信任対応前です。新規導入・通常更新には使用せず、修正版の署名付き配布情報を使用してください。

参考として、旧A/B実機試験用の候補版は `0.1.0-test.20261021` です。[署名済み配布情報](releases/0.1.0-test.20261021/release.json) と [署名](releases/0.1.0-test.20261021/release.sig) を使用し、次のコマンドで検証します。実行ファイルが通知する版は `0.1.0`、対応プロトコルは v2 です。

```sh
python3 scripts/community-release.py releases/0.1.0-test.20261021/release.json releases/0.1.0-test.20261021/release.sig release-public.pem --platform linux/amd64
```

一般配布の受入試験は未完了です。旧A/Bのイメージ・設定・データを保持し、別の専用Composeで試験してください。更新・復旧は[参加者向け手順](docs/participant.md#手動更新手動復旧)のとおり、検証したdigestを指定して手動で行います。

## 証明書対策と配布ビルド

`rustls-webpki 0.103.16` と `webpki-roots 1.0.10` を使用し、問題の12種類の証明書チェーンを拒否するオフライン回帰テストと、正常な証明書の検証を追加しました。コンテナのOS証明書も更新しています。ホストOSの更新だけでは、既存Probeの実行ファイルに組み込まれたCA情報は変わりません。

`0.*` タグの公開時にGitHub Actionsがamd64イメージをビルドします。Rust・配布ツールのテストと、外部通信を禁止した非root・読み取り専用での起動確認が成功した場合だけGHCRへ公開します。署名鍵はActionsへ渡しません。公開後にimage digestとビルド元コミットを確定し、既存の信頼済み鍵で配布情報を署名します。参加端末の更新は引き続き手動です。
