# Harvest Scope Community Probe

Harvest Scope の観測に任意で参加する Probe の配布候補です。このリポジトリには Probe と共通ライブラリ、コンテナ定義、参加者向け設定・検証ツールだけを収めています。Central のサーバーコードや運用設定は含みません。

**現段階はプレリリースです。** 実機での受入試験、配布イメージの署名・公開、運営が採用する手続きは完了していません。一般参加者向けの導入案内として扱わないでください。A/B の実機試験にも、このソースから作成した同一候補を使います。 現在、作成・起動確認した候補は amd64 のみで、arm64 は未検証です。

対象は Linux 上の Docker Compose（amd64／arm64）です。署名済み配布情報とイメージ digest の検証、手動での導入・更新・撤退手順は [参加者向け手順](docs/participant.md) に記載しています。Probe は非 root ユーザーで動作し、受信用ポートや Docker ソケットを使いません。標準の資源上限は CPU 0.1 コア、メモリ 256 MiB（追加 swap なし）です。並列数 1、毎秒 0.2 要求・バースト 1、日次通信量と未送信データは各 100 MiB です。参加者が自分の端末で設定を増やす場合に限り、上限を変更できます。

利用条件は [参加用途限定ライセンス](LICENSE) です。Harvest Scopeへの参加に必要な複製・ビルド・実行と、ローカル設定の変更を許可します。参加・停止・撤退は任意で、稼働保証・障害対応・ログ提出は求めません。一般的なソースコード改変や第三者への再配布を許可するライセンスではありません。Centralの利用許諾には適用しません。

開発用の確認コマンドは次のとおりです。

```sh
cargo test --locked --workspace
python3 -m unittest discover -s scripts/tests -p 'test_community_*.py'
docker build -t harvest-scope-community-probe:local .
```

コンテナは `PROBE_MODE=community` で起動します。Compose は検証済み digest を `.env.community` の `PROBE_IMAGE` に指定する方式で、イメージの取得や更新を自動では行いません。
