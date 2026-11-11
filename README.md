# Harvest Scope Network Monitor Probe

NEM・Symbolの公開ノードを検査し、結果を[Harvest Scope Network Monitor](https://monitor-api.7338.work/)へ送るプログラムです。

IDの申請は不要です。参加・停止・撤退は自由で、サーバー代や通信料は参加者の負担です。

試験配布版：[`0.1.1-test.20261112.1`](releases/0.1.1-test.20261112.1/README.md)

## 参加するには

Linuxのx86_64環境とDocker Composeが必要です。arm64は動作確認が済んでいません。受信用ポートの開放や、ウォレット・既存ノードの用意は不要です。

[参加者向け手順](docs/participant.md)に沿って導入してください。ダウンロードにGitHubアカウントは不要です。更新は手動で行います。

## 初期設定の上限

| 項目 | 設定 |
| --- | --- |
| CPU | 0.2コア |
| メモリ | 448MiB |
| 通信予算 | 20GiB/日 |

毎日20GiBを使うわけではありません。これらの上限は下げられます。変更方法や停止・撤退の操作は[参加者向け手順](docs/participant.md)を参照してください。

不具合や手順の質問は[GitHub Issues](https://github.com/tikpok/harvest-scope-probe/issues)へお寄せください。鍵や認証情報は投稿しないでください。

## ライセンス

[参加用途限定ライセンス](https://github.com/tikpok/harvest-scope-probe/blob/main/LICENSE)です。一般的な改変・再配布を認めるオープンソースライセンスではありません。利用前に全文を確認してください。
