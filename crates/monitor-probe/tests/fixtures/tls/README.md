# TLS回帰テスト用の公開証明書

2026年10月22日に取得した公開証明書チェーンです。秘密鍵は含みません。

- `*-emudhra.we-have-a-problem.com.chain`: https://emudhra.we-have-a-problem.com/ が提供する12種類の実証用チェーン。DV・EV・OV、G3B・G3C、直接署名・AAAクロス署名を検証します。
- `www.python.org.chain`: 正常なHTTPSを引き続き検証できることの確認用です。

テストはネットワークに接続せず、取得日の固定時刻でrustlsの実際の証明書検証を実行します。証明書の将来の失効期限で結果が変わらないようにしています。
