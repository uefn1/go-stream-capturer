# go-stream-capturer

GitHub ActionsとGitHub Releasesを用いた完全自動・サーバーレスのm3u8ストリーミング録画システム。

## 使い方3ステップ

1. **editor-m3u.ymlでチャンネル登録**  
   GitHubのActionsタブから `Editor M3U` ワークフローを手動実行(`workflow_dispatch`)し、録画したいm3u8のURL等を登録します。

2. **checker.ymlを手動1回起動**  
   Actionsタブから `Checker` ワークフローを起動します。このデーモンが60分間待機・監視・自身を再起動して録画の準備を整えます。

3. **reserve.ymlで予約**  
   Actionsタブから `Reserve` ワークフローで録画したいチャンネルと時間（JST）、分数を指定して予約キューを生成します。開始時刻になると自動的にGo言語を用いたダウンローダーが作動し、1録画1Releaseとしてアップロードされます。
