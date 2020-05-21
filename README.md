# edinet-api-tools
EDINET API 活用のための Python ツールのレポジトリです（非公式）。

EDINET API については、[EDINET 操作ガイド](https://disclosure.edinet-fsa.go.jp/EKW0EZ0015.html) の "EDINET API関連資料" に仕様等の説明があります。

---
## Requirement
※（）内の version は動作確認環境です。
* Python (3.7.5)  

Python の標準ライブラリに含まれない以下のライブラリ（およびそれらの依存ライブラリ）に依存しています。  
* requests (2.18.4)
* python-dateutil (2.6.1)
---
## Author
[github](https://github.com/sarubee "github"), [twitter](https://twitter.com/fire50net "twitter"), [blog](https://fire50.net/ "blog")

---
## License
Apache License 2.0

---
## Description
### edinet_api_fetch.py
EDINET API からデータを取得します。
#### ツールとして直接実行
* 使い方
```
$ python edinet_api_fetch.py [-h] --from YYYY-MM-DD --to YYYY-MM-DD --dir DIR [--full]
                             [--doc-code [NNN [NNN ...]]] [--need-sec-code] [--overwrite]
```
`--from` 指定日から `--to` 指定日までの期間のデータを EDINET API で取得し、`--dir` で指定したディレクトリに保存します。
* オプション
```
-h, --help         ヘルプ表示
--from YYYY-MM-DD  データ取得開始日 (必須) 
--to YYYY-MM-DD    データ取得終了日 (必須) 
--dir DIR          データ保存先ディレクトリ (必須) 
--full             書類取得 API の全 type のデータを取得します（デフォルト: 書類取得 API の type=1 のデータのみ取得します）
--doc-code [NNN [NNN ...]]
                   取得する書類種別コードを指定します (デフォルト: 全ての書類種別コードの書類を取得する)
--need-sec-code    証券コードの設定がない書類取得をスキップします（デフォルト: スキップしない）
--overwrite        既存のデータを上書きします（デフォルト: 上書きしない)
```
* 出力ツリー
```
{DIR}/
  ├- (YYYY-MM-DD)/
  |    ├- doc_list.json           # 書類一覧
  |    ├- (書類管理番号)/
  |    |   ├- (書類管理番号)_1.zip  # "_1" 等は書類取得 API の type に対応します
  |    |   ├- (書類管理番号)_2.pdf
  |    |   ├- (書類管理番号)_3.zip
  |    |   └- (書類管理番号)_4.zip
  :    :
```

#### import して利用
```python
from edinet_api_fetch import EdinetAPIFetcher
e = EdinetAPIFetcher()
# 指定日の書類一覧を取得
doc_list = e.fetch_doc_list_for_day("2020-05-01")
# 特定の書類管理番号のデータを取得して保存
e.save_docs_for_id("/path/to/outdir", "S100IITL")
```
