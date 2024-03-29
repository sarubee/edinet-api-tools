# edinet-api-tools
EDINET API 活用のための Python ツールのレポジトリです（非公式）。

EDINET API の仕様などは、[EDINET の公式サイト](https://disclosure2.edinet-fsa.go.jp) をご参照ください。

---
## Requirement
※（）内の version は動作確認環境です。
* Python (3.8.10)  

Python の標準ライブラリに含まれない以下のライブラリ（およびそれらの依存ライブラリ）に依存しています。  
* pandas (1.1.2)
* requests (2.22.0)
* python-dateutil (2.7.3)
* lxml (4.5.2)
---
## Author
[github](https://github.com/sarubee "github"), [twitter](https://twitter.com/fire50net "twitter"), [blog](https://fire50.net/ "blog")

---
## License
Apache License 2.0

---
## Description
### データの取得
EDINET API からデータを取得します。
#### edinet_api_fetch.py をツールとして直接実行
* 使い方
```
$ python edinet_api_fetch.py [-h] --from YYYY-MM-DD --to YYYY-MM-DD --dir DIR [--full]
                             [--doc-code [NNN [NNN ...]]] [--need-sec-code] [--overwrite]
```
`--from` 指定日から `--to` 指定日までの期間のデータを EDINET API で取得し、`--dir` で指定したディレクトリに保存します。
* 使用例
```
$ python edinet_api_fetch.py --from 2019-05-21 --to 2020-05-20 --dir edinet_sec_report_data --doc-code 120 --need-sec-code
```
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
  |    |   ├- (書類管理番号)_1.zip  # "_1" 等は書類取得 API の type に対応します。通常は 1 のみ,｀--full｀指定時は 1-4 をすべて取得します。
  |    |   ├- (書類管理番号)_2.pdf
  |    |   ├- (書類管理番号)_3.zip
  |    |   └- (書類管理番号)_4.zip
  :    :
```

### データのパース
有価証券報告書の基本的な財務データ取得用のクラスが用意してあります。

* 使用例 ("edinet_sec_report_data" ディレクトリに上記 `edinet_api_fetch.py` で取得したデータがある場合)
```python
import logging 
from edinet_api_parse_sec_report import *

logging.basicConfig(
    level = logging.INFO,
    format = "[%(asctime)s][%(levelname)s] %(message)s",
)

data_parser = BasicFinancialDataParser()
doc_parser = EdinetApiSecReportParser(data_parser)
parser = EdinetApiParser(doc_parser)
data = parser.parse("edinet_sec_report_data", "2019-05-20", "2020-05-19")
print(data)
```

### データの更新チェック
* 使用例（一日分の大量保有報告書をチェックする場合）
```python
from edinet_api_check import *

logging.basicConfig(
    level = logging.WARN,
    format = "[%(asctime)s][%(levelname)s] %(message)s",
)

# history_file を指定すると前回からの差分をチェックできます
#history_file = "check_history.json"
history_file = None

# 特定の銘柄のみチェック対象にする場合はコードを指定します
#sec_codes = ["3001", "6396", "8806", "8841", "9279"]
sec_codes = None

checker = EdinetApiHoldingsChecker(history_file=history_file)
r = checker.check(sec_codes=sec_codes, days=1)
print(r)
```
