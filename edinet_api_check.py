#!/usr/bin/env python
# -*- coding: utf-8 -*-

#   Copyright 2023 Sarubee
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

"""
edinet_api_check.py
- script to check if documents are submitted in EDINET
"""
import datetime 
import logging
logger = logging.getLogger(__name__)
import tempfile
from abc import ABCMeta, abstractmethod

from edinet_api_fetch import *
from edinet_api_parse import *
import xbrl_edinet

class EdinetApiCheckError(RuntimeError):
    pass

class EdinetApiCheckerAbs(metaclass=ABCMeta):
    def __init__(self, *, retry_interval=-1, history_file=None):
        self.fetcher = EdinetAPIFetcher(retry_interval)
        self.history_file = None if history_file is None else Path(history_file)

    def check(self, sec_codes=None, days=1):
        end_datetime = datetime.datetime.now()
        start_datetime = end_datetime - datetime.timedelta(days=days)
        end_date = end_datetime.date()
        date = start_datetime.date()

        count_h = {} 
        if self.history_file is not None:
            if self.history_file.exists():
                with open(self.history_file, "r") as f:
                    count_h = json.load(f)

        result = [] 
        while date <= end_date:
            j = self.fetcher.fetch_doc_list_for_day(date)
            if j is None:
                # 時間が早いと本日分の提出書類一覧取得に失敗することがある
                continue

            count = j["metadata"]["resultset"]["count"]

            date_key = date.isoformat()
            if date_key in count_h:
                skip_count = count_h[date_key]
                if count <= skip_count:
                    logger.info(f"No updates in {date}")
                    date += datetime.timedelta(days=1)
                    continue
            else:
                skip_count = 0

            for d in j["results"]:
                if d["seqNumber"] <= skip_count:
                    continue
                if d["submitDateTime"] is None:
                    # 提出日時がないやつは飛ばす
                    logger.warning(f"'submitDateTime' key is missing ({d['docID']}). Skip...")
                    continue
                else:
                    # 期限外は skip
                    submit_datetime = datetime.datetime.fromisoformat(d["submitDateTime"])
                    if submit_datetime < start_datetime:
                        continue
                r = self._check_one(d, sec_codes)
                if r is not None:
                    result.append(r)
            # 処理が終わったら update
            count_h[date_key] = j["metadata"]["resultset"]["count"]
            date += datetime.timedelta(days=1)

        # 更新後のデータを書き込み 
        if self.history_file is not None:
            with open(self.history_file, "w") as f:
                json.dump(count_h, f, indent=2)
        return result

    @abstractmethod
    def _check_one(self, d):
        pass

# 大量保有報告チェック 
class EdinetApiHoldingsChecker(EdinetApiCheckerAbs):
    def _check_one(self, d, sec_codes=None):
        if d["docTypeCode"] != "350": 
            return None
        if "特例対象株券等" in d["docDescription"]:
            # 特例対象株券等は除く
            return None
        r = self.fetcher.fetch_doc_one(d["docID"], EdinetAPIFetcher.DOC_TYPE_MAIN)
        if r is None:
            return None
        
        with tempfile.NamedTemporaryFile() as f:
            with open(f.name, "wb") as f:
                f.write(r.content)
            df = xbrl_edinet.parse_zip(f.name)
        # デバッグ用
        #with open(f"a/{d['docID']}.csv", mode="w", encoding="cp932", errors="ignore") as f:
        #    df.to_csv(f)

        result = {}
        result["id"] = d["docID"]
        result["title"] = DataParserAbs.get_text(df, "jplvh_cor", "DocumentTitleCoverPage")
        result["onset_datetime"] = d["submitDateTime"]
        result["reason"] = DataParserAbs.get_text(df, "jplvh_cor", "ReasonForFilingChangeReportCoverPage")
        if result["reason"] is not None and (any([k in result["reason"] for k in ["所在地", "住所"]])):
            # 所在地変更, 住所変更 で出してるのは除く。目的が複数のもあるかもしれないが。
            return None
        result["issuer_sec_code"] = DataParserAbs.get_text(df, "jplvh_cor", "SecurityCodeOfIssuer")
        if sec_codes is not None and result["issuer_sec_code"] not in sec_codes:
            return None
        result["issuer_name"] = DataParserAbs.get_text(df, "jplvh_cor", "NameOfIssuer")
        result["sec_code"] = DataParserAbs.get_text(df, "jpdei_cor", "SecurityCodeDEI")
        result["filer_name"] = DataParserAbs.get_text(df, "jpdei_cor", "FilerNameInJapaneseDEI")
        result["share_ratio_prev"] = DataParserAbs.get_float(df, "jplvh_cor", "HoldingRatioOfShareCertificatesEtcPerLastReport", "FilingDateInstant")
        result["share_ratio"] = DataParserAbs.get_float(df, "jplvh_cor", "HoldingRatioOfShareCertificatesEtc", "FilingDateInstant")
        if result["share_ratio_prev"] is None and result["share_ratio"] is None:
            # "FilingDateInstant" がなかった場合、context_id 指定無しでとってくる
            result["share_ratio_prev"] = DataParserAbs.get_float(df, "jplvh_cor", "HoldingRatioOfShareCertificatesEtcPerLastReport")
            result["share_ratio"] = DataParserAbs.get_float(df, "jplvh_cor", "HoldingRatioOfShareCertificatesEtc")
        # 保有目的は個別の提出者毎のようなのでこれだと複数とれてしまう
        #result["purpose"] = DataParserAbs.get_text(df, "jplvh_cor", "PurposeOfHolding")
        logger.info(f"Found: {result}")
        return result

# テスト
if __name__ == "__main__":
    import pprint

    logging.basicConfig(
        level = logging.INFO,
        format = "[%(asctime)s][%(levelname)s] %(message)s",
    )
    checker = EdinetApiHoldingsChecker(history_file="test.j")
    r = checker.check(sec_codes=None, days=1)
    pprint.pprint(r, sort_dicts=False)
