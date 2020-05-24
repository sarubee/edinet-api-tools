#!/usr/bin/env python
# -*- coding: utf-8 -*-

#   Copyright 2020 Sarubee
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software #   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

"""
edinet_api_parser_sec_report.py
- script to parse securities reports fetched via EDINET API
"""

import pandas as pd
import logging
logger = logging.getLogger(__name__)

import xbrl_edinet
from edinet_api_parser import *

# 有価証券報告書財務データの basic な parser
class BasicFinancialDataParser(DataParserAbs):
    def __init__(self):
        pass

    @staticmethod
    def get_jppfs_current_ins_int(df, id_):
        r = DataParserAbs.get_int(df, "jppfs_cor", id_, "CurrentYearInstant")
        if r is None:
            # 連結のがなかったら(重要性が乏しい場合は作成されない？)、非連結の値を取得する
            r = DataParserAbs.get_int(df, "jppfs_cor", id_, "CurrentYearInstant_NonConsolidatedMember")
        return r

    @staticmethod
    def get_jppfs_current_dur_int(df, id_):
        r = DataParserAbs.get_int(df, "jppfs_cor", id_, "CurrentYearDuration")
        if r is None:
            # 連結のがなかったら(重要性が乏しい場合は作成されない？)、非連結の値を取得する
            r = DataParserAbs.get_int(df, "jppfs_cor", id_, "CurrentYearDuration_NonConsolidatedMember")
        return r

    def parse(self, df):
        d = {}
        # BS 関連
        d["assets"] = BasicFinancialDataParser.get_jppfs_current_ins_int(df, "Assets") # 資産
        d["current_assets"] = BasicFinancialDataParser.get_jppfs_current_ins_int(df, "CurrentAssets") # 流動資産
        d["noncurrent_assets"] = BasicFinancialDataParser.get_jppfs_current_ins_int(df, "NoncurrentAssets") # 固定資産
        d["liabilities"] = BasicFinancialDataParser.get_jppfs_current_ins_int(df, "Liabilities") # 負債
        d["current_liabilities"] = BasicFinancialDataParser.get_jppfs_current_ins_int(df, "CurrentLiabilities") # 流動負債
        d["nonurrent_liabilities"] = BasicFinancialDataParser.get_jppfs_current_ins_int(df, "NoncurrentLiabilities") # 固定負債
        d["net_assets"] = BasicFinancialDataParser.get_jppfs_current_ins_int(df, "NetAssets") # 純資産
        d["liabilities_and_net_assets"] = BasicFinancialDataParser.get_jppfs_current_ins_int(df, "LiabilitiesAndNetAssets") # 負債純資産

        # PL 関連
        d["net_sales"] = BasicFinancialDataParser.get_jppfs_current_dur_int(df, "NetSales") # 売上高
        d["operating_income"] = BasicFinancialDataParser.get_jppfs_current_dur_int(df, "OperatingIncome") # 営業利益
        d["ordinary_income"] = BasicFinancialDataParser.get_jppfs_current_dur_int(df, "OrdinaryIncome") # 経常利益
        d["profit_loss"] = BasicFinancialDataParser.get_jppfs_current_dur_int(df, "ProfitLoss") # 純利益

        return d

# 有価証券報告書 parser
class EdinetApiSecReportParser(EdinetApiDocParserAbs):
    def __init__(self, data_parser):
        super().__init__(data_parser)

    # doc_list からデータを取得する対象を抽出
    def get_targets_from_doclist(self, doc_list):
        target_info_list = []
        for d in doc_list["results"]:
            # 書類種別コードが 120 以外、または提出者証券コードが空なものは Skip
            if d["docTypeCode"] != "120" or d["secCode"] is None:
                continue
            # 府令コードが 010 以外、または様式コードが 30000 以外はとりあえず warning 出して Skip
            if d["ordinanceCode"] != "010" or d["formCode"] != "030000":
                logger.warning(f"Skip document (ordinanceCode:{d['ordinanceCode']}, formCode:{d['formCode']}): {d} ...")
                continue
            # doc_list から使いそうな項目のみ選択
            keys = {"docID" : "doc_id", "secCode" : "sec_code", "filerName" : "name", "periodStart" : "start_date", "periodEnd" : "end_date", "submitDateTime" : "submit_datetime"}
            target_info_list.append({v : d[k] for k, v in keys.items()})
        return target_info_list

    # 一つの EDINET ID directory に対する parse
    def parse_id_dir(self, id_dir):
        zip_pattern = str(id_dir / "*_1.zip")
        zip_path = glob.glob(zip_pattern)
        if len(zip_path) < 1:
            raise EdinetApiParseError(f"No such file ({zip_pattern})")
        zip_path = zip_path[0]
        return self.parse_zip(zip_path)

    def parse_zip(self, zip_path):
        logger.info(f"parse {zip_path} ...")
        # データを取得してパース
        df = xbrl_edinet.parse_zip(zip_path)
        return self.data_parser.parse(df)

# テストコード
if __name__ == "__main__":
    logging.basicConfig(
        #level = logging.DEBUG,
        level = logging.INFO,
        format = "[%(asctime)s][%(levelname)s] %(message)s",
    )

    data_parser = BasicFinancialDataParser()
    doc_parser = EdinetApiSecReportParser(data_parser)
    parser = EdinetApiParser(doc_parser, cpu_count=None)
    r = parser.parse("edinet_sec_report_data", "2019-05-20", "2020-05-19")
    data_list = []
    for e in r:
        d = copy.deepcopy(e["info"])
        d.update(e["data"])
        data_list.append(d)
    pd.DataFrame(data_list).to_csv("aaa.csv", index=False)
