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
#   Unless required by applicable law or agreed to in writing, software #   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

"""
edinet_api_parse_sec_report.py
- script to parse securities reports fetched via EDINET API
"""

import pandas as pd
import numpy as np
import datetime
import logging
logger = logging.getLogger(__name__)

import xbrl_edinet
from edinet_api_parse import *

# 有価証券報告書財務データの basic な parser
class BasicFinancialDataParser(DataParserAbs):
    def __init__(self):
        pass

    @staticmethod
    def get_jppfs_current_ins(df, id_, as_float=False):
        func = DataParserAbs.get_float if as_float else DataParserAbs.get_int
        r = func(df, "jppfs_cor", id_, "CurrentYearInstant")
        if r is None:
            # 連結のがなかったら(重要性が乏しい場合は作成されない？)、非連結の値を取得する
            r = func(df, "jppfs_cor", id_, "CurrentYearInstant_NonConsolidatedMember")
        return r

    @staticmethod
    def get_jppfs_current_dur(df, id_, as_float=False):
        func = DataParserAbs.get_float if as_float else DataParserAbs.get_int
        r = func(df, "jppfs_cor", id_, "CurrentYearDuration")
        if r is None:
            # 連結のがなかったら(重要性が乏しい場合は作成されない？)、非連結の値を取得する
            r = func(df, "jppfs_cor", id_, "CurrentYearDuration_NonConsolidatedMember")
        return r

    @staticmethod
    def get_jpcrp_current_ins(df, id_, as_float=False):
        func = DataParserAbs.get_float if as_float else DataParserAbs.get_int
        # 連結の場合もある？
        r = func(df, "jpcrp_cor", id_, "CurrentYearInstant")
        if r is None:
            r = func(df, "jpcrp_cor", id_, "CurrentYearInstant_NonConsolidatedMember")
        return r

    @staticmethod
    def get_jpcrp_current_dur(df, id_, as_float=False):
        func = DataParserAbs.get_float if as_float else DataParserAbs.get_int
        # 連結の場合もある？
        r = func(df, "jpcrp_cor", id_, "CurrentYearDuration")
        if r is None:
            r = func(df, "jpcrp_cor", id_, "CurrentYearDuration_NonConsolidatedMember")
        return r

    def parse(self, df):
        d = {}
        # BS 関連
        d["assets"] = BasicFinancialDataParser.get_jppfs_current_ins(df, "Assets") # 資産
        d["current_assets"] = BasicFinancialDataParser.get_jppfs_current_ins(df, "CurrentAssets") # 流動資産
        d["noncurrent_assets"] = BasicFinancialDataParser.get_jppfs_current_ins(df, "NoncurrentAssets") # 固定資産
        d["liabilities"] = BasicFinancialDataParser.get_jppfs_current_ins(df, "Liabilities") # 負債
        d["current_liabilities"] = BasicFinancialDataParser.get_jppfs_current_ins(df, "CurrentLiabilities") # 流動負債
        d["noncurrent_liabilities"] = BasicFinancialDataParser.get_jppfs_current_ins(df, "NoncurrentLiabilities") # 固定負債
        d["net_assets"] = BasicFinancialDataParser.get_jppfs_current_ins(df, "NetAssets") # 純資産
        d["liabilities_and_net_assets"] = BasicFinancialDataParser.get_jppfs_current_ins(df, "LiabilitiesAndNetAssets") # 負債純資産

        # PL 関連
        d["net_sales"] = BasicFinancialDataParser.get_jppfs_current_dur(df, "NetSales") # 売上高
        d["operating_income"] = BasicFinancialDataParser.get_jppfs_current_dur(df, "OperatingIncome") # 営業利益
        d["ordinary_income"] = BasicFinancialDataParser.get_jppfs_current_dur(df, "OrdinaryIncome") # 経常利益
        d["profit_loss"] = BasicFinancialDataParser.get_jppfs_current_dur(df, "ProfitLoss") # 純利益

        # CF 関連
        d["operating_cashflow"] = BasicFinancialDataParser.get_jppfs_current_dur(df, "NetCashProvidedByUsedInOperatingActivities") # 営業キャッシュフロー
        d["investment_cashflow"] = BasicFinancialDataParser.get_jppfs_current_dur(df, "NetCashProvidedByUsedInInvestmentActivities") # 投資キャッシュフロー
        d["financing_cashflow"] = BasicFinancialDataParser.get_jppfs_current_dur(df, "NetCashProvidedByUsedInFinancingActivities") # 財務キャッシュフロー
        # フリーキャッシュフロー
        if d["operating_cashflow"] is not None and d["investment_cashflow"] is not None:
            d["free_cashflow"] = d["operating_cashflow"] + d["investment_cashflow"]
        else:
            d["free_cashflow"] = None

        return d

# 有価証券報告書 parser
class EdinetApiSecReportParser(EdinetApiDocParserAbs):
    def __init__(self, data_parser, *, debug_config={}):
        super().__init__(data_parser, debug_config=debug_config)
        # 指定があったら debug 用の csv を出力する
        self.debug_csv_dir = Path(debug_config["csv_dir"]) if "csv_dir" in debug_config else None
        # 指定があったら対象証券コードのみ処理
        self.debug_sec_codes = debug_config["sec_codes"] if "sec_codes" in debug_config else None

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
            # 対象証券コード以外は Skip
            if self.debug_sec_codes is not None and d["secCode"] not in self.debug_sec_codes:
                continue
            # doc_list から使いそうな項目のみ選択
            keys = {"docID" : "doc_id", "secCode" : "sec_code", "filerName" : "name", "periodStart" : "start_date", "periodEnd" : "end_date"}
            info = {v : d[k] for k, v in keys.items()}
            info["submit_date"] = str(datetime.datetime.fromisoformat(d["submitDateTime"]).date())
            target_info_list.append(info)
        return target_info_list

    # 一つの EDINET ID directory に対する parse
    def parse_id_dir(self, id_dir):
        zip_pattern = str(id_dir / "*_1.zip")
        zip_path = glob.glob(zip_pattern)
        if len(zip_path) < 1:
            raise EdinetApiParseError(f"No such file ({zip_pattern})")
        zip_path = Path(zip_path[0])
        r = self.parse_zip(zip_path)

        if self.debug_pdf_dir is not None:
            self.save_pdf(id_dir)

        return r

    def parse_zip(self, zip_path):
        logger.info(f"parse {zip_path} ...")
        # データを取得してパース
        df = xbrl_edinet.parse_zip(zip_path)
        if df is not None and self.debug_csv_dir is not None:
            with open(self.debug_csv_dir / f"{zip_path.parent.name}.csv", mode="w", encoding="cp932", errors="ignore") as f:
                df.to_csv(f, index=False)


        return self.data_parser.parse(df)

# テストコード
if __name__ == "__main__":
    logging.basicConfig(
        #level = logging.DEBUG,
        level = logging.WARN,
        format = "[%(asctime)s][%(levelname)s] %(message)s",
    )

    data_parser = BasicFinancialDataParser()
    doc_parser = EdinetApiSecReportParser(data_parser, debug_config={"csv_dir" : "debug", "sec_codes" : ["45280"]})
    #doc_parser = EdinetApiSecReportParser(data_parser, debug_config={"csv_dir" : "debug"})
    parser = EdinetApiParser(doc_parser, cpu_count=None)
    #parser = EdinetApiParser(doc_parser, cpu_count=1)
    r = parser.parse("/Z/itaru/development/edinet_data", "2022-01-01", "2022-12-31")
