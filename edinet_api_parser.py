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
edinet_api_parser.py
- script to parse data fetched via EDINET API
"""

from pathlib import Path
from datetime import date
from dateutil.relativedelta import relativedelta
import logging
logger = logging.getLogger(__name__)
import json
from abc import ABCMeta, abstractmethod
import copy
import glob
from multiprocessing import Pool
import re
import shutil

import xbrl_edinet
from xbrl_edinet import XbrlEdinetParseError

class EdinetApiParseError(RuntimeError):
    pass
# 想定外のエラー
class EdinetApiParseUnxepectedError(RuntimeError):
    pass

# データ parser 基底クラス
class DataParserAbs(metaclass=ABCMeta):
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def parse(self, df):
        # EdinetApiDocParserAbs.parse_id_dir() 中で呼び出される
        # dataframe をパースして何かの値を返す
        pass

    @staticmethod
    def get_row(df, ns_pre, tag, context_id=None):
        # 行を抜き出す
        if ns_pre is not None:
            # ns_pre は正規表現として match する
            df = df[df["ns_pre"].str.match(ns_pre)]
        cond = f"(tag == '{tag}')"
        if context_id is not None:
            cond += f" & (context_id == '{context_id}')"
        r = df.query(cond)

        if len(r) < 1:
            return None
        elif len(r) > 1:
            EdinetApiParseUnexpectedError(f"Multiple rows exist! (condition: {cond})")
        return r.iloc[0]

    @staticmethod
    def get_text(df, ns_pre, tag, context_id=None):
        r = DataParserAbs.get_row(df, ns_pre, tag, context_id)
        if r is None or r["text"] is None:
            return None
        s = r["text"].replace("\n", "").replace("\u3000", "  ").replace("\xa0", " ")
        return s

    @staticmethod
    def get_int(df, ns_pre, tag, context_id=None):
        r = DataParserAbs.get_text(df, ns_pre, tag, context_id)
        if r is None:
            return None
        return int(r)

    @staticmethod
    def get_float(df, ns_pre, tag, context_id=None):
        r = DataParserAbs.get_text(df, ns_pre, tag, context_id)
        if r is None:
            return None
        return float(r)

# 書類種別 parser 基底クラス
class EdinetApiDocParserAbs(metaclass=ABCMeta):
    @abstractmethod
    def __init__(self, data_parser, *, debug_config={}):
        self.data_parser = data_parser
        self.debug_pdf_dir = Path(debug_config["pdf_dir"]) if "pdf_dir" in debug_config else None

    @abstractmethod
    # doc_list からデータを取得する対象を抽出
    def get_targets_from_doclist(self, doc_list):
        pass

    @abstractmethod
    # 一つの EDINET ID directory に対する parse
    def parse_id_dir(self, id_dir):
        pass

    def save_pdf(self, id_dir):
        # debug 指定があれば pdf を指定ディレクトリにコピーする
        pdf_pattern = str(id_dir / "*_2.pdf")
        pdf_path = glob.glob(pdf_pattern)
        if len(pdf_path) > 0:
            pdf_path = Path(pdf_path[0])
            shutil.copyfile(pdf_path, self.debug_pdf_dir / pdf_path.name)

class EdinetApiParser:
    def __init__(self, doc_parser, *, cpu_count=None):
        self.doc_parser = doc_parser
        self.cpu_count = cpu_count

    def _parse_target_safe(self, id_dir):
        """multi processing 実行用
        """
        try:
            return self.doc_parser.parse_id_dir(id_dir)
        except (EdinetApiParseError, XbrlEdinetParseError) as e:
            logger.warning(e)
            logger.warning(f"Skip {id_dir}...")
            return None

    def parse(self, data_dir, start_day, end_day):
        """データをパースして pandas.DataFrame にして返す関数
        """

        # data_dir は EdinetAPIFetcher.save_docs_for_period() の保存先ディレクトリ
        data_dir = Path(data_dir)

        if isinstance(start_day, str):
            start_day = date.fromisoformat(start_day)
        if isinstance(end_day, str):
            end_day = date.fromisoformat(end_day)

        # 取得対象全データを格納
        id_dirs = []
        info_list = []
        day = start_day
        while day <= end_day:
            logger.debug(f"{day} START")
            day_dir = data_dir / str(day)
            doc_list_path = day_dir / "doc_list.json"
            if doc_list_path.exists():
                with open(doc_list_path, "r") as f:
                    doc_list = json.load(f)
                info_list_day = self.doc_parser.get_targets_from_doclist(doc_list)
                for info in info_list_day:
                    id_ = info["doc_id"]
                    id_dir = day_dir / id_
                    if id_dir.exists():
                        info_list.append(info)
                        id_dirs.append(id_dir)
                    else:
                        logger.warning(f"Document does not exist!")
                        logger.warning(f"Skip {id_dir}...")
            else:
                logger.warning(f"Document list does not exist ({doc_list_path}). Skip...")
            day += relativedelta(days=1)
        # multi process で実行
        data_list = Pool(self.cpu_count).map(self._parse_target_safe, id_dirs)

        results = []
        for i, d in zip(info_list, data_list):
            results.append({"info" : i, "data" : d})

        return results
