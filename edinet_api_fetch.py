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
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

"""
edinet_api_fetch.py
- script to fetch data via EDINET API
"""

import argparse 
import requests
import time
from urllib.parse import urljoin
from pathlib import Path
from datetime import date
from dateutil.relativedelta import relativedelta
import logging
logger = logging.getLogger(__name__)
import json
import os 

class EdinetFetchError(RuntimeError):
    pass

class EdinetAPIFetcher:
    # 取得 URL
    URL_API = "https://disclosure.edinet-fsa.go.jp/api/v1/"
    URL_DOC_LIST = URL_API + "documents.json"
    URL_DOC = URL_API + "documents/"
    # 文書 type
    DOC_TYPE_MAIN = 1 
    DOC_TYPE_PDF = 2 
    DOC_TYPE_ADDITIONAL = 3
    DOC_TYPE_ENG = 4 
    DOC_TYPE_LIST_FULL = [1, 2, 3, 4]

    def __init__(self, retry_interval=-1):
        # 取得エラー時の retry 間隔 [sec]
        # 負数なら retry しない
        self.retry_interval = retry_interval 

    @staticmethod
    def _doc_ext(doc_type):
        """ doc_type に対応する拡張子

        Parameters
        ----------
        doc_type : int 
            取得する文書 type
        Returns
        -------
        str
        """
        if doc_type == EdinetAPIFetcher.DOC_TYPE_PDF:
            return "pdf"
        else: # EdinetAPIFetcher.DOC_TYPE_MAIN, EdinetAPIFetcher.DOC_TYPE_ADDITIONAL, EdinetAPIFetcher.DOC_TYPE_ENG
            return "zip"

    @staticmethod
    def _fetch(url, params, headers):
        """API データ取得用の基本関数

        Parameters
        ----------
        url : str
            データ取得先 URL
        params : dict
            取得用パラメータ
        headers : dict
            取得用ヘッダ

        Returns
        -------
        Response
        """

        # データ取得
        # 終わったら負荷をかけないように 1 秒休む
        r = requests.get(url, params=params, headers=headers)
        time.sleep(1)
        # status チェック (あまり意味ないかも)
        r.raise_for_status()

        return r 

    def _fetch_doc_one(self, doc_id, doc_type):
        """文書コード、type を指定して取得する関数

        Parameters
        ----------
        doc_id : str 
            取得する文書コード
        doc_type : int 
            取得する文書 type

        Returns
        -------
        dict or None(Not Found 時)
            取得したデータ
        """
        params =  {"type" : doc_type}
        headers = {}

        url = urljoin(EdinetAPIFetcher.URL_DOC, doc_id)   
        while True:
            logger.info(f"fetching document (doc_id: {doc_id}, type: {doc_type})...")
            r = EdinetAPIFetcher._fetch(url, params, headers)
            if r.headers["Content-Type"] != "application/json;charset=utf-8": 
                # 取得成功
                return r
            # 失敗時
            meta = r.json()["metadata"]
            status = int(meta["status"])
            msg = meta["message"]
            err_msg = f"Failed to fetch a document!! (doc_id: {doc_id}, doc_type: {doc_type}): {msg}({status})"
            if status == 400:
                # Bad Request
                # リクエスト側の問題なのでエラーにしておく
                raise EdinetFetchError(err_msg)
            elif status == 404:
                # Not Found 
                # warning を出してスルー 
                logger.warning(err_msg)
                logger.warning("Skip...")
                return None
            # ここまでくるのは status=500 (Internal Server Error) ？
            if self.retry_interval < 0:
                raise EdinetFetchError(err_msg)
            else:
                # 指定時間待機して retry
                logger.warning(err_msg)
                logger.warning(f"Wait for retry ({self.retry_interval}sec) ...")
                time.sleep(self.retry_interval) 

    def fetch_doc_list_for_day(self, day, only_meta=False):
        """指定日の書類一覧を取得する関数

        Parameters
        ----------
        day : str (isoformat) or datetime.date
            日付 (YYYY-MM-DD)
        only_meta : bool 
            True:  メタデータのみ
            False: 提出書類一覧及びメタデータ

        Returns
        -------
        dict or None(Not Found 時)
            取得したデータ
        """
        target_type = 1 if only_meta else 2 

        params =  {"date" : str(day), "type" : target_type}
        headers = {}

        while True:
            logger.info(f"fetching document list (date: {day}, type: {target_type})...")
            r = EdinetAPIFetcher._fetch(EdinetAPIFetcher.URL_DOC_LIST, params, headers)
            j = r.json()
            meta = j["metadata"]
            status = int(meta["status"])
            msg = meta["message"]
            err_msg = f"Failed to fetch document list!! (day: {day}): {msg}({status})"
            if status == 200:
                # 取得成功
                return j
            elif status == 400:
                # Bad Request
                # リクエスト側の問題なのでエラーにしておく
                raise EdinetFetchError(err_msg)
            elif status == 404:
                # Not Found 
                # warning を出してスルー 
                logger.warning(err_msg)
                logger.warning("Skip...")
                return None
            # ここまでくるのは status=500 (Internal Server Error) ？
            if self.retry_interval < 0:
                raise EdinetFetchError(err_msg)
            else:
                # 指定時間待機して retry
                logger.warning(err_msg)
                logger.warning(f"Wait for retry ({self.retry_interval}sec) ...")
                time.sleep(self.retry_interval) 

    def save_docs_for_id(self, outdir, doc_id, *, doc_types=None, overwrite=False):
        """指定文書コードのデータを保存

        Parameters
        ----------
        doc_id : str 
            取得する文書コード
        outdir : Path or str
            出力ディレクトリパス 
        doc_types : list 
            取得する文書 type
        overwrite : bool 
            ファイルが存在する場合に上書きするか

        Returns
        -------
        """
        outdir = Path(outdir)
        if doc_types is None:
            # デフォルトは main データだけ
            doc_types = [EdinetAPIFetcher.DOC_TYPE_MAIN] 

        os.makedirs(outdir, exist_ok=True)
        for doc_type in doc_types:
            r = self._fetch_doc_one(doc_id, doc_type)
            if r is None:
                continue
            ext = EdinetAPIFetcher._doc_ext(doc_type)
            outpath = Path(f"{outdir / doc_id}_{doc_type}.{ext}")
            if not overwrite and outpath.exists():
                logger.info(f"File exists ({outpath}). Skip...")
            else:
                with open(outpath, "wb") as f:
                    f.write(r.content)

    def save_docs_for_day(self, outdir, day, *, doc_types=None, overwrite=False):
        """指定日のデータを保存

        Parameters
        ----------
        day : str or datetime.date 
            日付 (YYYY-MM-DD)
        outdir : Path or str
            出力ディレクトリパス 
        doc_types : list 
            取得する文書 type
        overwrite : bool 
            ファイルが存在する場合に上書きするか

        Returns
        -------
        """
        outdir = Path(outdir)
        doc_list_path = outdir / "doc_list.json"
        if not overwrite and doc_list_path.exists():
            logger.info(f"File exists ({doc_list_path}). Loading existing file....")
            with open(doc_list_path, "r") as f:
                j = json.load(f) 
        else:
            j = api.fetch_doc_list_for_day(day)
            if j is None:
                return
            os.makedirs(outdir, exist_ok=True)
            with open(doc_list_path, "w") as f:
                json.dump(j, f, indent=2, ensure_ascii=False)
        for d in j["results"]:
            doc_id = d["docID"]
            outdir_id = outdir / doc_id
            self.save_docs_for_id(outdir_id, doc_id, doc_types=doc_types, overwrite=overwrite)

    def save_docs_for_period(self, outdir, start_day, end_day, *, doc_types=None, overwrite=False):
        """指定期間のデータを保存

        Parameters
        ----------
        outdir : str or Path
            出力ディレクトリ
        start_day : str (isoformat) or datetime.date
            取得開始日
        end_day : str (isoformat) or datetime.date
            取得終了日
        doc_types : list 
            取得する文書 type
        overwrite : bool 
            ファイルが存在する場合に上書きするか

        Returns
        -------
        """
        outdir = Path(outdir)
        if isinstance(start_day, str):
            start_day = date.fromisoformat(start_day)
        if isinstance(end_day, str):
            end_day = date.fromisoformat(end_day)
        day = start_day
        while day <= end_day:
            daydir = Path(outdir) / str(day)
            self.save_docs_for_day(daydir, day, doc_types=doc_types, overwrite=overwrite)
            day += relativedelta(days=1)


# 直接実行時 
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--from", metavar="YYYY-MM-DD", help="start daty", required=True, dest="start_day")
    parser.add_argument("--to", metavar="YYYY-MM-DD", help="end day", required=True, dest="end_day")
    parser.add_argument("--dir", help="directory to store data", type=Path, required=True)
    parser.add_argument("--full", help="fetch full data", action="store_true", default=False)
    parser.add_argument("--overwrite", help="overwrite existing data", action="store_true", default=False)
    args = parser.parse_args()
    logging.basicConfig(
        level = logging.INFO,
        format = "[%(asctime)s][%(levelname)s] %(message)s",
    )

    start_day = date.fromisoformat(args.start_day) 
    end_day = date.fromisoformat(args.end_day)
    if args.full:
        doc_types = EdinetAPIFetcher.DOC_TYPE_LIST_FULL
    else:
        doc_types = [EdinetAPIFetcher.DOC_TYPE_MAIN]
    api = EdinetAPIFetcher(retry_interval=60)
    api.save_docs_for_period(args.dir, start_day, end_day, doc_types=doc_types, overwrite=args.overwrite)
