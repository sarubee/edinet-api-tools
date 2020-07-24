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
import shutil

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

    def _fetcher_err_handling_common(self, r, ctype):
        # fetch 失敗時の共通処理
        if ctype == "application/json;charset=utf-8":
            meta = r.json()["metadata"]
            status = int(meta["status"])
            msg = f"{meta['message']}({status})"
            if status == 400:
                # Bad Request
                # リクエスト側の問題なのでエラーにしておく
                return {"handling" : "error", "message" : msg}
            elif status == 404:
                # Not Found
                # Skip する
                return {"handling" : "skip", "message" : msg}
            else:
                # あとは status=500 (Internal Server Error) ？
                # retry 設定があれば retry
                if self.retry_interval < 0:
                    return {"handling" : "error", "message" : msg}
                else:
                    return {"handling" : "retry", "message" : msg}
        elif ctype == "text/html":
            # sorry 画面っぽいやつが出る場合がある
            # retry 設定があれば retry
            msg = f"Invalid content type ({ctype})"
            if self.retry_interval < 0:
                return {"handling" : "error", "message" : msg}
            else:
                return {"handling" : "retry", "message" : msg}
        else:
            # ありうる？とりあえずエラーにしておく
            msg = f"Unexpected content type ({ctype})!!"
            return {"handling" : "error", "message" : msg}

    def fetch_doc_one(self, doc_id, doc_type):
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
        err_msg_base = f"Failed to fetch a document!! (doc_id: {doc_id}, doc_type: {doc_type})"
        while True:
            logger.info(f"fetching document (doc_id: {doc_id}, type: {doc_type})...")
            r = EdinetAPIFetcher._fetch(url, params, headers)
            ctype = r.headers["Content-Type"].replace(" ", "")
            if (doc_type == EdinetAPIFetcher.DOC_TYPE_PDF and ctype == "application/pdf") or (doc_type != EdinetAPIFetcher.DOC_TYPE_PDF and ctype == "application/octet-stream"):
                # 取得成功
                return r
            # エラー時の処理
            e = self._fetcher_err_handling_common(r, ctype)
            err_msg = err_msg_base + f": {e['message']}"
            if e["handling"] == "error":
                raise EdinetFetchError(err_msg)
            elif e["handling"] == "skip":
                # warning を出してスルー
                logger.warning(err_msg)
                logger.warning("Skip...")
                return None
            elif e["handling"] == "retry":
                logger.warning(err_msg)
                logger.warning(f"Wait for retry ({self.retry_interval}sec) ...")
                time.sleep(self.retry_interval)
                continue

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

        err_msg_base = f"Failed to fetch document list!! (day: {day})"
        while True:
            logger.info(f"fetching document list (date: {day}, type: {target_type})...")
            r = EdinetAPIFetcher._fetch(EdinetAPIFetcher.URL_DOC_LIST, params, headers)
            ctype = r.headers["Content-Type"].replace(" ", "")
            if ctype == "application/json;charset=utf-8":
                j = r.json()
                meta = j["metadata"]
                status = int(meta["status"])
                msg = meta["message"]
                if status == 200:
                    # 取得成功
                    return j
            # エラー時の処理
            e = self._fetcher_err_handling_common(r, ctype)
            err_msg = err_msg_base + f": {e['message']}"
            if e["handling"] == "error":
                raise EdinetFetchError(err_msg)
            elif e["handling"] == "skip":
                # warning を出してスルー
                logger.warning(err_msg)
                logger.warning("Skip...")
                return None
            elif e["handling"] == "retry":
                logger.warning(err_msg)
                logger.warning(f"Wait for retry ({self.retry_interval}sec) ...")
                time.sleep(self.retry_interval)
                continue

    def save_docs_for_id(self, outdir, doc_id, *, doc_types=None):
        """指定文書コードのデータを保存

        Parameters
        ----------
        doc_id : str
            取得する文書コード
        outdir : Path or str
            出力ディレクトリパス
        doc_types : list
            取得する文書 type

        Returns
        -------
        """
        outdir = Path(outdir)
        shutil.rmtree(outdir, ignore_errors=True)
        os.makedirs(outdir)

        if doc_types is None:
            # デフォルトは main データだけ
            doc_types = [EdinetAPIFetcher.DOC_TYPE_MAIN]

        for doc_type in doc_types:
            ext = EdinetAPIFetcher._doc_ext(doc_type)
            outpath = Path(f"{outdir / doc_id}_{doc_type}.{ext}")
            r = self.fetch_doc_one(doc_id, doc_type)
            if r is None:
                continue
            with open(outpath, "wb") as f:
                f.write(r.content)

    def save_docs_for_day(self, outdir, day, *, doc_types=None, doc_codes=None, need_sec_code=False):
        """指定日のデータを保存

        Parameters
        ----------
        day : str or datetime.date
            日付 (YYYY-MM-DD)
        outdir : Path or str
            出力ディレクトリパス
        doc_types : list
            取得する文書 type
        doc_codes : list
            取得する書類種別コード
        need_sec_code : bool
            証券コードがない書類をスキップするか

        Returns
        -------
        """
        outdir = Path(outdir)
        shutil.rmtree(outdir, ignore_errors=True)

        doc_list_path = outdir / "doc_list.json"
        j = api.fetch_doc_list_for_day(day)
        if j is None:
            return
        os.makedirs(outdir)
        with open(doc_list_path, "w") as f:
            json.dump(j, f, indent=2, ensure_ascii=False)

        for d in j["results"]:
            if doc_codes is not None and not d["docTypeCode"] in doc_codes:
                continue
            if need_sec_code and d["secCode"] is None:
                continue
            doc_id = d["docID"]
            outdir_id = outdir / doc_id
            self.save_docs_for_id(outdir_id, doc_id, doc_types=doc_types)

    def save_docs_for_period(self, outdir, start_day, end_day, *, doc_types=None, doc_codes=None, need_sec_code=False):
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
        doc_codes : list
            取得する書類種別コード
        need_sec_code : bool
            証券コードがない書類をスキップするか

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
            self.save_docs_for_day(daydir, day, doc_types=doc_types, doc_codes=doc_codes, need_sec_code=need_sec_code)
            day += relativedelta(days=1)


# 直接実行時
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--from", metavar="YYYY-MM-DD", help="start daty", required=True, dest="start_day")
    parser.add_argument("--to", metavar="YYYY-MM-DD", help="end day", required=True, dest="end_day")
    parser.add_argument("--dir", help="directory to store data", type=Path, required=True)
    parser.add_argument("--full", help="fetch full data", action="store_true", default=False)
    parser.add_argument("--doc-code", metavar="NNN", help="fetch documents with specific document code", nargs="*", dest="doc_codes")
    parser.add_argument("--need-sec-code", help="skip documents without sec code", action="store_true", default=False)
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
    api.save_docs_for_period(args.dir, start_day, end_day, doc_types=doc_types, doc_codes=args.doc_codes, need_sec_code=args.need_sec_code)
