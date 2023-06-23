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
edinet_api_fetch.py
- script to fetch data via EDINET API
"""

import argparse
import requests
from requests.exceptions import SSLError
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
import copy

class EdinetFetchError(RuntimeError):
    pass

class EdinetAPIFetcher:
    # 取得 URL
    URL_API = "https://disclosure.edinet-fsa.go.jp/api/v1/"
    URL_DOC_LIST = URL_API + "documents.json"
    URL_DOC = URL_API + "documents/"
    # 文書 type
    # TODO: CSV (type==5) は未対応!! まだ取得できないようなので
    DOC_TYPE_MAIN = 1
    DOC_TYPE_PDF = 2
    DOC_TYPE_ATTACH = 3
    DOC_TYPE_ENG = 4
    DOC_TYPE_LIST_FULL = [1, 2, 3, 4]

    def __init__(self, *, fetch_interval=2, retry_interval=-1):
        # 取得間隔 [sec]
        self.fetch_interval = fetch_interval
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
        else: # EdinetAPIFetcher.DOC_TYPE_MAIN, EdinetAPIFetcher.DOC_TYPE_ATTACH, EdinetAPIFetcher.DOC_TYPE_ENG
            return "zip"

    def _fetch(self, url, params, headers):
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

        # データ取得 (適当に timeout 時間を設定しておく)
        r = requests.get(url, params=params, headers=headers, timeout=60)
        # 終わったら負荷をかけないように一定時間休む
        time.sleep(self.fetch_interval)
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

    def _prepare_for_retry(self, msg):
        logger.warning(msg)
        logger.warning(f"Wait for retry ({self.retry_interval}sec) ...")
        time.sleep(self.retry_interval)

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
            try:
                r = self._fetch(url, params, headers)
            except SSLError as e:
                if self.retry_interval < 0:
                    raise
                # たまに SSLError が発生するのでその場合はリトライ
                self._prepare_for_retry(str(e))
                continue
            ctype = r.headers["Content-Type"].replace(" ", "")
            if (doc_type == EdinetAPIFetcher.DOC_TYPE_PDF and ctype == "application/pdf") or (doc_type != EdinetAPIFetcher.DOC_TYPE_PDF and ctype == "application/octet-stream"):
                # 取得成功
                return r
            # 取得失敗時の処理
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
                self._prepare_for_retry(err_msg)
                continue

    def fetch_daily_doc_list(self, day, only_meta=False):
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
            r = self._fetch(EdinetAPIFetcher.URL_DOC_LIST, params, headers)
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
        if outdir.exists():
            # outdir が存在してたら警告を出して消す
            logger.warning(f"Removing existing output directory ({outdir}) ...")
            shutil.rmtree(outdir)
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

    def save_daily(self, outdir, day, *, doc_types=None, doc_codes=None, need_sec_code=False, skip_if_list_exists=False, list_name="list.json"):
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
        skip_if_list_exists : bool
            リストファイルが存在する場合その日をスキップするか
        list_name : str
            リストファイル名

        Returns
        -------
        """
        def valid_doc_types(doc_types, d):
            # Flag 的に存在しない文書 type はスキップ
            result = copy.deepcopy(doc_types)
            # NOTE: xbrlFlag:"0" でも DOC_TYPE_MAIN は一応取得しておく。xbrl 以外もあるので
            flag_dict = {"pdfFlag": EdinetAPIFetcher.DOC_TYPE_PDF, "attachDocFlag": EdinetAPIFetcher.DOC_TYPE_ATTACH, "englishDocFlag": EdinetAPIFetcher.DOC_TYPE_ENG}
            for k, v in flag_dict.items(): 
                if v in result and d[k] == "0":
                    result.remove(v)
            return result

        outdir = Path(outdir)
        list_path = outdir / list_name
        if skip_if_list_exists and list_path.exists():
            # skip_if_list_exists=True で list があったらスキップ
            logger.warning(f"Skipped: '{list_path}' already exists.")
            return
        if outdir.exists():
            # outdir が存在してたら消す
            shutil.rmtree(outdir)
        os.makedirs(outdir)

        j = api.fetch_daily_doc_list(day)
        if j is None:
            return
        for d in j["results"]:
            if doc_codes is not None and not d["docTypeCode"] in doc_codes:
                continue
            if need_sec_code and d["secCode"] is None:
                continue
            doc_id = d["docID"]
            outdir_id = outdir / doc_id
            self.save_docs_for_id(outdir_id, doc_id, doc_types=valid_doc_types(doc_types, d))

        # 全部取得したら最後に list を出力
        # 最後に出力することでこれがあるかどうかで一通り全部取得できたチェックにも使えるように
        with open(list_path, "w") as f:
            json.dump(j, f, indent=2, ensure_ascii=False)

    def save_period(self, outdir, start_day, end_day, *, doc_types=None, doc_codes=None, need_sec_code=False, skip_if_list_exists=True):
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
        skip_if_list_exists : bool
            リストファイルが存在する場合その日をスキップするか

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
            self.save_daily(daydir, day, doc_types=doc_types, doc_codes=doc_codes, need_sec_code=need_sec_code, skip_if_list_exists=skip_if_list_exists)
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
    api.save_period(args.dir, start_day, end_day, doc_types=doc_types, doc_codes=args.doc_codes, need_sec_code=args.need_sec_code)
