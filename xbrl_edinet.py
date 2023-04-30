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
xbrl_edinet.py
- script to parse EDINET XBRL
"""

import pandas as pd
from pathlib import Path
from zipfile import ZipFile
import re
import logging
logger = logging.getLogger(__name__)
from lxml import etree
from datetime import date

class XbrlEdinetParseError(RuntimeError):
    pass
# 想定外のエラー
class XbrlEdinetParseUnexpectedError(RuntimeError):
    pass

def xbrl_name_info(name):
    # jpcrp030000-asr-001_X99999-000_2012-03-31_01_2012-06-28.xbrl
    # のようなファイル名をパースする

    # jp で始まってなかったらエラー
    if name[0:2] != "jp":
        raise XbrlEdinetParseError(f"Unsupported XBRL filename ({name}) !")
    info = {}
    info["cabinet_order_code"] = name[2:5]                  # 府令略号
    info["style_code"] = name[5:11]                         # 様式略号
    info["report_code"] = name[12:15]                       # 報告書略号
    info["serial_number"] = int(name[16:19])                # 報告書連番
    info["edinet_code"] = name[20:26]                       # EDINET コード or ファンドコード
    info["additional_number"] = int(name[27:30])            # 追番
    info["end_day"] = date.fromisoformat(name[31:41])       # 報告対象期間期末日 or 報告義務発生日
    info["submission_number"] = int(name[42:44])            # 報告書提出回数
    info["submission_day"] = date.fromisoformat(name[45:55])# 報告書提出日
    return info

def parse_zip(zip_path):
    with ZipFile(zip_path) as z:
        # zip ファイルから XBRL/PublicDoc/*.xbrl ファイルを取り出して読む
        # 対象の xbrl ファイルがない場合、複数ある場合、はとりあえずエラーにする
        xbrl_file = None
        for name in z.namelist():
            if re.match('XBRL/PublicDoc/.*\.xbrl$', name) is not None:
                if xbrl_file is not None:
                    XbrlEdinetParseError(f"Multiple xbrl files ('XBRL/Public/*.xbrl') in {zip_path}!")
                xbrl_file = name
        if xbrl_file is None:
            XbrlEdinetParseUnexpectedError(f"No xbrl file ('XBRL/Public/*.xbrl') in {zip_path}!")
        root = etree.fromstring(z.read(xbrl_file))
        xbrl_name = Path(xbrl_file).name
    logger.debug(f"XBRL filename: {xbrl_name}")
    ninfo = xbrl_name_info(xbrl_name)
    nsmap = root.nsmap
    logger.debug(f"Namespace: {nsmap}")

    ## debug 用
    ## nsmap の全タグをそのまま csv 出力する
    #for ns_pre, ns in nsmap.items():
    #    elements = []
    #    for elem in root.findall(f".//{{{ns}}}*"):
    #        d = {}
    #        d["tag"] = re.sub("^{[^}]*}", "", elem.tag)
    #        d["attrib"] = elem.attrib
    #        d["text"] = elem.text
    #        elements.append(d)
    #    df = pd.DataFrame(elements)
    #    df.to_csv(f"debug/{ns_pre}.csv", index=False)

    # context タグを取得 (xbrli)
    contexts = {}
    for e_content in root.findall(f"./{{{nsmap['xbrli']}}}context"):
        id_ = e_content.get("id")
        # TODO: 区別がつかないものがあるので id 自体をそのまま入れてる。もうちょっとちゃんとできるかも
        c = {"context_id" : id_, "instant" : None, "start_date" : None, "end_date" : None, "nonconsolidated" : None}
        # 日付
        e_period = e_content.find(f"./{{{nsmap['xbrli']}}}period")
        if e_period is not None:
            for t, s in zip(["instant", "startDate", "endDate"], ["instant", "start_date", "end_date"]):
                e_date = e_period.find(f"./{{{nsmap['xbrli']}}}{t}")
                if e_date is not None:
                    c[s] = e_date.text
        # 非連結かどうか
        if "NonConsolidatedMember" in id_:
            c["nonconsolidated"] = True
        contexts[id_] = c

    # 値を取得する名前空間
    target_key = []
    target_key.append("jpdei_cor")
    target_key.append(f"jp{ninfo['cabinet_order_code']}{ninfo['style_code']}-{ninfo['report_code']}_{ninfo['edinet_code']}-{str(ninfo['additional_number']).zfill(3)}")
    target_key.append(f"jp{ninfo['cabinet_order_code']}_cor")
    target_key.append(f"jp{ninfo['cabinet_order_code']}-{ninfo['report_code']}_cor")  # 報告書略号が入ってる場合もある？
    target_key.append("jppfs_cor")
    nsmap_target = {k : nsmap[k] for k in target_key if k in nsmap}

    # 値を取得
    xbrl_data = []
    for ns_pre, ns in nsmap_target.items():
        for elem in root.findall(f".//{{{ns}}}*"):
            d = {"ns_pre" : ns_pre}
            d["tag"] = re.sub("^{[^}]*}", "", elem.tag)
            for k, v in contexts[elem.get("contextRef")].items():
                d[k] = v
            d["text"] = elem.text
            d["unit"] = elem.get("unitRef")
            xbrl_data.append(d)
    df = pd.DataFrame(xbrl_data)
    # 重複データを除く
    df = df[~df.duplicated()]
    return df

# テストコード
if __name__ == "__main__":
    logging.basicConfig(
        level = logging.DEBUG,
        format = "[%(asctime)s][%(levelname)s] %(message)s",
    )

    # 一つの zip ファイルの  parse テスト
    df = parse_zip("xxx.zip")
    df.to_csv("xxx.csv", index=False, encoding="utf_8_sig")
