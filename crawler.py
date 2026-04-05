"""
Azbil Web仮想工場 生産計画表 クロールスクリプト

1日2回GitHub Actionsから実行される。
- 生産計画表（板金本体）を4週分クロール
- Googleスプレッドシートに結果を書き出す
- 前回から変更があればメールで通知する
"""

import base64
import os
import re
import json
import smtplib
import sys
import time
import traceback
from datetime import datetime, timezone, timedelta
from urllib.parse import urljoin, urlparse, parse_qs, unquote
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import gspread
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright


# ──────────────────────────────────────────
# 設定
# ──────────────────────────────────────────
SITE_URL       = "https://v-factory.azbil.com/rweb/WALOG/asp/WALOG.asp"
LOGIN_ID       = os.environ["SITE_LOGIN_ID"]
LOGIN_PASSWORD = os.environ["SITE_LOGIN_PASSWORD"]

MAIL_USER     = "sinki@shinki-kk.co.jp"
MAIL_PASSWORD = os.environ["MAIL_PASSWORD"]
MAIL_TO       = "sinki@shinki-kk.co.jp"
SMTP_SERVER   = "smtp.lolipop.jp"
SMTP_PORT     = 465

SPREADSHEET_ID  = os.environ["SPREADSHEET_ID"]          # スプレッドシートのID
SHEET_MAIN      = "最新データ"                           # メインシート名
SHEET_BACKUP    = "前回データ"                           # バックアップシート名
SHEET_CHANGES   = "変更履歴"                             # 変更履歴シート名
SHEET_SETTINGS  = "設定"                                 # 設定シート名
SPREADSHEET_URL = f"https://docs.google.com/spreadsheets/d/{os.environ['SPREADSHEET_ID']}"

# 取得する項目の列ヘッダー
HEADERS = ["積上日", "工事番号", "外形図番", "盤種類", "本数", "出図日", "組配協力会社名"]

# 外形図番サフィックス（E・G・H・I・N・O・P は聞き間違い防止のため除外）
SUFFIX_CHARS = list("ABCDFJKLMQRS")

# Playwright: ログイン・カレンダー操作
_PW_TIMEOUT_MS = 60_000
# 工程詳細は DOM が出ればよい（load や毎回 new_page だと GitHub 上で極端に遅くなる）
_PW_DETAIL_NAV_MS = 20_000


def _route_skip_images_fonts(route):
    """詳細の連続取得時の転送量削減（表の input はそのまま）"""
    try:
        if route.request.resource_type in ("image", "font", "media"):
            route.abort()
        else:
            route.continue_()
    except Exception:
        try:
            route.continue_()
        except Exception:
            pass


# ──────────────────────────────────────────
# スプレッドシート操作
# ──────────────────────────────────────────
def get_sheet_client():
    """Google Sheets APIに接続する"""
    creds_json = os.environ["GOOGLE_SHEETS_CREDENTIALS"]
    creds_dict = json.loads(creds_json)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(creds)


def read_sheet(client, sheet_name):
    """指定シートの全データを取得する（1行目の取得日時・2行目のヘッダーを除く）"""
    sh = client.open_by_key(SPREADSHEET_ID)
    try:
        ws = sh.worksheet(sheet_name)
        rows = ws.get_all_values()
        return rows[2:] if len(rows) > 2 else []  # 1行目:取得日時、2行目:ヘッダー
    except gspread.WorksheetNotFound:
        return []


# ユーザーが編集できる設定項目（設定項目, デフォルト値, 説明）
_DEFAULT_SETTINGS = [
    ("通知先メールアドレス",   MAIL_TO,  "カンマ区切りで複数指定可（例：a@example.com,b@example.com）"),
    ("変更なし時もメール送信", "はい",   "はい / いいえ"),
]

# スケジュール表示行のキー（参考表示のみ・編集不可）
_SCHEDULE_KEY = "クロール実行時刻（参考・変更不可）"


def _read_schedule_jst():
    """crawl.yml のcron設定を読み取り、JST時刻のリストを返す"""
    try:
        yml_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                ".github", "workflows", "crawl.yml")
        with open(yml_path, encoding="utf-8") as f:
            content = f.read()
        crons = re.findall(r"cron:\s*['\"](\d+)\s+(\d+)\s+\*\s+\*\s+\*['\"]", content)
        times = []
        for minute, hour in crons:
            jst_hour = (int(hour) + 9) % 24
            times.append(f"{jst_hour:02d}:{int(minute):02d}")
        return sorted(times)
    except Exception:
        return []


def _format_settings_sheet(ws, num_user_rows):
    """設定シートの書式を設定する（ヘッダー行：青、スケジュール行：グレー）"""
    requests = [
        # ヘッダー行（1行目）：青背景・白太字
        {
            "repeatCell": {
                "range": {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": 1,
                          "startColumnIndex": 0, "endColumnIndex": 3},
                "cell": {"userEnteredFormat": {
                    "backgroundColor": {"red": 0.27, "green": 0.51, "blue": 0.71},
                    "textFormat": {"bold": True,
                                   "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}},
                }},
                "fields": "userEnteredFormat(backgroundColor,textFormat)",
            }
        },
        # スケジュール行（最終行）：グレー背景・斜体
        {
            "repeatCell": {
                "range": {"sheetId": ws.id,
                          "startRowIndex": num_user_rows + 1,
                          "endRowIndex":   num_user_rows + 2,
                          "startColumnIndex": 0, "endColumnIndex": 3},
                "cell": {"userEnteredFormat": {
                    "backgroundColor": {"red": 0.85, "green": 0.85, "blue": 0.85},
                    "textFormat": {"italic": True},
                }},
                "fields": "userEnteredFormat(backgroundColor,textFormat)",
            }
        },
    ]
    ws.spreadsheet.batch_update({"requests": requests})


def read_settings(client):
    """
    設定シートを読み込んで辞書で返す。
    シートが存在しない場合はデフォルト値で新規作成する。
    スケジュール表示行は毎回最新の crawl.yml 値に更新する。
    """
    sh = client.open_by_key(SPREADSHEET_ID)
    schedule_times = _read_schedule_jst()
    schedule_val   = " / ".join(schedule_times) if schedule_times else "（取得できませんでした）"

    try:
        ws   = sh.worksheet(SHEET_SETTINGS)
        rows = ws.get_all_values()

        # ユーザー設定を読み込む（スケジュール行はスキップ）
        settings = {}
        for row in rows[1:]:
            if len(row) >= 2 and row[0].strip() and _SCHEDULE_KEY not in row[0]:
                settings[row[0].strip()] = row[1].strip()

        # デフォルト値で未設定項目を補完
        for key, default, _ in _DEFAULT_SETTINGS:
            if key not in settings:
                settings[key] = default

        # スケジュール行を最新値に同期
        schedule_row_idx = None
        for i, row in enumerate(rows):
            if row and _SCHEDULE_KEY in row[0]:
                schedule_row_idx = i + 1  # 1-based
                break
        if schedule_row_idx:
            ws.update_cell(schedule_row_idx, 2, schedule_val)
        else:
            next_row = len(rows) + 1
            ws.update(f"A{next_row}:C{next_row}",
                      [[_SCHEDULE_KEY, schedule_val, "変更はClaudeにご依頼ください"]])
            _format_settings_sheet(ws, len(_DEFAULT_SETTINGS))

        return settings

    except gspread.WorksheetNotFound:
        # シートが存在しない場合はデフォルト値＋スケジュール行で新規作成
        ws = sh.add_worksheet(title=SHEET_SETTINGS, rows=30, cols=5)
        header    = ["設定項目", "値", "説明"]
        user_data = [[key, val, desc] for key, val, desc in _DEFAULT_SETTINGS]
        sched_row = [_SCHEDULE_KEY, schedule_val, "変更はClaudeにご依頼ください"]
        ws.update([header] + user_data + [sched_row])
        _format_settings_sheet(ws, len(_DEFAULT_SETTINGS))
        print("[設定] 設定シートを新規作成しました", flush=True)
        return {key: val for key, val, _ in _DEFAULT_SETTINGS}


def write_sheet(client, sheet_name, rows, run_datetime="", changes=None):
    """指定シートを上書きする。1行目に取得日時、2行目にヘッダー、3行目以降にデータ"""
    sh = client.open_by_key(SPREADSHEET_ID)
    try:
        ws = sh.worksheet(sheet_name)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=sheet_name, rows=500, cols=20)

    meta_row = [f"取得日時：{run_datetime}"] if run_datetime else [""]
    ws.update([meta_row, HEADERS] + rows)
    format_sheet(ws, rows)
    if changes:
        _colorize_changes_in_sheet(ws, rows, changes)


def format_sheet(ws, rows):
    """スプレッドシートの書式を設定する"""
    num_cols = len(HEADERS)
    requests = []

    # ── ヘッダー行（2行目）：背景色・白太字・中央揃え ──
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": ws.id,
                "startRowIndex": 1,   # 0-based で2行目
                "endRowIndex": 2,
                "startColumnIndex": 0,
                "endColumnIndex": num_cols,
            },
            "cell": {
                "userEnteredFormat": {
                    "backgroundColor": {"red": 0.27, "green": 0.51, "blue": 0.71},
                    "textFormat": {
                        "bold": True,
                        "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
                    },
                    "horizontalAlignment": "CENTER",
                }
            },
            "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)",
        }
    })

    # ── 積上日グループの区切り：グループが変わる行の上に横罫線 ──
    # データは3行目以降（0-based index 2〜）
    prev_date = None
    for i, row in enumerate(rows):
        current_date = row[0] if row else ""
        if prev_date is not None and current_date != prev_date:
            sheet_row_index = i + 2   # 0-based: 1行目=meta, 2行目=header, 3行目〜=data
            requests.append({
                "updateBorders": {
                    "range": {
                        "sheetId": ws.id,
                        "startRowIndex": sheet_row_index,
                        "endRowIndex": sheet_row_index + 1,
                        "startColumnIndex": 0,
                        "endColumnIndex": num_cols,
                    },
                    "top": {
                        "style": "SOLID_MEDIUM",
                        "color": {"red": 0.0, "green": 0.0, "blue": 0.0},
                    },
                }
            })
        prev_date = current_date

    if requests:
        ws.spreadsheet.batch_update({"requests": requests})


def _colorize_changes_in_sheet(ws, rows, changes):
    """最新データシートの追加・変更行に色を付ける"""
    added_keys   = {c["工事番号"] for c in changes if c["種別"] == "追加"}
    changed_keys = {c["工事番号"] for c in changes if c["種別"] == "変更"}

    color_added   = {"red": 0.85, "green": 0.93, "blue": 0.83}  # 薄緑：追加
    color_changed = {"red": 1.00, "green": 0.95, "blue": 0.80}  # 薄黄：変更

    requests = []
    for i, row in enumerate(rows):
        if len(row) <= 1:
            continue
        key = row[1]
        if key in added_keys:
            color = color_added
        elif key in changed_keys:
            color = color_changed
        else:
            continue
        sheet_row = i + 2   # 0-based: meta=0, header=1, data starts at 2
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": sheet_row,
                    "endRowIndex": sheet_row + 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": len(HEADERS),
                },
                "cell": {"userEnteredFormat": {"backgroundColor": color}},
                "fields": "userEnteredFormat(backgroundColor)",
            }
        })

    if requests:
        ws.spreadsheet.batch_update({"requests": requests})


def write_changes_sheet(client, changes, run_datetime=""):
    """変更履歴シートに今回の変更内容を書き出す"""
    sh = client.open_by_key(SPREADSHEET_ID)
    try:
        ws = sh.worksheet(SHEET_CHANGES)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=SHEET_CHANGES, rows=500, cols=10)

    meta_row       = [f"確認日時：{run_datetime}"] if run_datetime else [""]
    change_headers = ["種別", "工事番号", "変更項目", "変更前", "変更後"]

    changes_sheet_url = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit#gid={ws.id}"

    if not changes:
        ws.update([meta_row, ["変更はありませんでした"]])
        return changes_sheet_url

    change_rows = []
    for c in changes:
        key = c["工事番号"]
        if c["種別"] == "追加":
            for i, h in enumerate(HEADERS):
                change_rows.append(["追加", key, h, "-", c["内容"][i] if i < len(c["内容"]) else ""])
        elif c["種別"] == "削除":
            for i, h in enumerate(HEADERS):
                change_rows.append(["削除", key, h, c["内容"][i] if i < len(c["内容"]) else "", "-"])
        elif c["種別"] == "変更":
            for diff in c["差分"]:
                parts   = diff.split("：", 1)
                field   = parts[0]
                vals    = parts[1].split(" → ") if len(parts) > 1 else ["", ""]
                old_val = vals[0] if vals else ""
                new_val = vals[1] if len(vals) > 1 else ""
                change_rows.append(["変更", key, field, old_val, new_val])

    ws.update([meta_row, change_headers] + change_rows)

    # 書式設定
    requests = []

    # ヘッダー行（2行目）
    requests.append({
        "repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": 1, "endRowIndex": 2,
                      "startColumnIndex": 0, "endColumnIndex": 5},
            "cell": {"userEnteredFormat": {
                "backgroundColor": {"red": 0.27, "green": 0.51, "blue": 0.71},
                "textFormat": {"bold": True, "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}},
                "horizontalAlignment": "CENTER",
            }},
            "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)",
        }
    })

    # データ行の色付け
    color_map = {
        "追加": {"red": 0.85, "green": 0.93, "blue": 0.83},  # 薄緑
        "削除": {"red": 0.96, "green": 0.80, "blue": 0.80},  # 薄赤
        "変更": {"red": 1.00, "green": 0.95, "blue": 0.80},  # 薄黄
    }
    for i, row in enumerate(change_rows):
        color = color_map.get(row[0])
        if color:
            sheet_row = i + 2   # 0-based: meta=0, header=1, data starts at 2
            requests.append({
                "repeatCell": {
                    "range": {"sheetId": ws.id, "startRowIndex": sheet_row, "endRowIndex": sheet_row + 1,
                              "startColumnIndex": 0, "endColumnIndex": 5},
                    "cell": {"userEnteredFormat": {"backgroundColor": color}},
                    "fields": "userEnteredFormat(backgroundColor)",
                }
            })

    if requests:
        ws.spreadsheet.batch_update({"requests": requests})

    return changes_sheet_url


# ──────────────────────────────────────────
# クロール処理
# ──────────────────────────────────────────
def _is_frame_alive(fr):
    """フレームが生きている（detached でない）か確認する"""
    try:
        fr.evaluate("1")
        return True
    except Exception:
        return False


def _resolve_calendar_root(page):
    """
    カレンダーがある BODY フレームを返す。
    検索ボタンや「次の2週」でフレームが差し替わると、以前の Frame は detached になるため毎回取り直す。
    detached なフレームはスキップし、生きているフレームだけを返す。
    """
    # name="BODY" で見つかっても detached なら使わない
    body = page.frame(name="BODY")
    if body is not None and _is_frame_alive(body):
        return body

    # name で見つからない場合、URL に "W20_body" を含む生きたフレームを探す
    for f in page.frames:
        if f.url and "W20_body" in f.url and _is_frame_alive(f):
            return f

    return page


def _rows_per_unit(banshu):
    """盤種類から1台あたりのカレンダー行数を返す（自立型=2行、壁掛型=1行）"""
    if "自" in banshu:
        return 2
    return 1  # 壁掛型、またはその他


def expand_jobs(jobs_with_rowspan):
    """
    (job_row, rowspan) のリストを受け取り、本数ごとに行を展開して返す。

    - rowspan ÷ rows_per_unit(盤種類) = この日の台数
    - 同じ (工事番号, 外形図番) が複数日にまたがる場合、積上日順にサフィックスを連続付与
    - 台数が合計1台の場合はサフィックスなし
    - 展開後の各行は「本数=1」（1行1台）

    サフィックス順: A B C D F J K L M Q R S
    """
    from collections import defaultdict

    # (工事番号, 外形図番) でグループ化（出現順を保持）
    groups = defaultdict(list)
    key_order = []
    seen_keys = set()
    for job, rowspan in jobs_with_rowspan:
        key = (job[1], job[2])  # (工事番号, 外形図番)
        if key not in seen_keys:
            key_order.append(key)
            seen_keys.add(key)
        groups[key].append((job, rowspan))

    result = []
    for key in key_order:
        entries = groups[key]
        # 積上日（job[0]）でソート
        entries.sort(key=lambda x: x[0][0])

        banshu = entries[0][0][3]  # 盤種類（全エントリ共通のはず）
        rpu = _rows_per_unit(banshu)

        # 各エントリの1日分の台数を算出
        per_day = [(job, max(1, rowspan // rpu)) for job, rowspan in entries]
        total_units = sum(u for _, u in per_day)

        if total_units <= 1:
            # サフィックス不要：そのまま出力
            result.append(entries[0][0][:])
            continue

        # サフィックスを付与して1行1台に展開
        suffix_idx = 0
        for job, units in per_day:
            for _ in range(units):
                new_job = job[:]
                if suffix_idx < len(SUFFIX_CHARS):
                    new_job[2] = job[2] + SUFFIX_CHARS[suffix_idx]
                suffix_idx += 1
                new_job[4] = "1"  # 本数は1（1行1台）
                result.append(new_job)

    return result


def crawl():
    """サイトにログインして生産計画表を4週分クロールし、ジョブ一覧を返す"""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        # browser.new_page() だけのとき、環境によっては context.new_page() が使えない
        context = browser.new_context()
        context.set_default_navigation_timeout(_PW_TIMEOUT_MS)
        context.set_default_timeout(_PW_TIMEOUT_MS)
        page = context.new_page()
        print("[クロール] ブラウザ起動・サイトへ接続中…", flush=True)

        # ── ログイン ──
        page.goto(SITE_URL, wait_until="load")
        page.screenshot(path="screenshot_login.png")

        # ログインフォームの入力欄を探す（name属性が不明なためtype属性で検索）
        page.locator('input[type="text"]').first.fill(LOGIN_ID)
        page.locator('input[type="password"]').first.fill(LOGIN_PASSWORD)
        page.locator('input[type="submit"]').first.click()
        # ログイン後のリダイレクトが完全に終わるまで待つ
        try:
            page.wait_for_load_state("networkidle", timeout=_PW_TIMEOUT_MS)
        except Exception:
            page.wait_for_load_state("load", timeout=_PW_TIMEOUT_MS)
        page.screenshot(path="screenshot_after_login.png")

        # ── 生産計画表リンクをクリック ──
        page.locator('text=生産計画表').wait_for(timeout=_PW_TIMEOUT_MS)
        page.click('text=生産計画表')
        page.wait_for_load_state("load", timeout=_PW_TIMEOUT_MS)

        # ── ページがフレーム構成のため、HEADフレームにアクセス ──
        head_frame = page.frame(name="HEAD")
        if head_frame is None:
            head_frame = next((f for f in page.frames if f.url and "W20_head" in f.url), None)

        # HEADフレームの読み込みを待つ
        if head_frame is not None:
            head_frame.wait_for_load_state("load", timeout=_PW_TIMEOUT_MS)

        # ── 検索条件：板金本体を選択して検索（HEADフレーム内）──
        # ラジオボタンのvalue属性は英語: BanSub/BanMain/Kumihai/Shukka
        head_frame.check('input[value="BanMain"]')
        head_frame.click('input[value="検索"]')
        page.wait_for_load_state("load", timeout=_PW_TIMEOUT_MS)
        page.screenshot(path="screenshot_calendar.png")

        # カレンダー表示後は詳細だけ連続で開く。タブの作り直しを減らし、画像等は落とす
        # ただし、href収集中は画像ブロックしない（calendar.jpgが必要）
        # → href収集を全部済ませてから画像ブロックを有効にする

        # ── フェーズ1：1〜2週目と3〜4週目のhrefをすべて先に収集する ──
        # （詳細ページを開くとASPセッションが壊れるため、ボタン操作を先に済ませる）
        all_detail_items = []  # (url, rowspan) のリスト

        # 1〜2週目のhref収集
        calendar_root = _resolve_calendar_root(page)
        week12_items = _collect_detail_hrefs(calendar_root, page)
        print(f"[クロール] 1〜2週目 詳細リンク数：{len(week12_items)}件", flush=True)
        all_detail_items.extend(week12_items)

        # ── 「次の2週」ボタンで3〜4週目へ移動（詳細ページをまだ開いていないのでセッションは健全）──
        button_frame = None
        for fr in page.frames:
            try:
                fr.evaluate("1")           # detached なら例外
                cnt = fr.locator('input[name="QS_NextWeek"]').count()
                if cnt > 0:
                    button_frame = fr
                    break
            except Exception:
                pass

        if button_frame is None:
            print("[クロール] 次の2週ボタンが見つかりませんでした。スキップします。", flush=True)
        else:
            btn_url = getattr(button_frame, "url", "N/A")
            print(f"[クロール] 次の2週ボタン発見: frame={button_frame.name!r} url={btn_url[:80]}", flush=True)

            # TARGET="_top" なのでトップフレームがリロードされる
            with page.expect_navigation(wait_until="load", timeout=_PW_TIMEOUT_MS):
                button_frame.locator('input[name="QS_NextWeek"]').click()

            # CmnWaitNonClear.asp → W20.asp へのリダイレクトを待つ
            print(f"[クロール] 移動後ページURL: {page.url[:100]}", flush=True)
            try:
                page.wait_for_url(
                    lambda url: "CmnWait" not in url,
                    timeout=_PW_TIMEOUT_MS,
                )
                print(f"[クロール] 最終ページURL: {page.url[:100]}", flush=True)
            except Exception as e:
                print(f"[クロール] CmnWait後の遷移タイムアウト: {e}", flush=True)

            # BODYフレームの読み込みを待つ
            time.sleep(3)
            week12_url_set = {url for url, _ in week12_items}
            new_body = None
            for attempt in range(15):
                candidate = _resolve_calendar_root(page)
                if candidate is not page and _is_frame_alive(candidate):
                    try:
                        candidate.wait_for_load_state("load", timeout=_PW_TIMEOUT_MS)
                        new_items = _collect_detail_hrefs(candidate, page)
                        new_url_set = {url for url, _ in new_items}
                        if new_items and new_url_set != week12_url_set:
                            new_body = candidate
                            print(f"[クロール] 3〜4週目の新データ確認OK（{len(new_items)}件、試行{attempt+1}回目）", flush=True)
                            all_detail_items.extend(new_items)
                            break
                        else:
                            print(f"[クロール] DOMまだ更新されていない（href同一、試行{attempt+1}回目）", flush=True)
                    except Exception:
                        pass
                else:
                    print(f"[クロール] BODYフレーム待機中…（試行{attempt+1}回目）", flush=True)
                time.sleep(2)

            if new_body is None:
                print("[クロール] 警告：3〜4週目のDOM更新が確認できませんでした", flush=True)

            # スクリーンショット
            try:
                page.screenshot(path="screenshot_calendar_week34.png")
                print("[クロール] 3〜4週目スクリーンショット保存OK", flush=True)
            except Exception as e:
                print(f"[クロール] スクリーンショット失敗: {e}", flush=True)

        # ── フェーズ2：収集したURLをもとに詳細ページを取得 ──
        # URL重複を除去（順序維持）
        seen = set()
        unique_items = []
        for (u, rs) in all_detail_items:
            if u not in seen:
                seen.add(u)
                unique_items.append((u, rs))
        all_detail_items = unique_items

        print(f"[クロール] 全詳細リンク数（重複除去後）：{len(all_detail_items)}件", flush=True)

        # テストモード
        if os.environ.get("TEST_MODE") == "true":
            all_detail_items = all_detail_items[:6]  # 各週3件ずつ程度
            print(f"  ※テストモード：{len(all_detail_items)}件に絞って取得します", flush=True)

        # 画像ブロックを有効にして転送量を削減
        context.route("**/*", _route_skip_images_fonts)

        raw_jobs = []  # (job_row, rowspan) の一時リスト
        if all_detail_items:
            detail_page = page.context.new_page()
            detail_page.set_default_navigation_timeout(_PW_DETAIL_NAV_MS)
            detail_page.set_default_timeout(5_000)
            try:
                for i, (detail_url, rowspan) in enumerate(all_detail_items, start=1):
                    print(f"  詳細取得 {i}/{len(all_detail_items)} …", flush=True)
                    try:
                        detail_page.goto(
                            detail_url,
                            wait_until="load",
                            timeout=_PW_DETAIL_NAV_MS,
                        )
                        time.sleep(0.5)
                        job = extract_job_detail(detail_page)
                        if job:
                            raw_jobs.append((job, rowspan))
                    except Exception as e:
                        print(f"  （スキップ {i}）{e}", flush=True)
            finally:
                detail_page.close()

        browser.close()

    return expand_jobs(raw_jobs)


# 相対リンク解決のフォールバック（フレームURLが取れないとき）
_DEFAULT_LINK_BASE = "https://v-factory.azbil.com/rweb/WALOG/asp/W20_body.asp"


def _frame_resolve_base(fr):
    """フレーム内の相対hrefを解決するための基準URL（そのフレームのドキュメントURL）"""
    u = getattr(fr, "url", None) or ""
    return u if u.startswith("http") else _DEFAULT_LINK_BASE


def _resolve_cmnlinknonclear(url):
    """
    CmnLinkNonClear.asp?RtnURL=... は中継ページなので、
    RtnURL パラメータから実際の遷移先URLを取り出して直接使う。
    """
    if "cmnlinknonclear.asp" not in url.lower():
        return url
    parsed = urlparse(url)
    params = parse_qs(parsed.query, keep_blank_values=True)
    rtn = None
    for k, v in params.items():
        if k.lower() == "rtnurl":
            rtn = v[0]
            break
    if not rtn:
        return url
    rtn_path = unquote(rtn)  # 例: ../../LPP/asp/W26.asp?KouBan=...
    base = url.split("?")[0]  # CmnLinkNonClear.asp の絶対URL
    resolved = urljoin(base, rtn_path)
    print(f"  [リダイレクト解決] {resolved}", flush=True)
    return resolved


def _detail_page_url(href, base_url=None):
    """詳細画面への絶対URLを組み立てる（../../CMN/... など相対パスは urljoin で解決）"""
    if not href:
        return None
    href = href.strip()
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("/"):
        return f"https://v-factory.azbil.com{href}"
    base = base_url or _DEFAULT_LINK_BASE
    return urljoin(base, href)


def _looks_like_job_detail_href(href):
    """工程詳細へ飛ぶリンクかどうか（表記揺れ・大文字小文字を許容）"""
    if not href:
        return False
    u = href.strip()
    low = u.lower()
    if low.startswith("javascript:") or low.startswith("#") or low.startswith("mailto:"):
        return False
    # 旧：WALOG 詳細直リンク
    if "walog_detail" in low:
        return True
    if "walog" in low and "detail" in low:
        return True
    # 実サイトのカレンダーアイコン：中継 → RtnURL 内に W21（工程）/ W26 等
    if "cmnlinknonclear.asp" in low and "rtnurl" in low:
        return True
    return False


def _gather_hrefs_from_frame(fr):
    """フレーム内のカレンダーアイコン（calendar.jpg）を含むリンクの href と rowspan を列挙"""
    # detached なフレームはスキップ
    if not _is_frame_alive(fr):
        return []
    collected = []
    # rowspan = 親 <td> の rowSpan。1台あたりの行数（自立型=2/壁掛型=1）× 本数 になる
    expr = (
        "els => els.map(e => {"
        "  const td = e.closest('td');"
        "  return { href: e.getAttribute('href'), rowspan: td ? td.rowSpan : 1 };"
        "}).filter(o => o.href && o.href.trim())"
    )
    try:
        collected.extend(fr.eval_on_selector_all("a:has(img[src*='calendar.jpg'])", expr))
    except Exception as e:
        fr_url = getattr(fr, "url", "N/A")
        print(f"  [_gather_hrefs] {fr.name!r} ({fr_url[:60]}) エラー: {e}", flush=True)
    return collected  # [{"href": str, "rowspan": int}]


def _iter_calendar_frames(calendar_root, page):
    """カレンダー候補となるフレーム（BODY とその子 iframe）を列挙。detached フレームは除外。"""
    if calendar_root is page:
        return [f for f in page.frames if _is_frame_alive(f)]
    out = []

    def walk(fr):
        if _is_frame_alive(fr):
            out.append(fr)
            for ch in fr.child_frames:
                walk(ch)

    walk(calendar_root)
    return out


def _collect_detail_hrefs(calendar_root, page):
    """
    詳細URL と rowspan を重複なく収集（(url, rowspan) のリストを返す）。
    rowspan = 親 <td> の rowSpan 値（盤種類の行数/台 × 本数 に相当）。
    """
    ordered = []
    seen = set()
    all_raw = []

    def process_frame(fr):
        try:
            part = _gather_hrefs_from_frame(fr)
        except Exception:
            return
        base_url = _frame_resolve_base(fr)
        for item in part:
            h       = item["href"]
            rowspan = item.get("rowspan", 1)
            all_raw.append(h)
            if not _looks_like_job_detail_href(h):
                continue
            full = _resolve_cmnlinknonclear(_detail_page_url(h, base_url))
            if not full or full in seen:
                continue
            seen.add(full)
            ordered.append((full, rowspan))

    for fr in _iter_calendar_frames(calendar_root, page):
        process_frame(fr)

    if not ordered:
        for fr in page.frames:
            if _is_frame_alive(fr):
                process_frame(fr)

    if not ordered:
        sample = []
        sset = set()
        for s in all_raw:
            if s and s not in sset and len(sample) < 15:
                sset.add(s)
                sample.append(s)
        if sample:
            print(f"詳細候補0件。画面内のリンク例（最大15）: {sample}")

    return ordered  # [(url, rowspan)]


def scrape_calendar(page, calendar_root):
    """現在表示されているカレンダーの全ジョブ詳細を取得する"""
    jobs = []

    detail_urls = _collect_detail_hrefs(calendar_root, page)
    print(f"詳細リンク数：{len(detail_urls)}件（この2週分の画面）", flush=True)

    # テストモード：最初の3件だけ取得して動作確認を素早く行う
    if os.environ.get("TEST_MODE") == "true":
        detail_urls = detail_urls[:3]
        print(f"  ※テストモード：{len(detail_urls)}件に絞って取得します", flush=True)

    if not detail_urls:
        return jobs

    # タブを毎回 new/close しない（Chrome で人が連続クリックするのに近く、CI でも速い）
    detail_page = page.context.new_page()
    detail_page.set_default_navigation_timeout(_PW_DETAIL_NAV_MS)
    detail_page.set_default_timeout(5_000)   # 要素が見つからない場合は5秒で諦める
    try:
        for i, detail_url in enumerate(detail_urls, start=1):
            print(f"  詳細取得 {i}/{len(detail_urls)} …", flush=True)
            try:
                detail_page.goto(
                    detail_url,
                    wait_until="load",
                    timeout=_PW_DETAIL_NAV_MS,
                )
                time.sleep(0.5)
                job = extract_job_detail(detail_page)
                if job:
                    jobs.append(job)
            except Exception as e:
                print(f"  （スキップ {i}）{e}", flush=True)
    finally:
        detail_page.close()

    return jobs


def extract_job_detail(page):
    """工程詳細画面から必要項目を取得する"""
    def get_input(name):
        try:
            return page.locator(f'input[name="{name}"]').first.input_value().strip()
        except Exception:
            return ""

    def get_td_text(label):
        """ラベルTD → コロンTD → 値TD の順で2つ隣のTDテキストを取得"""
        try:
            return page.locator(f'td:has-text("{label}") + td + td').first.inner_text(timeout=3000).strip()
        except Exception:
            return ""

    def get_bkgdate():
        """URLのBkgDateパラメータをbase64デコードしてカレンダー日付を取得"""
        try:
            params = parse_qs(urlparse(page.url).query)
            b64 = params.get("BkgDate", [""])[0]
            if not b64:
                return ""
            padding = (4 - len(b64) % 4) % 4
            return base64.b64decode(b64 + "=" * padding).decode("utf-8").strip()
        except Exception:
            return ""

    calendar_date = get_bkgdate()                        # カレンダー日付（URLから）
    koujiban_raw  = get_input("QS_WorkNo")
    koujiban      = re.sub(r'^\d+-', '', koujiban_raw)   # 先頭の「1-」などの数字+ハイフンを除去
    gaiken        = get_input("QS_DwgNo")                # 外形図番
    banshu        = get_td_text("盤種類")                 # 盤種類
    honsuu        = get_input("QS_Ukeire")               # 本数
    shizubi_raw   = get_td_text("予実績")                 # 出図日（板金・塗装列の予実績）
    try:
        shizubi = datetime.strptime(shizubi_raw, "%y-%m-%d").strftime("%Y/%m/%d")
    except Exception:
        shizubi = shizubi_raw  # 変換できない場合はそのまま
    kumiai        = get_input("QS_KumiCorpName")         # 組配協力会社名

    if not koujiban:
        return None

    return [calendar_date, koujiban, gaiken, banshu, honsuu, shizubi, kumiai]


# ──────────────────────────────────────────
# 変更検知
# ──────────────────────────────────────────
def detect_changes(old_rows, new_rows, settings=None):
    """
    前回データと今回データを比較し、変更点を返す。
    工事番号（列インデックス1）をキーとして比較する。
    settings で通知する項目を絞り込める。
    """
    def _should_notify(header):
        """この項目の変更を通知するか（設定シートで「いいえ」にした項目は無視）"""
        if settings is None:
            return True
        key = f"「{header}」変更を通知"
        return settings.get(key, "はい").strip() != "いいえ"

    changes = []

    old_dict = {row[1]: row for row in old_rows if len(row) > 1}
    new_dict = {row[1]: row for row in new_rows if len(row) > 1}

    # 追加されたジョブ
    for key in new_dict:
        if key not in old_dict:
            changes.append({"種別": "追加", "工事番号": key, "内容": new_dict[key]})

    # 削除されたジョブ
    for key in old_dict:
        if key not in new_dict:
            changes.append({"種別": "削除", "工事番号": key, "内容": old_dict[key]})

    # 変更されたジョブ
    for key in new_dict:
        if key in old_dict:
            old = old_dict[key]
            new = new_dict[key]
            diff_items = []
            for i, header in enumerate(HEADERS):
                if header in ("積上日", "工事番号"):
                    continue
                if not _should_notify(header):
                    continue
                old_val = old[i] if i < len(old) else ""
                new_val = new[i] if i < len(new) else ""
                if old_val != new_val:
                    diff_items.append(f"{header}：{old_val} → {new_val}")
            if diff_items:
                changes.append({"種別": "変更", "工事番号": key, "差分": diff_items})

    return changes


# ──────────────────────────────────────────
# メール送信
# ──────────────────────────────────────────
def send_email(changes, new_rows, changes_sheet_url="", mail_to_list=None):
    """変更内容をメールで送信する"""
    now = datetime.now(timezone(timedelta(hours=9))).strftime("%Y年%m月%d日 %H:%M")
    subject = f"【生産計画表】変更通知 {now}"

    # メール本文を作成
    lines = [f"生産計画表の変更を検知しました。（確認日時：{now}）\n"]

    for c in changes:
        lines.append(f"■ {c['種別']}　工事番号：{c['工事番号']}")
        if c["種別"] == "追加":
            for i, h in enumerate(HEADERS):
                if i < len(c["内容"]):
                    lines.append(f"  {h}：{c['内容'][i]}")
        elif c["種別"] == "削除":
            lines.append("  （カレンダーから削除されました）")
            for i, h in enumerate(HEADERS):
                if i < len(c["内容"]):
                    lines.append(f"  {h}：{c['内容'][i]}")
        elif c["種別"] == "変更":
            for diff in c["差分"]:
                lines.append(f"  {diff}")
        lines.append("")

    lines.append(f"\n今回のクロールで取得したジョブ数：{len(new_rows)}件")
    url = changes_sheet_url or SPREADSHEET_URL
    lines.append(f"\n変更履歴シートで確認：\n{url}")

    body = "\n".join(lines)
    recipients = mail_to_list or [MAIL_TO]

    msg = MIMEMultipart()
    msg["From"]    = MAIL_USER
    msg["To"]      = ", ".join(recipients)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain", "utf-8"))

    with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
        server.login(MAIL_USER, MAIL_PASSWORD)
        server.sendmail(MAIL_USER, recipients, msg.as_string())

    print(f"メール送信完了（宛先：{', '.join(recipients)}）")


def send_no_change_email(new_rows, changes_sheet_url="", mail_to_list=None):
    """変更なしの場合も確認メールを送る"""
    now = datetime.now(timezone(timedelta(hours=9))).strftime("%Y年%m月%d日 %H:%M")
    subject = f"【生産計画表】変更なし {now}"
    url = changes_sheet_url or SPREADSHEET_URL
    body = (
        f"生産計画表に変更はありませんでした。（確認日時：{now}）\n"
        f"取得ジョブ数：{len(new_rows)}件\n\n"
        f"変更履歴シートで確認：\n{url}"
    )
    recipients = mail_to_list or [MAIL_TO]

    msg = MIMEMultipart()
    msg["From"]    = MAIL_USER
    msg["To"]      = ", ".join(recipients)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain", "utf-8"))

    with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
        server.login(MAIL_USER, MAIL_PASSWORD)
        server.sendmail(MAIL_USER, recipients, msg.as_string())

    print(f"変更なしメール送信完了（宛先：{', '.join(recipients)}）")


# ──────────────────────────────────────────
# メイン処理
# ──────────────────────────────────────────
def main():
    print("クロール開始")

    print("[ステップ1] Googleスプレッドシートに接続しています…")
    client = get_sheet_client()
    print("[ステップ1] 接続OK")

    print("[ステップ1b] 設定シートを読み込んでいます…")
    settings = read_settings(client)
    print(f"[ステップ1b] OK（通知先：{settings.get('通知先メールアドレス')}）")

    print("[ステップ2] シート「最新データ」を読み込んでいます…")
    old_rows = read_sheet(client, SHEET_MAIN)
    print(f"[ステップ2] OK（前回データ：{len(old_rows)}件）")

    print("[ステップ3] サイトをクロールしています（時間がかかることがあります）…")
    new_rows = crawl()
    print(f"[ステップ3] OK（今回のクロール結果：{len(new_rows)}件）")

    print("[ステップ4] 前回との差分を計算しています…")
    changes = detect_changes(old_rows, new_rows, settings=settings)
    print(f"[ステップ4] OK（変更件数：{len(changes)}件）")

    run_datetime = datetime.now(timezone(timedelta(hours=9))).strftime("%Y年%m月%d日 %H:%M")
    print("[ステップ5] スプレッドシートに書き込んでいます…")
    write_sheet(client, SHEET_BACKUP, old_rows, run_datetime)
    print("[ステップ5a] バックアップシート「前回データ」更新OK")
    write_sheet(client, SHEET_MAIN, new_rows, run_datetime, changes=changes)
    print("[ステップ5b] メインシート「最新データ」更新OK（変更行に色付け済み）")
    changes_sheet_url = write_changes_sheet(client, changes, run_datetime)
    print("[ステップ5c] 変更履歴シート更新OK")
    print("スプレッドシート更新完了")

    print("[ステップ6] メールを送っています…")
    # 通知先を設定シートから取得（カンマ区切りで複数対応）
    mail_to_list = [addr.strip() for addr in settings.get("通知先メールアドレス", MAIL_TO).split(",") if addr.strip()]
    send_no_change = settings.get("変更なし時もメール送信", "はい").strip() != "いいえ"

    if changes:
        send_email(changes, new_rows, changes_sheet_url, mail_to_list)
    elif send_no_change:
        send_no_change_email(new_rows, changes_sheet_url, mail_to_list)
    else:
        print("[ステップ6] 変更なし・メール送信スキップ（設定：変更なし時もメール送信＝いいえ）")
    print("[ステップ6] OK")

    print("処理完了")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("--- エラーが発生しました（以下をGitHubのログ全文として控えてください）---")
        traceback.print_exc()
        sys.exit(1)
