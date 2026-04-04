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
from datetime import datetime
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

SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]          # スプレッドシートのID
SHEET_MAIN     = "最新データ"                            # メインシート名
SHEET_BACKUP   = "前回データ"                            # バックアップシート名

# 取得する項目の列ヘッダー
HEADERS = ["積上日", "工事番号", "外形図番", "盤種類", "本数", "出図日", "組配協力会社名"]

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


def write_sheet(client, sheet_name, rows, run_datetime=""):
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


# ──────────────────────────────────────────
# クロール処理
# ──────────────────────────────────────────
def _resolve_calendar_root(page):
    """
    カレンダーがある BODY フレームを返す。
    検索ボタンや「次の2週」でフレームが差し替わると、以前の Frame は detached になるため毎回取り直す。
    """
    body = page.frame(name="BODY")
    if body is None:
        body = next((f for f in page.frames if f.url and "W20_body" in f.url), None)
    return body if body is not None else page


def crawl():
    """サイトにログインして生産計画表を4週分クロールし、ジョブ一覧を返す"""
    jobs = []

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
        page.wait_for_load_state("load", timeout=_PW_TIMEOUT_MS)
        page.screenshot(path="screenshot_after_login.png")

        # ── 生産計画表リンクをクリック ──
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
        context.route("**/*", _route_skip_images_fonts)

        # ── 1〜2週目と3〜4週目の2回クロール ──
        for week_range in ["1〜2週目", "3〜4週目"]:
            # 検索直後・週切り替え直後は必ずフレームを取り直す（古い Frame は detached になる）
            calendar_root = _resolve_calendar_root(page)
            jobs += scrape_calendar(page, calendar_root)

            if week_range == "1〜2週目":
                # 各フレームのURL・「次の2週」の有無を出力して原因調査
                print("[クロール] フレーム一覧:", flush=True)
                for fr in page.frames:
                    try:
                        has_btn = fr.locator('text=次の2週').count() > 0
                        print(f"  name={fr.name!r} url={fr.url[:80]} 次の2週={has_btn}", flush=True)
                    except Exception as e:
                        print(f"  name={fr.name!r} 取得失敗: {e}", flush=True)

                # 全フレームから「次の2週」を探してクリック
                clicked = False
                for fr in page.frames:
                    try:
                        btn = fr.locator('text=次の2週')
                        if btn.count() > 0:
                            btn.first.click()
                            clicked = True
                            print(f"[クロール] 「次の2週」クリック成功（frame: {fr.name!r}）", flush=True)
                            break
                    except Exception:
                        pass
                if not clicked:
                    print("[クロール] 警告：「次の2週」ボタンが見つかりませんでした", flush=True)
                time.sleep(5)   # フレーム差し替わりを待つ

                # BODYフレームから直接スクリーンショットを取得
                body_frame = _resolve_calendar_root(page)
                try:
                    body_frame.locator("body").screenshot(path="screenshot_calendar_week34.png")
                except Exception:
                    page.screenshot(path="screenshot_calendar_week34.png")
                print("[クロール] 3〜4週目スクリーンショット保存完了", flush=True)

        browser.close()

    return jobs


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
    """フレーム内のカレンダーアイコン（calendar.jpg）を含むリンクのhrefを列挙"""
    collected = []
    expr = "els => els.map(e => e.getAttribute('href')).filter(h => h && h.trim())"
    try:
        collected.extend(fr.eval_on_selector_all("a:has(img[src*='calendar.jpg'])", expr))
    except Exception:
        pass
    return collected


def _iter_calendar_frames(calendar_root, page):
    """カレンダー候補となるフレーム（BODY とその子 iframe）を列挙"""
    if calendar_root is page:
        return list(page.frames)
    out = []

    def walk(fr):
        out.append(fr)
        for ch in fr.child_frames:
            walk(ch)

    walk(calendar_root)
    return out


def _collect_detail_hrefs(calendar_root, page):
    """
    詳細URLを重複なく収集（絶対URLのリストを返す）。
    カレンダーアイコンは CmnLinkNonClear.asp?RtnURL=... のため、フレームURL基準で相対パスを解決する。
    """
    ordered = []
    seen = set()
    all_raw = []

    def process_frame(fr):
        try:
            part = _gather_hrefs_from_frame(fr)
        except Exception:
            return
        all_raw.extend(part)
        base_url = _frame_resolve_base(fr)
        for h in part:
            if not _looks_like_job_detail_href(h):
                continue
            full = _resolve_cmnlinknonclear(_detail_page_url(h, base_url))
            if not full or full in seen:
                continue
            seen.add(full)
            ordered.append(full)

    for fr in _iter_calendar_frames(calendar_root, page):
        process_frame(fr)

    if not ordered:
        for fr in page.frames:
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

    return ordered


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
def detect_changes(old_rows, new_rows):
    """
    前回データと今回データを比較し、変更点を返す。
    工事番号（列インデックス1）をキーとして比較する。
    """
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
                if header == "積上日":
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
def send_email(changes, new_rows):
    """変更内容をメールで送信する"""
    now = datetime.now().strftime("%Y年%m月%d日 %H:%M")
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
            lines.append("  （このジョブはカレンダーから削除されました）")
        elif c["種別"] == "変更":
            for diff in c["差分"]:
                lines.append(f"  {diff}")
        lines.append("")

    lines.append(f"\n今回のクロールで取得したジョブ数：{len(new_rows)}件")

    body = "\n".join(lines)

    msg = MIMEMultipart()
    msg["From"]    = MAIL_USER
    msg["To"]      = MAIL_TO
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain", "utf-8"))

    with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
        server.login(MAIL_USER, MAIL_PASSWORD)
        server.send_message(msg)

    print("メール送信完了")


def send_no_change_email(new_rows):
    """変更なしの場合も確認メールを送る"""
    now = datetime.now().strftime("%Y年%m月%d日 %H:%M")
    subject = f"【生産計画表】変更なし {now}"
    body = f"生産計画表に変更はありませんでした。（確認日時：{now}）\n取得ジョブ数：{len(new_rows)}件"

    msg = MIMEMultipart()
    msg["From"]    = MAIL_USER
    msg["To"]      = MAIL_TO
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain", "utf-8"))

    with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
        server.login(MAIL_USER, MAIL_PASSWORD)
        server.send_message(msg)

    print("変更なしメール送信完了")


# ──────────────────────────────────────────
# メイン処理
# ──────────────────────────────────────────
def main():
    print("クロール開始")

    print("[ステップ1] Googleスプレッドシートに接続しています…")
    client = get_sheet_client()
    print("[ステップ1] 接続OK")

    print("[ステップ2] シート「最新データ」を読み込んでいます…")
    old_rows = read_sheet(client, SHEET_MAIN)
    print(f"[ステップ2] OK（前回データ：{len(old_rows)}件）")

    print("[ステップ3] サイトをクロールしています（時間がかかることがあります）…")
    new_rows = crawl()
    print(f"[ステップ3] OK（今回のクロール結果：{len(new_rows)}件）")

    print("[ステップ4] 前回との差分を計算しています…")
    changes = detect_changes(old_rows, new_rows)
    print(f"[ステップ4] OK（変更件数：{len(changes)}件）")

    run_datetime = datetime.now().strftime("%Y年%m月%d日 %H:%M")
    print("[ステップ5] スプレッドシートに書き込んでいます…")
    write_sheet(client, SHEET_BACKUP, old_rows, run_datetime)
    print("[ステップ5a] バックアップシート「前回データ」更新OK")
    write_sheet(client, SHEET_MAIN, new_rows, run_datetime)
    print("[ステップ5b] メインシート「最新データ」更新OK")
    print("スプレッドシート更新完了")

    print("[ステップ6] メールを送っています…")
    if changes:
        send_email(changes, new_rows)
    else:
        send_no_change_email(new_rows)
    print("[ステップ6] OK")

    print("処理完了")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("--- エラーが発生しました（以下をGitHubのログ全文として控えてください）---")
        traceback.print_exc()
        sys.exit(1)
