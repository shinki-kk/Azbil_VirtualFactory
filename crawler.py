"""
Azbil Web仮想工場 生産計画表 クロールスクリプト

1日2回GitHub Actionsから実行される。
- 生産計画表（板金本体）を4週分クロール
- Googleスプレッドシートに結果を書き出す
- 前回から変更があればメールで通知する
"""

import os
import json
import smtplib
import sys
import traceback
from datetime import datetime
from urllib.parse import urljoin
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
HEADERS = ["取得日時", "工事番号", "外形図番", "盤種類", "本数", "板金・塗装（予定日）", "組配協力会社名"]


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
    """指定シートの全データを取得する（ヘッダー行を除く）"""
    sh = client.open_by_key(SPREADSHEET_ID)
    try:
        ws = sh.worksheet(sheet_name)
        rows = ws.get_all_values()
        return rows[1:] if len(rows) > 1 else []  # 1行目はヘッダーなので除く
    except gspread.WorksheetNotFound:
        return []


def write_sheet(client, sheet_name, rows):
    """指定シートを上書きする"""
    sh = client.open_by_key(SPREADSHEET_ID)
    try:
        ws = sh.worksheet(sheet_name)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=sheet_name, rows=500, cols=20)

    if rows:
        ws.update([HEADERS] + rows)
    else:
        ws.update([HEADERS])


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
        page = context.new_page()
        print("[クロール] ブラウザ起動・サイトへ接続中…", flush=True)

        # ── ログイン ──
        page.goto(SITE_URL)
        page.wait_for_load_state("networkidle")
        page.screenshot(path="screenshot_login.png")

        # ログインフォームの入力欄を探す（name属性が不明なためtype属性で検索）
        page.locator('input[type="text"]').first.fill(LOGIN_ID)
        page.locator('input[type="password"]').first.fill(LOGIN_PASSWORD)
        page.locator('input[type="submit"]').first.click()
        page.wait_for_load_state("networkidle")
        page.screenshot(path="screenshot_after_login.png")

        # ── 生産計画表リンクをクリック ──
        page.click('text=生産計画表')
        page.wait_for_load_state("networkidle")

        # ── ページがフレーム構成のため、HEADフレームにアクセス ──
        head_frame = page.frame(name="HEAD")
        if head_frame is None:
            head_frame = next((f for f in page.frames if f.url and "W20_head" in f.url), None)

        # HEADフレームの読み込みを待つ
        if head_frame is not None:
            head_frame.wait_for_load_state("networkidle")

        # ── 検索条件：板金本体を選択して検索（HEADフレーム内）──
        # ラジオボタンのvalue属性は英語: BanSub/BanMain/Kumihai/Shukka
        head_frame.check('input[value="BanMain"]')
        head_frame.click('input[value="検索"]')
        page.wait_for_load_state("networkidle")
        page.screenshot(path="screenshot_calendar.png")

        # ── 1〜2週目と3〜4週目の2回クロール ──
        for week_range in ["1〜2週目", "3〜4週目"]:
            # 検索直後・週切り替え直後は必ずフレームを取り直す（古い Frame は detached になる）
            calendar_root = _resolve_calendar_root(page)
            jobs += scrape_calendar(page, calendar_root)

            if week_range == "1〜2週目":
                calendar_root = _resolve_calendar_root(page)
                calendar_root.click('text=次の2週')
                page.wait_for_load_state("networkidle")

        browser.close()

    return jobs


# 相対リンク解決のフォールバック（フレームURLが取れないとき）
_DEFAULT_LINK_BASE = "https://v-factory.azbil.com/rweb/WALOG/asp/W20_body.asp"


def _frame_resolve_base(fr):
    """フレーム内の相対hrefを解決するための基準URL（そのフレームのドキュメントURL）"""
    u = getattr(fr, "url", None) or ""
    return u if u.startswith("http") else _DEFAULT_LINK_BASE


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
    """フレーム内の a / area の href を列挙（画像マップ対応）"""
    collected = []
    expr = "els => els.map(e => e.getAttribute('href')).filter(h => h && h.trim())"
    for sel in ("a[href]", "area[href]"):
        try:
            collected.extend(fr.eval_on_selector_all(sel, expr))
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
            full = _detail_page_url(h, base_url)
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
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    detail_urls = _collect_detail_hrefs(calendar_root, page)
    print(f"詳細リンク数：{len(detail_urls)}件（この2週分の画面）", flush=True)

    for i, detail_url in enumerate(detail_urls, start=1):
        print(f"  詳細取得 {i}/{len(detail_urls)} …", flush=True)
        detail_page = page.context.new_page()
        detail_page.goto(detail_url)
        detail_page.wait_for_load_state("networkidle")

        job = extract_job_detail(detail_page, now)
        if job:
            jobs.append(job)

        detail_page.close()

    return jobs


def extract_job_detail(page, now):
    """工程詳細画面から必要項目を取得する"""
    def get_value(label):
        """ラベルに対応する入力欄の値を取得する"""
        try:
            # ラベルテキストを含むtdの次のtd内のinput/textを取得
            value = page.locator(f'td:has-text("{label}") + td input').first.input_value()
            return value.strip()
        except Exception:
            return ""

    def get_text(label):
        """ラベルに対応するtdのテキストを取得する"""
        try:
            value = page.locator(f'td:has-text("{label}") + td').first.inner_text()
            return value.strip()
        except Exception:
            return ""

    # 板金・塗装の予定日は「出図」セクションの特定セルから取得
    def get_bankin_date():
        try:
            # 「板金・塗装」列の「予定」行のセルを取得
            value = page.locator('td:has-text("板金・塗装")').first
            # 予定行（2行目）の値
            date_cell = page.locator('table:has(td:has-text("板金・塗装")) tr').nth(1).locator('td').nth(1)
            return date_cell.inner_text().strip()
        except Exception:
            return ""

    koujiban    = get_value("工事番号")
    gaiken      = get_value("外形図番")
    banshu      = get_value("盤種類")
    honsuu      = get_value("本数")
    bankin_date = get_bankin_date()
    kumiai      = get_value("組配協力会社名")

    # 工事番号が取れなかった場合はスキップ
    if not koujiban:
        return None

    return [now, koujiban, gaiken, banshu, honsuu, bankin_date, kumiai]


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
                if header == "取得日時":
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

    print("[ステップ5] スプレッドシートに書き込んでいます…")
    write_sheet(client, SHEET_BACKUP, old_rows)
    print("[ステップ5a] バックアップシート「前回データ」更新OK")
    write_sheet(client, SHEET_MAIN, new_rows)
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
