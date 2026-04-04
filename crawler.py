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
def crawl():
    """サイトにログインして生産計画表を4週分クロールし、ジョブ一覧を返す"""
    jobs = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

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
            head_frame = next((f for f in page.frames if "W20_head" in f.url), None)

        body_frame = page.frame(name="BODY")
        if body_frame is None:
            body_frame = next((f for f in page.frames if "W20_body" in f.url), None)

        # HEADフレームの読み込みを待つ
        if head_frame is not None:
            head_frame.wait_for_load_state("networkidle")

        # カレンダーは多くの場合 BODY フレーム内。メインの page だけではリンクが0件になる。
        calendar_root = body_frame if body_frame is not None else page

        # ── 検索条件：板金本体を選択して検索（HEADフレーム内）──
        # ラジオボタンのvalue属性は英語: BanSub/BanMain/Kumihai/Shukka
        head_frame.check('input[value="BanMain"]')
        head_frame.click('input[value="検索"]')
        page.wait_for_load_state("networkidle")
        page.screenshot(path="screenshot_calendar.png")

        # ── 1〜2週目と3〜4週目の2回クロール ──
        for week_range in ["1〜2週目", "3〜4週目"]:
            jobs += scrape_calendar(page, calendar_root)

            if week_range == "1〜2週目":
                # 「次の2週」はカレンダーと同じフレーム内にあることが多い
                calendar_root.click('text=次の2週')
                page.wait_for_load_state("networkidle")

        browser.close()

    return jobs


def _detail_page_url(href):
    """詳細画面への絶対URLを組み立てる"""
    if not href:
        return None
    href = href.strip()
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("/"):
        return f"https://v-factory.azbil.com{href}"
    return f"https://v-factory.azbil.com/rweb/WALOG/asp/{href}"


def scrape_calendar(page, calendar_root):
    """現在表示されているカレンダーの全ジョブ詳細を取得する"""
    jobs = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    # カレンダー内の詳細リンク（BODYフレーム内を優先。0件なら従来どおりpage全体も試す）
    detail_links = calendar_root.locator('a[href*="WALOG_DETAIL"]').all()
    if not detail_links:
        detail_links = page.locator('a[href*="WALOG_DETAIL"]').all()
    detail_urls = [link.get_attribute("href") for link in detail_links]
    print(f"詳細リンク数：{len(detail_urls)}件（この2週分の画面）")

    for href in detail_urls:
        detail_url = _detail_page_url(href)
        if not detail_url:
            continue
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
