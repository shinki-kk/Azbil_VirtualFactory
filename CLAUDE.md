# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## ユーザーについて

- 非エンジニアの生産管理者
- 専門用語を避け、分かりやすい日本語で説明すること
- 回答はすべて日本語で行うこと

## プロジェクト概要

詳細な仕様は [SPEC.md](SPEC.md) を参照。

Azbil Web仮想工場サイトの生産計画表を1日2回クロールし、変更をGoogleスプレッドシートへ記録・メール通知するプログラム。

## 技術構成（予定）

- 実行環境：GitHub Actions（クラウド上で自動実行）
- クロール：Python + Playwright（ログインが必要なサイトのため）
- 通知：Gmail（SMTP）
- データ保存：Googleスプレッドシート（Google Sheets API）

## 認証情報の管理

パスワードやAPIキーはコードに直接書かず、GitHub ActionsのSecretsに登録して環境変数として使用する。

| 環境変数名 | 内容 |
|-----------|------|
| `SITE_LOGIN_ID` | サイトのログインID |
| `SITE_LOGIN_PASSWORD` | サイトのログインパスワード |
| `GMAIL_PASSWORD` | Gmailのアプリパスワード |
| `GOOGLE_SHEETS_CREDENTIALS` | Google Sheets APIの認証情報（JSON） |
