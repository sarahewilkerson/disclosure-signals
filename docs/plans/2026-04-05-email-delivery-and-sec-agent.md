# Plan: Email Delivery + SEC Agent Configuration

**Date:** 2026-04-05
**Status:** Approved
**Scope:** Add email sending to daily job + configure SEC user-agent for live ingestion

## Task Description

The daily pipeline produces a brief saved to disk but nobody sees it. Add Gmail SMTP email delivery (FedResearch pattern) and configure SEC user-agent for live EDGAR ingestion.

## Completion Criteria

- `send_brief_email()` constructs valid MIME message
- SEC_USER_AGENT passes DirectEdgarClient validation
- `.env.signals.example` committed with documented format
- End-to-end: jobctl run produces email (when credentials available)

## Risks

1. Gmail app password on unreachable Hetzner — build infrastructure, verify when password available
2. Email deliverability — test minimal smtplib send first
3. SEC rate limiting on first full ingest — existing retry/delay handles it

## Verification

- Mock SMTP test for message construction
- Standalone SMTP smoke test before integration
- `.env.signals.example` in repo
