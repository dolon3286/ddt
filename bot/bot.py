#!/usr/bin/env python3
import asyncio
import json
import logging
import os
import pickle
import re
import shlex
import shutil
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

APPLE_MUSIC_URL_RE = re.compile(
    r"^https://(?:beta\\.music|music|classical\\.music)\\.apple\\.com/\\w{2}/.+$"
)


@dataclass(frozen=True)
class BotConfig:
    token_pickle_path: Path
    downloader_cmd: List[str]
    work_root: Path
    rate_limit_seconds: int
    settings_path: Path


@dataclass
class AppSettings:
    token: str
    drive_folder_id: str
    max_concurrent: int
    admin_ids: List[int] = field(default_factory=list)


@dataclass
class BotState:
    settings: AppSettings
    semaphore: asyncio.Semaphore
    pending_actions: Dict[int, str] = field(default_factory=dict)


class RateLimiter:
    def __init__(self, min_interval_seconds: int) -> None:
        self._min_interval = min_interval_seconds
        self._lock = asyncio.Lock()
        self._last_seen: dict[int, float] = {}

    async def allow(self, user_id: int) -> Tuple[bool, float]:
        async with self._lock:
            now = time.time()
            last = self._last_seen.get(user_id, 0)
            remaining = self._min_interval - (now - last)
            if remaining > 0:
                return False, remaining
            self._last_seen[user_id] = now
            return True, 0


def load_config() -> BotConfig:
    token_pickle_path = Path(os.environ.get("TOKEN_PICKLE_PATH", "token.pickle"))
    downloader_cmd = shlex.split(os.environ.get("DOWNLOADER_CMD", "go run main.go"))
    work_root = Path(os.environ.get("BOT_WORK_DIR", "./bot_work"))
    rate_limit_seconds = int(os.environ.get("BOT_RATE_LIMIT_SECONDS", "30"))
    settings_path = Path(os.environ.get("BOT_SETTINGS_PATH", "bot/settings.json"))

    return BotConfig(
        token_pickle_path=token_pickle_path,
        downloader_cmd=downloader_cmd,
        work_root=work_root,
        rate_limit_seconds=rate_limit_seconds,
        settings_path=settings_path,
    )


def load_settings(path: Path) -> AppSettings:
    settings = {
        "bot_token": "",
        "drive_folder_id": "",
        "max_concurrent": 2,
        "admin_ids": [],
    }
    if path.exists():
        with path.open("r", encoding="utf-8") as handle:
            settings.update(json.load(handle))

    env_token = os.environ.get("BOT_TOKEN", "").strip()
    env_drive = os.environ.get("DRIVE_FOLDER_ID", "").strip()
    env_max = os.environ.get("BOT_MAX_CONCURRENT", "").strip()
    env_admins = os.environ.get("BOT_ADMIN_IDS", "").strip()

    if env_token:
        settings["bot_token"] = env_token
    if env_drive:
        settings["drive_folder_id"] = env_drive
    if env_max:
        settings["max_concurrent"] = int(env_max)
    if env_admins:
        settings["admin_ids"] = [int(value) for value in env_admins.split(",") if value]

    token = str(settings.get("bot_token", "")).strip()
    drive_folder_id = str(settings.get("drive_folder_id", "")).strip()
    max_concurrent = int(settings.get("max_concurrent", 2))
    admin_ids = [int(value) for value in settings.get("admin_ids", [])]

    if not token:
        raise ValueError("BOT_TOKEN (or bot_token in settings) is required")
    if not drive_folder_id:
        raise ValueError("DRIVE_FOLDER_ID (or drive_folder_id in settings) is required")

    return AppSettings(
        token=token,
        drive_folder_id=drive_folder_id,
        max_concurrent=max_concurrent,
        admin_ids=admin_ids,
    )


def save_settings(path: Path, settings: AppSettings) -> None:
    payload = {
        "bot_token": settings.token,
        "drive_folder_id": settings.drive_folder_id,
        "max_concurrent": settings.max_concurrent,
        "admin_ids": settings.admin_ids,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)


def is_admin(user_id: int, settings: AppSettings) -> bool:
    return user_id in settings.admin_ids


def build_drive_service(token_pickle_path: Path):
    with token_pickle_path.open("rb") as handle:
        creds = pickle.load(handle)
    if isinstance(creds, Credentials) and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        with token_pickle_path.open("wb") as handle:
            pickle.dump(creds, handle)
    return build("drive", "v3", credentials=creds)


def iter_files(root: Path) -> Iterable[Path]:
    for path in root.rglob("*"):
        if path.is_file():
            yield path


def upload_files_sync(token_pickle_path: Path, folder_id: str, files: Iterable[Path]) -> List[str]:
    service = build_drive_service(token_pickle_path)
    links: List[str] = []
    for file_path in files:
        media = MediaFileUpload(file_path, resumable=True)
        metadata = {"name": file_path.name, "parents": [folder_id]}
        created = (
            service.files()
            .create(body=metadata, media_body=media, fields="id, webViewLink")
            .execute()
        )
        link = created.get("webViewLink")
        if link:
            links.append(link)
    return links


def validate_url(url: str) -> bool:
    return bool(APPLE_MUSIC_URL_RE.match(url))


async def run_downloader(cmd: List[str], url: str, work_dir: Path) -> None:
    process = await asyncio.create_subprocess_exec(
        *cmd,
        url,
        cwd=str(work_dir),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    if stdout:
        logging.info("Downloader stdout:\n%s", stdout.decode(errors="replace"))
    if stderr:
        logging.warning("Downloader stderr:\n%s", stderr.decode(errors="replace"))
    if process.returncode != 0:
        raise RuntimeError(f"Downloader failed with exit code {process.returncode}")


async def handle_download(
    chat_id: int,
    url: str,
    bot,
    config: BotConfig,
    state: BotState,
) -> None:
    async with state.semaphore:
        work_dir = Path(tempfile.mkdtemp(dir=config.work_root))
        try:
            await bot.send_message(chat_id=chat_id, text="Download started")
            await run_downloader(config.downloader_cmd, url, work_dir)
            files = list(iter_files(work_dir))
            if not files:
                raise RuntimeError("No files produced by downloader")
            links = await asyncio.to_thread(
                upload_files_sync,
                config.token_pickle_path,
                state.settings.drive_folder_id,
                files,
            )
            if not links:
                raise RuntimeError("Upload completed but no links were returned")
            message = "Upload complete:\n" + "\n".join(links)
            await bot.send_message(chat_id=chat_id, text=message)
        except Exception as exc:
            logging.exception("Job failed")
            await bot.send_message(chat_id=chat_id, text=f"Error: {exc}")
        finally:
            shutil.rmtree(work_dir, ignore_errors=True)


def build_application(config: BotConfig, settings: AppSettings) -> Application:
    config.work_root.mkdir(parents=True, exist_ok=True)
    rate_limiter = RateLimiter(config.rate_limit_seconds)
    state = BotState(settings=settings, semaphore=asyncio.Semaphore(settings.max_concurrent))

    async def dl_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_chat or not update.message:
            return
        if not context.args:
            await update.message.reply_text("Usage: /dl <apple-music-url>")
            return
        url = context.args[0].strip()
        if not validate_url(url):
            await update.message.reply_text("Invalid Apple Music URL.")
            return
        if update.effective_user:
            allowed, remaining = await rate_limiter.allow(update.effective_user.id)
            if not allowed:
                await update.message.reply_text(
                    f"Rate limit exceeded. Try again in {int(remaining)}s."
                )
                return
        context.application.create_task(
            handle_download(
                chat_id=update.effective_chat.id,
                url=url,
                bot=context.bot,
                config=config,
                state=state,
            )
        )

    async def uset_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_chat or not update.message or not update.effective_user:
            return
        if not state.settings.admin_ids:
            state.settings.admin_ids.append(update.effective_user.id)
            save_settings(config.settings_path, state.settings)
            await update.message.reply_text("Admin list initialized. You are now admin.")
        if not is_admin(update.effective_user.id, state.settings):
            await update.message.reply_text("Admin only command.")
            return
        keyboard = [
            [InlineKeyboardButton("Set Drive Folder ID", callback_data="uset:drive")],
            [InlineKeyboardButton("Set Max Tasks", callback_data="uset:max")],
            [InlineKeyboardButton("Add Admin User ID", callback_data="uset:add_admin")],
            [InlineKeyboardButton("Set Bot Token", callback_data="uset:token")],
            [InlineKeyboardButton("Show Settings", callback_data="uset:show")],
        ]
        await update.message.reply_text(
            "Settings menu:",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )

    async def uset_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.callback_query or not update.effective_user:
            return
        query = update.callback_query
        await query.answer()
        if not is_admin(update.effective_user.id, state.settings):
            await query.edit_message_text("Admin only.")
            return
        action = query.data or ""
        if action == "uset:show":
            admins = ", ".join(str(admin_id) for admin_id in state.settings.admin_ids)
            message = (
                "Current settings:\n"
                f"Drive folder ID: {state.settings.drive_folder_id}\n"
                f"Max tasks: {state.settings.max_concurrent}\n"
                f"Admins: {admins or 'none'}"
            )
            await query.edit_message_text(message)
            return
        if action in {"uset:drive", "uset:max", "uset:add_admin", "uset:token"}:
            state.pending_actions[update.effective_user.id] = action
            prompts = {
                "uset:drive": "Send the new Drive folder ID.",
                "uset:max": "Send the max concurrent tasks (number).",
                "uset:add_admin": "Send the admin user ID to add.",
                "uset:token": "Send the new bot token (restart required).",
            }
            await query.edit_message_text(prompts[action])

    async def handle_setting_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_user or not update.message:
            return
        action = state.pending_actions.get(update.effective_user.id)
        if not action:
            return
        if not is_admin(update.effective_user.id, state.settings):
            await update.message.reply_text("Admin only.")
            return
        value = update.message.text.strip()
        if action == "uset:drive":
            if not value:
                await update.message.reply_text("Drive folder ID cannot be empty.")
                return
            state.settings.drive_folder_id = value
            save_settings(config.settings_path, state.settings)
            await update.message.reply_text("Drive folder ID updated.")
        elif action == "uset:max":
            try:
                max_tasks = int(value)
                if max_tasks < 1:
                    raise ValueError
            except ValueError:
                await update.message.reply_text("Max tasks must be a positive number.")
                return
            state.settings.max_concurrent = max_tasks
            state.semaphore = asyncio.Semaphore(max_tasks)
            save_settings(config.settings_path, state.settings)
            await update.message.reply_text("Max tasks updated.")
        elif action == "uset:add_admin":
            try:
                admin_id = int(value)
            except ValueError:
                await update.message.reply_text("Admin user ID must be a number.")
                return
            if admin_id not in state.settings.admin_ids:
                state.settings.admin_ids.append(admin_id)
                save_settings(config.settings_path, state.settings)
            await update.message.reply_text(f"Admin added: {admin_id}")
        elif action == "uset:token":
            if not value:
                await update.message.reply_text("Bot token cannot be empty.")
                return
            state.settings.token = value
            save_settings(config.settings_path, state.settings)
            await update.message.reply_text("Bot token updated. Restart required.")
        state.pending_actions.pop(update.effective_user.id, None)

    application = Application.builder().token(settings.token).build()
    application.add_handler(CommandHandler("dl", dl_command))
    application.add_handler(CommandHandler("uset", uset_command))
    application.add_handler(CallbackQueryHandler(uset_callback, pattern=r"^uset:"))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_setting_input))
    return application


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    config = load_config()
    settings = load_settings(config.settings_path)
    application = build_application(config, settings)
    application.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
