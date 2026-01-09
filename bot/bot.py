#!/usr/bin/env python3
import asyncio
import logging
import os
import pickle
import re
import shlex
import shutil
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Tuple

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

APPLE_MUSIC_URL_RE = re.compile(
    r"^https://(?:beta\\.music|music|classical\\.music)\\.apple\\.com/\\w{2}/.+$"
)


@dataclass(frozen=True)
class BotConfig:
    token: str
    drive_folder_id: str
    token_pickle_path: Path
    downloader_cmd: List[str]
    work_root: Path
    max_concurrent: int
    rate_limit_seconds: int


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
    token = os.environ.get("BOT_TOKEN", "").strip()
    drive_folder_id = os.environ.get("DRIVE_FOLDER_ID", "").strip()
    token_pickle_path = Path(os.environ.get("TOKEN_PICKLE_PATH", "token.pickle"))
    downloader_cmd = shlex.split(os.environ.get("DOWNLOADER_CMD", "go run main.go"))
    work_root = Path(os.environ.get("BOT_WORK_DIR", "./bot_work"))
    max_concurrent = int(os.environ.get("BOT_MAX_CONCURRENT", "2"))
    rate_limit_seconds = int(os.environ.get("BOT_RATE_LIMIT_SECONDS", "30"))

    if not token:
        raise ValueError("BOT_TOKEN is required")
    if not drive_folder_id:
        raise ValueError("DRIVE_FOLDER_ID is required")

    return BotConfig(
        token=token,
        drive_folder_id=drive_folder_id,
        token_pickle_path=token_pickle_path,
        downloader_cmd=downloader_cmd,
        work_root=work_root,
        max_concurrent=max_concurrent,
        rate_limit_seconds=rate_limit_seconds,
    )


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
    semaphore: asyncio.Semaphore,
) -> None:
    async with semaphore:
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
                config.drive_folder_id,
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


def build_application(config: BotConfig) -> Application:
    config.work_root.mkdir(parents=True, exist_ok=True)
    semaphore = asyncio.Semaphore(config.max_concurrent)
    rate_limiter = RateLimiter(config.rate_limit_seconds)

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
                semaphore=semaphore,
            )
        )

    application = Application.builder().token(config.token).build()
    application.add_handler(CommandHandler("dl", dl_command))
    return application


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    config = load_config()
    application = build_application(config)
    application.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
