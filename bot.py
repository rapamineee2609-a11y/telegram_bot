import os
import re
import time
import json
import sqlite3
import tempfile
import shutil
import asyncio
import signal
import atexit
from datetime import datetime, timedelta
from urllib.parse import urlparse
from typing import Dict, Optional, Any, List
from concurrent.futures import ThreadPoolExecutor

import yt_dlp
import requests
import aiohttp
from PIL import Image
from io import BytesIO
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, TelegramError
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes
)
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

# ====================== KONFIGURASI ======================
BOT_TOKEN = os.getenv("8523651928:AAEimKRIzQFLH0i9AHk9OoWpwsXnblUstt8")
if not BOT_TOKEN:
    raise ValueError("❌ BOT_TOKEN tidak ditemukan di environment!")

OWNER_RAW = os.getenv("7466580390", "")
try:
    OWNER_ID = int(OWNER_RAW) if OWNER_RAW else None
except ValueError:
    OWNER_ID = None
    logger.warning("⚠️ OWNER_ID tidak valid, owner commands dinonaktifkan.")

MAX_VIDEO_SIZE_MB = 50
MAX_IMAGE_SIZE_MB = 20
DOWNLOAD_TIMEOUT = 300
RATE_LIMIT_WINDOW = 10
RATE_LIMIT_MAX = 5
MAX_CONCURRENT_DOWNLOADS_PER_USER = 2
TEMP_DIR = "temp"
DB_PATH = "data.db"
os.makedirs(TEMP_DIR, exist_ok=True)

# ====================== LOGGING ======================
logger.remove()
logger.add(lambda msg: print(msg), level="INFO")
logger.add("bot.log", rotation="1 day", retention="7 days", level="DEBUG")

# ====================== CEK FFMPEG ======================
def check_ffmpeg() -> bool:
    return shutil.which('ffmpeg') is not None

if not check_ffmpeg():
    logger.error("❌ FFmpeg tidak ditemukan! Install ffmpeg terlebih dahulu.")

# ====================== DATABASE SQLITE (SINGLETON) ======================
db_conn = sqlite3.connect(DB_PATH, check_same_thread=False)
db_conn.row_factory = sqlite3.Row

def init_db():
    with db_conn:
        db_conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_active INTEGER
            )
        ''')
        db_conn.execute('''
            CREATE TABLE IF NOT EXISTS downloads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                media_type TEXT,
                url TEXT,
                status TEXT,
                error TEXT,
                created_at INTEGER
            )
        ''')
        db_conn.execute('''
            CREATE TABLE IF NOT EXISTS active_downloads (
                user_id INTEGER PRIMARY KEY,
                filename TEXT,
                start_time INTEGER
            )
        ''')

init_db()

# Pisahkan read dan write untuk efisiensi
def db_execute(query: str, params: tuple = ()):
    with db_conn:
        return db_conn.execute(query, params).lastrowid

def db_fetchone(query: str, params: tuple = ()):
    return db_conn.execute(query, params).fetchone()

def db_fetchall(query: str, params: tuple = ()):
    return db_conn.execute(query, params).fetchall()

# ====================== SHUTDOWN HANDLER ======================
def cleanup_processes():
    for user_id, proc in list(active_processes.items()):
        if proc and proc.returncode is None:
            try:
                proc.terminate()
                logger.info(f"Terminated process for user {user_id}")
            except:
                pass
    active_processes.clear()

def close_db():
    try:
        db_conn.close()
        logger.info("Database connection closed.")
    except:
        pass

def shutdown_handler():
    cleanup_processes()
    close_db()

atexit.register(shutdown_handler)
signal.signal(signal.SIGINT, lambda s, f: shutdown_handler())
signal.signal(signal.SIGTERM, lambda s, f: shutdown_handler())

# ====================== RATE LIMIT ======================
user_requests: Dict[int, List[float]] = {}
RATE_LIMIT_CLEANUP_INTERVAL = 3600

def clean_rate_limit():
    now = time.time()
    for uid in list(user_requests.keys()):
        user_requests[uid] = [t for t in user_requests[uid] if now - t < RATE_LIMIT_WINDOW]
        if not user_requests[uid]:
            del user_requests[uid]

async def rate_limit_cleanup():
    while True:
        await asyncio.sleep(RATE_LIMIT_CLEANUP_INTERVAL)
        clean_rate_limit()

def check_rate_limit(user_id: int) -> bool:
    now = time.time()
    if user_id not in user_requests:
        user_requests[user_id] = []
    user_requests[user_id] = [t for t in user_requests[user_id] if now - t < RATE_LIMIT_WINDOW]
    user_requests[user_id].append(now)
    return len(user_requests[user_id]) <= RATE_LIMIT_MAX

# ====================== SEMAPHORE PER USER ======================
user_semaphores: Dict[int, asyncio.Semaphore] = {}

def get_user_semaphore(user_id: int) -> asyncio.Semaphore:
    if user_id not in user_semaphores:
        user_semaphores[user_id] = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS_PER_USER)
    return user_semaphores[user_id]

# ====================== THREAD POOL ======================
executor = ThreadPoolExecutor(max_workers=4)

async def run_in_thread(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(executor, func, *args, **kwargs)

# ====================== URL VALIDASI ======================
def is_valid_url(url: str) -> bool:
    try:
        result = urlparse(url)
        return all([result.scheme in ('http', 'https'), result.netloc])
    except:
        return False

# ====================== ACTIVE PROCESSES (CANCEL) DENGAN LOCK ======================
active_processes: Dict[int, asyncio.subprocess.Process] = {}
active_files: Dict[int, str] = {}
_process_lock = asyncio.Lock()

async def register_download(user_id: int, filename: str = None):
    async with _process_lock:
        if filename:
            active_files[user_id] = filename

async def unregister_download(user_id: int):
    async with _process_lock:
        active_files.pop(user_id, None)
        active_processes.pop(user_id, None)

async def cancel_download(user_id: int) -> bool:
    async with _process_lock:
        proc = active_processes.pop(user_id, None)
        if proc and proc.returncode is None:
            try:
                proc.terminate()
                asyncio.create_task(force_kill_after(proc, user_id, 3))
                return True
            except:
                pass
        filename = active_files.pop(user_id, None)
        if filename and os.path.exists(filename):
            try:
                os.remove(filename)
            except:
                pass
        db_execute('DELETE FROM active_downloads WHERE user_id = ?', (user_id,))
        return False

async def force_kill_after(proc, user_id: int, delay: int):
    await asyncio.sleep(delay)
    if proc.returncode is None:
        try:
            proc.kill()
            logger.warning(f"Force killed process for user {user_id}")
        except:
            pass
    async with _process_lock:
        active_processes.pop(user_id, None)

# ====================== DOWNLOADER (SUBPROCESS + PROGRESS) ======================
async def download_with_progress(url: str, media_type: str, user_id: int, status_msg) -> str:
    unique_id = f"{user_id}_{int(time.time())}"
    output_template = os.path.join(TEMP_DIR, f'%(title)s_{unique_id}.%(ext)s')

    cmd = ['yt-dlp']
    if media_type == 'video':
        cmd.extend(['-f', 'bestvideo[height<=1080]+bestaudio/best', '--merge-output-format', 'mp4'])
    elif media_type == 'audio':
        cmd.extend(['-f', 'bestaudio/best', '-x', '--audio-format', 'mp3', '--audio-quality', '192K'])
    elif media_type == 'image':
        cmd.extend(['-f', 'best[ext=jpg]/best[ext=png]/best'])
    else:
        cmd.extend(['-f', 'best'])
    cmd.extend(['-o', output_template, '--no-warnings', '--no-check-certificate', '--no-playlist',
                '--socket-timeout', '30', '--retries', '5', '--fragment-retries', '5', url])

    logger.info(f"Starting download for user {user_id}: {url[:100]}...")
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    async with _process_lock:
        active_processes[user_id] = process

    last_update = 0
    last_pct = 0
    while True:
        line = await process.stderr.readline()
        if not line:
            break
        text = line.decode('utf-8', errors='ignore')
        match = re.search(r'\[(?:download|ffmpeg|Merger)\]\s+([\d.]+)%', text)
        if match:
            pct = float(match.group(1))
            now = time.time()
            if pct - last_pct >= 5 and now - last_update >= 1:
                last_update = now
                last_pct = pct
                try:
                    await status_msg.edit_text(f"⏳ Downloading... {pct:.1f}%")
                except TelegramError:
                    pass  # Ignore if message was deleted

    stdout, stderr = await process.communicate()
    async with _process_lock:
        active_processes.pop(user_id, None)

    if process.returncode != 0:
        error_msg = stderr.decode().strip() or "Unknown error"
        raise Exception(f"yt-dlp failed: {error_msg[:200]}")

    # Cari file hasil download
    extensions = {
        'video': ['.mp4', '.mkv', '.webm', '.avi', '.mov'],
        'audio': ['.mp3', '.m4a', '.aac', '.ogg', '.wav'],
        'image': ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp']
    }
    exts = extensions.get(media_type, ['.mp4', '.mp3', '.jpg'])
    found = None
    latest_time = 0
    for f in os.listdir(TEMP_DIR):
        if unique_id in f:
            for ext in exts:
                if f.endswith(ext):
                    path = os.path.join(TEMP_DIR, f)
                    mtime = os.path.getmtime(path)
                    if mtime > latest_time:
                        latest_time = mtime
                        found = path
                    break
    if not found:
        files = [os.path.join(TEMP_DIR, f) for f in os.listdir(TEMP_DIR) if unique_id in f]
        if files:
            found = max(files, key=os.path.getmtime)
    if not found:
        raise FileNotFoundError(f"File tidak ditemukan untuk unique_id: {unique_id}")

    size_mb = os.path.getsize(found) / (1024 * 1024)
    limit = MAX_VIDEO_SIZE_MB if media_type != 'image' else MAX_IMAGE_SIZE_MB
    if size_mb > limit:
        os.remove(found)
        raise ValueError(f'File terlalu besar ({size_mb:.1f}MB > {limit}MB)')

    return found

# ====================== INFO DENGAN TIMEOUT ======================
def get_info_sync(url: str) -> dict:
    opts = {'quiet': True, 'no_warnings': True, 'ignoreerrors': True}
    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=False)
        if not info:
            raise ValueError('Gagal ambil info media')
        return {
            'title': info.get('title', '?'),
            'duration': info.get('duration', 0),
            'uploader': info.get('uploader', '?'),
            'views': info.get('view_count', 0),
            'extractor': info.get('extractor', '?'),
        }

# ====================== UPLOAD FALLBACK ======================
def upload_to_0x0(data: bytes) -> str:
    files = {'file': ('image.jpg', data)}
    resp = requests.post('https://0x0.st', files=files, timeout=30)
    if resp.status_code == 200:
        return resp.text.strip()
    raise Exception(f'Upload gagal (HTTP {resp.status_code})')

def upload_to_catbox(data: bytes) -> str:
    files = {'fileToUpload': ('image.jpg', data)}
    resp = requests.post('https://catbox.moe/user/api.php', files=files, data={'reqtype': 'fileupload'}, timeout=30)
    if resp.status_code == 200:
        url = resp.text.strip()
        if url.startswith('http'):
            return url
    raise Exception(f'Upload Catbox gagal (HTTP {resp.status_code})')

def upload_to_hosting(data: bytes, retries: int = 2) -> str:
    for attempt in range(retries + 1):
        try:
            return upload_to_0x0(data)
        except Exception as e:
            if attempt == retries:
                break
            logger.warning(f"0x0.st attempt {attempt+1} failed: {e}, retrying...")
            time.sleep(2 ** attempt)
    for attempt in range(retries + 1):
        try:
            return upload_to_catbox(data)
        except Exception as e:
            if attempt == retries:
                raise
            logger.warning(f"Catbox attempt {attempt+1} failed: {e}, retrying...")
            time.sleep(2 ** attempt)
    raise Exception("Semua layanan upload gagal setelah retry")

# ====================== IMAGE TOOLS ======================
def compress_image(data: bytes, quality: int = 70) -> bytes:
    img = Image.open(BytesIO(data))
    output = BytesIO()
    img.save(output, format='JPEG', quality=quality, optimize=True)
    return output.getvalue()

def convert_image(data: bytes, fmt: str) -> bytes:
    if fmt not in ['jpg', 'png', 'webp']:
        raise ValueError('Format tidak didukung: jpg, png, webp')
    img = Image.open(BytesIO(data))
    output = BytesIO()
    img.save(output, format=fmt.upper())
    return output.getvalue()

def get_image_info(data: bytes) -> dict:
    img = Image.open(BytesIO(data))
    return {
        'width': img.width,
        'height': img.height,
        'format': img.format,
        'size_kb': len(data) / 1024
    }

# ====================== KEYBOARD ======================
def main_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📥 Downloader", callback_data="menu_downloader")],
        [InlineKeyboardButton("🖼 Image Tools", callback_data="menu_image")],
        [InlineKeyboardButton("⚙️ Settings", callback_data="menu_settings")],
        [InlineKeyboardButton("❓ Help", callback_data="menu_help")],
    ])

def downloader_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎬 Video", callback_data="dl_video")],
        [InlineKeyboardButton("🎵 Audio", callback_data="dl_audio")],
        [InlineKeyboardButton("🖼 Image", callback_data="dl_image")],
        [InlineKeyboardButton("📄 Info", callback_data="dl_info")],
        [InlineKeyboardButton("⬅ Back", callback_data="back_home")],
        [InlineKeyboardButton("🏠 Home", callback_data="home")],
        [InlineKeyboardButton("❌ Close", callback_data="close")],
    ])

def image_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Image to URL", callback_data="img_to_url")],
        [InlineKeyboardButton("⬇ URL to Image", callback_data="img_from_url")],
        [InlineKeyboardButton("📐 Image Info", callback_data="img_info")],
        [InlineKeyboardButton("🔄 Convert JPG/PNG/WEBP", callback_data="img_convert")],
        [InlineKeyboardButton("⬅ Back", callback_data="back_home")],
        [InlineKeyboardButton("🏠 Home", callback_data="home")],
        [InlineKeyboardButton("❌ Close", callback_data="close")],
    ])

def back_home_close():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅ Back", callback_data="back_home")],
        [InlineKeyboardButton("🏠 Home", callback_data="home")],
        [InlineKeyboardButton("❌ Close", callback_data="close")],
    ])

def cancel_button():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel")],
    ])

# ====================== HANDLERS ======================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db_execute(
        'INSERT OR REPLACE INTO users (user_id, username, first_name, last_active) VALUES (?, ?, ?, ?)',
        (user.id, user.username, user.first_name, int(time.time()))
    )
    await update.message.reply_text(
        f"👋 Selamat datang, {user.first_name}!\n\n"
        "📥 Download video/audio/gambar dari TikTok, YouTube, Pinterest, FB, X, IG, dll.\n"
        "🖼 Tools gambar: upload, download, kompres, konversi.\n\n"
        "Pilih menu:",
        reply_markup=main_menu()
    )

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == 'home' or data == 'back_home':
        await query.message.edit_text("🏠 Menu Utama:", reply_markup=main_menu())
        return
    if data == 'close':
        await query.message.delete()
        return
    if data == 'menu_downloader':
        await query.message.edit_text("📥 Pilih jenis:", reply_markup=downloader_menu())
        return
    if data == 'menu_image':
        await query.message.edit_text("🖼 Pilih tool:", reply_markup=image_menu())
        return
    if data == 'menu_settings':
        total_users = db_fetchone('SELECT COUNT(*) FROM users')[0]
        total_downloads = db_fetchone('SELECT COUNT(*) FROM downloads')[0]
        await query.message.edit_text(
            f"⚙️ Statistik\n\n👥 User: {total_users}\n📥 Download: {total_downloads}",
            reply_markup=back_home_close()
        )
        return
    if data == 'menu_help':
        await query.message.edit_text(
            "❓ Bantuan\n\n"
            "📌 Downloader: pilih jenis, kirim URL.\n"
            "📌 Image Tools: upload foto, URL ke gambar, info, konversi.\n"
            "📌 Format didukung: TikTok, YouTube, Pinterest, FB, X, IG, dll.",
            reply_markup=back_home_close()
        )
        return

    if data.startswith('dl_'):
        context.user_data['media_type'] = data.replace('dl_', '')
        await query.message.edit_text(
            f"📎 Kirim URL {context.user_data['media_type']}:\nKetik /cancel untuk batal.",
            reply_markup=cancel_button()
        )
        return

    if data == 'cancel':
        context.user_data.clear()
        await query.message.edit_text("❌ Dibatalkan.", reply_markup=back_home_close())
        return

    if data == 'img_to_url':
        context.user_data['img_action'] = 'to_url'
        await query.message.edit_text("🔄 Kirim foto untuk di-upload ke URL:", reply_markup=cancel_button())
        return
    if data == 'img_from_url':
        context.user_data['img_action'] = 'from_url'
        await query.message.edit_text("⬇ Kirim URL gambar:", reply_markup=cancel_button())
        return
    if data == 'img_info':
        context.user_data['img_action'] = 'info'
        await query.message.edit_text("📐 Kirim foto untuk info:", reply_markup=cancel_button())
        return
    if data == 'img_convert':
        context.user_data['img_action'] = 'convert'
        await query.message.edit_text(
            "🔄 Kirim foto dengan caption format (jpg/png/webp):\n"
            "Contoh: /convert png",
            reply_markup=cancel_button()
        )
        return

async def handle_url(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not check_rate_limit(user_id):
        await update.message.reply_text("⏳ Terlalu banyak permintaan. Tunggu 10 detik.")
        return

    url = update.message.text.strip()
    if not re.match(r'https?://', url):
        await update.message.reply_text("❌ Kirim URL yang valid (http/https), bukan teks biasa.")
        return

    if not is_valid_url(url):
        await update.message.reply_text("❌ URL tidak valid.")
        return

    # Validasi media_type
    media_type = context.user_data.get('media_type')
    if media_type not in ['video', 'audio', 'image', 'info']:
        await update.message.reply_text(
            "❌ Pilih jenis media dulu dari menu Downloader.",
            reply_markup=downloader_menu()
        )
        return

    status_msg = await update.message.reply_text("⏳ Memproses...")
    sem = get_user_semaphore(user_id)

    try:
        async with sem:
            if media_type == 'info':
                try:
                    info = await asyncio.wait_for(
                        run_in_thread(get_info_sync, url),
                        timeout=30
                    )
                except asyncio.TimeoutError:
                    raise Exception("Timeout saat mengambil info media (30 detik).")
                text = (
                    f"📄 Info Media\n\n"
                    f"📌 Judul: {info['title']}\n"
                    f"⏱ Durasi: {info['duration']}s\n"
                    f"👤 Uploader: {info['uploader']}\n"
                    f"👁 Views: {info['views']}\n"
                    f"🔌 Platform: {info['extractor']}"
                )
                await status_msg.edit_text(text, reply_markup=back_home_close())
                db_execute(
                    'INSERT INTO downloads (user_id, media_type, url, status, created_at) VALUES (?, ?, ?, ?, ?)',
                    (user_id, 'info', url, 'success', int(time.time()))
                )
                return

            await register_download(user_id)
            try:
                filename = await asyncio.wait_for(
                    download_with_progress(url, media_type, user_id, status_msg),
                    timeout=DOWNLOAD_TIMEOUT
                )
            except asyncio.TimeoutError:
                raise Exception(f"Download timeout ({DOWNLOAD_TIMEOUT} detik).")
            await register_download(user_id, filename)

            if media_type == 'image':
                try:
                    with open(filename, 'rb') as f:
                        Image.open(f).verify()
                except:
                    with open(filename, 'rb') as f:
                        await update.message.reply_document(
                            f,
                            caption="⚠️ File bukan gambar, dikirim sebagai dokumen.",
                            reply_markup=back_home_close()
                        )
                    os.remove(filename)
                    await unregister_download(user_id)
                    db_execute(
                        'INSERT INTO downloads (user_id, media_type, url, status, created_at) VALUES (?, ?, ?, ?, ?)',
                        (user_id, media_type, url, 'success', int(time.time()))
                    )
                    await status_msg.delete()
                    return

            with open(filename, 'rb') as f:
                if media_type == 'video':
                    await update.message.reply_video(f, caption="✅ Video selesai!", reply_markup=back_home_close())
                elif media_type == 'audio':
                    await update.message.reply_audio(f, caption="✅ Audio selesai!", reply_markup=back_home_close())
                elif media_type == 'image':
                    await update.message.reply_photo(f, caption="✅ Gambar selesai!", reply_markup=back_home_close())
            os.remove(filename)
            await unregister_download(user_id)
            db_execute(
                'INSERT INTO downloads (user_id, media_type, url, status, created_at) VALUES (?, ?, ?, ?, ?)',
                (user_id, media_type, url, 'success', int(time.time()))
            )
            await status_msg.delete()
            logger.info(f"Download completed: user={user_id}, type={media_type}, url={url[:100]}...")

    except asyncio.CancelledError:
        await status_msg.edit_text("❌ Download dibatalkan.", reply_markup=back_home_close())
        await unregister_download(user_id)
        db_execute(
            'INSERT INTO downloads (user_id, media_type, url, status, error, created_at) VALUES (?, ?, ?, ?, ?, ?)',
            (user_id, media_type, url, 'cancelled', 'User cancelled', int(time.time()))
        )
    except Exception as e:
        logger.error(f"Download error for user {user_id}: {e}")
        filename = active_files.get(user_id)
        if filename and os.path.exists(filename):
            try:
                os.remove(filename)
            except:
                pass
        await unregister_download(user_id)
        db_execute(
            'INSERT INTO downloads (user_id, media_type, url, status, error, created_at) VALUES (?, ?, ?, ?, ?, ?)',
            (user_id, media_type, url, 'failed', str(e), int(time.time()))
        )
        await status_msg.edit_text(f"❌ Gagal: {str(e)[:200]}", reply_markup=back_home_close())

async def handle_image(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not check_rate_limit(user_id):
        await update.message.reply_text("⏳ Terlalu banyak permintaan. Tunggu 10 detik.")
        return

    action = context.user_data.get('img_action')
    if not action:
        await update.message.reply_text("❌ Pilih dulu menu Image Tools.")
        return

    photo = update.message.photo
    document = update.message.document
    if not photo and not document:
        await update.message.reply_text("❌ Kirim foto atau file gambar.")
        return

    status_msg = await update.message.reply_text("⏳ Memproses...")
    try:
        if photo:
            file = await update.message.bot.get_file(photo[-1].file_id)
        else:
            if not document.mime_type or not document.mime_type.startswith('image/'):
                await status_msg.edit_text("❌ File bukan gambar.", reply_markup=back_home_close())
                return
            file = await update.message.bot.get_file(document.file_id)

        data = await file.download_as_bytearray()
        if len(data) > 20 * 1024 * 1024:
            await status_msg.edit_text("❌ Gambar terlalu besar (>20MB).", reply_markup=back_home_close())
            return

        if action == 'to_url':
            url = await run_in_thread(upload_to_hosting, bytes(data))
            await status_msg.edit_text(f"🔗 URL gambar:\n{url}", reply_markup=back_home_close())

        elif action == 'info':
            info = await run_in_thread(get_image_info, bytes(data))
            text = f"🖼 Info Gambar\n\n📐 {info['width']}x{info['height']}\n📦 {info['size_kb']:.1f} KB\n🖌 {info['format']}"
            await status_msg.edit_text(text, reply_markup=back_home_close())

        elif action == 'convert':
            caption = update.message.caption or ''
            fmt = 'png'
            if 'jpg' in caption.lower():
                fmt = 'jpg'
            elif 'png' in caption.lower():
                fmt = 'png'
            elif 'webp' in caption.lower():
                fmt = 'webp'
            converted = await run_in_thread(convert_image, bytes(data), fmt)
            with tempfile.NamedTemporaryFile(suffix=f'.{fmt}', delete=False) as tmp:
                tmp.write(converted)
                tmp_path = tmp.name
            await update.message.reply_document(
                document=open(tmp_path, 'rb'),
                caption=f"✅ Konversi ke {fmt.upper()} selesai!",
                reply_markup=back_home_close()
            )
            os.remove(tmp_path)
            await status_msg.delete()

    except Exception as e:
        logger.error(f"Image error: {e}")
        await status_msg.edit_text(f"❌ Error: {str(e)[:200]}", reply_markup=back_home_close())

async def handle_image_url(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not check_rate_limit(user_id):
        await update.message.reply_text("⏳ Terlalu banyak permintaan. Tunggu 10 detik.")
        return

    url = update.message.text.strip()
    if not is_valid_url(url):
        await update.message.reply_text("❌ URL tidak valid.")
        return

    status_msg = await update.message.reply_text("⏳ Mendownload gambar...")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.head(url, timeout=aiohttp.ClientTimeout(total=10)) as head_resp:
                if head_resp.status != 200:
                    raise Exception(f'Gagal mengakses URL (HTTP {head_resp.status})')
                content_type = head_resp.headers.get('Content-Type', '')
                if not content_type.startswith('image/'):
                    raise ValueError('URL bukan gambar')

        def fetch_image():
            resp = requests.get(url, timeout=30)
            if resp.status_code != 200:
                raise Exception(f'Gagal download (HTTP {resp.status_code})')
            return resp.content

        data = await run_in_thread(fetch_image)
        if len(data) > 20 * 1024 * 1024:
            raise ValueError('Gambar terlalu besar (>20MB)')

        with tempfile.NamedTemporaryFile(suffix='.jpg', delete=False) as tmp:
            tmp.write(data)
            tmp_path = tmp.name

        await update.message.reply_photo(
            photo=open(tmp_path, 'rb'),
            caption="✅ Gambar dari URL",
            reply_markup=back_home_close()
        )
        os.remove(tmp_path)
        await status_msg.delete()
    except Exception as e:
        await status_msg.edit_text(f"❌ Error: {str(e)[:200]}", reply_markup=back_home_close())

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    cancelled = await cancel_download(user_id)
    if cancelled:
        await update.message.reply_text("✅ Download dibatalkan.", reply_markup=back_home_close())
    else:
        await update.message.reply_text("❌ Tidak ada download aktif.", reply_markup=back_home_close())
    context.user_data.clear()

# ====================== OWNER COMMANDS ======================
async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if OWNER_ID is None or update.effective_user.id != OWNER_ID:
        await update.message.reply_text("❌ Hanya owner.")
        return
    text = ' '.join(context.args)
    if not text:
        await update.message.reply_text("❌ Masukkan pesan. Contoh: /broadcast Hello")
        return
    users = db_fetchall('SELECT user_id FROM users')
    sent = 0
    for row in users:
        try:
            await update.message.bot.send_message(row[0], f"📢 Broadcast\n\n{text}")
            sent += 1
            await asyncio.sleep(0.2)
        except:
            pass
    await update.message.reply_text(f"✅ Broadcast terkirim ke {sent} user.")

async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if OWNER_ID is None or update.effective_user.id != OWNER_ID:
        await update.message.reply_text("❌ Hanya owner.")
        return
    total_users = db_fetchone('SELECT COUNT(*) FROM users')[0]
    total_downloads = db_fetchone('SELECT COUNT(*) FROM downloads')[0]
    success = db_fetchone("SELECT COUNT(*) FROM downloads WHERE status='success'")[0]
    failed = db_fetchone("SELECT COUNT(*) FROM downloads WHERE status='failed'")[0]
    await update.message.reply_text(
        f"📊 *Statistik Bot*\n\n"
        f"👥 Total User: {total_users}\n"
        f"📥 Total Download: {total_downloads}\n"
        f"✅ Berhasil: {success}\n"
        f"❌ Gagal: {failed}",
        parse_mode='Markdown'
    )

# ====================== CLEANUP TASK ======================
async def cleanup_task():
    while True:
        await asyncio.sleep(3600)
        now = time.time()
        for f in os.listdir(TEMP_DIR):
            path = os.path.join(TEMP_DIR, f)
            try:
                if os.path.isfile(path) and (now - os.path.getmtime(path)) > 7200:
                    os.remove(path)
            except:
                pass
        cutoff = int((datetime.now() - timedelta(days=7)).timestamp())
        db_execute('DELETE FROM downloads WHERE created_at < ?', (cutoff,))
        db_execute('DELETE FROM active_downloads WHERE start_time < ?', (int(time.time() - 7200),))

# ====================== ERROR HANDLER ======================
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Update {update} caused error {context.error}")
    if update and update.message:
        await update.message.reply_text("⚠️ Terjadi kesalahan. Coba lagi nanti.")

# ====================== MAIN ======================
async def main():
    if not BOT_TOKEN:
        logger.error("❌ BOT_TOKEN tidak ditemukan!")
        return

    if not check_ffmpeg():
        logger.warning("⚠️ FFmpeg tidak ditemukan! Fitur audio/video merge mungkin gagal.")

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("cancel", cancel))
    if OWNER_ID is not None:
        app.add_handler(CommandHandler("broadcast", broadcast))
        app.add_handler(CommandHandler("stats", stats_cmd))
    else:
        logger.warning("⚠️ OWNER_ID tidak diset, owner commands dinonaktifkan.")

    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_url))
    app.add_handler(MessageHandler(filters.PHOTO, handle_image))
    app.add_handler(MessageHandler(filters.Document.IMAGE, handle_image))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_image_url))
    app.add_error_handler(error_handler)

    asyncio.create_task(cleanup_task())
    asyncio.create_task(rate_limit_cleanup())

    logger.info("🤖 Bot Telegram Downloader started...")
    await app.run_polling()

if __name__ == "__main__":
    asyncio.run(main())
