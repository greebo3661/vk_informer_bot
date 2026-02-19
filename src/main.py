import os
import asyncio
import json
import logging
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple, Optional

import pandas as pd
from bot.bot import Bot
from bot.handler import MessageHandler, BotButtonCommandHandler, EventType
from bot.event import Event

# === Logging ===
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# === Config ===
VKT_BOT_TOKEN = os.getenv("VKT_BOT_TOKEN")
VKT_BASE_URL = os.getenv("VKT_BASE_URL", "https://myteam.mail.ru/bot/v1")
DATA_FILE = os.getenv("DATA_FILE", "/data/vacation_data.json")
FILE_PATH = os.getenv("FILE_PATH", "/data/latest.xlsx")

logger.info(f"Config: BASE_URL={VKT_BASE_URL}, DATA_FILE={DATA_FILE}")

if not VKT_BOT_TOKEN:
    raise RuntimeError("VKT_BOT_TOKEN must be set")

# === Bot instance ===
bot = Bot(token=VKT_BOT_TOKEN, api_url_base=VKT_BASE_URL)

# === In-memory state: chats waiting for threshold input ===
# chat_id -> "awaiting_threshold"
pending_state: Dict[str, str] = {}


# === JSON File Storage ===
def load_data() -> Dict:
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load data file: {e}")
    return {"vacations": {}, "settings": {}, "notifications": {}}


def save_data(data: Dict):
    os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# === Keyboard ===
def get_menu_keyboard():
    return [
        [
            {"text": "📊 Статус", "callbackData": "cmd_status", "style": "primary"},
            {"text": "ℹ️ Помощь", "callbackData": "cmd_help", "style": "primary"},
        ],
        [
            {"text": "📅 Расписание", "callbackData": "cmd_schedule", "style": "primary"},
            {"text": "🔔 Уведомления", "callbackData": "cmd_notifications", "style": "primary"},
        ],
        [
            {"text": "📢 Установить этот чат", "callbackData": "cmd_set_channel", "style": "attention"},
        ]
    ]


# === Filters — must be a SINGLE callable ===
def message_filter(event):
    return event.type == EventType.NEW_MESSAGE


def button_filter(event):
    return event.type == EventType.CALLBACK_QUERY


# === Sync wrappers ===
def on_message_handler(bot: Bot, event):
    try:
        asyncio.create_task(on_message_async(bot, event))
    except Exception as e:
        logger.error(f"Failed to schedule message handler: {e}")


def on_button_handler(bot: Bot, event):
    try:
        asyncio.create_task(on_button_click_async(bot, event))
    except Exception as e:
        logger.error(f"Failed to schedule button handler: {e}")


# === Message handler ===
async def on_message_async(bot: Bot, event):
    chat_id = event.from_chat
    text = (getattr(event, 'text', '') or '').strip()
    cmd = text.lower().split()[0] if text else ''

    logger.info(f"MSG from={chat_id!r} text={text!r}")
    # Log raw event data to see file structure
    raw_data = getattr(event, 'data', {}) or {}
    if raw_data:
        logger.info(f"RAW event.data keys: {list(raw_data.keys()) if isinstance(raw_data, dict) else type(raw_data)}")
        if isinstance(raw_data, dict) and raw_data.get('parts'):
            logger.info(f"RAW parts: {raw_data['parts']}")

    # --- Handle "awaiting threshold" state ---
    if pending_state.get(chat_id) == "awaiting_threshold":
        try:
            days = int(text)
            if days <= 0:
                raise ValueError("must be positive")
            # Save threshold
            data = load_data()
            data["settings"].setdefault(chat_id, {})["notify_days"] = days
            save_data(data)
            del pending_state[chat_id]

            count = len(data.get("vacations", []))
            bot.send_text(
                chat_id=chat_id,
                text=(
                    f"✅ Настройка сохранена!\n\n"
                    f"📅 Загружено записей: {count}\n"
                    f"⏰ Уведомления: за {days} дн. до начала отпуска\n\n"
                    f"Бот будет присылать напоминания каждый день в 9:00."
                ),
                inline_keyboard_markup=get_menu_keyboard()
            )
            return
        except ValueError:
            bot.send_text(
                chat_id=chat_id,
                text="❌ Введите целое положительное число (например: 7)"
            )
            return

    # --- Check for file attachment (parts or URL in text) ---
    parts = getattr(event, 'parts', None) or []
    for part in parts:
        ptype = getattr(part, 'type', None) or (part.get('type') if isinstance(part, dict) else None)
        if ptype == 'file':
            file_id = (getattr(part, 'fileId', None) or
                       (part.get('payload', {}).get('fileId') if isinstance(part, dict) else None))
            if file_id:
                await process_file_by_id(bot, chat_id, file_id)
                return

    # File can also arrive as a URL in text (myteam sends xlsx as a link)
    # The fileId is in event.data['parts'][0]['payload']['fileId']
    if text and ('files.myteam.mail.ru' in text or 'myteam.mail.ru' in text):
        # Try to get fileId from parts first (preferred — uses API)
        file_id_from_parts = None
        if isinstance(raw_data, dict):
            for part in raw_data.get('parts', []):
                if isinstance(part, dict) and part.get('type') == 'file':
                    file_id_from_parts = part.get('payload', {}).get('fileId')
                    break
        if file_id_from_parts:
            await process_file_by_id(bot, chat_id, file_id_from_parts)
        else:
            await process_file_by_url(bot, chat_id, text)
        return

    # Also check event.data parts directly (file without URL in text)
    if isinstance(raw_data, dict):
        for part in raw_data.get('parts', []):
            if isinstance(part, dict) and part.get('type') == 'file':
                payload = part.get('payload', {})
                file_id = payload.get('fileId')
                if file_id:
                    await process_file_by_id(bot, chat_id, file_id)
                    return

    # --- Commands ---
    if cmd == '/start':
        bot.send_text(
            chat_id=chat_id,
            text=(
                "👋 Привет! Я бот для управления графиком отпусков.\n\n"
                "Что я умею:\n"
                "📎 Принимать Excel-файл с графиком отпусков\n"
                "⏰ Напоминать о предстоящих отпусках за нужное кол-во дней\n"
                "📢 Отправлять уведомления в указанный чат\n\n"
                "👇 Отправьте Excel-файл чтобы начать, или выберите действие:"
            ),
            inline_keyboard_markup=get_menu_keyboard()
        )

    elif cmd == '/help':
        send_help(bot, chat_id)

    elif cmd == '/set_channel':
        set_channel_action(bot, chat_id)

    elif cmd == '/status':
        send_status(bot, chat_id)

    else:
        bot.send_text(
            chat_id=chat_id,
            text=(
                "Отправьте Excel-файл (.xlsx) с графиком отпусков,\n"
                "или воспользуйтесь меню:"
            ),
            inline_keyboard_markup=get_menu_keyboard()
        )


# === Button handler ===
async def on_button_click_async(bot: Bot, event):
    # callback data is in event.data dict
    raw_data = getattr(event, 'data', {}) or {}
    callback_data = ''
    query_id = ''
    if isinstance(raw_data, dict):
        callback_data = raw_data.get('callbackData', '')
        query_id = raw_data.get('queryId', '')
    chat_id = event.from_chat
    logger.info(f"BUTTON from={chat_id!r} data={callback_data!r} query_id={query_id!r}")

    try:
        if query_id:
            bot.answer_callback_query(query_id=query_id, text="")
    except Exception as e:
        logger.warning(f"answer_callback_query failed: {e}")

    if callback_data == 'cmd_status':
        send_status(bot, chat_id)
    elif callback_data == 'cmd_help':
        send_help(bot, chat_id)
    elif callback_data == 'cmd_schedule':
        send_schedule(bot, chat_id)
    elif callback_data == 'cmd_notifications':
        pending_state[chat_id] = "awaiting_threshold"
        bot.send_text(
            chat_id=chat_id,
            text="🔔 Измените срок оповещения об отпуске (в днях):\n\nВведите число (например: 7)"
        )
    elif callback_data == 'cmd_set_channel':
        set_channel_action(bot, chat_id)


# === Helpers ===
def send_help(bot: Bot, chat_id: str):
    bot.send_text(
        chat_id=chat_id,
        text=(
            "ℹ️ Справка\n\n"
            "1. Отправьте Excel-файл (.xlsx) с графиком отпусков.\n"
            "   Бот найдёт столбцы: ФИО, Организация, Кол-во дней, Дата.\n\n"
            "2. После загрузки бот спросит за сколько дней\n"
            "   до отпуска присылать уведомление.\n\n"
            "3. Команды:\n"
            "   /start — начало работы\n"
            "   /status — статус и настройки\n"
            "   /set_channel — установить этот чат для уведомлений\n\n"
            "4. Уведомления приходят ежедневно в 9:00."
        )
    )


def set_channel_action(bot: Bot, chat_id: str):
    data = load_data()
    data["settings"].setdefault(chat_id, {})
    data["settings"]["hr_chat_id"] = chat_id
    save_data(data)
    bot.send_text(chat_id=chat_id, text="✅ Этот чат установлен для уведомлений об отпусках.")


def send_schedule(bot: Bot, chat_id: str):
    """Show the next 5 upcoming notification send dates for this chat."""
    data = load_data()
    all_vacations = data.get("vacations", {})
    vacations = all_vacations.get(chat_id, []) if isinstance(all_vacations, dict) else all_vacations
    settings = data.get("settings", {})
    chat_settings = settings.get(chat_id, {})
    notify_days = chat_settings.get("notify_days")

    if not vacations:
        bot.send_text(
            chat_id=chat_id,
            text="📅 Расписание\n\nДанные об отпусках не загружены.",
            inline_keyboard_markup=get_menu_keyboard()
        )
        return

    if not notify_days:
        bot.send_text(
            chat_id=chat_id,
            text="📅 Расписание\n\nПорог уведомлений не настроен. Загрузите Excel-файл.",
            inline_keyboard_markup=get_menu_keyboard()
        )
        return

    today = datetime.now().date()
    upcoming = []
    for v in vacations:
        try:
            start_dt = datetime.strptime(v["start_date"], "%Y-%m-%d").date()
            notify_date = start_dt - timedelta(days=notify_days)
            if notify_date >= today:
                upcoming.append((notify_date, start_dt, v["fio"]))
        except Exception:
            continue

    upcoming.sort(key=lambda x: x[0])
    top5 = upcoming[:5]

    if not top5:
        bot.send_text(
            chat_id=chat_id,
            text=(
                f"📅 Расписание\n\n"
                f"Ближайших уведомлений нет.\n"
                f"Все отпуска уже начались или уведомления были отправлены."
            ),
            inline_keyboard_markup=get_menu_keyboard()
        )
        return

    lines = [f"📅 Расписание уведомлений (за {notify_days} дн.)\n"]
    for notify_date, start_dt, fio in top5:
        lines.append(f"• {notify_date.strftime('%d.%m.%Y')} — {fio} (отпуск с {start_dt.strftime('%d.%m.%Y')})")

    bot.send_text(
        chat_id=chat_id,
        text="\n".join(lines),
        inline_keyboard_markup=get_menu_keyboard()
    )


def send_status(bot: Bot, chat_id: str):
    data = load_data()
    vacations = data.get("vacations", {})
    if isinstance(vacations, dict):
        count = len(vacations.get(chat_id, []))
    else:
        count = len(vacations)
    chat_settings = data.get("settings", {}).get(chat_id, {})
    notify_days = chat_settings.get("notify_days", "не задано")

    bot.send_text(
        chat_id=chat_id,
        text=(
            f"📊 Статус бота\n\n"
            f"📅 Записей об отпусках: {count}\n"
            f"⏰ Уведомлять за: {notify_days} дн.\n"
        ),
        inline_keyboard_markup=get_menu_keyboard()
    )


# === File processing ===
async def _download_and_parse(bot: Bot, chat_id: str, url: str):
    """Download file from URL using bot's session (avoids DNS issues), parse, and ask for threshold."""
    loop = asyncio.get_running_loop()
    try:
        # Use bot's own http_session which already has a working connection
        def do_download():
            try:
                # Try bot's session first (same DNS resolution as API calls)
                resp = bot.http_session.get(url, timeout=30)
                resp.raise_for_status()
                return resp.content
            except Exception:
                # Fallback to plain requests
                return requests.get(url, timeout=30).content

        content = await loop.run_in_executor(None, do_download)
    except Exception as e:
        bot.send_text(chat_id=chat_id, text=f"❌ Ошибка скачивания файла: {e}")
        return

    os.makedirs(os.path.dirname(FILE_PATH) or '.', exist_ok=True)
    with open(FILE_PATH, "wb") as f:
        f.write(content)

    def parse_job():
        df = pd.read_excel(FILE_PATH, header=None)
        return parse_vacation_df(df)

    try:
        rows, errors = await loop.run_in_executor(None, parse_job)
    except Exception as e:
        bot.send_text(chat_id=chat_id, text=f"❌ Ошибка чтения Excel: {e}")
        return

    if not rows:
        bot.send_text(
            chat_id=chat_id,
            text=(
                "⚠️ Не удалось найти данные в файле.\n\n"
                "Убедитесь, что таблица содержит столбцы:\n"
                "ФИО, Организация, Кол-во дней, Дата начала"
            )
        )
        return

    data = load_data()
    if not isinstance(data.get("vacations"), dict):
        data["vacations"] = {}
    if not isinstance(data.get("notifications"), dict):
        data["notifications"] = {}
    data["vacations"][chat_id] = rows
    data["notifications"][chat_id] = []
    save_data(data)

    pending_state[chat_id] = "awaiting_threshold"

    sample = rows[:3]
    sample_text = "\n".join(
        f"• {r['fio']} — {r['start_date']} ({r['days']} дн.)"
        for r in sample
    )
    if len(rows) > 3:
        sample_text += f"\n  ...и ещё {len(rows) - 3}"

    msg = f"✅ Загружено {len(rows)} записей"
    if errors:
        msg += f"\n⚠️ Пропущено: {len(errors)} строк"

    bot.send_text(
        chat_id=chat_id,
        text=(
            f"{msg}\n\n"
            f"Пример:\n{sample_text}\n\n"
            f"⏰ За сколько дней до начала отпуска присылать уведомление?\n"
            f"Введите число (например: 7)"
        )
    )


async def process_file_by_url(bot: Bot, chat_id: str, url: str):
    """Handle file sent as a direct URL in message text."""
    bot.send_text(chat_id=chat_id, text="📂 Файл получен, обрабатываю...")
    await _download_and_parse(bot, chat_id, url)


async def process_file_by_id(bot: Bot, chat_id: str, file_id: str):
    """Handle file sent as file_id attachment — uses bot API to get download URL."""
    bot.send_text(chat_id=chat_id, text="📂 Файл получен, обрабатываю...")
    try:
        file_info = bot.get_file_info(file_id=file_id).json()
        logger.info(f"file_info response: {file_info}")
        if not file_info.get("ok"):
            bot.send_text(chat_id=chat_id, text="❌ Ошибка получения файла от сервера.")
            return
        url = file_info.get("url")
        if not url:
            bot.send_text(chat_id=chat_id, text="❌ Сервер не вернул URL файла.")
            return
        await _download_and_parse(bot, chat_id, url)
    except Exception as e:
        logger.error(f"process_file_by_id error: {e}", exc_info=True)
        bot.send_text(chat_id=chat_id, text=f"❌ Ошибка: {e}")


async def process_file(bot: Bot, chat_id: str, file_id: str):
    """Legacy alias."""
    await process_file_by_id(bot, chat_id, file_id)






# === Excel parsing ===
def parse_vacation_df(df_raw: pd.DataFrame) -> Tuple[List[Dict], List[Dict]]:
    """
    Parses vacation Excel with merged/multi-row headers.
    Strategy:
    1. Try to find header row by keywords (ФИО, дата, etc.)
    2. If not found, scan rows to find first row where col 0 looks like a name
       and col 4 (or col 3) looks like a date.
    3. Use detected column layout to parse all data rows.
    """
    rows, errors = [], []

    # --- Step 1: Try keyword-based header detection ---
    col_map = {}
    data_start = None

    for i in range(min(20, len(df_raw))):
        row_vals = [str(v).lower().strip() for v in df_raw.iloc[i].tolist()]

        fio_idx = next((j for j, v in enumerate(row_vals)
                        if "фио" in v or "фамили" in v or "сотрудник" in v), -1)
        date_idx = next((j for j, v in enumerate(row_vals)
                         if ("дата" in v and ("план" in v or "начал" in v or "запланир" in v))
                         or v == "дата"), -1)
        days_idx = next((j for j, v in enumerate(row_vals)
                         if ("кол" in v and "дн" in v) or "календарн" in v), -1)
        org_idx = next((j for j, v in enumerate(row_vals)
                        if "организ" in v or "филиал" in v or "компани" in v), -1)

        if fio_idx != -1 and date_idx != -1:
            col_map = {
                'fio': fio_idx,
                'org': org_idx if org_idx != -1 else (fio_idx + 1),
                'days': days_idx if days_idx != -1 else (fio_idx + 2),
                'start_date': date_idx,
            }
            data_start = i + 1
            logger.info(f"Header found at row {i}: col_map={col_map}")
            break

    # --- Step 2: Auto-detect by scanning for first data row ---
    if not col_map:
        logger.info("No keyword header found, auto-detecting data start...")
        for i in range(min(30, len(df_raw))):
            row = df_raw.iloc[i]
            vals = row.tolist()

            # Look for a row where col 0 is a non-empty string (name)
            # and any column contains a date-like value
            col0 = str(vals[0]).strip() if len(vals) > 0 else ''
            if not col0 or col0 == 'nan' or col0.isdigit():
                continue

            # Check if any column looks like a date
            date_col = -1
            for j, v in enumerate(vals):
                if pd.isna(v):
                    continue
                v_str = str(v).strip()
                # Check for date patterns: dd.mm.yyyy or Timestamp
                if isinstance(v, pd.Timestamp):
                    date_col = j
                    break
                import re
                if re.match(r'\d{2}\.\d{2}\.\d{4}', v_str) or re.match(r'\d{4}-\d{2}-\d{2}', v_str):
                    date_col = j
                    break

            if date_col != -1:
                # Found data row — determine column layout
                # Days column: find a column with a small integer (1-365) before date_col
                days_col = -1
                for j in range(1, date_col):
                    v = vals[j]
                    if pd.isna(v):
                        continue
                    try:
                        d = int(float(str(v)))
                        if 1 <= d <= 365:
                            days_col = j
                            break
                    except (ValueError, TypeError):
                        pass

                col_map = {
                    'fio': 0,
                    'org': 1,
                    'days': days_col if days_col != -1 else 2,
                    'start_date': date_col,
                }
                data_start = i
                logger.info(f"Auto-detected data start at row {i}: col_map={col_map}")
                break

    if not col_map:
        logger.error("Could not detect column layout")
        return [], []

    # --- Step 3: Parse data rows ---
    for i in range(data_start, len(df_raw)):
        row = df_raw.iloc[i]

        def gv(key, default):
            idx = col_map.get(key, default)
            if idx >= len(row):
                return ""
            val = row.iloc[idx]
            return val if pd.notna(val) else ""

        fio = str(gv('fio', 0)).strip()
        org = str(gv('org', 1)).strip()
        days_raw = str(gv('days', 2)).strip()
        date_cell = gv('start_date', 4)

        # Skip empty, header-like, or numeric-only rows
        if not fio or fio in ('nan', '') or fio.isdigit():
            continue
        if not date_cell or str(date_cell).strip() in ('nan', ''):
            continue

        try:
            # Parse date
            if isinstance(date_cell, pd.Timestamp):
                start_dt = date_cell.to_pydatetime()
            elif isinstance(date_cell, datetime):
                start_dt = date_cell
            else:
                d_str = str(date_cell).strip()
                start_dt = None
                for fmt in ["%d.%m.%Y", "%Y-%m-%d", "%d.%m.%y", "%d/%m/%Y"]:
                    try:
                        start_dt = datetime.strptime(d_str, fmt)
                        break
                    except ValueError:
                        pass
                if not start_dt:
                    start_dt = pd.to_datetime(d_str, dayfirst=True).to_pydatetime()

            # Parse days
            days = int(float(days_raw)) if days_raw and days_raw != 'nan' else 0
            end_dt = start_dt + timedelta(days=days)

            rows.append({
                'fio': fio,
                'org': org,
                'days': days,
                'start_date': start_dt.strftime("%Y-%m-%d"),
                'end_date': end_dt.strftime("%Y-%m-%d"),
            })
        except Exception as e:
            errors.append({'row': i + 1, 'error': str(e), 'fio': fio})
            logger.debug(f"Parse error row {i+1} fio={fio!r}: {e}")

    logger.info(f"Parsed {len(rows)} rows, {len(errors)} errors")
    return rows, errors





# === Polling loop ===
async def polling_loop(bot: Bot):
    logger.info("Polling loop started")
    loop = asyncio.get_running_loop()

    bot.dispatcher.add_handler(
        MessageHandler(filters=message_filter, callback=on_message_handler)
    )
    bot.dispatcher.add_handler(
        BotButtonCommandHandler(filters=button_filter, callback=on_button_handler)
    )

    while True:
        try:
            response = await loop.run_in_executor(None, bot.events_get, 5)
            if response and response.status_code == 200:
                data_json = response.json()
                events = data_json.get("events", [])
                if events:
                    logger.info(f"Got {len(events)} event(s): {[e['type'] for e in events]}")
                for ev_data in events:
                    try:
                        ev_type = EventType(ev_data["type"])
                        event = Event(type_=ev_type, data=ev_data["payload"])
                        bot.dispatcher.dispatch(event)
                    except Exception as e:
                        logger.error(f"Dispatch error: {e} | raw={ev_data}")
            await asyncio.sleep(0.05)
        except Exception as e:
            logger.error(f"Polling error: {e}")
            await asyncio.sleep(5)


# === Notifier loop — runs daily at 9:00 ===
async def notifier_loop(bot: Bot):
    logger.info("Notifier loop started")
    while True:
        try:
            now = datetime.now()
            # Calculate seconds until next 9:00
            next_run = now.replace(hour=9, minute=0, second=0, microsecond=0)
            if now >= next_run:
                next_run += timedelta(days=1)
            wait_seconds = (next_run - now).total_seconds()
            logger.info(f"Notifier: next run at {next_run} (in {wait_seconds/3600:.1f}h)")
            await asyncio.sleep(wait_seconds)

            # Run notifications
            await send_notifications(bot)

        except Exception as e:
            logger.error(f"Notifier loop error: {e}")
            await asyncio.sleep(3600)


async def send_notifications(bot: Bot):
    """Check vacations and send reminders based on per-chat threshold."""
    data = load_data()
    all_vacations = data.get("vacations", {})
    all_notifications = data.get("notifications", {})
    settings = data.get("settings", {})

    # Migrate legacy list format on-the-fly
    if isinstance(all_vacations, list):
        logger.warning("Migrating legacy vacations list to per-chat dict")
        all_vacations = {}
        data["vacations"] = all_vacations
    if isinstance(all_notifications, list):
        all_notifications = {}
        data["notifications"] = all_notifications

    today = datetime.now().date()
    sent_count = 0

    # Find all chats that have a notify_days setting
    for chat_id, chat_settings in settings.items():
        if chat_id == "hr_chat_id":
            continue
        notify_days = chat_settings.get("notify_days")
        if not notify_days:
            continue

        vacations = all_vacations.get(chat_id, [])
        chat_notified = set(
            f"{n['vacation_id']}_{today}"
            for n in all_notifications.get(chat_id, [])
        )

        for idx, v in enumerate(vacations):
            try:
                start_dt = datetime.strptime(v["start_date"], "%Y-%m-%d").date()
                days_left = (start_dt - today).days

                if 0 <= days_left <= notify_days:
                    key = f"{idx}_{today}"
                    if key not in chat_notified:
                        msg = (
                            f"⏰ Напоминание об отпуске\n\n"
                            f"👤 {v['fio']}\n"
                            f"🏢 {v.get('org', '')}\n"
                            f"📅 Начало: {v['start_date']}\n"
                            f"📅 Конец: {v['end_date']}\n"
                            f"🗓 Дней: {v['days']}\n\n"
                            f"До начала отпуска: {days_left} дн."
                        )
                        try:
                            bot.send_text(chat_id=chat_id, text=msg)
                            all_notifications.setdefault(chat_id, []).append({
                                "vacation_id": idx,
                                "sent_at": str(today),
                            })
                            chat_notified.add(key)
                            sent_count += 1
                        except Exception as e:
                            logger.error(f"Failed to send notification to {chat_id}: {e}")
            except Exception as e:
                logger.debug(f"Notification check error for vacation {idx}: {e}")

    if sent_count:
        save_data(data)
        logger.info(f"Sent {sent_count} notification(s)")
    else:
        logger.info("Notifier: no notifications due today")


# === Main ===
def main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Verify identity
    try:
        me = bot.self_get().json()
        logger.info(f"Bot identity: {me}")
    except Exception as e:
        logger.error(f"self_get failed: {e}")

    loop.create_task(polling_loop(bot))
    loop.create_task(notifier_loop(bot))

    logger.info("Bot running forever...")
    loop.run_forever()


if __name__ == "__main__":
    main()
