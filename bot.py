import asyncio
import json
import logging
import os
import random
import string
from html import escape
from datetime import datetime, timedelta, time
from pathlib import Path
from zoneinfo import ZoneInfo

import asyncpg

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import CommandStart, BaseFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.base import StorageKey
from aiogram.fsm.storage.redis import RedisStorage
from redis.asyncio import Redis
from aiogram.types import (
    Message, CallbackQuery,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove,
    InlineKeyboardMarkup, InlineKeyboardButton,
    InputMediaPhoto,
)

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

def local_now() -> datetime:
    """Текущее локальное время бота в наивном datetime (для сравнений в логике)."""
    tz_name = os.getenv("BOT_TIMEZONE", "Asia/Almaty")
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo("Asia/Almaty")
    return datetime.now(tz).replace(tzinfo=None)

def load_env_file(path: str = ".env") -> None:
    env_path = Path(path)
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))

load_env_file()

# ═══════════════════════════════════════════════════════════
TOKEN        = os.getenv("BOT_TOKEN")
WORKER_ID    = 6140148323
DATABASE_URL = os.getenv("DATABASE_URL")
# ═══════════════════════════════════════════════════════════

storage: RedisStorage | None = None  # глобальный storage, инициализируется в main()
redis_client: Redis | None = None  # Redis клиент для FSM
db_pool: asyncpg.Pool | None = None

ACTIVE_STATUSES = frozenset({"pending", "price_sent", "waiting_worker", "on_way"})

# ── БД ────────────────────────────────────────────────────
def normalize_database_url(url: str) -> str:
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql://", 1)
    return url

async def init_db_pool() -> None:
    global db_pool
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL не задан. Добавьте его в .env или переменные окружения.")
    db_pool = await asyncpg.create_pool(normalize_database_url(DATABASE_URL), min_size=1, max_size=5)

async def close_db_pool() -> None:
    global db_pool
    if db_pool is not None:
        await db_pool.close()
        db_pool = None

async def init_redis() -> None:
    global redis_client, storage
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
    redis_client = Redis.from_url(redis_url, decode_responses=True)
    storage = RedisStorage(redis=redis_client)

async def close_redis() -> None:
    global redis_client
    if redis_client is not None:
        await redis_client.close()
        redis_client = None

async def init_db() -> None:
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                order_id TEXT PRIMARY KEY,
                user_id BIGINT NOT NULL,
                service TEXT,
                block TEXT,
                floor INTEGER,
                apt TEXT,
                trash_type TEXT,
                bags INTEGER,
                price TEXT,
                worker_price INTEGER,
                order_time TEXT,
                comment TEXT,
                photo_id TEXT,
                status TEXT NOT NULL,
                needs_price BOOLEAN NOT NULL DEFAULT FALSE,
                worker_msg_id BIGINT,
                data_json JSONB NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS reviews (
                id SERIAL PRIMARY KEY,
                order_id TEXT REFERENCES orders(order_id),
                user_id BIGINT,
                review_text TEXT,
                skipped BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TEXT NOT NULL
            )
        """)

async def migrate_db() -> None:
    """Безопасная миграция с проверкой и timeout для больших таблиц"""
    async with db_pool.acquire() as conn:
        try:
            col_type = await conn.fetchval(
                """
                SELECT data_type FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'orders' AND column_name = 'data_json'
                """
            )
            if col_type == "text":
                # Миграция с timeout для безопасности на больших таблицах
                await conn.execute("SET statement_timeout = '300s'")
                await conn.execute(
                    "ALTER TABLE orders ALTER COLUMN data_json TYPE JSONB USING data_json::jsonb"
                )
                await conn.execute("RESET statement_timeout")
                logger.info("Миграция data_json завершена успешно")
        except asyncpg.exceptions.QueryCanceledError:
            logger.warning("Миграция data_json отменена по timeout (таблица слишком большая). Пропускаем.")
        except Exception as e:
            logger.warning(f"Ошибка при миграции data_json: {e}. Продолжаем работу.")

def order_data_from_db(value) -> dict:
    if isinstance(value, dict):
        return value
    return json.loads(value)

async def save_order(order_id: str, user_id: int, data: dict, status: str, needs_price: bool, worker_msg_id: int | None = None) -> None:
    now = datetime.now().isoformat(timespec="seconds")
    async with db_pool.acquire() as conn:
        existing = await conn.fetchval("SELECT created_at FROM orders WHERE order_id = $1", order_id)
        created_at = existing or now
        await conn.execute(
            """
            INSERT INTO orders (
                order_id, user_id, service, block, floor, apt, trash_type, bags, price,
                worker_price, order_time, comment, photo_id, status, needs_price,
                worker_msg_id, data_json, created_at, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
            ON CONFLICT (order_id) DO UPDATE SET
                user_id = EXCLUDED.user_id,
                service = EXCLUDED.service,
                block = EXCLUDED.block,
                floor = EXCLUDED.floor,
                apt = EXCLUDED.apt,
                trash_type = EXCLUDED.trash_type,
                bags = EXCLUDED.bags,
                price = EXCLUDED.price,
                worker_price = EXCLUDED.worker_price,
                order_time = EXCLUDED.order_time,
                comment = EXCLUDED.comment,
                photo_id = EXCLUDED.photo_id,
                status = EXCLUDED.status,
                needs_price = EXCLUDED.needs_price,
                worker_msg_id = EXCLUDED.worker_msg_id,
                data_json = EXCLUDED.data_json,
                updated_at = EXCLUDED.updated_at
            """,
            order_id, user_id,
            data.get("service"), data.get("block"), data.get("floor"), data.get("apt"),
            data.get("trash_type"), data.get("bags"), data.get("price"),
            data.get("worker_price"), data.get("order_time"), data.get("comment"),
            data.get("photo_id"), status, needs_price, worker_msg_id,
            json.dumps(data), created_at, now,
        )

async def update_order_status(order_id: str | None, status: str) -> None:
    if not order_id:
        return
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE orders SET status = $1, updated_at = $2 WHERE order_id = $3",
            status, datetime.now().isoformat(timespec="seconds"), order_id,
        )

async def update_order_worker_price(order_id: str, price: int, data: dict) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE orders SET worker_price = $1, data_json = $2, status = $3, updated_at = $4 WHERE order_id = $5",
            price, json.dumps(data), "price_sent",
            datetime.now().isoformat(timespec="seconds"), order_id,
        )

async def update_order_worker_message(order_id: str, worker_msg_id: int) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE orders SET worker_msg_id = $1, updated_at = $2 WHERE order_id = $3",
            worker_msg_id, datetime.now().isoformat(timespec="seconds"), order_id,
        )

async def save_review(order_id: str | None, user_id: int, review_text: str = "", skipped: bool = False) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO reviews (order_id, user_id, review_text, skipped, created_at) VALUES ($1, $2, $3, $4, $5)",
            order_id, user_id, review_text, skipped, datetime.now().isoformat(timespec="seconds"),
        )

async def fetch_order(order_id: str) -> dict | None:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT user_id, status, worker_msg_id, data_json FROM orders WHERE order_id = $1",
            order_id,
        )
    if not row or row["status"] not in ACTIVE_STATUSES:
        return None
    return {
        "user_id": row["user_id"],
        "status": row["status"],
        "worker_msg_id": row["worker_msg_id"],
        "data": order_data_from_db(row["data_json"]),
    }

async def count_active_orders() -> int:
    async with db_pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT COUNT(*) FROM orders WHERE status = ANY($1::text[])",
            list(ACTIVE_STATUSES),
        )

# ── Хелперы FSM ───────────────────────────────────────────
# FIX: мёржим данные вместо полной перезаписи, чтобы не потерять данные клиента
async def set_client_state(bot_id: int, user_id: int, state: State, data: dict | None = None) -> None:
    key = StorageKey(bot_id=bot_id, chat_id=user_id, user_id=user_id)
    await storage.set_state(key=key, state=state)
    if data is not None:
        current = await storage.get_data(key=key)
        await storage.set_data(key=key, data={**current, **data})

# ── Генерация номера заявки ───────────────────────────────
def generate_order_id() -> str:
    date = local_now().strftime("%d%m%y")
    rand = "".join(random.choices(string.ascii_uppercase + string.digits, k=4))
    return f"ЖК-{date}-{rand}"

def get_price(bags: int, lang: str = "ru") -> str:
    if bags <= 3:
        return "500 ₸"
    if bags <= 6:
        return "1 000 ₸"
    if bags <= 10:
        return "1 500 ₸"
    return "по оценке сотрудника" if lang == "ru" else "қызметкердің бағалауы бойынша"

WORK_START = time(9, 0)
WORK_END = time(18, 0)


def is_working_day(dt: datetime) -> bool:
    return dt.weekday() < 5


def is_within_working_hours(dt: datetime) -> bool:
    return is_working_day(dt) and WORK_START <= dt.time() < WORK_END


def next_working_date(from_dt: datetime) -> datetime:
    d = from_dt
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return d


def nearest_working_date(now: datetime) -> datetime:
    if not is_working_day(now):
        return next_working_date(now)
    if now.time() >= WORK_END:
        return next_working_date(now + timedelta(days=1))
    return now


def resolve_custom_datetime(now: datetime, requested_h: int, requested_m: int) -> tuple[datetime, bool]:
    adjusted = False
    target = nearest_working_date(now)
    target_date = target.date()

    req_time = time(requested_h, requested_m)
    if req_time < WORK_START:
        req_time = WORK_START
        adjusted = True
    elif req_time > WORK_END:
        req_time = WORK_END
        adjusted = True

    result = datetime.combine(target_date, req_time)

    # Если указанное время уже прошло сегодня, ставим ближайшее рабочее время.
    if target_date == now.date() and is_working_day(now) and result < now:
        rounded_min = now.minute if now.minute == 0 else now.minute + (5 - now.minute % 5)
        hour = now.hour
        if rounded_min >= 60:
            hour += 1
            rounded_min -= 60

        if hour > WORK_END.hour or (hour == WORK_END.hour and rounded_min > WORK_END.minute):
            next_day = next_working_date(now + timedelta(days=1))
            result = datetime.combine(next_day.date(), WORK_START)
        else:
            result = datetime.combine(now.date(), time(hour, rounded_min))
        adjusted = True

    return result, adjusted

# ═══════════════════════════════════════════ FSM ═══════════
class Worker(StatesGroup):
    ready          = State()
    entering_price = State()

class Order(StatesGroup):
    choosing_language = State()
    choosing_service = State()
    choosing_block   = State()
    entering_floor   = State()
    entering_apt     = State()
    choosing_trash   = State()
    entering_bags    = State()
    sending_photo    = State()
    choosing_time    = State()
    entering_time    = State()
    asking_comment   = State()
    entering_comment = State()
    confirming       = State()
    editing          = State()
    waiting_price    = State()
    price_confirm    = State()
    price_declined   = State()
    waiting_worker   = State()
    leaving_review   = State()

# ═══════════════════════════════════════ ФИЛЬТРЫ ═══════════
class IsWorker(BaseFilter):
    async def __call__(self, message: Message) -> bool:
        return message.from_user.id == WORKER_ID

class IsClient(BaseFilter):
    async def __call__(self, message: Message) -> bool:
        return message.from_user.id != WORKER_ID

class IsWorkerCB(BaseFilter):
    async def __call__(self, callback: CallbackQuery) -> bool:
        return callback.from_user.id == WORKER_ID

# ═══════════════════════════════════════ КЛАВИАТУРЫ ════════
BTN = {
    "ru": {
        "main_service": "🗑 Вынести мусор",
        "main_help": "🆘 Помощь",
        "back": "◀️ Назад",
        "cancel": "❌ Отменить заявку",
        "trash_domestic": "🏠 Бытовой",
        "trash_construction": "🧱 Строительный",
        "trash_large": "📦 Крупногабаритный",
        "time_now": "⚡ Сейчас",
        "time_hour": "🕐 В течение часа",
        "time_custom": "🕒 Указать время",
        "comment_add": "💬 Добавить комментарий",
        "comment_skip": "➡️ Без комментария",
        "confirm_ok": "✅ Подтвердить",
        "confirm_edit": "✏️ Изменить",
        "price_accept": "✅ Принять цену",
        "review_skip": "⏭ Пропустить отзыв",
        "edit_block": "🔄 Изменить блок",
        "edit_floor": "🔄 Изменить этаж",
        "edit_apt": "🔄 Изменить квартиру",
        "edit_trash": "🔄 Изменить тип мусора",
        "edit_bags": "🔄 Изменить кол-во пакетов",
        "edit_photo": "🔄 Изменить фото",
        "edit_time": "🔄 Изменить время",
        "edit_comment": "🔄 Изменить комментарий",
        "photo_done": "✅ Готово",
    },
    "kk": {
        "main_service": "🗑 Қоқысты шығару",
        "main_help": "🆘 Көмек",
        "back": "◀️ Артқа",
        "cancel": "❌ Өтінімді бас тарту",
        "trash_domestic": "🏠 Тұрмыстық",
        "trash_construction": "🧱 Құрылыс",
        "trash_large": "📦 Ірі мөлшерлі",
        "time_now": "⚡ Қазір",
        "time_hour": "🕐 Сағат ішінде",
        "time_custom": "🕒 Уақытты көрсету",
        "comment_add": "💬 Түсіндіру қосу",
        "comment_skip": "➡️ Түсіндірісіз",
        "confirm_ok": "✅ Растау",
        "confirm_edit": "✏️ Өзгерту",
        "price_accept": "✅ Бағасын қабылдау",
        "review_skip": "⏭ Өндіктемені өткізіп жіберу",
        "edit_block": "🔄 Блокты өзгерту",
        "edit_floor": "🔄 Қабатты өзгерту",
        "edit_apt": "🔄 Пәтерді өзгерту",
        "edit_trash": "🔄 Қоқыс түрін өзгерту",
        "edit_bags": "🔄 Қап санын өзгерту",
        "edit_photo": "🔄 Суретті өзгерту",
        "edit_time": "🔄 Уақытты өзгерту",
        "edit_comment": "🔄 Пікірді өзгерту",
        "photo_done": "✅ Дамалды",
    },
}


def kb_main(lang: str):
    b = BTN.get(lang, BTN["ru"])
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text=b["main_service"]), KeyboardButton(text=b["main_help"])],
    ], resize_keyboard=True)


def kb_nav(lang: str):
    b = BTN.get(lang, BTN["ru"])
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text=b["back"]), KeyboardButton(text=b["cancel"])],
    ], resize_keyboard=True)


def kb_blocks(lang: str):
    b = BTN.get(lang, BTN["ru"])
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="Блок 1"), KeyboardButton(text="Блок 2"), KeyboardButton(text="Блок 3")],
        [KeyboardButton(text="Блок 4"), KeyboardButton(text="Блок 5"), KeyboardButton(text="Блок 6")],
        [KeyboardButton(text=b["back"]), KeyboardButton(text=b["cancel"])],
    ], resize_keyboard=True)


def kb_trash(lang: str):
    b = BTN.get(lang, BTN["ru"])
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text=b["trash_domestic"])],
        [KeyboardButton(text=b["trash_construction"]), KeyboardButton(text=b["trash_large"])],
        [KeyboardButton(text=b["back"]), KeyboardButton(text=b["cancel"])],
    ], resize_keyboard=True)


def kb_time(lang: str, allow_presets: bool = True):
    b = BTN.get(lang, BTN["ru"])
    rows = []
    if allow_presets:
        rows.append([KeyboardButton(text=b["time_now"]), KeyboardButton(text=b["time_hour"])])
    rows.append([KeyboardButton(text=b["time_custom"])])
    rows.append([KeyboardButton(text=b["back"]), KeyboardButton(text=b["cancel"])])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def kb_comment(lang: str):
    b = BTN.get(lang, BTN["ru"])
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text=b["comment_add"]), KeyboardButton(text=b["comment_skip"])],
        [KeyboardButton(text=b["back"]), KeyboardButton(text=b["cancel"])],
    ], resize_keyboard=True)


def kb_photo(lang: str):
    """Клавиатура для отправки фото: Готово и Назад"""
    b = BTN.get(lang, BTN["ru"])
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text=b["photo_done"]), KeyboardButton(text=b["back"])],
        [KeyboardButton(text=b["cancel"])],
    ], resize_keyboard=True)


def kb_confirm(lang: str):
    b = BTN.get(lang, BTN["ru"])
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text=b["confirm_ok"])],
        [KeyboardButton(text=b["confirm_edit"]), KeyboardButton(text=b["cancel"])],
    ], resize_keyboard=True)


def kb_price_confirm(lang: str):
    b = BTN.get(lang, BTN["ru"])
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text=b["price_accept"])],
        [KeyboardButton(text=b["cancel"])],
    ], resize_keyboard=True)


def kb_review(lang: str):
    b = BTN.get(lang, BTN["ru"])
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text=b["review_skip"])],
    ], resize_keyboard=True)


def kb_edit(data: dict, lang: str):
    b = BTN.get(lang, BTN["ru"])
    rows = [
        [KeyboardButton(text=b["edit_block"])],
        [KeyboardButton(text=b["edit_floor"])],
        [KeyboardButton(text=b["edit_apt"])],
        [KeyboardButton(text=b["edit_trash"])],
    ]
    if data.get("trash_type") == "🏠 Бытовой":
        rows.append([KeyboardButton(text=b["edit_bags"])])
    rows.append([KeyboardButton(text=b["edit_photo"])])
    rows.append([KeyboardButton(text=b["edit_time"])])
    rows.append([KeyboardButton(text=b["edit_comment"])])
    rows.append([KeyboardButton(text=b["back"]), KeyboardButton(text=b["cancel"])])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)

def ikb_worker_new(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="💰 Указать цену", callback_data=f"set_price:{order_id}")
    ]])

def ikb_worker_status(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🚗 В пути",    callback_data=f"status:on_way:{order_id}"),
        InlineKeyboardButton(text="✅ Выполнено", callback_data=f"status:done:{order_id}"),
    ]])

def ikb_worker_on_way(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Выполнено", callback_data=f"status:done:{order_id}"),
    ]])

# ═══════════════════════════════════════════ ХЕЛПЕРЫ ═══════
TRASH_TYPES = {
    "🏠 Бытовой", "🧱 Строительный", "📦 Крупногабаритный",
    "🏠 Тұрмыстық", "🧱 Құрылыс", "📦 Ірі мөлшерлі",
}
BLOCKS      = {"Блок 1", "Блок 2", "Блок 3", "Блок 4", "Блок 5", "Блок 6"}
NEEDS_PRICE = {"🧱 Строительный", "📦 Крупногабаритный"}

TRASH_CANONICAL = {
    "🏠": "🏠 Бытовой",
    "🧱": "🧱 Строительный",
    "📦": "📦 Крупногабаритный",
}

TRASH_LOCALIZED = {
    "ru": {
        "🏠 Бытовой": "🏠 Бытовой",
        "🧱 Строительный": "🧱 Строительный",
        "📦 Крупногабаритный": "📦 Крупногабаритный",
    },
    "kk": {
        "🏠 Бытовой": "🏠 Тұрмыстық",
        "🧱 Строительный": "🧱 Құрылыс",
        "📦 Крупногабаритный": "📦 Ірі мөлшерлі",
    },
}

# ── Тексты на двух языках ─────────────────────────────────
MESSAGES = {
    "ru": {
        "welcome": "👋 Добро пожаловать в сервис вашего жилого комплекса!\n\n⏰ <b>Обратите внимание:</b> услуги выполняются по будням с 09:00 до 18:00.\n📅 Суббота и воскресенье — выходные, в эти дни мы не работаем.\n\n📌 Пожалуйста, отвечайте кнопками ниже.\n📌 Если кнопки не видны, нажмите значок клавиатуры внизу экрана рядом со строкой ввода.\n\nВыберите нужную услугу 👇",
        "choose_language": "Выберите язык / Тілді таңдаңыз:",
        "service": "Вынос мусора",
        "block_choice": "🏢 Укажите номер вашего блока",
        "floor_prompt": "🪜 Введите номер этажа (1–30):",
        "floor_invalid": "🚫 Введите число от 1 до 30:",
        "apt_prompt": "🚪 Введите номер квартиры",
        "apt_invalid_format": "🚫 Введите корректный номер квартиры (например: 68 или 68А):",
        "apt_invalid_max": "🚫 Такой квартиры нет. Введите номер квартиры от 1 до 153:",
        "trash_choice": "🗑 Выберите тип мусора",
        "trash_invalid": "⚠️ Выберите тип мусора с помощью кнопок ниже.",
        "bags_prompt": "📦 Сколько мусорных пакетов?\n\n💰 <b>Тарифы:</b>\n• до 3 пакетов — 500 ₸\n• до 6 — 1 000 ₸\n• до 10 — 1 500 ₸\n• более 10 — по оценке сотрудника\n\nЦена указана за 1 обычный мусорный пакет.\n\n❗ Если пакет больше стандартного магазинного пакета или его невозможно поднять одной рукой — считается как несколько пакетов.\n\nВведите количество",
        "bags_invalid": "🚫 Введите корректное количество пакетов:",
        "photo_prompt_domestic": "💰 Стоимость вывоза: <b>{price}</b>\n\n📸 Пришлите фото мусора",
        "photo_prompt_domestic_calc": "💰 Стоимость рассчитывается сотрудником по фото.\n\n📸 Пришлите фото мусора",
        "photo_prompt_other": "📸 Для <b>{trash}</b> цена рассчитывается сотрудником.\n\nПришлите фото мусора",
        "photo_invalid": "📸 Пожалуйста, отправьте <b>фотографию</b> мусора.",
        "time_choice": "⏰ Когда вы хотите принять заказ?\n\nℹ️ Работаем только по будням с 09:00 до 18:00.",
        "time_custom_prompt": "⚠️ Заявка будет установлена на ближайшее рабочее время.\nБлижайший рабочий день: <b>{nearest_date}</b>.\n\nВведите время в формате <b>ЧЧ:ММ</b> (например, 14:30).\nРабочее время: 09:00 – 18:00.",
        "time_invalid": "⚠️ Выберите вариант с помощью кнопок ниже.",
        "time_format_error": "🚫 Неверный формат. Введите как <b>ЧЧ:ММ</b>, например 14:30",
        "time_range_error": "🚫 Время должно быть в диапазоне <b>09:00 – 18:00</b>",
        "time_preset_unavailable": "⛔ Сейчас нельзя выбрать «Сейчас» или «В течение часа».\nМы работаем только по будням с 09:00 до 18:00.\n\nВыберите «🕒 Указать время» — заявка будет поставлена на ближайшее рабочее время.",
        "time_custom_adjusted": "ℹ️ Установлено ближайшее рабочее время: <b>{time} ({date})</b>.",
        "comment_choice": "💬 Хотите добавить комментарий к заявке?",
        "comment_prompt": "✏️ Напишите ваш комментарий",
        "comment_invalid": "⚠️ Выберите вариант с помощью кнопок ниже.",
        "confirm_prompt": "<b>Всё верно?</b>",
        "change_lang": "\n\n💬 <i>Чтобы изменить язык, введите /start</i>",
        "summary": "📋 <b>Заявка <code>{order_id}</code></b>",
        "order_waiting": "⏳ <b>Заявка отправлена работнику!</b>\n\nОжидайте — работник оценит объём и пришлёт вам цену.",
        "order_confirmed": "⏳ <b>Статус: В ожидании</b>\n\nСотрудник скоро придёт к вам!",
        "cancel_msg": "❌ Заявка отменена. Возвращаемся в главное меню.",
        "help_text": "<b>Служба поддержки ЖК</b>\n\nТелефон: +7 707 331 8287\nВремя работы: 09:00 — 18:00",
        "price_sent": "💰 <b>Работник оценил вашу заявку!</b>",
        "price_waiting": "💰 <b>Укажите цену для клиента:</b>",
        "price_fixed": "ℹ️ Цена фиксированная. Можете приступать!",
        "on_way": "🚗 <b>Статус заявки <code>{order_id}</code>: Работник едет к вам!</b>",
        "done": "✅ <b>Статус заявки <code>{order_id}</code>: Выполнено!</b>\n\nСпасибо, что воспользовались нашим сервисом!\nНапишите отзыв о выполненной работе или нажмите кнопку ниже, чтобы пропустить.",
        "worker_welcome": "👷 Добро пожаловать, сотрудник!\n\nСюда будут приходить заявки от клиентов.\nПо заявкам с оценкой — нажмите «💰 Указать цену».\nПо готовым заявкам — меняйте статус кнопками под сообщением.\n\nНачните работу!",
        "review_prompt": "Напишите отзыв текстом или нажмите «⏭ Пропустить отзыв».",
        "review_thank": "Спасибо за отзыв! Возвращаемся в главное меню.",
    },
    "kk": {
        "welcome": "👋 Өз пәтерінің қызметіне қош келдіңіз!\n\n⏰ <b>Ескертпе:</b> қызмет тек жұмыс күндері 09:00-ден 18:00-ге дейін орындалады.\n📅 Сенбі және жексенбі — демалыс, бұл күндері біз жұмыс істемейміз.\n\n📌 Төмендегі батырмалармен жауап беріңіз.\n📌 Егер батырмалар көрінбесе, енгізу жолағының жанындағы пернетақта белгісін басыңыз.\n\nҚажетті қызметті таңдаңыз 👇",
        "choose_language": "Выберите язык / Тілді таңдаңыз:",
        "service": "Қоқысты шығару",
        "block_choice": "🏢 Блок номеріңізді көрсетіңіз",
        "floor_prompt": "🪜 Қабат нөмерін (1–30) енгізіңіз:",
        "floor_invalid": "🚫 1-ден 30-ға дейін сан енгізіңіз:",
        "apt_prompt": "🚪 Пәтер нөмерін енгізіңіз",
        "apt_invalid_format": "🚫 Дұрыс пәтер нөмерін енгізіңіз (мысалы: 68 немесе 68А):",
        "apt_invalid_max": "🚫 Мындай пәтер жоқ. Пәтер нөмерін 1-ден 153-ке дейін енгізіңіз:",
        "trash_choice": "🗑 Қоқыс түрін таңдаңыз",
        "trash_invalid": "⚠️ Төмендегі түймелер арқылы қоқыс түрін таңдаңыз.",
        "bags_prompt": "📦 Қоқыс пакеті қанша?\n\n💰 <b>Тарифтар:</b>\n• 3 пакетке дейін — 500 ₸\n• 6 пакетке дейін — 1 000 ₸\n• 10 пакетке дейін — 1 500 ₸\n• 10-нан көп — қызметкердің бағалауы бойынша\n\nБаға 1 кәдімгі қоқыс пакетіне көрсетілген.\n\n❗ Егер пакет стандартты дүкен пакетінен үлкен болса немесе оны бір қолмен көтеру мүмкін болмаса — ол бірнеше пакет болып есептеледі.\n\nСаны енгізіңіз",
        "bags_invalid": "🚫 Дұрыс сәліне саны енгізіңіз:",
        "photo_prompt_domestic": "💰 Шығару құны: <b>{price}</b>\n\n📸 Қоқыстың суретін жібер",
        "photo_prompt_domestic_calc": "💰 Баланы фото арқылы қызметкер есептейді.\n\n📸 Қоқыстың суретін жібер",
        "photo_prompt_other": "📸 <b>{trash}</b> үшін баланы қызметкер есептейді.\n\nҚоқыстың суретін жібер",
        "photo_invalid": "📸 Өтінегі <b>қоқыстың суретін</b> жібер.",
        "time_choice": "⏰ Өтіністі қашан қабылдағыңыз келеді?\n\nℹ️ Біз тек жұмыс күндері 09:00-ден 18:00-ге дейін жұмыс істейміз.",
        "time_custom_prompt": "⚠️ Өтінім ең жақын жұмыс уақытына қойылады.\nЕң жақын жұмыс күні: <b>{nearest_date}</b>.\n\nУақытты <b>СС:ММ</b> форматында енгізіңіз (мысалы, 14:30).\nЖұмыс уақыты: 09:00 – 18:00.",
        "time_invalid": "⚠️ Төмендегі түймелер арқылы опцияны таңдаңыз.",
        "time_format_error": "🚫 Бұл формат дұрыс емес. <b>СС:ММ</b> форматында енгізіңіз, мысалы 14:30:",
        "time_range_error": "🚫 Уақыт <b>09:00 – 18:00</b> аралығында болуы керек:",
        "time_preset_unavailable": "⛔ Қазір «Қазір» немесе «Сағат ішінде» таңдауға болмайды.\nБіз тек жұмыс күндері 09:00-ден 18:00-ге дейін жұмыс істейміз.\n\n«🕒 Уақытты көрсету» таңдаңыз — өтінім ең жақын жұмыс уақытына қойылады.",
        "time_custom_adjusted": "ℹ️ Ең жақын жұмыс уақыты орнатылды: <b>{time} ({date})</b>.",
        "comment_choice": "💬 Өтіністіге пікір қосқыңыз келе ме?",
        "comment_prompt": "✏️ Өз пікіріңізді жазыңыз",
        "comment_invalid": "⚠️ Төмендегі түймелер арқылы опцияны таңдаңыз.",
        "confirm_prompt": "<b>Барлық дұрыс па?</b>",
        "change_lang": "\n\n💬 <i>Тілді өзгерту үшін /start енгізіңіз</i>",
        "summary": "📋 <b>Өтіністі <code>{order_id}</code></b>",
        "order_waiting": "⏳ <b>Өтіністі қызметкерге жіберді!</b>\n\nКүтіңіз — қызметкер көлемді бағалап, сізге баланы жіберді.",
        "order_confirmed": "⏳ <b>Статус: Күтілуде</b>\n\nҚызметкер сізге тез келеді!",
        "cancel_msg": "❌ Өтіністі бас тартылды. Басты мәзірге орал.",
        "help_text": "<b>ЖК Қолдау Қызметі</b>\n\nТелефон: +7 707 331 8287\nЖұмыс уақыты: 09:00 — 18:00",
        "price_sent": "💰 <b>Қызметкер сіздің өтіністіңізді бағалады!</b>",
        "price_waiting": "💰 <b>Клиентке баланы қойыңыз:</b>",
        "price_fixed": "ℹ️ Баланы бекітіліген. Басталуға болады!",
        "on_way": "🚗 <b>Өтіністің статусы <code>{order_id}</code>: Қызметкер сізге баратын жолда!</b>",
        "done": "✅ <b>Өтіністің статусы <code>{order_id}</code>: Орындалды!</b>\n\nБіздің қызметті пайдалағаныңыз үшін рахмет!\nОрындалған жұмыс туралы пікіріңізді жазыңыз немесе өткізу үшін төмендегі түймені басыңыз.",
        "worker_welcome": "👷 Қош келдіңіз, сотрудник!\n\nМұнда клиенттердің өтіністері келеді.\nБаланы білдіруге тиісті өтіністер үшін — «💰 Баланы қойыңыз» басыңыз.\nДайын өтіністер үшін — хабар астындағы түймелер арқылы статусты өзгерту.\n\nЖұмысты бастаңыз!",
        "review_prompt": "Пікіріңізді мәтін түрінде жазыңыз немесе «⏭ Отзывды өткізіңіз» басыңыз.",
        "review_thank": "Пікіріңіз үшін рахмет! Басты мәзірге орал.",
    }
}

def msg(lang: str, key: str, **kwargs) -> str:
    """Получить сообщение на нужном языке"""
    text = MESSAGES.get(lang, MESSAGES["ru"]).get(key, "")
    return text.format(**kwargs) if kwargs else text

def order_summary(data: dict, include_worker_price: bool = False, lang: str = "ru") -> str:
    trash        = data.get("trash_type", "—")
    bags         = data.get("bags")
    price        = data.get("price", "")
    comment      = data.get("comment", "")
    time_str     = data.get("order_time", "—")
    worker_price = data.get("worker_price", "")

    localized_trash = TRASH_LOCALIZED.get(lang, TRASH_LOCALIZED["ru"]).get(trash, trash)

    if lang == "ru":
        bags_line = f"\n📦 Кол-во пакетов: {bags}" if bags else ""
        price_line = f"\n💰 Стоимость:      {price}" if price else ""
        wprice_line = f"\n💰 Цена:           {worker_price} ₸" if (include_worker_price and worker_price) else ""
        comm_line = f"\n💬 Комментарий:    {comment}" if comment else ""
        title = "📋 <b>Заявка"
        service_label = "📦 Услуга"
        trash_label = "🗑 Тип мусора"
        block_label = "🏢 Блок"
        floor_label = "🪜 Этаж"
        apt_label = "🚪 Квартира"
        time_label = "⏰ Время"
    else:
        bags_line = f"\n📦 Қап саны:       {bags}" if bags else ""
        price_line = f"\n💰 Құны:           {price}" if price else ""
        wprice_line = f"\n💰 Бағасы:         {worker_price} ₸" if (include_worker_price and worker_price) else ""
        comm_line = f"\n💬 Пікір:          {comment}" if comment else ""
        title = "📋 <b>Өтінім"
        service_label = "📦 Қызмет"
        trash_label = "🗑 Қоқыс түрі"
        block_label = "🏢 Блок"
        floor_label = "🪜 Қабат"
        apt_label = "🚪 Пәтер"
        time_label = "⏰ Уақыт"

    return (
        f"{title} <code>{data.get('order_id', '—')}</code></b>\n\n"
        f"{service_label}:     {data.get('service', '—')}\n"
        f"{trash_label}: {localized_trash}"
        f"{bags_line}"
        f"{price_line}"
        f"{wprice_line}\n"
        f"{block_label}:       {data.get('block', '—')}\n"
        f"{floor_label}:       {data.get('floor', '—')}\n"
        f"{apt_label}:   {data.get('apt', '—')}\n"
        f"{time_label}:      {time_str}"
        f"{comm_line}"
    )

async def show_confirm(message: Message, state: FSMContext):
    await state.set_state(Order.confirming)
    data = await state.get_data()
    photo_ids = data.get("photo_ids", [])
    lang = data.get("language", "ru")
    confirm_text = msg(lang, "confirm_prompt")
    text = order_summary(data, lang=lang) + f"\n\n{confirm_text}"
    
    if photo_ids:
        # Отправляем все фото как медиа-группу (альбом)
        media = [InputMediaPhoto(media=pid, caption=text if i == 0 else "") for i, pid in enumerate(photo_ids)]
        await message.answer_media_group(media=media)
        # Отправляем кнопки отдельно после медиа-группы
        await message.answer("👆 " + ("Выше ваша заявка. Всё верно?" if lang == "ru" else "Жоғарыда өтініміңіз. Барлығы дұрыс па?"), 
                           reply_markup=kb_confirm(lang))
    else:
        await message.answer(text, reply_markup=kb_confirm(lang))

async def go_to_comment(message: Message, state: FSMContext):
    await state.set_state(Order.asking_comment)
    data = await state.get_data()
    lang = data.get("language", "ru")
    prompt = msg(lang, "comment_choice")
    await message.answer(prompt, reply_markup=kb_comment(lang))

async def go_to_time(message: Message, state: FSMContext):
    await state.set_state(Order.choosing_time)
    data = await state.get_data()
    lang = data.get("language", "ru")
    prompt = msg(lang, "time_choice")
    allow_presets = is_within_working_hours(local_now())
    await message.answer(prompt, reply_markup=kb_time(lang, allow_presets=allow_presets))

async def ask_photo(message: Message, state: FSMContext):
    data = await state.get_data()
    trash = data.get("trash_type", "")
    await state.set_state(Order.sending_photo)
    lang = data.get("language", "ru")
    await state.update_data(photo_ids=[])  # Инициализируем пустой список фото
    if trash == "🏠 Бытовой" or "Бытовой" in str(trash):
        bags  = data.get("bags") or 0
        price = get_price(bags, lang)
        await state.update_data(price=price)
        if bags > 10:
            if lang == "ru":
                note = "💰 Стоимость рассчитывается сотрудником по фото."
            else:
                note = "💰 Баланы фото арқылы қызметкер есептейді."
        else:
            if lang == "ru":
                note = f"💰 Стоимость вывоза: <b>{price}</b>"
            else:
                note = f"💰 Шығару құны: <b>{price}</b>"
        
        await message.answer(
            f"{note}\n\n📸 " + ("Пришлите фото мусора (можно несколько)" if lang == "ru" else "Қоқыстың суретін жібер (бірнеше болса болады)"),
            reply_markup=kb_photo(lang)
        )
    else:
        prompt_photo = msg(lang, "photo_prompt_other", trash=trash)
        await message.answer(prompt_photo, reply_markup=kb_photo(lang))

async def send_order_to_worker(bot: Bot, data: dict, order_id: str, needs_price: bool):
    photo_ids = data.get("photo_ids", [])
    lang = data.get("language", "ru")
    if needs_price:
        if lang == "ru":
            text = f"📬 <b>Новая заявка!</b>\n\n{order_summary(data, lang='ru')}\n\n💰 <b>Укажите цену для клиента:</b>"
        else:
            text = f"📬 <b>Жаңа өтінім!</b>\n\n{order_summary(data, lang='kk')}\n\n💰 <b>Клиентке бағаны енгізіңіз:</b>"
    else:
        if lang == "ru":
            text = f"📬 <b>Новая заявка!</b>\n\n{order_summary(data, lang='ru')}\n\nℹ️ Цена фиксированная. Можете приступать!"
        else:
            text = f"📬 <b>Жаңа өтінім!</b>\n\n{order_summary(data, lang='kk')}\n\nℹ️ Баға бекітілген. Іске кірісе беріңіз!"
    
    kb = ikb_worker_new(order_id) if needs_price else ikb_worker_status(order_id)
    if photo_ids:
        # Отправляем все фото как медиа-группу (альбом)
        media = [InputMediaPhoto(media=pid, caption=text if i == 0 else "") for i, pid in enumerate(photo_ids)]
        messages = await bot.send_media_group(chat_id=WORKER_ID, media=media)
        msg = messages[0]  # Берём первое сообщение для worker_msg_id
        # Отправляем кнопки отдельным сообщением после медиа-группы
        await bot.send_message(WORKER_ID, "👆 " + ("Выше заявка. Действуйте!" if lang == "ru" else "Жоғарыда өтініс. Іс істей бастаңыз!"), 
                              reply_markup=kb)
    else:
        msg = await bot.send_message(WORKER_ID, text, reply_markup=kb)
    
    await update_order_worker_message(order_id, msg.message_id)

# ═══════════════════════════════════════════ РОУТЕРЫ ═══════
worker_router = Router()
client_router = Router()

# ══════════════════════════════════ РАБОТНИК ═══════════════

@worker_router.message(IsWorker(), CommandStart())
async def worker_start(message: Message, state: FSMContext):
    await state.set_state(Worker.ready)
    await message.answer(
        "👷 Добро пожаловать, сотрудник!\n\n"
        "Сюда будут приходить заявки от клиентов.\n"
        "По заявкам с оценкой — нажмите «💰 Указать цену».\n"
        "По готовым заявкам — меняйте статус кнопками под сообщением.\n\n"
        "Начните работу!\n\n"
        "─────────────────────\n\n"
        "👷 Қош келдіңіз, сотрудник!\n\n"
        "Мұнда клиенттердің өтіністері келеді.\n"
        "Баланы білдіруге тиісті өтіністер үшін — «💰 Баланы қойыңыз» басыңыз.\n"
        "Дайын өтіністер үшін — хабар астындағы түймелер арқылы статусты өзгерту.\n\n"
        "Жұмысты бастаңыз!",
        reply_markup=ReplyKeyboardRemove(),
    )

@worker_router.callback_query(IsWorkerCB(), F.data.startswith("set_price:"))
async def worker_set_price_prompt(callback: CallbackQuery, state: FSMContext):
    order_id = callback.data.split(":", 1)[1]
    order = await fetch_order(order_id)
    if not order or order["status"] != "pending":
        await callback.answer("❌ Заявка не найдена или уже закрыта. / ❌ Өтіністі таба алмадым немесе ол жабылды.", show_alert=True)
        return
    await state.set_state(Worker.entering_price)
    await state.update_data(pricing_order=order_id)
    await callback.message.answer(f"💰 Введите цену (₸) для заявки / 💰 Өтіністі үшін баланы (₸) енгізіңіз <code>{order_id}</code>:")
    await callback.answer()

@worker_router.message(IsWorker(), Worker.entering_price)
async def worker_enter_price(message: Message, state: FSMContext, bot: Bot):
    text = message.text.strip().replace(" ", "")
    if not text.isdigit() or int(text) <= 0:
        await message.answer("🚫 Введите корректную сумму (только цифры, больше 0): / 🚫 Дұрыс сумма енгізіңіз (тек сандар, 0-ден артық):")
        return
    data = await state.get_data()
    order_id = data.get("pricing_order")
    order = await fetch_order(order_id) if order_id else None
    if not order or order["status"] != "pending":
        await message.answer("❌ Заявка не найдена. / ❌ Өтіністі таба алмадым.")
        await state.set_state(Worker.ready)
        return

    price = int(text)
    order_data = {**order["data"], "worker_price": price}
    await update_order_worker_price(order_id, price, order_data)
    await state.set_state(Worker.ready)
    await message.answer(
        f"✅ Цена <b>{price} ₸</b> отправлена клиенту.\n"
        f"Ожидайте подтверждения.\n\n"
        f"─────────────────────\n\n"
        f"✅ Баланы <b>{price} ₸</b> клиентке жіберді.\n"
        f"Растауды күтіңіз."
    )

    user_id  = order["user_id"]
    photo_ids = order_data.get("photo_ids", [])
    client_lang = order_data.get("language", "ru")
    summary = order_summary(order_data, include_worker_price=True, lang=client_lang)
    if client_lang == "ru":
        client_text = (
            f"💰 <b>Работник оценил вашу заявку!</b>\n\n"
            f"🔥 <b>ВАША ЦЕНА: {price} ₸</b>\n\n"
            f"{summary}\n\n"
            f"Подтверждаете заказ?"
        )
    else:
        client_text = (
            f"💰 <b>Қызметкер өтініміңізді бағалады!</b>\n\n"
            f"🔥 <b>СІЗДІҢ БАҒАҢЫЗ: {price} ₸</b>\n\n"
            f"{summary}\n\n"
            f"Өтінімді растайсыз ба?"
        )

    if photo_ids:
        # Отправляем все фото как медиа-группу (альбом)
        media = [InputMediaPhoto(media=pid, caption=client_text if i == 0 else "") for i, pid in enumerate(photo_ids)]
        await bot.send_media_group(chat_id=user_id, media=media)
        # Отправляем кнопки отдельным сообщением после медиа-группы
        await bot.send_message(user_id, "👆 " + ("Выше ваша заявка. Что вы решили?" if client_lang == "ru" else "Жоғарыда өтініміңіз. Не істедіңіз?"), 
                              reply_markup=kb_price_confirm(client_lang))
    else:
        await bot.send_message(user_id, client_text, reply_markup=kb_price_confirm(client_lang))

    # Мёржим данные — не перезаписываем целиком
    await set_client_state(bot.id, user_id, Order.price_confirm, {"order_id": order_id})

@worker_router.callback_query(IsWorkerCB(), F.data.startswith("status:"))
async def worker_status(callback: CallbackQuery, bot: Bot):
    parts    = callback.data.split(":", 2)
    status   = parts[1]
    order_id = parts[2]

    order = await fetch_order(order_id)
    if not order:
        await callback.answer("❌ Заявка не найдена. / ❌ Өтіністі таба алмадым.", show_alert=True)
        return

    user_id = order["user_id"]
    client_lang = order["data"].get("language", "ru")

    if status == "on_way":
        if order["status"] != "waiting_worker":
            await callback.answer("❌ Заявка уже в другом статусе.", show_alert=True)
            return
        await update_order_status(order_id, "on_way")
        await callback.message.edit_reply_markup(reply_markup=ikb_worker_on_way(order_id))
        if client_lang == "ru":
            await bot.send_message(user_id, f"🚗 <b>Статус заявки <code>{order_id}</code>: Работник едет к вам!</b>")
        else:
            await bot.send_message(user_id, f"🚗 <b>Өтінім статусы <code>{order_id}</code>: Қызметкер сізге келе жатыр!</b>")
        await callback.answer("Статус: В пути")

    elif status == "done":
        if order["status"] not in ("waiting_worker", "on_way"):
            await callback.answer("❌ Заявка уже закрыта.", show_alert=True)
            return
        await update_order_status(order_id, "done")
        await callback.message.edit_reply_markup(reply_markup=None)
        await callback.message.answer(f"✅ Заявка <code>{order_id}</code> выполнена и закрыта.")
        if client_lang == "ru":
            done_text = (
                f"✅ <b>Статус заявки <code>{order_id}</code>: Выполнено!</b>\n\n"
                "Спасибо, что воспользовались нашим сервисом!\n"
                "Напишите отзыв о выполненной работе или нажмите кнопку ниже, чтобы пропустить."
            )
        else:
            done_text = (
                f"✅ <b>Өтінім статусы <code>{order_id}</code>: Орындалды!</b>\n\n"
                "Қызметімізді пайдаланғаныңызға рахмет!\n"
                "Орындалған жұмыс туралы пікір жазыңыз немесе өткізіп жіберу үшін төмендегі батырманы басыңыз."
            )
        await bot.send_message(user_id, done_text, reply_markup=kb_review(client_lang))
        # Мёржим данные — не перезаписываем целиком
        await set_client_state(bot.id, user_id, Order.leaving_review, {"review_order": order_id})
        await callback.answer("Заявка закрыта!")

# ══════════════════════════════════ КЛИЕНТ ═════════════════

@client_router.message(IsClient(), CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    # Читаем данные ДО очистки состояния
    data     = await state.get_data()
    order_id = data.get("order_id")
    await update_order_status(order_id, "canceled")
    await state.clear()
    await state.set_state(Order.choosing_language)
    
    # Показываем экран выбора языка
    welcome_text = (
        "👋 Өз пәтерінің қызметіне қош келдіңіз!\n\n"
        "⏰ <b>Ескертпе:</b> қызмет тек жұмыс күндері 09:00-ден 18:00-ге дейін орындалады.\n"
        "📅 Сенбі және жексенбі — демалыс, бұл күндері біз жұмыс істемейміз.\n\n"
        "📌 Төмендегі батырмалармен жауап беріңіз.\n"
        "📌 Егер батырмалар көрінбесе, енгізу жолағының жанындағы пернетақта белгісін басыңыз.\n\n"
        "Тілді таңдаңыз 👇\n\n"
        "─────────────────────\n\n"
        "👋 Добро пожаловать в сервис вашего жилого комплекса!\n\n"
        "⏰ <b>Обратите внимание:</b> услуги выполняются по будням с 09:00 до 18:00.\n"
        "📅 Суббота и воскресенье — выходные, в эти дни мы не работаем.\n\n"
        "📌 Пожалуйста, отвечайте кнопками ниже.\n"
        "📌 Если кнопки не видны, нажмите значок клавиатуры внизу экрана рядом со строкой ввода.\n\n"
        "Выберите язык 👇"
    )
    
    lang_kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🇷🇺 Русский"), KeyboardButton(text="🇰🇿 Қазақша")]
    ], resize_keyboard=True)
    
    await message.answer(welcome_text, reply_markup=lang_kb)

@client_router.message(IsClient(), Order.choosing_language, F.text.in_({"🇷🇺 Русский", "🇰🇿 Қазақша"}))
async def choose_language(message: Message, state: FSMContext):
    lang = "ru" if "Русский" in message.text else "kk"
    await state.update_data(language=lang)
    await state.set_state(Order.choosing_service)
    
    welcome = msg(lang, "welcome") + msg(lang, "change_lang")
    await message.answer(welcome, reply_markup=kb_main(lang))

@client_router.message(IsClient(), F.text.startswith("🆘"))
async def help_handler(message: Message, state: FSMContext):
    data = await state.get_data()
    lang = data.get("language", "ru")
    help_text = msg(lang, "help_text")
    help_text += msg(lang, "change_lang")
    await message.answer(help_text)

@client_router.message(IsClient(), F.text.startswith("❌"))
async def cancel_handler(message: Message, state: FSMContext, bot: Bot):
    # Читаем данные ДО очистки состояния
    data          = await state.get_data()
    current_state = await state.get_state()
    order_id  = data.get("order_id")
    lang      = data.get("language", "ru")

    if current_state == Order.price_confirm.state and order_id:
        await update_order_status(order_id, "price_declined")
        if lang == "ru":
            await bot.send_message(WORKER_ID, f"❌ Клиент отказался от цены по заявке <code>{order_id}</code>")
        else:
            await bot.send_message(WORKER_ID, f"❌ Клиент баланы арнайтты <code>{order_id}</code>")
        await state.set_state(Order.price_declined)
    elif order_id:
        await update_order_status(order_id, "canceled")
        await state.clear()
        await state.update_data(language=lang)
        await state.set_state(Order.choosing_service)
    else:
        await state.clear()
        await state.update_data(language=lang)
        await state.set_state(Order.choosing_service)
    
    if current_state == Order.price_confirm.state:
        # Остаемся в price_declined, не показываем меню
        return
    
    # Возвращаемся в главное меню на выбранном языке
    welcome = msg(lang, "welcome")
    welcome += msg(lang, "change_lang")
    await message.answer(welcome, reply_markup=kb_main(lang))

# ── Отзыв ─────────────────────────────────────────────────
@client_router.message(IsClient(), Order.leaving_review, F.text.startswith("⏭"))
async def skip_review(message: Message, state: FSMContext):
    data = await state.get_data()
    order_id = data.get("review_order")
    lang = data.get("language", "ru")  # Сохраняем язык перед очисткой
    await save_review(order_id, message.from_user.id, skipped=True)
    await state.clear()
    await state.update_data(language=lang)  # Восстанавливаем язык
    await state.set_state(Order.choosing_service)
    if lang == "ru":
        text = "Спасибо! Возвращаемся в главное меню."
    else:
        text = "Рахмет! Басты мәзірге ораламыз."
    await message.answer(text, reply_markup=kb_main(lang))

@client_router.message(IsClient(), Order.leaving_review, F.text)
async def process_review(message: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    order_id = data.get("review_order")
    lang = data.get("language", "ru")  # Сохраняем язык перед очисткой
    review = message.text.strip()
    if not review:
        prompt = msg(lang, "review_prompt")
        await message.answer(prompt, reply_markup=kb_review(lang))
        return
    if lang == "ru":
        await bot.send_message(WORKER_ID, f"💬 <b>Новый отзыв по заявке <code>{order_id}</code></b>\n\n{escape(review)}")
    else:
        await bot.send_message(WORKER_ID, f"💬 <b>Өтінім бойынша жаңа пікір <code>{order_id}</code></b>\n\n{escape(review)}")
    await save_review(order_id, message.from_user.id, review_text=review)
    await state.clear()
    await state.update_data(language=lang)  # Восстанавливаем язык
    await state.set_state(Order.choosing_service)
    text = msg(lang, "review_thank")
    await message.answer(text, reply_markup=kb_main(lang))

@client_router.message(IsClient(), Order.leaving_review)
async def review_invalid(message: Message, state: FSMContext):
    data = await state.get_data()
    lang = data.get("language", "ru")
    prompt = msg(lang, "review_prompt")
    await message.answer(prompt, reply_markup=kb_review(lang))

# ── Услуга ────────────────────────────────────────────────
@client_router.message(IsClient(), Order.choosing_service, F.text.startswith("🗑"))
async def garbage_service(message: Message, state: FSMContext):
    data = await state.get_data()
    lang = data.get("language", "ru")
    await state.update_data(service=msg(lang, "service"), order_id=generate_order_id())
    await state.set_state(Order.choosing_block)
    prompt = msg(lang, "block_choice")
    prompt += msg(lang, "change_lang")
    await message.answer(prompt, reply_markup=kb_blocks(lang))

# ── Блок ──────────────────────────────────────────────────
@client_router.message(IsClient(), Order.choosing_block, F.text.startswith("◀️"))
async def block_back(message: Message, state: FSMContext):
    data = await state.get_data()
    lang = data.get("language", "ru")
    await state.set_state(Order.choosing_service)
    await message.answer(msg(lang, "welcome"), reply_markup=kb_main(lang))

@client_router.message(IsClient(), Order.choosing_block, F.text.in_(BLOCKS))
async def process_block(message: Message, state: FSMContext):
    await state.update_data(block=message.text)
    data = await state.get_data()
    if data.get("editing"):
        await state.update_data(editing=False); await show_confirm(message, state); return
    lang = data.get("language", "ru")
    await state.set_state(Order.entering_floor)
    await message.answer(msg(lang, "floor_prompt"), reply_markup=kb_nav(lang))

@client_router.message(IsClient(), Order.choosing_block)
async def block_invalid(message: Message, state: FSMContext):
    data = await state.get_data()
    lang = data.get("language", "ru")
    if lang == "ru":
        await message.answer("⚠️ Выберите блок с помощью кнопок ниже.")
    else:
        await message.answer("⚠️ Төмендегі түймелер арқылы блокты таңдаңыз.")

# ── Этаж ──────────────────────────────────────────────────
@client_router.message(IsClient(), Order.entering_floor, F.text.startswith("◀️"))
async def floor_back(message: Message, state: FSMContext):
    data = await state.get_data()
    if data.get("editing"):
        await state.update_data(editing=False); await show_confirm(message, state); return
    lang = data.get("language", "ru")
    await state.set_state(Order.choosing_block)
    await message.answer(msg(lang, "block_choice"), reply_markup=kb_blocks(lang))

@client_router.message(IsClient(), Order.entering_floor)
async def process_floor(message: Message, state: FSMContext):
    text = message.text.strip()
    data = await state.get_data()
    lang = data.get("language", "ru")
    if not text.isdigit() or not (1 <= int(text) <= 30):
        await message.answer(msg(lang, "floor_invalid"))
        return
    await state.update_data(floor=int(text))
    data = await state.get_data()
    if data.get("editing"):
        await state.update_data(editing=False); await show_confirm(message, state); return
    await state.set_state(Order.entering_apt)
    await message.answer(msg(lang, "apt_prompt"), reply_markup=kb_nav(lang))

# ── Квартира ──────────────────────────────────────────────
@client_router.message(IsClient(), Order.entering_apt, F.text.startswith("◀️"))
async def apt_back(message: Message, state: FSMContext):
    data = await state.get_data()
    if data.get("editing"):
        await state.update_data(editing=False); await show_confirm(message, state); return
    lang = data.get("language", "ru")
    await state.set_state(Order.entering_floor)
    prompt = msg(lang, "floor_prompt")
    await message.answer(prompt, reply_markup=kb_nav(lang))

@client_router.message(IsClient(), Order.entering_apt)
async def process_apt(message: Message, state: FSMContext):
    text = message.text.strip().lower()

    import re

    # Разрешаем: 68, 68а, 68б и т.д.
    if not re.fullmatch(r"\d+[а-яa-z]?", text):
        data = await state.get_data()
        lang = data.get("language", "ru")
        error = msg(lang, "apt_invalid_format")
        await message.answer(error)
        return

    num = int(re.match(r"\d+", text).group())

    if num > 153:
        data = await state.get_data()
        lang = data.get("language", "ru")
        error = msg(lang, "apt_invalid_max")
        await message.answer(error)
        return

    await state.update_data(apt=text.upper())

    data = await state.get_data()
    if data.get("editing"):
        await state.update_data(editing=False)
        await show_confirm(message, state)
        return

    lang = data.get("language", "ru")
    await state.set_state(Order.choosing_trash)
    prompt = msg(lang, "trash_choice")
    await message.answer(prompt, reply_markup=kb_trash(lang))

# ── Тип мусора ────────────────────────────────────────────
@client_router.message(IsClient(), Order.choosing_trash, F.text.startswith("◀️"))
async def trash_back(message: Message, state: FSMContext):
    data = await state.get_data()
    if data.get("editing"):
        await state.update_data(editing=False); await show_confirm(message, state); return
    lang = data.get("language", "ru")
    await state.set_state(Order.entering_apt)
    prompt = msg(lang, "apt_prompt")
    await message.answer(prompt, reply_markup=kb_nav(lang))

@client_router.message(IsClient(), Order.choosing_trash, F.text.in_(TRASH_TYPES) | F.text.startswith("🏠") | F.text.startswith("🧱") | F.text.startswith("📦"))
async def process_trash(message: Message, state: FSMContext):
    lang = (await state.get_data()).get("language", "ru")
    icon = message.text.split()[0] if message.text else ""
    trash = TRASH_CANONICAL.get(icon, message.text.strip())
    await state.update_data(trash_type=trash, bags=None, price=None)
    data = await state.get_data()
    editing = data.get("editing")
    if trash == "🏠 Бытовой":
        if editing: await state.update_data(editing=False)
        await state.set_state(Order.entering_bags)
        await message.answer(msg(lang, "bags_prompt"), reply_markup=kb_nav(lang))
    else:
        if editing: await state.update_data(editing=False, photo_id=None)
        await ask_photo(message, state)

@client_router.message(IsClient(), Order.choosing_trash)
async def trash_invalid(message: Message, state: FSMContext):
    data = await state.get_data()
    lang = data.get("language", "ru")
    error = msg(lang, "trash_invalid")
    await message.answer(error)

# ── Кол-во пакетов ────────────────────────────────────────
@client_router.message(IsClient(), Order.entering_bags, F.text.startswith("◀️"))
async def bags_back(message: Message, state: FSMContext):
    data = await state.get_data()
    if data.get("editing"):
        await state.update_data(editing=False); await show_confirm(message, state); return
    lang = data.get("language", "ru")
    await state.set_state(Order.choosing_trash)
    await message.answer(msg(lang, "trash_choice"), reply_markup=kb_trash(lang))

@client_router.message(IsClient(), Order.entering_bags)
async def process_bags(message: Message, state: FSMContext):
    text = message.text.strip()
    if not text.isdigit() or int(text) <= 0:
        data = await state.get_data()
        lang = data.get("language", "ru")
        error = msg(lang, "bags_invalid")
        await message.answer(error)
        return
    bags = int(text)
    await state.update_data(bags=bags, price=get_price(bags))
    data = await state.get_data()
    if data.get("editing"):
        await state.update_data(editing=False); await show_confirm(message, state); return
    await ask_photo(message, state)

# ── Фото ──────────────────────────────────────────────────
@client_router.message(IsClient(), Order.sending_photo, F.text.startswith("◀️"))
async def photo_back(message: Message, state: FSMContext):
    data = await state.get_data()
    if data.get("editing"):
        await state.update_data(editing=False); await show_confirm(message, state); return
    lang = data.get("language", "ru")
    if data.get("trash_type") == "🏠 Бытовой" or "Бытовой" in str(data.get("trash_type")):
        await state.set_state(Order.entering_bags)
        await message.answer(msg(lang, "bags_prompt"), reply_markup=kb_nav(lang))
    else:
        await state.set_state(Order.choosing_trash)
        await message.answer(msg(lang, "trash_choice"), reply_markup=kb_trash(lang))

@client_router.message(IsClient(), Order.sending_photo, F.photo)
async def process_photo(message: Message, state: FSMContext):
    data = await state.get_data()
    lang = data.get("language", "ru")
    photo_ids = data.get("photo_ids", [])
    photo_ids.append(message.photo[-1].file_id)
    await state.update_data(photo_ids=photo_ids)
    
    count = len(photo_ids)
    if lang == "ru":
        text = f"✅ Фото добавлено! (Всего: {count})"
    else:
        text = f"✅ Сурет қосылды! (Барлығы: {count})"
    await message.answer(text, reply_markup=kb_photo(lang))

@client_router.message(IsClient(), Order.sending_photo, F.text.startswith("✅"))
async def photo_done(message: Message, state: FSMContext):
    """Завершение отправки фото и переход к времени"""
    data = await state.get_data()
    photo_ids = data.get("photo_ids", [])
    lang = data.get("language", "ru")
    
    if not photo_ids:
        error = "❌ " + ("Пожалуйста, загрузите хотя бы одно фото" if lang == "ru" else "Кем дегенде бір суретті жүктеңіз")
        await message.answer(error, reply_markup=kb_photo(lang))
        return
    
    # Сохраняем первое фото как основное (для совместимости)
    await state.update_data(photo_id=photo_ids[0])
    
    if data.get("editing"):
        await state.update_data(editing=False)
        await show_confirm(message, state)
        return
    
    await go_to_time(message, state)

@client_router.message(IsClient(), Order.sending_photo)
async def photo_invalid(message: Message, state: FSMContext):
    data = await state.get_data()
    lang = data.get("language", "ru")
    error = msg(lang, "photo_invalid")
    await message.answer(error, reply_markup=kb_photo(lang))

# ── Время ─────────────────────────────────────────────────
@client_router.message(IsClient(), Order.choosing_time, F.text.startswith("◀️"))
async def time_back(message: Message, state: FSMContext):
    data = await state.get_data()
    if data.get("editing"):
        await state.update_data(editing=False); await show_confirm(message, state); return
    await ask_photo(message, state)

@client_router.message(IsClient(), Order.choosing_time, F.text.in_({"⚡ Сейчас", "🕐 В течение часа"}) | F.text.startswith("⚡") | F.text.startswith("🕐"))
async def process_time_preset(message: Message, state: FSMContext):
    data = await state.get_data()
    lang = data.get("language", "ru")

    now = local_now()
    if not is_within_working_hours(now):
        await message.answer(msg(lang, "time_preset_unavailable"), reply_markup=kb_time(lang, allow_presets=False))
        return

    if message.text.startswith("⚡"):
        label = "Сейчас" if lang == "ru" else "Қазір"
    else:
        label = "В течение часа" if lang == "ru" else "Сағат ішінде"
    await state.update_data(order_time=label)
    data = await state.get_data()
    if data.get("editing"):
        await state.update_data(editing=False); await show_confirm(message, state); return
    await go_to_comment(message, state)

@client_router.message(IsClient(), Order.choosing_time, F.text.startswith("🕒"))
async def process_time_custom(message: Message, state: FSMContext):
    now = local_now()
    nearest_date = nearest_working_date(now).strftime("%d.%m.%Y")
    await state.set_state(Order.entering_time)
    data = await state.get_data()
    lang = data.get("language", "ru")
    prompt = msg(lang, "time_custom_prompt", nearest_date=nearest_date)
    await message.answer(prompt, reply_markup=kb_nav(lang))

@client_router.message(IsClient(), Order.choosing_time)
async def time_invalid(message: Message, state: FSMContext):
    data = await state.get_data()
    lang = data.get("language", "ru")
    error = msg(lang, "time_invalid")
    await message.answer(error)

@client_router.message(IsClient(), Order.entering_time, F.text.startswith("◀️"))
async def entering_time_back(message: Message, state: FSMContext):
    data = await state.get_data()
    if data.get("editing"):
        await state.update_data(editing=False); await show_confirm(message, state); return
    await go_to_time(message, state)

@client_router.message(IsClient(), Order.entering_time)
async def process_entering_time(message: Message, state: FSMContext):
    text = message.text.strip()
    try:
        t = datetime.strptime(text, "%H:%M")
        h = t.hour
        m = t.minute
    except ValueError:
        data = await state.get_data()
        lang = data.get("language", "ru")
        error = msg(lang, "time_format_error")
        await message.answer(error)
        return

    data = await state.get_data()
    lang = data.get("language", "ru")
    scheduled_dt, adjusted = resolve_custom_datetime(local_now(), h, m)

    out_time = scheduled_dt.strftime("%H:%M")
    out_date = scheduled_dt.strftime("%d.%m.%Y")
    await state.update_data(order_time=f"{out_time} ({out_date})")

    if adjusted:
        await message.answer(msg(lang, "time_custom_adjusted", time=out_time, date=out_date))

    data = await state.get_data()
    if data.get("editing"):
        await state.update_data(editing=False); await show_confirm(message, state); return
    await go_to_comment(message, state)

# ── Комментарий ───────────────────────────────────────────
@client_router.message(IsClient(), Order.asking_comment, F.text.startswith("◀️"))
async def comment_ask_back(message: Message, state: FSMContext):
    data = await state.get_data()
    if data.get("editing"):
        await state.update_data(editing=False); await show_confirm(message, state); return
    await go_to_time(message, state)

@client_router.message(IsClient(), Order.asking_comment, F.text.startswith("➡️"))
async def skip_comment(message: Message, state: FSMContext):
    await state.update_data(comment="")
    data = await state.get_data()
    if data.get("editing"):
        await state.update_data(editing=False); await show_confirm(message, state); return
    await show_confirm(message, state)

@client_router.message(IsClient(), Order.asking_comment, F.text.startswith("💬"))
async def ask_comment_text(message: Message, state: FSMContext):
    await state.set_state(Order.entering_comment)
    data = await state.get_data()
    lang = data.get("language", "ru")
    prompt = msg(lang, "comment_prompt")
    await message.answer(prompt, reply_markup=kb_nav(lang))

@client_router.message(IsClient(), Order.asking_comment)
async def comment_ask_invalid(message: Message, state: FSMContext):
    data = await state.get_data()
    lang = data.get("language", "ru")
    error = msg(lang, "comment_invalid")
    await message.answer(error)

@client_router.message(IsClient(), Order.entering_comment, F.text.startswith("◀️"))
async def comment_back(message: Message, state: FSMContext):
    data = await state.get_data()
    if data.get("editing"):
        await state.update_data(editing=False); await show_confirm(message, state); return
    await go_to_comment(message, state)

@client_router.message(IsClient(), Order.entering_comment)
async def process_comment(message: Message, state: FSMContext):
    await state.update_data(comment=message.text.strip())
    data = await state.get_data()
    if data.get("editing"):
        await state.update_data(editing=False); await show_confirm(message, state); return
    await show_confirm(message, state)

# ── Подтверждение заявки ──────────────────────────────────
@client_router.message(IsClient(), Order.confirming, F.text.startswith("✅"))
async def confirm_order(message: Message, state: FSMContext, bot: Bot):
    data        = await state.get_data()
    order_id    = data["order_id"]
    trash       = data.get("trash_type", "")
    lang        = data.get("language", "ru")
    # FIX: используем or 0 чтобы избежать ошибки если bags == None
    bags        = data.get("bags") or 0
    needs_price = trash in NEEDS_PRICE or (trash == "🏠 Бытовой" and bags > 10)

    await save_order(
        order_id=order_id, user_id=message.from_user.id, data=data,
        status="pending" if needs_price else "waiting_worker",
        needs_price=needs_price,
    )

    if needs_price:
        await state.set_state(Order.waiting_price)
        msg_text = msg(lang, "order_waiting")
        await message.answer(
            msg_text,
            reply_markup=ReplyKeyboardRemove(),
        )
    else:
        await state.set_state(Order.waiting_worker)
        photo_id = data.get("photo_id")
        status_msg = msg(lang, "order_confirmed")
        text = order_summary(data) + f"\n\n{status_msg}"
        if photo_id:
            await message.answer_photo(photo=photo_id, caption=text, reply_markup=ReplyKeyboardRemove())
        else:
            await message.answer(text, reply_markup=ReplyKeyboardRemove())

    await send_order_to_worker(bot, data, order_id, needs_price)

@client_router.message(IsClient(), Order.confirming, F.text.startswith("✏️"))
async def edit_order(message: Message, state: FSMContext):
    await state.set_state(Order.editing)
    data = await state.get_data()
    lang = data.get("language", "ru")
    prompt = "✏️ " + ("Что именно хотите изменить?" if lang == "ru" else "Нақты түзету қалайсыз?")
    await message.answer(prompt, reply_markup=kb_edit(data, lang))

# ── Клиент принимает цену ─────────────────────────────────
@client_router.message(IsClient(), Order.price_confirm, F.text.startswith("✅"))
async def client_accept_price(message: Message, state: FSMContext, bot: Bot):
    data     = await state.get_data()
    order_id = data.get("order_id")
    lang = data.get("language", "ru")
    order = await fetch_order(order_id) if order_id else None
    if not order or order["status"] != "price_sent":
        lang = data.get("language", "ru")  # Сохраняем язык перед очисткой
        await state.clear()
        await state.update_data(language=lang)  # Восстанавливаем язык
        await state.set_state(Order.choosing_service)
        
        welcome = msg(lang, "welcome")
        welcome += msg(lang, "change_lang")
        await message.answer(welcome, reply_markup=kb_main(lang))
        return

    await update_order_status(order_id, "waiting_worker")
    await state.set_state(Order.waiting_worker)

    order_data = order["data"]
    lang = order_data.get("language", lang)
    photo_id   = order_data.get("photo_id")
    status_msg = msg(lang, "order_confirmed")
    text = order_summary(order_data, include_worker_price=True) + f"\n\n{status_msg}"
    if photo_id:
        await message.answer_photo(photo=photo_id, caption=text, reply_markup=ReplyKeyboardRemove())
    else:
        await message.answer(text, reply_markup=ReplyKeyboardRemove())

    if lang == "ru":
        final_text = f"✅ <b>Клиент подтвердил заказ!</b>\n\n{order_summary(order_data, include_worker_price=True, lang='ru')}"
    else:
        final_text = f"✅ <b>Клиент өтінімді растады!</b>\n\n{order_summary(order_data, include_worker_price=True, lang='kk')}"
    if photo_id:
        await bot.send_photo(WORKER_ID, photo=photo_id, caption=final_text, reply_markup=ikb_worker_status(order_id))
    else:
        await bot.send_message(WORKER_ID, final_text, reply_markup=ikb_worker_status(order_id))

# ── Редактирование ────────────────────────────────────────
@client_router.message(IsClient(), Order.editing, F.text.startswith("◀️"))
async def edit_back(message: Message, state: FSMContext):
    await show_confirm(message, state)

@client_router.message(IsClient(), Order.editing, F.text.in_({BTN["ru"]["edit_block"], BTN["kk"]["edit_block"]}))
async def edit_block(message: Message, state: FSMContext):
    lang = (await state.get_data()).get("language", "ru")
    await state.update_data(editing=True); await state.set_state(Order.choosing_block)
    await message.answer(msg(lang, "block_choice"), reply_markup=kb_blocks(lang))

@client_router.message(IsClient(), Order.editing, F.text.in_({BTN["ru"]["edit_floor"], BTN["kk"]["edit_floor"]}))
async def edit_floor(message: Message, state: FSMContext):
    lang = (await state.get_data()).get("language", "ru")
    await state.update_data(editing=True); await state.set_state(Order.entering_floor)
    await message.answer(msg(lang, "floor_prompt"), reply_markup=kb_nav(lang))

@client_router.message(IsClient(), Order.editing, F.text.in_({BTN["ru"]["edit_apt"], BTN["kk"]["edit_apt"]}))
async def edit_apt(message: Message, state: FSMContext):
    lang = (await state.get_data()).get("language", "ru")
    await state.update_data(editing=True); await state.set_state(Order.entering_apt)
    await message.answer(msg(lang, "apt_prompt"), reply_markup=kb_nav(lang))

@client_router.message(IsClient(), Order.editing, F.text.in_({BTN["ru"]["edit_trash"], BTN["kk"]["edit_trash"]}))
async def edit_trash(message: Message, state: FSMContext):
    lang = (await state.get_data()).get("language", "ru")
    await state.update_data(editing=True); await state.set_state(Order.choosing_trash)
    await message.answer(msg(lang, "trash_choice"), reply_markup=kb_trash(lang))

@client_router.message(IsClient(), Order.editing, F.text.in_({BTN["ru"]["edit_bags"], BTN["kk"]["edit_bags"]}))
async def edit_bags(message: Message, state: FSMContext):
    lang = (await state.get_data()).get("language", "ru")
    await state.update_data(editing=True); await state.set_state(Order.entering_bags)
    await message.answer(msg(lang, "bags_prompt"), reply_markup=kb_nav(lang))

@client_router.message(IsClient(), Order.editing, F.text.startswith("🔄 Изменить фото"))
async def edit_photo(message: Message, state: FSMContext):
    await state.update_data(editing=True, photo_id=None)
    await ask_photo(message, state)

@client_router.message(IsClient(), Order.editing, F.text.in_({BTN["ru"]["edit_time"], BTN["kk"]["edit_time"]}))
async def edit_time(message: Message, state: FSMContext):
    lang = (await state.get_data()).get("language", "ru")
    await state.update_data(editing=True); await state.set_state(Order.choosing_time)
    allow_presets = is_within_working_hours(local_now())
    await message.answer(msg(lang, "time_choice"), reply_markup=kb_time(lang, allow_presets=allow_presets))

@client_router.message(IsClient(), Order.editing, F.text.in_({BTN["ru"]["edit_comment"], BTN["kk"]["edit_comment"]}))
async def edit_comment(message: Message, state: FSMContext):
    lang = (await state.get_data()).get("language", "ru")
    await state.update_data(editing=True); await state.set_state(Order.asking_comment)
    await message.answer(msg(lang, "comment_choice"), reply_markup=kb_comment(lang))

# ══════════════════════════════════ ЗАПУСК ═════════════════
async def main():
    if not TOKEN:
        raise RuntimeError("BOT_TOKEN не задан. Добавьте его в .env или переменные окружения.")

    await init_redis()
    await init_db_pool()
    try:
        await init_db()
        await migrate_db()
        active_count = await count_active_orders()
        logger.info("Активных заявок в БД: %d", active_count)

        bot     = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
        dp      = Dispatcher(storage=storage)

        dp.include_router(worker_router)
        dp.include_router(client_router)

        logger.info("Бот запущен...")
        await dp.start_polling(bot)
    finally:
        await close_db_pool()
        await close_redis()

if __name__ == "__main__":
    asyncio.run(main())