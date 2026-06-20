import asyncio
import json
import logging
import os
import random
import string
from html import escape
from datetime import datetime
from pathlib import Path

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
)

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

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
    async with db_pool.acquire() as conn:
        col_type = await conn.fetchval(
            """
            SELECT data_type FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'orders' AND column_name = 'data_json'
            """
        )
        if col_type == "text":
            await conn.execute(
                "ALTER TABLE orders ALTER COLUMN data_json TYPE JSONB USING data_json::jsonb"
            )

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
            price, data, "price_sent",
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
    date = datetime.now().strftime("%d%m%y")
    rand = "".join(random.choices(string.ascii_uppercase + string.digits, k=4))
    return f"ЖК-{date}-{rand}"

def get_price(bags: int) -> str:
    if bags <= 3:    return "500 ₸"
    elif bags <= 6:  return "1 000 ₸"
    elif bags <= 10: return "1 500 ₸"
    else:            return "по оценке сотрудника"

# ═══════════════════════════════════════════ FSM ═══════════
class Worker(StatesGroup):
    ready          = State()
    entering_price = State()

class Order(StatesGroup):
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
def kb_main():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🗑 Вынести мусор / 🗑 Қоқысты шығару"), KeyboardButton(text="🆘 Помощь / 🆘 Көмек")],
    ], resize_keyboard=True)

def kb_nav():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="◀️ Назад / ◀️ Артқа"), KeyboardButton(text="❌ Отменить заявку / ❌ Өтінімді бас тарту")],
    ], resize_keyboard=True)

def kb_blocks():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="Блок 1"), KeyboardButton(text="Блок 2"), KeyboardButton(text="Блок 3")],
        [KeyboardButton(text="Блок 4"), KeyboardButton(text="Блок 5"), KeyboardButton(text="Блок 6")],
        [KeyboardButton(text="◀️ Назад / ◀️ Артқа"), KeyboardButton(text="❌ Отменить заявку / ❌ Өтінімді бас тарту")],
    ], resize_keyboard=True)

def kb_trash():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🏠 Бытовой / 🏠 Тұрмыстық")],
        [KeyboardButton(text="🧱 Строительный / 🧱 Құрылыс"), KeyboardButton(text="📦 Крупногабаритный / 📦 Ірі мөлшерлі")],
        [KeyboardButton(text="◀️ Назад / ◀️ Артқа"), KeyboardButton(text="❌ Отменить заявку / ❌ Өтінімді бас тарту")],
    ], resize_keyboard=True)

def kb_time():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="⚡ Сейчас / ⚡ Қазір"), KeyboardButton(text="🕐 В течение часа / 🕐 Сағат ішінде")],
        [KeyboardButton(text="🕒 Указать время / 🕒 Уақытты көрсету")],
        [KeyboardButton(text="◀️ Назад / ◀️ Артқа"), KeyboardButton(text="❌ Отменить заявку / ❌ Өтінімді бас тарту")],
    ], resize_keyboard=True)

def kb_comment():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="💬 Добавить комментарий / 💬 Түсіндіру қосу"), KeyboardButton(text="➡️ Без комментария / ➡️ Түсіндірісіз")],
        [KeyboardButton(text="◀️ Назад / ◀️ Артқа"), KeyboardButton(text="❌ Отменить заявку / ❌ Өтінімді бас тарту")],
    ], resize_keyboard=True)

def kb_confirm():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="✅ Подтвердить / ✅ Растау")],
        [KeyboardButton(text="✏️ Изменить / ✏️ Өзгерту"), KeyboardButton(text="❌ Отменить заявку / ❌ Өтінімді бас тарту")],
    ], resize_keyboard=True)

def kb_price_confirm():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="✅ Принять цену / ✅ Бағасын қабылдау")],
        [KeyboardButton(text="❌ Отменить заявку / ❌ Өтінімді бас тарту")],
    ], resize_keyboard=True)

def kb_review():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="⏭ Пропустить отзыв / ⏭ Өндіктемені өткізіп жіберу")],
    ], resize_keyboard=True)

def kb_edit(data: dict):
    rows = [
        [KeyboardButton(text="🔄 Изменить блок")],
        [KeyboardButton(text="🔄 Изменить этаж")],
        [KeyboardButton(text="🔄 Изменить квартиру")],
        [KeyboardButton(text="🔄 Изменить тип мусора")],
    ]
    if data.get("trash_type") == "🏠 Бытовой":
        rows.append([KeyboardButton(text="🔄 Изменить кол-во пакетов")])
    rows.append([KeyboardButton(text="🔄 Изменить фото")])
    rows.append([KeyboardButton(text="🔄 Изменить время")])
    rows.append([KeyboardButton(text="🔄 Изменить комментарий")])
    rows.append([KeyboardButton(text="◀️ Назад"), KeyboardButton(text="❌ Отменить заявку")])
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
TRASH_TYPES = {"🏠 Бытовой", "🧱 Строительный", "📦 Крупногабаритный"}
BLOCKS      = {"Блок 1", "Блок 2", "Блок 3", "Блок 4", "Блок 5", "Блок 6"}
NEEDS_PRICE = {"🧱 Строительный", "📦 Крупногабаритный"}

def order_summary(data: dict, include_worker_price: bool = False) -> str:
    trash        = data.get("trash_type", "—")
    bags         = data.get("bags")
    price        = data.get("price", "")
    comment      = data.get("comment", "")
    time_str     = data.get("order_time", "—")
    worker_price = data.get("worker_price", "")

    bags_line   = f"\n📦 Кол-во пакетов: {bags}" if bags else ""
    price_line  = f"\n💰 Стоимость:      {price}" if price and price != "по оценке сотрудника" else ""
    wprice_line = f"\n💰 Цена:           {worker_price} ₸" if (include_worker_price and worker_price) else ""
    comm_line   = f"\n💬 Комментарий:    {comment}" if comment else ""

    return (
        f"📋 <b>Заявка <code>{data.get('order_id', '—')}</code></b>\n\n"
        f"📦 Услуга:     {data.get('service', '—')}\n"
        f"🗑 Тип мусора: {trash}"
        f"{bags_line}"
        f"{price_line}"
        f"{wprice_line}\n"
        f"🏢 Блок:       {data.get('block', '—')}\n"
        f"🪜 Этаж:       {data.get('floor', '—')}\n"
        f"🚪 Квартира:   {data.get('apt', '—')}\n"
        f"⏰ Время:      {time_str}"
        f"{comm_line}"
    )

async def show_confirm(message: Message, state: FSMContext):
    await state.set_state(Order.confirming)
    data = await state.get_data()
    photo_id = data.get("photo_id")
    text = order_summary(data) + "\n\n<b>Всё верно?</b>\n\n─────────────────────\n\n<b>Барлық дұрыс па?</b>"
    if photo_id:
        await message.answer_photo(photo=photo_id, caption=text, reply_markup=kb_confirm())
    else:
        await message.answer(text, reply_markup=kb_confirm())

async def go_to_comment(message: Message, state: FSMContext):
    await state.set_state(Order.asking_comment)
    await message.answer(
        "💬 Хотите добавить комментарий к заявке?\n\n"
        "─────────────────────\n\n"
        "💬 Өтіністіге пікір қосқыңыз келе ме?",
        reply_markup=kb_comment()
    )

async def go_to_time(message: Message, state: FSMContext):
    await state.set_state(Order.choosing_time)
    await message.answer(
        "⏰ Когда вы хотите принять заказ?\n\n"
        "─────────────────────\n\n"
        "⏰ Өтіністі қашан қабылдағыңыз келеді?",
        reply_markup=kb_time()
    )

async def ask_photo(message: Message, state: FSMContext):
    data = await state.get_data()
    trash = data.get("trash_type", "")
    await state.set_state(Order.sending_photo)
    if trash == "🏠 Бытовой" or "Бытовой" in str(trash):
        bags  = data.get("bags") or 0
        price = get_price(bags)
        await state.update_data(price=price)
        if bags > 10:
            note_ru = "💰 Стоимость рассчитывается сотрудником по фото."
            note_kk = "💰 Баланы фото арқылы қызметкер есептейді."
        else:
            note_ru = f"💰 Стоимость вывоза: <b>{price}</b>"
            note_kk = f"💰 Шығару құны: <b>{price}</b>"
        
        await message.answer(
            f"{note_ru}\n\n"
            f"📸 Пришлите фото мусора\n\n"
            f"─────────────────────\n\n"
            f"{note_kk}\n\n"
            f"📸 Қоқыстың суретін жібер:",
            reply_markup=kb_nav()
        )
    else:
        await message.answer(
            f"📸 Для <b>{trash}</b> цена рассчитывается сотрудником.\n\n"
            f"Пришлите фото мусора\n\n"
            f"─────────────────────\n\n"
            f"📸 <b>{trash}</b> үшін баланы қызметкер есептейді.\n\n"
            f"Қоқыстың суретін жібер:",
            reply_markup=kb_nav(),
        )

async def send_order_to_worker(bot: Bot, data: dict, order_id: str, needs_price: bool):
    photo_id = data.get("photo_id")
    if needs_price:
        text = (
            f"📬 <b>Новая заявка!</b>\n\n{order_summary(data)}\n\n"
            f"💰 <b>Укажите цену для клиента:</b>\n\n"
            f"─────────────────────\n\n"
            f"📬 <b>Жаңа өтіністі!</b>\n\n{order_summary(data)}\n\n"
            f"💰 <b>Клиентке баланы қойыңыз:</b>"
        )
    else:
        text = (
            f"📬 <b>Новая заявка!</b>\n\n{order_summary(data)}\n\n"
            f"ℹ️ Цена фиксированная. Можете приступать!\n\n"
            f"─────────────────────\n\n"
            f"📬 <b>Жаңа өтіністі!</b>\n\n{order_summary(data)}\n\n"
            f"ℹ️ Баланы бекітіліген. Басталуға болады!"
        )
    
    kb = ikb_worker_new(order_id) if needs_price else ikb_worker_status(order_id)
    if photo_id:
        msg = await bot.send_photo(WORKER_ID, photo=photo_id, caption=text, reply_markup=kb)
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
    photo_id = order_data.get("photo_id")
    summary    = order_summary(order_data, include_worker_price=True)
    client_text = (
        f"💰 <b>Работник оценил вашу заявку!</b>\n\n{summary}\n\n"
        f"Подтверждаете заказ?\n\n"
        f"─────────────────────\n\n"
        f"💰 <b>Қызметкер сіздің өтіністіңізді бағалады!</b>\n\n{summary}\n\n"
        f"Өтіністі растайсыз ба?"
    )

    if photo_id:
        await bot.send_photo(user_id, photo=photo_id, caption=client_text, reply_markup=kb_price_confirm())
    else:
        await bot.send_message(user_id, client_text, reply_markup=kb_price_confirm())

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

    if status == "on_way":
        if order["status"] != "waiting_worker":
            await callback.answer("❌ Заявка уже в другом статусе.", show_alert=True)
            return
        await update_order_status(order_id, "on_way")
        await callback.message.edit_reply_markup(reply_markup=ikb_worker_on_way(order_id))
        await bot.send_message(user_id, 
            f"🚗 <b>Статус заявки <code>{order_id}</code>: Работник едет к вам!</b>\n\n"
            f"─────────────────────\n\n"
            f"🚗 <b>Өтіністің статусы <code>{order_id}</code>: Қызметкер сізге баратын жолда!</b>"
        )
        await callback.answer("Статус: В пути")

    elif status == "done":
        if order["status"] not in ("waiting_worker", "on_way"):
            await callback.answer("❌ Заявка уже закрыта.", show_alert=True)
            return
        await update_order_status(order_id, "done")
        await callback.message.edit_reply_markup(reply_markup=None)
        await callback.message.answer(f"✅ Заявка <code>{order_id}</code> выполнена и закрыта.")
        await bot.send_message(
            user_id,
            f"✅ <b>Статус заявки <code>{order_id}</code>: Выполнено!</b>\n\n"
            "Спасибо, что воспользовались нашим сервисом!\n"
            "Напишите отзыв о выполненной работе или нажмите кнопку ниже, чтобы пропустить.\n\n"
            "─────────────────────\n\n"
            f"✅ <b>Өтіністің статусы <code>{order_id}</code>: Орындалды!</b>\n\n"
            "Біздің қызметті пайдалағаныңыз үшін рахмет!\n"
            "Орындалған жұмыс туралы пікіріңізді жазыңыз немесе өткізу үшін төмендегі түймені басыңыз.",
            reply_markup=kb_review(),
        )
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
    await state.set_state(Order.choosing_service)
    await message.answer(
        "👋 Добро пожаловать в сервис вашего жилого комплекса!\n\n"
        "⏰ <b>Обратите внимание:</b> услуги выполняются с 09:00 до 18:00.\n\n"
        "Выберите нужную услугу 👇\n\n"
        "─────────────────────\n\n"
        "👋 Өз пәтерінің қызметіне қош келдіңіз!\n\n"
        "⏰ <b>Ескертпе:</b> қызметтері сағат 09:00-ден 18:00-ға дейін.\n\n"
        "Қажетті қызметті таңдаңыз 👇",
        reply_markup=kb_main(),
    )

@client_router.message(IsClient(), F.text.startswith("🆘"))
async def help_handler(message: Message):
    await message.answer(
        "<b>Служба поддержки ЖК</b>\n\n"
        "Телефон: +7 (XXX) XXX-XX-XX\n"
        "Время работы: 09:00 — 18:00\n\n"
        "─────────────────────\n\n"
        "<b>ЖК Қолдау Қызметі</b>\n\n"
        "Телефон: +7 (XXX) XXX-XX-XX\n"
        "Жұмыс уақыты: 09:00 — 18:00"
    )

@client_router.message(IsClient(), F.text.startswith("❌"))
async def cancel_handler(message: Message, state: FSMContext, bot: Bot):
    # Читаем данные ДО очистки состояния
    data          = await state.get_data()
    current_state = await state.get_state()
    order_id  = data.get("order_id")

    if current_state == Order.price_confirm.state and order_id:
        await update_order_status(order_id, "price_declined")
        await bot.send_message(WORKER_ID, f"❌ Клиент отказался от заявки / ❌ Клиент өтіністі бас тартты <code>{order_id}</code> көрсетілген баланың кейін.")
    elif order_id:
        await update_order_status(order_id, "canceled")

    await state.clear()
    await state.set_state(Order.choosing_service)
    await message.answer(
        "❌ Заявка отменена. Возвращаемся в главное меню. / ❌ Өтіністі бас тартылды. Басты мәзірге орал.",
        reply_markup=kb_main(),
    )

# ── Отзыв ─────────────────────────────────────────────────
@client_router.message(IsClient(), Order.leaving_review, F.text == "⏭ Пропустить отзыв")
async def skip_review(message: Message, state: FSMContext):
    data = await state.get_data()
    order_id = data.get("review_order")
    await save_review(order_id, message.from_user.id, skipped=True)
    await state.clear()
    await state.set_state(Order.choosing_service)
    await message.answer("Спасибо! Возвращаемся в главное меню. / Рахмет! Басты мәзірге орал.", reply_markup=kb_main())

@client_router.message(IsClient(), Order.leaving_review, F.text)
async def process_review(message: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    order_id = data.get("review_order")
    review = message.text.strip()
    if not review:
        await message.answer("Напишите отзыв текстом или нажмите «⏭ Пропустить отзыв». / Пікіріңізді мәтін түрінде жазыңыз немесе «⏭ Отзывды өткізіңіз» басыңыз.", reply_markup=kb_review())
        return
    await bot.send_message(WORKER_ID, f"💬 <b>Новый отзыв по заявке / 💬 <b>Өтіністі бойынша жаңа пікір <code>{order_id}</code></b>\n\n{escape(review)}")
    await save_review(order_id, message.from_user.id, review_text=review)
    await state.clear()
    await state.set_state(Order.choosing_service)
    await message.answer("Спасибо за отзыв! Возвращаемся в главное меню. / Пікіріңіз үшін рахмет! Басты мәзірге орал.", reply_markup=kb_main())

@client_router.message(IsClient(), Order.leaving_review)
async def review_invalid(message: Message):
    await message.answer("Напишите отзыв текстом или нажмите «⏭ Пропустить отзыв». / Пікіріңізді мәтін түрінде жазыңыз немесе «⏭ Отзывды өткізіңіз» басыңыз.", reply_markup=kb_review())

# ── Услуга ────────────────────────────────────────────────
@client_router.message(IsClient(), Order.choosing_service, F.text.startswith("🗑"))
async def garbage_service(message: Message, state: FSMContext):
    await state.update_data(service="Вынос мусора", order_id=generate_order_id())
    await state.set_state(Order.choosing_block)
    await message.answer("🏢 Укажите номер вашего блока / 🏢 Блок номеріңізді көрсетіңіз:", reply_markup=kb_blocks())

# ── Блок ──────────────────────────────────────────────────
@client_router.message(IsClient(), Order.choosing_block, F.text.startswith("◀️"))
async def block_back(message: Message, state: FSMContext):
    await state.set_state(Order.choosing_service)
    await message.answer("Выберите услугу / Қызметті таңдаңыз:", reply_markup=kb_main())

@client_router.message(IsClient(), Order.choosing_block, F.text.in_(BLOCKS))
async def process_block(message: Message, state: FSMContext):
    await state.update_data(block=message.text)
    data = await state.get_data()
    if data.get("editing"):
        await state.update_data(editing=False); await show_confirm(message, state); return
    await state.set_state(Order.entering_floor)
    await message.answer("🪜 Введите номер этажа (1–30):", reply_markup=kb_nav())

@client_router.message(IsClient(), Order.choosing_block)
async def block_invalid(message: Message):
    await message.answer("⚠️ Выберите блок с помощью кнопок выше. / ⚠️ Жоғарыдағы түймелер арқылы блокты таңдаңыз.")

# ── Этаж ──────────────────────────────────────────────────
@client_router.message(IsClient(), Order.entering_floor, F.text.startswith("◀️"))
async def floor_back(message: Message, state: FSMContext):
    data = await state.get_data()
    if data.get("editing"):
        await state.update_data(editing=False); await show_confirm(message, state); return
    await state.set_state(Order.choosing_block)
    await message.answer("🏢 Укажите номер блока / 🏢 Блок номеріңізді көрсетіңіз:", reply_markup=kb_blocks())

@client_router.message(IsClient(), Order.entering_floor)
async def process_floor(message: Message, state: FSMContext):
    text = message.text.strip()
    if not text.isdigit() or not (1 <= int(text) <= 30):
        await message.answer("🚫 Введите число от 1 до 30: / 🚫 1-ден 30-ға дейін сан енгізіңіз:"); return
    await state.update_data(floor=int(text))
    data = await state.get_data()
    if data.get("editing"):
        await state.update_data(editing=False); await show_confirm(message, state); return
    await state.set_state(Order.entering_apt)
    await message.answer("🚪 Введите номер квартиры / 🚪 Пәтер нөмерін енгізіңіз:", reply_markup=kb_nav())

# ── Квартира ──────────────────────────────────────────────
@client_router.message(IsClient(), Order.entering_apt, F.text.startswith("◀️"))
async def apt_back(message: Message, state: FSMContext):
    data = await state.get_data()
    if data.get("editing"):
        await state.update_data(editing=False); await show_confirm(message, state); return
    await state.set_state(Order.entering_floor)
    await message.answer("🪜 Введите номер этажа (1–30) / 🪜 Қабат нөмерін (1–30) енгізіңіз:", reply_markup=kb_nav())

@client_router.message(IsClient(), Order.entering_apt)
async def process_apt(message: Message, state: FSMContext):
    text = message.text.strip().lower()

    import re

    # Разрешаем: 68, 68а, 68б и т.д.
    if not re.fullmatch(r"\d+[а-яa-z]?", text):
        await message.answer(
            "🚫 Введите корректный номер квартиры (например: 68 или 68А): / 🚫 Дұрыс пәтер нөмерін енгізіңіз (мысалы: 68 немесе 68А):"
        )
        return

    num = int(re.match(r"\d+", text).group())

    if num > 153:
        await message.answer(
            "🚫 Такой квартиры нет. Введите номер квартиры от 1 до 153: / 🚫 Мындай пәтер жоқ. Пәтер нөмерін 1-ден 153-ке дейін енгізіңіз:"
        )
        return

    await state.update_data(apt=text.upper())

    data = await state.get_data()
    if data.get("editing"):
        await state.update_data(editing=False)
        await show_confirm(message, state)
        return

    await state.set_state(Order.choosing_trash)
    await message.answer("🗑 Выберите тип мусора:", reply_markup=kb_trash())

# ── Тип мусора ────────────────────────────────────────────
@client_router.message(IsClient(), Order.choosing_trash, F.text.startswith("◀️"))
async def trash_back(message: Message, state: FSMContext):
    data = await state.get_data()
    if data.get("editing"):
        await state.update_data(editing=False); await show_confirm(message, state); return
    await state.set_state(Order.entering_apt)
    await message.answer("🚪 Введите номер квартиры / 🚪 Квартира нөмеріңізді енгізіңіз:", reply_markup=kb_nav())

@client_router.message(IsClient(), Order.choosing_trash, F.text.in_(TRASH_TYPES) | F.text.startswith("🏠 Бытовой") | F.text.startswith("🌳 Крупногаб") | F.text.startswith("♻️ Вторсырье") | F.text.startswith("🪟 Стеклопак"))
async def process_trash(message: Message, state: FSMContext):
    trash = message.text.split("/")[0].strip()
    await state.update_data(trash_type=trash, bags=None, price=None)
    data = await state.get_data()
    editing = data.get("editing")
    if "Бытовой" in trash or "Тұрмыстық" in trash:
        if editing: await state.update_data(editing=False)
        await state.set_state(Order.entering_bags)
        await message.answer(
            "📦 Сколько мусорных пакетов (30–60 л)? / 📦 Қоқыс сәлінеде қанша (30–60 л)?\n\n"
            "💰 <b>Тарифы / Тарифтар:</b>\n• до 3 пакетов — 500 ₸ / 3 сәліне дейін — 500 ₸\n"
            "• до 6 — 1 000 ₸ / 6 дейін — 1 000 ₸\n• до 10 — 1 500 ₸ / 10 дейін — 1 500 ₸\n"
            "• более 10 — по оценке сотрудника / 10-дан артық — қызметкердің бағалауы бойынша\n\nВведите количество / Саны енгізіңіз:",
            reply_markup=kb_nav(),
        )
    else:
        if editing: await state.update_data(editing=False, photo_id=None)
        await ask_photo(message, state)

@client_router.message(IsClient(), Order.choosing_trash)
async def trash_invalid(message: Message):
    await message.answer("⚠️ Выберите тип мусора с помощью кнопок выше. / ⚠️ Жоғарыдағы түймелер арқылы қоқыс түрін таңдаңыз.")

# ── Кол-во пакетов ────────────────────────────────────────
@client_router.message(IsClient(), Order.entering_bags, F.text.startswith("◀️"))
async def bags_back(message: Message, state: FSMContext):
    data = await state.get_data()
    if data.get("editing"):
        await state.update_data(editing=False); await show_confirm(message, state); return
    await state.set_state(Order.choosing_trash)
    await message.answer("🗑 Выберите тип мусора / 🗑 Қоқыс түрін таңдаңыз:", reply_markup=kb_trash())

@client_router.message(IsClient(), Order.entering_bags)
async def process_bags(message: Message, state: FSMContext):
    text = message.text.strip()
    if not text.isdigit() or int(text) <= 0:
        await message.answer("🚫 Введите корректное количество пакетов: / 🚫 Дұрыс сәліне саны енгізіңіз:"); return
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
    if data.get("trash_type") == "🏠 Бытовой" or "Бытовой" in str(data.get("trash_type")):
        await state.set_state(Order.entering_bags)
        await message.answer("📦 Введите количество пакетов / 📦 Сәліне саны енгізіңіз:", reply_markup=kb_nav())
    else:
        await state.set_state(Order.choosing_trash)
        await message.answer("🗑 Выберите тип мусора / 🗑 Қоқыс түрін таңдаңыз:", reply_markup=kb_trash())

@client_router.message(IsClient(), Order.sending_photo, F.photo)
async def process_photo(message: Message, state: FSMContext):
    await state.update_data(photo_id=message.photo[-1].file_id)
    data = await state.get_data()
    if data.get("editing"):
        await state.update_data(editing=False); await show_confirm(message, state); return
    await go_to_time(message, state)

@client_router.message(IsClient(), Order.sending_photo)
async def photo_invalid(message: Message):
    await message.answer("📸 Пожалуйста, отправьте <b>фотографию</b> мусора. / 📸 Өтінегі <b>қоқыстың суретін</b> жібер.ink.")

# ── Время ─────────────────────────────────────────────────
@client_router.message(IsClient(), Order.choosing_time, F.text.startswith("◀️"))
async def time_back(message: Message, state: FSMContext):
    data = await state.get_data()
    if data.get("editing"):
        await state.update_data(editing=False); await show_confirm(message, state); return
    await ask_photo(message, state)

@client_router.message(IsClient(), Order.choosing_time, F.text.in_({"⚡ Сейчас", "🕐 В течение часа"}) | F.text.startswith("⚡") | F.text.startswith("🕐"))
async def process_time_preset(message: Message, state: FSMContext):
    label = "Сейчас" if "Сейчас" in message.text else "В течение часа"
    await state.update_data(order_time=label)
    data = await state.get_data()
    if data.get("editing"):
        await state.update_data(editing=False); await show_confirm(message, state); return
    await go_to_comment(message, state)

@client_router.message(IsClient(), Order.choosing_time, F.text.startswith("🕒"))
async def process_time_custom(message: Message, state: FSMContext):
    today = datetime.now().strftime("%d.%m.%Y")
    await state.set_state(Order.entering_time)
    await message.answer(
        f"⚠️ Заявку можно оставить только на <b>сегодня ({today})</b>. / ⚠️ Өтіністі тек <b>бүгінге ({today})</b> ғана қалдыруға болады.\\n\\n"
        "Введите время в формате <b>ЧЧ:ММ</b> (например, 14:30). / Уақытты <b>СС:ММ</b> форматында енгізіңіз (мысалы, 14:30).\\n"
        "Доступное время: 09:00 – 18:00. / Қолжетімді уақыт: 09:00 – 18:00.", reply_markup=kb_nav(),
    )

@client_router.message(IsClient(), Order.choosing_time)
async def time_invalid(message: Message):
    await message.answer("⚠️ Выберите вариант с помощью кнопок выше. / ⚠️ Жоғарыдағы түймелер арқылы опцияны таңдаңыз.")

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
    except ValueError:
        await message.answer("🚫 Неверный формат. Введите как <b>ЧЧ:ММ</b>, например 14:30 / 🚫 Бұл формат дұрыс емес. <b>СС:ММ</b> форматында енгізіңіз, мысалы 14:30:"); return
    if not (9 <= h < 18):
        await message.answer("🚫 Время должно быть в диапазоне <b>09:00 – 18:00</b> / 🚫 Уақыт <b>09:00 – 18:00</b> аралығында болуы керек:"); return
    today = datetime.now().strftime("%d.%m.%Y")
    await state.update_data(order_time=f"{text} ({today})")
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
    await message.answer("✏️ Напишите ваш комментарий / ✏️ Өз пікіріңізді жазыңыз:", reply_markup=kb_nav())

@client_router.message(IsClient(), Order.asking_comment)
async def comment_ask_invalid(message: Message):
    await message.answer("⚠️ Выберите вариант с помощью кнопок выше. / ⚠️ Жоғарыдағы түймелер арқылы опцияны таңдаңыз.")

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
        await message.answer(
            "⏳ <b>Заявка отправлена работнику!</b>\n\n"
            "Ожидайте — работник оценит объём и пришлёт вам цену.\n\n"
            "─────────────────────\n\n"
            "⏳ <b>Өтіністі қызметкерге жіберді!</b>\n\n"
            "Күтіңіз — қызметкер көлемді бағалап, сізге баланы жіберді.",
            reply_markup=ReplyKeyboardRemove(),
        )
    else:
        await state.set_state(Order.waiting_worker)
        photo_id = data.get("photo_id")
        text = order_summary(data) + "\n\n⏳ <b>Статус: В ожидании / Статус: Күтілуде</b>\n\nСотрудник скоро придёт к вам! / Қызметкер сізге тез келеді!"
        if photo_id:
            await message.answer_photo(photo=photo_id, caption=text, reply_markup=ReplyKeyboardRemove())
        else:
            await message.answer(text, reply_markup=ReplyKeyboardRemove())

    await send_order_to_worker(bot, data, order_id, needs_price)

@client_router.message(IsClient(), Order.confirming, F.text.startswith("✏️"))
async def edit_order(message: Message, state: FSMContext):
    await state.set_state(Order.editing)
    data = await state.get_data()
    await message.answer("✏️ Что именно хотите изменить? / ✏️ Нақты түзету қалайсыз?", reply_markup=kb_edit(data))

# ── Клиент принимает цену ─────────────────────────────────
@client_router.message(IsClient(), Order.price_confirm, F.text.startswith("✅"))
async def client_accept_price(message: Message, state: FSMContext, bot: Bot):
    data     = await state.get_data()
    order_id = data.get("order_id")
    order = await fetch_order(order_id) if order_id else None
    if not order or order["status"] != "price_sent":
        await state.clear()
        await state.set_state(Order.choosing_service)
        await message.answer("❌ Заявка не найдена или уже закрыта. Возвращаемся в главное меню. / ❌ Өтіністі таба алмадым немесе ол жабылды. Басты мәзірге орал.", reply_markup=kb_main())
        return

    await update_order_status(order_id, "waiting_worker")
    await state.set_state(Order.waiting_worker)

    order_data = order["data"]
    photo_id   = order_data.get("photo_id")
    text = order_summary(order_data, include_worker_price=True) + "\n\n⏳ <b>Статус: В ожидании / Статус: Күтілуде</b>\n\nСотрудник скоро придёт к вам! / Қызметкер сізге тез келеді!"
    if photo_id:
        await message.answer_photo(photo=photo_id, caption=text, reply_markup=ReplyKeyboardRemove())
    else:
        await message.answer(text, reply_markup=ReplyKeyboardRemove())

    final_text = f"✅ <b>Клиент подтвердил заказ! / ✅ <b>Клиент біліктемесін растады!</b>\n\n{order_summary(order_data, include_worker_price=True)}"
    if photo_id:
        await bot.send_photo(WORKER_ID, photo=photo_id, caption=final_text, reply_markup=ikb_worker_status(order_id))
    else:
        await bot.send_message(WORKER_ID, final_text, reply_markup=ikb_worker_status(order_id))

# ── Редактирование ────────────────────────────────────────
@client_router.message(IsClient(), Order.editing, F.text.startswith("◀️"))
async def edit_back(message: Message, state: FSMContext):
    await show_confirm(message, state)

@client_router.message(IsClient(), Order.editing, F.text.startswith("🔄 Изменить блок"))
async def edit_block(message: Message, state: FSMContext):
    await state.update_data(editing=True); await state.set_state(Order.choosing_block)
    await message.answer("🏢 Выберите новый блок / 🏢 Жаңа блок таңдаңыз:", reply_markup=kb_blocks())

@client_router.message(IsClient(), Order.editing, F.text.startswith("🔄 Изменить этаж"))
async def edit_floor(message: Message, state: FSMContext):
    await state.update_data(editing=True); await state.set_state(Order.entering_floor)
    await message.answer("🪜 Введите новый этаж / 🪜 Жаңа қабатты енгізіңіз:", reply_markup=kb_nav())

@client_router.message(IsClient(), Order.editing, F.text.startswith("🔄 Изменить квартиру"))
async def edit_apt(message: Message, state: FSMContext):
    await state.update_data(editing=True); await state.set_state(Order.entering_apt)
    await message.answer("🚪 Введите новый номер квартиры / 🚪 Жаңа пәтер нөмерін енгізіңіз:", reply_markup=kb_nav())

@client_router.message(IsClient(), Order.editing, F.text.startswith("🔄 Изменить тип мусора"))
async def edit_trash(message: Message, state: FSMContext):
    await state.update_data(editing=True); await state.set_state(Order.choosing_trash)
    await message.answer("🗑 Выберите новый тип мусора / 🗑 Жаңа қоқыс түрін таңдаңыз:", reply_markup=kb_trash())

@client_router.message(IsClient(), Order.editing, F.text.startswith("🔄 Изменить кол-во пакетов"))
async def edit_bags(message: Message, state: FSMContext):
    await state.update_data(editing=True); await state.set_state(Order.entering_bags)
    await message.answer("📦 Введите новое количество пакетов / 📦 Жаңа сәліне санын енгізіңіз:", reply_markup=kb_nav())

@client_router.message(IsClient(), Order.editing, F.text.startswith("🔄 Изменить фото"))
async def edit_photo(message: Message, state: FSMContext):
    await state.update_data(editing=True, photo_id=None)
    await ask_photo(message, state)

@client_router.message(IsClient(), Order.editing, F.text.startswith("🔄 Изменить время"))
async def edit_time(message: Message, state: FSMContext):
    await state.update_data(editing=True); await state.set_state(Order.choosing_time)
    await message.answer("⏰ Когда вы хотите принять заказ? / ⏰ Өтіністі қашан қабылдағыңыз келеді?", reply_markup=kb_time())

@client_router.message(IsClient(), Order.editing, F.text.startswith("🔄 Изменить комментарий"))
async def edit_comment(message: Message, state: FSMContext):
    await state.update_data(editing=True); await state.set_state(Order.asking_comment)
    await message.answer("💬 Хотите добавить комментарий? / 💬 Пікір қосқыңыз келе ме?", reply_markup=kb_comment())

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