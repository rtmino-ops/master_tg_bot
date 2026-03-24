import asyncio
import hashlib
import hmac
import json
import os
import re
import uuid
from collections import defaultdict
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, Any

import aiohttp
import aiosqlite
from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
try:
    from aiogram.fsm.storage.redis import RedisStorage
    from redis.asyncio import Redis
except Exception:  # optional redis for persistent FSM
    RedisStorage = None
    Redis = None
from aiogram.types import (
    CallbackQuery,
    Contact,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)
from dotenv import load_dotenv
from aiohttp import web

load_dotenv()

# ------------------- ИНИЦИАЛИЗАЦИЯ БОТА -------------------
bot = Bot(
    token=os.getenv("BOT_TOKEN", "").strip(),
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)


def build_fsm_storage():
    redis_url = os.getenv("REDIS_URL", "").strip()
    if redis_url and RedisStorage and Redis:
        try:
            redis = Redis.from_url(redis_url)
            return RedisStorage(redis=redis)
        except Exception as e:
            print(f"Не удалось инициализировать RedisStorage, используется MemoryStorage: {e}")
    return MemoryStorage()


dp = Dispatcher(storage=build_fsm_storage())
router = Router()


class AutoAnswerCallbackMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: CallbackQuery, data):
        try:
            return await handler(event, data)
        finally:
            with suppress(Exception):
                await event.answer()


dp.include_router(router)
router.callback_query.middleware(AutoAnswerCallbackMiddleware())


@dataclass
class Settings:
    bot_token: str
    admin_ids: set[int]
    support_username: str
    db_path: str
    min_order_price: int
    max_order_price: int
    min_withdrawal_amount: int
    yookassa_shop_id: str
    yookassa_secret_key: str
    payments_return_url: str
    payments_webhook_secret: str
    base_webhook_url: str
    order_payment_timeout_hours: int
    active_order_timeout_hours: int
    client_confirmation_timeout_hours: int


SETTINGS = Settings(
    bot_token=os.getenv("BOT_TOKEN", "").strip(),
    admin_ids={int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()},
    support_username=os.getenv("SUPPORT_USERNAME", "").strip(),
    db_path=os.getenv("DATABASE_PATH", "app/data/navseruki.db").strip(),
    min_order_price=int(os.getenv("MIN_ORDER_PRICE", "50")),
    max_order_price=int(os.getenv("MAX_ORDER_PRICE", "50000")),
    min_withdrawal_amount=int(os.getenv("MIN_WITHDRAWAL_AMOUNT", "50")),
    yookassa_shop_id=os.getenv("YOOKASSA_SHOP_ID", "").strip(),
    yookassa_secret_key=os.getenv("YOOKASSA_SECRET_KEY", "").strip(),
    payments_return_url=os.getenv("PAYMENTS_RETURN_URL", "https://t.me/YourBot").strip(),
    payments_webhook_secret=os.getenv("PAYMENTS_WEBHOOK_SECRET", "").strip(),
    base_webhook_url=os.getenv("BASE_WEBHOOK_URL", "").strip(),
    order_payment_timeout_hours=int(os.getenv("ORDER_PAYMENT_TIMEOUT_HOURS", "24")),
    active_order_timeout_hours=int(os.getenv("ACTIVE_ORDER_TIMEOUT_HOURS", "48")),
    client_confirmation_timeout_hours=int(os.getenv("CLIENT_CONFIRMATION_TIMEOUT_HOURS", "24")),
)

if not SETTINGS.bot_token:
    raise RuntimeError("BOT_TOKEN is not configured")

db_dir = os.path.dirname(SETTINGS.db_path)
if db_dir:
    os.makedirs(db_dir, exist_ok=True)

ORDER_STATUS_LABELS = {
    "pending_review": "🟣 На проверке",
    "approved_open": "🟢 Открыт для откликов",
    "waiting_customer_payment": "🟠 Ждём оплату заказчика",
    "in_progress": "🟡 В работе",
    "done_waiting_confirmation": "🟢 Ждём подтверждение",
    "manual_review": "🟣 Ручная проверка",
    "result_pending_review": "🟢 Проверяем результат",
    "completed": "🟢 Завершено",
    "cancelled": "⚫ Отменено",
    "rejected": "⚫ Отклонено",
    # совместимость со старыми данными
    "active": "🟢 Открыт для откликов",
    "expired_payment": "⚫ Не оплачено вовремя",
    "expired_unassigned": "⚫ Не найден исполнитель",
}

CATEGORY_OPTIONS = [
    ("car_help", "🚗 Помощь с машиной"),
    ("buy_deliver", "🛒 Купить и привезти"),
    ("pickup_dropoff", "📦 Забрать и отнести"),
    ("help_on_site", "🛠 Помочь на месте"),
    ("other", "✍️ Другое"),
]

WHEN_OPTIONS = [
    ("now", "⚡ Сейчас"),
    ("within_hour", "🕐 В течение часа"),
    ("today", "📅 Сегодня"),
    ("manual", "✍️ Указать вручную"),
]

CANDIDATE_PAYMENT_TIMEOUT_MINUTES = max(1, int(os.getenv("CANDIDATE_PAYMENT_TIMEOUT_MINUTES", "10")))


class CustomerReg(StatesGroup):
    name = State()
    phone = State()


class CreateOrder(StatesGroup):
    category = State()
    title = State()
    pickup = State()
    dropoff = State()
    when_choice = State()
    when_manual = State()
    details = State()
    price = State()
    confirm = State()


class ExecutorReg(StatesGroup):
    name = State()
    age = State()
    phone = State()
    confirm = State()


class WithdrawFlow(StatesGroup):
    amount = State()
    phone = State()
    bank = State()
    confirm = State()


class ChangePriceFlow(StatesGroup):
    amount = State()
    confirm = State()


class AdminRejectOrderComment(StatesGroup):
    comment = State()


class AdminRejectWithdrawalComment(StatesGroup):
    comment = State()


class AdminSearchOrder(StatesGroup):
    order_id = State()


class AdminBalanceAdjust(StatesGroup):
    user_tg_id = State()
    amount = State()


class SupportFlow(StatesGroup):
    waiting_for_message = State()


class DepositFlow(StatesGroup):
    waiting_for_amount = State()


# ------------------- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ -------------------
def escape_html(text: str) -> str:
    return (
        (text or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def support_url() -> str:
    if SETTINGS.support_username:
        return f"https://t.me/{SETTINGS.support_username}"
    return "https://t.me"


def format_price(amount: int) -> str:
    return f"{amount} ₽"


def normalize_phone(raw: str) -> Optional[str]:
    digits = re.sub(r"\D", "", raw or "")
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    if len(digits) == 11 and digits.startswith("7"):
        return "+" + digits
    return None


def is_admin(user_id: int) -> bool:
    return user_id in SETTINGS.admin_ids


def parse_callback_tail_int(callback_data: Optional[str], expected_prefix: str) -> Optional[int]:
    if not isinstance(callback_data, str) or not callback_data.startswith(expected_prefix):
        return None
    tail = callback_data[len(expected_prefix):].strip()
    if not tail.isdigit():
        return None
    return int(tail)


async def get_customer_order_for_user(user_id: int, order_id: int) -> Optional[dict[str, Any]]:
    async with db_pool.acquire() as conn:
        cur = await conn.execute(
            "SELECT * FROM orders WHERE id = ? AND customer_id = ?",
            (order_id, user_id),
        )
        row = await cur.fetchone()
        return dict(row) if row else None


async def get_order_participant_for_user(user_id: int, order_id: int) -> Optional[dict[str, Any]]:
    async with db_pool.acquire() as conn:
        cur = await conn.execute(
            """
            SELECT * FROM orders
            WHERE id = ?
              AND (customer_id = ? OR executor_id = ? OR candidate_executor_id = ?)
            """,
            (order_id, user_id, user_id, user_id),
        )
        row = await cur.fetchone()
        return dict(row) if row else None


async def get_latest_result_proof(order_id: int, executor_user_id: int) -> Optional[dict[str, Any]]:
    async with db_pool.acquire() as conn:
        cur = await conn.execute(
            """
            SELECT sm.*, st.id AS ticket_id
            FROM support_tickets st
            JOIN support_messages sm ON sm.ticket_id = st.id
            WHERE st.order_id = ?
              AND st.user_id = ?
              AND sm.sender_type = 'user'
              AND sm.sender_id = ?
              AND sm.message_type = 'result_proof'
              AND (
                    (sm.text IS NOT NULL AND TRIM(sm.text) != '')
                    OR sm.file_id IS NOT NULL
                  )
            ORDER BY sm.id DESC
            LIMIT 1
            """,
            (order_id, executor_user_id, executor_user_id),
        )
        row = await cur.fetchone()
        return dict(row) if row else None


def format_deadline(dt_text: Optional[str]) -> str:
    if not dt_text:
        return "—"
    try:
        return datetime.fromisoformat(dt_text).strftime("%d.%m %H:%M")
    except Exception:
        return dt_text


def support_ticket_label(ticket_id: int, order_id: Optional[int] = None) -> str:
    if order_id is not None:
        return f"Обращение #{ticket_id} по заданию #{order_id}"
    return f"Обращение #{ticket_id}"


def support_ticket_label_lower(ticket_id: int, order_id: Optional[int] = None) -> str:
    if order_id is not None:
        return f"обращение #{ticket_id} по заданию #{order_id}"
    return f"обращение #{ticket_id}"


def build_support_prompt_text(order_id: Optional[int], existing_ticket_id: Optional[int]) -> str:
    if existing_ticket_id is not None:
        return (
            f"💬 Продолжим {support_ticket_label_lower(existing_ticket_id, order_id)}.\n\n"
            "Напишите следующее сообщение.\n"
            "Оно будет добавлено в это же обращение.\n\n"
            "Вы можете отправить текст или фото."
        )
    if order_id is not None:
        return (
            f"⚠️ Проблема по заданию #{order_id}\n\n"
            "Опишите ситуацию. Поддержка ответит сюда.\n\n"
            "Вы можете отправить текст или фото."
        )
    return (
        "💬 Напишите ваше сообщение для поддержки.\n\n"
        "Вы можете отправить текст или фото."
    )


def build_support_sent_text(ticket_id: int, order_id: Optional[int]) -> str:
    if order_id is not None:
        return (
            f"✅ Сообщение отправлено. Обращение #{ticket_id} по заданию #{order_id}\n\n"
            "Ответ придёт в этот чат.\n"
            "Если нужно продолжить диалог, снова нажмите кнопку поддержки — следующее сообщение добавится в это же обращение."
        )
    return (
        f"✅ Сообщение отправлено. Обращение #{ticket_id}\n\n"
        "Ответ придёт в этот чат.\n"
        "Если нужно продолжить диалог, снова нажмите кнопку поддержки — следующее сообщение добавится в это же обращение."
    )


def build_support_admin_text(
    ticket_id: int,
    user_telegram_id: int,
    role: str,
    order_id: Optional[int],
    message_text: str,
    has_photo: bool,
    message_type: str = "support",
) -> str:
    title = "🆕 Новое сообщение в обращении"
    if message_type == "result_proof":
        title = "🧾 Новый proof результата"

    lines = [
        f"{title} #{ticket_id}",
        "",
        f"Ticket ID: #{ticket_id}",
        f"Telegram ID: {user_telegram_id}",
    ]
    if order_id is not None:
        lines.append(f"Order ID: #{order_id}")
    lines.append(f"Роль: {escape_html(role)}")
    lines.append(f"Тип: {escape_html(message_type)}")
    lines.append("")
    lines.append("Сообщение:")
    lines.append(escape_html(message_text) if message_text else "—")
    if has_photo:
        lines.append("")
        lines.append("Вложение: фото")
    return "\n".join(lines)



ALLOWED_ORDER_TRANSITIONS: dict[str, set[str]] = {
    "pending_review": {"approved_open", "rejected", "cancelled"},
    "approved_open": {"waiting_customer_payment", "cancelled"},
    "waiting_customer_payment": {"approved_open", "in_progress", "cancelled"},
    "in_progress": {"done_waiting_confirmation", "manual_review", "cancelled", "completed"},
    "done_waiting_confirmation": {"manual_review", "completed", "cancelled"},
    "manual_review": {"completed", "cancelled", "in_progress"},
    "completed": set(),
    "cancelled": set(),
    "rejected": set(),
    "expired_payment": set(),
    "expired_unassigned": set(),
}


class AccessDeniedError(Exception):
    pass


class InvalidOrderTransitionError(Exception):
    pass


def ensure_order_transition(from_status: str, to_status: str):
    allowed = ALLOWED_ORDER_TRANSITIONS.get(from_status)
    if allowed is None:
        raise InvalidOrderTransitionError(f"Unknown order status: {from_status}")
    if to_status not in allowed:
        raise InvalidOrderTransitionError(f"Invalid order transition: {from_status} -> {to_status}")


async def get_order_with_access_context(order_id: int) -> Optional[dict[str, Any]]:
    async with db_pool.acquire() as conn:
        cur = await conn.execute(
            """
            SELECT
                o.*,
                cu.telegram_id AS customer_tg_id,
                ex.telegram_id AS executor_tg_id
            FROM orders o
            JOIN users cu ON cu.id = o.customer_id
            LEFT JOIN users ex ON ex.id = o.executor_id
            WHERE o.id = ?
            """,
            (order_id,),
        )
        row = await cur.fetchone()
        return dict(row) if row else None


async def assert_customer_order_access(order_id: int, tg_id: int) -> dict[str, Any]:
    order = await get_order_with_access_context(order_id)
    if not order:
        raise AccessDeniedError("order_not_found")
    if order["customer_tg_id"] != tg_id:
        raise AccessDeniedError("forbidden")
    return order


async def assert_executor_order_access(order_id: int, tg_id: int) -> dict[str, Any]:
    order = await get_order_with_access_context(order_id)
    if not order:
        raise AccessDeniedError("order_not_found")
    if order.get("executor_tg_id") != tg_id:
        raise AccessDeniedError("forbidden")
    return order


async def assert_support_order_access(order_id: int, tg_id: int, role: str) -> dict[str, Any]:
    order = await get_order_with_access_context(order_id)
    if not order:
        raise AccessDeniedError("order_not_found")
    if role == "admin":
        return order
    if role.startswith("executor"):
        if order.get("executor_tg_id") != tg_id:
            raise AccessDeniedError("forbidden")
        return order
    if order["customer_tg_id"] != tg_id:
        raise AccessDeniedError("forbidden")
    return order


# ------------------- РАБОТА С БАЗОЙ ДАННЫХ (ПУЛ СОЕДИНЕНИЙ) -------------------
class SQLitePool:
    def __init__(self, path: str, max_connections: int = 5):
        self.path = path
        self.max_connections = max_connections
        self._pool = asyncio.Queue()
        self._initialized = False

    async def initialize(self):
        conn = await aiosqlite.connect(self.path)
        conn.row_factory = aiosqlite.Row
        await conn.execute("PRAGMA foreign_keys = ON")
        await conn.execute("PRAGMA journal_mode = WAL")
        await conn.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER NOT NULL UNIQUE,
                name TEXT,
                age INTEGER,
                phone TEXT,
                balance INTEGER NOT NULL DEFAULT 0,
                is_executor_profile_created INTEGER NOT NULL DEFAULT 0,
                is_executor_approved INTEGER NOT NULL DEFAULT 0,
                is_blocked INTEGER NOT NULL DEFAULT 0,
                active_order_id INTEGER,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_id INTEGER NOT NULL,
                executor_id INTEGER,
                candidate_executor_id INTEGER,
                category_code TEXT NOT NULL,
                category_label TEXT NOT NULL,
                title TEXT NOT NULL,
                pickup_text TEXT NOT NULL,
                pickup_lat REAL,
                pickup_lon REAL,
                dropoff_text TEXT NOT NULL,
                dropoff_lat REAL,
                dropoff_lon REAL,
                dropoff_required INTEGER NOT NULL DEFAULT 1,
                when_type TEXT NOT NULL,
                when_text TEXT NOT NULL,
                details_text TEXT,
                price_amount INTEGER NOT NULL,
                hold_amount INTEGER NOT NULL DEFAULT 0,
                actual_hold_amount INTEGER NOT NULL DEFAULT 0,
                payment_deadline_at TEXT,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(customer_id) REFERENCES users(id),
                FOREIGN KEY(executor_id) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                order_id INTEGER,
                amount INTEGER NOT NULL,
                type TEXT NOT NULL,
                comment TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(user_id) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS withdrawal_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                requested_amount INTEGER NOT NULL,
                phone TEXT NOT NULL,
                bank_name TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                comment TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                processed_at TEXT,
                FOREIGN KEY(user_id) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS support_tickets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                telegram_id INTEGER NOT NULL,
                role TEXT,
                order_id INTEGER,
                status TEXT NOT NULL DEFAULT 'open',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                closed_at TEXT,
                FOREIGN KEY(user_id) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS support_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticket_id INTEGER NOT NULL,
                sender_type TEXT NOT NULL,
                sender_id INTEGER NOT NULL,
                text TEXT,
                file_id TEXT,
                message_type TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(ticket_id) REFERENCES support_tickets(id)
            );

            CREATE TABLE IF NOT EXISTS payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                amount INTEGER NOT NULL,
                provider TEXT NOT NULL,
                external_payment_id TEXT NOT NULL UNIQUE,
                idempotence_key TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                payment_url TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                paid_at TEXT,
                raw_payload TEXT,
                FOREIGN KEY(user_id) REFERENCES users(id)
            );

            CREATE INDEX IF NOT EXISTS idx_users_tg ON users(telegram_id);
            CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
            CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders(customer_id);
            CREATE INDEX IF NOT EXISTS idx_orders_executor ON orders(executor_id);
            CREATE INDEX IF NOT EXISTS idx_withdrawals_status ON withdrawal_requests(status);
            CREATE INDEX IF NOT EXISTS idx_payments_ext_id ON payments(external_payment_id);
            CREATE INDEX IF NOT EXISTS idx_payments_user ON payments(user_id);
            CREATE INDEX IF NOT EXISTS idx_support_tickets_user ON support_tickets(user_id);
            CREATE INDEX IF NOT EXISTS idx_support_messages_ticket ON support_messages(ticket_id);
        """)
        await conn.commit()

        try:
            await conn.execute("ALTER TABLE orders ADD COLUMN actual_hold_amount INTEGER NOT NULL DEFAULT 0")
            await conn.execute("UPDATE orders SET actual_hold_amount = hold_amount")
            await conn.commit()
        except aiosqlite.OperationalError:
            pass

        try:
            await conn.execute("ALTER TABLE orders ADD COLUMN candidate_executor_id INTEGER")
            await conn.commit()
        except aiosqlite.OperationalError:
            pass

        try:
            await conn.execute("ALTER TABLE orders ADD COLUMN payment_deadline_at TEXT")
            await conn.commit()
        except aiosqlite.OperationalError:
            pass

        try:
            await conn.execute("ALTER TABLE orders ADD COLUMN result_sent_to_customer_at TEXT")
            await conn.commit()
        except aiosqlite.OperationalError:
            pass

        await conn.execute("UPDATE support_messages SET message_type = 'support' WHERE message_type IS NULL")
        await conn.commit()

        await conn.close()

        for _ in range(self.max_connections):
            pooled = await aiosqlite.connect(self.path)
            pooled.row_factory = aiosqlite.Row
            await pooled.execute("PRAGMA foreign_keys = ON")
            await pooled.execute("PRAGMA journal_mode = WAL")
            await self._pool.put(pooled)

        self._initialized = True

    @asynccontextmanager
    async def acquire(self):
        if not self._initialized:
            await self.initialize()
        conn = await self._pool.get()
        try:
            yield conn
        finally:
            await self._pool.put(conn)

    async def close(self):
        while not self._pool.empty():
            conn = await self._pool.get()
            await conn.close()


db_pool = SQLitePool(SETTINGS.db_path)
USER_LOCKS: dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)
ORDER_LOCKS: dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)
WITHDRAW_LOCKS: dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)
SUPPORT_TICKET_LOCKS: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)


def user_lock(user_id: int) -> asyncio.Lock:
    return USER_LOCKS[user_id]


def order_lock(order_id: int) -> asyncio.Lock:
    return ORDER_LOCKS[order_id]


def withdraw_lock(withdrawal_id: int) -> asyncio.Lock:
    return WITHDRAW_LOCKS[withdrawal_id]


def support_ticket_lock(user_id: int, order_id: Optional[int]) -> asyncio.Lock:
    key = f"{user_id}:{order_id if order_id is not None else 'general'}"
    return SUPPORT_TICKET_LOCKS[key]


# ------------------- MIDDLEWARE ДЛЯ STALE CALLBACKS -------------------
ACTION_CALLBACKS_EXACT = {
    "global:cancel",
    "common:main",
    "common:how",
    "profile:open",
    "balance:open",
    "customer:create_order",
    "customer:orders",
    "customer:orders:active",
    "customer:orders:done",
    "executor:start",
    "executor:reg:start",
    "executor:available",
    "executor:my_order",
    "withdraw:start",
    "deposit:start",
    "admin:main",
    "admin:orders",
    "admin:orders:new",
    "admin:orders:awaiting_payment",
    "admin:orders:active",
    "admin:orders:progress",
    "admin:orders:result",
    "admin:orders:waiting_client",
    "admin:orders:expired",
    "admin:orders:search",
    "admin:orders:manual_review",
    "admin:executors",
    "admin:withdrawals",
    "admin:withdrawals:pending",
    "admin:withdrawals:paid",
    "admin:withdrawals:rejected",
    "admin:balances",
    "admin:balances:add",
    "admin:balances:sub",
}

ACTION_CALLBACKS_PREFIX = (
    "support:general",
    "support:order:",
    "cust:view:",
    "cust:pay:",
    "cust:cancel:ask:",
    "cust:cancel:",
    "cust:done:ask:",
    "cust:done:",
    "cust:changeprice:",
    "exec:viewavailable:",
    "exec:take:",
    "exec:sendresult:",
    "exec:sent:",
    "admin:order:view:",
    "admin:publish:",
    "admin:reject:no:",
    "admin:reject:comment:",
    "admin:returnpool:",
    "admin:sendcustomer:",
    "admin:rework:",
    "admin:forcedone:",
    "admin:cancel:",
    "admin:executor:view:",
    "admin:executor:approve:",
    "admin:executor:reject:",
    "admin:executor:block:",
    "admin:withdraw:view:",
    "admin:withdraw:pay:",
    "admin:withdraw:reject:no:",
    "admin:withdraw:reject:comment:",
)

FSM_CALLBACKS_EXACT = {
    "order:pay": CreateOrder.confirm,
    "order:submit": CreateOrder.confirm,
    "order:edit": CreateOrder.confirm,
    "order:dropoff:onsite": CreateOrder.dropoff,
    "order:skipdetails": CreateOrder.details,
    "executor:reg:save": ExecutorReg.confirm,
    "withdraw:create": WithdrawFlow.confirm,
    "cust:changeprice:save": ChangePriceFlow.confirm,
}

FSM_CALLBACKS_PREFIX = {
    "order:cat:": CreateOrder.category,
    "order:when:": CreateOrder.when_choice,
    "order:edit:": CreateOrder.confirm,
    "deposit:amount:": DepositFlow.waiting_for_amount,
    "withdraw:bank:": WithdrawFlow.bank,
}


def get_expected_fsm_state(callback_data: str) -> Optional[str]:
    state_obj = FSM_CALLBACKS_EXACT.get(callback_data)
    if state_obj is not None:
        return state_obj.state
    for prefix, state_class in FSM_CALLBACKS_PREFIX.items():
        if callback_data.startswith(prefix):
            return state_class.state
    return None


def is_action_callback(callback_data: str) -> bool:
    if callback_data in ACTION_CALLBACKS_EXACT:
        return True
    return any(callback_data.startswith(prefix) for prefix in ACTION_CALLBACKS_PREFIX)


async def show_stale_callback(event: CallbackQuery):
    await event.answer("❌ Сценарий устарел. Начните заново.", show_alert=True)
    try:
        await event.message.edit_text(
            "Сценарий устарел. Нажмите /start, чтобы начать сначала.",
            reply_markup=main_menu_kb()
        )
    except Exception:
        try:
            await bot.send_message(
                event.from_user.id,
                "Сценарий устарел. Нажмите /start, чтобы начать сначала.",
                reply_markup=main_menu_kb()
            )
        except Exception:
            pass


class StaleCallbackMiddleware:
    async def __call__(self, handler, event: CallbackQuery, data: dict):
        callback_data = event.data or ""

        expected_state = get_expected_fsm_state(callback_data)
        if expected_state is not None:
            state = data.get("state")
            if not state:
                await show_stale_callback(event)
                return

            current_state = await state.get_state()
            if current_state != expected_state:
                await show_stale_callback(event)
                return

            return await handler(event, data)

        if is_action_callback(callback_data):
            return await handler(event, data)

        return await handler(event, data)


dp.callback_query.middleware(StaleCallbackMiddleware())


# ------------------- ОСНОВНЫЕ БИЗНЕС-ФУНКЦИИ -------------------
async def ensure_user(telegram_id: int) -> dict[str, Any]:
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT OR IGNORE INTO users (telegram_id) VALUES (?)",
            (telegram_id,)
        )
        await conn.commit()
        cur = await conn.execute("SELECT * FROM users WHERE telegram_id = ?", (telegram_id,))
        user = await cur.fetchone()
        return dict(user)


async def get_user_by_id(user_id: int) -> Optional[dict[str, Any]]:
    async with db_pool.acquire() as conn:
        cur = await conn.execute("SELECT * FROM users WHERE id = ?", (user_id,))
        row = await cur.fetchone()
        return dict(row) if row else None


async def get_order_by_id(order_id: int) -> Optional[dict[str, Any]]:
    async with db_pool.acquire() as conn:
        cur = await conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,))
        row = await cur.fetchone()
        return dict(row) if row else None


async def notify_user(tg_id: int, text: str, markup: Optional[InlineKeyboardMarkup] = None) -> bool:
    try:
        await bot.send_message(tg_id, text, reply_markup=markup)
        return True
    except Exception as e:
        print(f"Ошибка отправки сообщения пользователю {tg_id}: {e}")
        return False


# ------------------- KEYBOARD BUILDERS -------------------
def main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Разместить задание", callback_data="customer:create_order")],
        [InlineKeyboardButton(text="💼 Хочу заработать", callback_data="executor:start")],
        [InlineKeyboardButton(text="❓ Как это работает", callback_data="common:how")],
        [
            InlineKeyboardButton(text="📝 Написать в поддержку", callback_data="support:general"),
            InlineKeyboardButton(text="💬 Связаться с оператором", url=support_url())
        ],
        [InlineKeyboardButton(text="👤 Профиль", callback_data="profile:open")],
    ])



def main_back_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🏠 Главное меню", callback_data="common:main")]])


def profile_keyboard(user: dict[str, Any]) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="💰 Баланс", callback_data="balance:open")],
    ]
    if user.get("name") and user.get("phone"):
        rows.append([InlineKeyboardButton(text="📜 Мои задания", callback_data="customer:orders")])
    if user.get("is_executor_profile_created"):
        rows.append([InlineKeyboardButton(text="📋 Доступные задания", callback_data="executor:available")])
        rows.append([InlineKeyboardButton(text="📌 Моё задание", callback_data="executor:my_order")])
    rows.append([
        InlineKeyboardButton(text="📝 Написать в поддержку", callback_data="support:general"),
        InlineKeyboardButton(text="💬 Связаться с оператором", url=support_url())
    ])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="common:main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def how_it_works_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Разместить задание", callback_data="customer:create_order")],
        [InlineKeyboardButton(text="💼 Хочу заработать", callback_data="executor:start")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="common:main")],
    ])


def send_contact_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📱 Отправить контакт", request_contact=True)]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def category_kb() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text=label, callback_data=f"order:cat:{code}")] for code, label in CATEGORY_OPTIONS]
    rows.append([InlineKeyboardButton(text="❌ Отмена", callback_data="global:cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def when_kb() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text=label, callback_data=f"order:when:{code}")] for code, label in WHEN_OPTIONS]
    rows.append([InlineKeyboardButton(text="❌ Отмена", callback_data="global:cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def dropoff_choice_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚫 Только по месту", callback_data="order:dropoff:onsite")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="global:cancel")],
    ])


def order_confirm_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Отправить на проверку", callback_data="order:pay")],
        [InlineKeyboardButton(text="✏️ Изменить", callback_data="order:edit")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="global:cancel")],
    ])


def order_edit_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Описание", callback_data="order:edit:title")],
        [InlineKeyboardButton(text="📍 Откуда", callback_data="order:edit:pickup")],
        [InlineKeyboardButton(text="📍 Куда", callback_data="order:edit:dropoff")],
        [InlineKeyboardButton(text="⏰ Когда", callback_data="order:edit:when")],
        [InlineKeyboardButton(text="📌 Детали", callback_data="order:edit:details")],
        [InlineKeyboardButton(text="💰 Оплату", callback_data="order:edit:price")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="global:cancel")],
    ])


def order_pay_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Отправить на проверку", callback_data="order:submit")],
        [InlineKeyboardButton(text="💬 Связаться с оператором", url=support_url())],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="global:cancel")],
    ])


def balance_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Пополнить", callback_data="deposit:start")],
        [InlineKeyboardButton(text="💸 Вывести", callback_data="withdraw:start")],
        [InlineKeyboardButton(text="⚠️ Проблема с балансом", callback_data="support:general")],
        [InlineKeyboardButton(text="💬 Связаться с оператором", url=support_url())],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="profile:open")],
    ])


def deposit_amount_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="100 ₽", callback_data="deposit:amount:100")],
        [InlineKeyboardButton(text="500 ₽", callback_data="deposit:amount:500")],
        [InlineKeyboardButton(text="1000 ₽", callback_data="deposit:amount:1000")],
        [InlineKeyboardButton(text="Другая сумма", callback_data="deposit:amount:other")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="global:cancel")],
    ])


def withdraw_banks_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Сбербанк", callback_data="withdraw:bank:Сбербанк")],
        [InlineKeyboardButton(text="Тинькофф", callback_data="withdraw:bank:Тинькофф")],
        [InlineKeyboardButton(text="ВТБ", callback_data="withdraw:bank:ВТБ")],
        [InlineKeyboardButton(text="Альфа-Банк", callback_data="withdraw:bank:Альфа-Банк")],
        [InlineKeyboardButton(text="📝 Другой", callback_data="withdraw:bank:manual")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="global:cancel")],
    ])


def withdraw_confirm_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Создать заявку", callback_data="withdraw:create")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="global:cancel")],
    ])


def customer_orders_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⌛ Активные", callback_data="customer:orders:active")],
        [InlineKeyboardButton(text="✅ Завершённые", callback_data="customer:orders:done")],
        [InlineKeyboardButton(text="📝 Разместить задание", callback_data="customer:create_order")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="profile:open")],
    ])


def customer_order_actions_kb(order_id: int, status: str) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []

    if status == "waiting_customer_payment":
        rows.append([InlineKeyboardButton(text="💳 Оплатить заказ", callback_data=f"cust:pay:{order_id}")])
        rows.append([InlineKeyboardButton(text="❌ Отменить задание", callback_data=f"cust:cancel:ask:{order_id}")])

    elif status in {"pending_review", "approved_open"}:
        if status == "approved_open":
            rows.append([InlineKeyboardButton(text="💰 Изменить вознаграждение", callback_data=f"cust:changeprice:{order_id}")])
        rows.append([InlineKeyboardButton(text="❌ Отменить задание", callback_data=f"cust:cancel:ask:{order_id}")])

    if status == "done_waiting_confirmation":
        rows.append([InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"cust:done:ask:{order_id}")])

    rows.append([InlineKeyboardButton(text="⚠️ Проблема по заданию", callback_data=f"support:order:{order_id}")])
    rows.append([InlineKeyboardButton(text="💬 Связаться с оператором", url=support_url())])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="customer:orders")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def customer_cancel_confirm_kb(order_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Да, отменить", callback_data=f"cust:cancel:{order_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"cust:view:{order_id}")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="global:cancel")],
    ])


def customer_done_confirm_kb(order_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Да, подтвердить", callback_data=f"cust:done:{order_id}")],
        [InlineKeyboardButton(text="💬 Связаться с оператором", url=support_url())],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"cust:view:{order_id}")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="global:cancel")],
    ])


def change_price_confirm_kb(diff: int) -> InlineKeyboardMarkup:
    extra = ""
    if diff > 0:
        extra = f"\nС баланса спишется ещё {format_price(diff)}"
    elif diff < 0:
        extra = f"\nНа баланс вернётся {format_price(-diff)}"
    text = "✅ Сохранить" + extra
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=text, callback_data="cust:changeprice:save")]])


def executor_start_kb(user: dict[str, Any]) -> InlineKeyboardMarkup:
    if user.get("is_executor_profile_created") and not user.get("is_executor_approved"):
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="common:main")],
            [InlineKeyboardButton(text="📝 Написать в поддержку", callback_data="support:general")],
        ])
    if user.get("is_executor_approved"):
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📋 Доступные задания", callback_data="executor:available")],
            [InlineKeyboardButton(text="💰 Баланс", callback_data="balance:open")],
            [InlineKeyboardButton(text="📝 Написать в поддержку", callback_data="support:general")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="common:main")],
        ])
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚀 Начать", callback_data="executor:reg:start")],
        [InlineKeyboardButton(text="📝 Написать в поддержку", callback_data="support:general")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="common:main")],
    ])


def executor_reg_confirm_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Всё верно", callback_data="executor:reg:save")],
        [InlineKeyboardButton(text="✏️ Изменить", callback_data="executor:reg:start")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="global:cancel")],
    ])


def available_orders_list_kb(orders: list[dict]) -> InlineKeyboardMarkup:
    buttons = []
    for o in orders:
        route = f"{o['pickup_text']} → {o['dropoff_text']}"
        if len(route) > 30:
            route = route[:27] + "..."
        text = f"📦 #{o['id']} {o['category_label']} {format_price(o['price_amount'])} {route}"
        buttons.append([InlineKeyboardButton(text=text, callback_data=f"exec:viewavailable:{o['id']}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="profile:open")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def available_order_kb(order_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Готов взять", callback_data=f"exec:take:{order_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="executor:available")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="global:cancel")],
    ])


def my_order_kb(order_id: int, status: str) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if status == "in_progress":
        rows.append([InlineKeyboardButton(text="📤 Отправить результат", callback_data=f"exec:sendresult:{order_id}")])
    rows.append([InlineKeyboardButton(text="⚠️ Проблема по заданию", callback_data=f"support:order:{order_id}")])
    rows.append([InlineKeyboardButton(text="💬 Связаться с оператором", url=support_url())])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="profile:open")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def send_result_kb(order_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📤 Отправить proof результата", callback_data=f"support:proof:{order_id}")],
        [InlineKeyboardButton(text="✅ Я отправил результат", callback_data=f"exec:sent:{order_id}")],
        [InlineKeyboardButton(text="⚠️ Проблема по заданию", callback_data=f"support:order:{order_id}")],
        [InlineKeyboardButton(text="💬 Связаться с оператором", url=support_url())],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="executor:my_order")],
    ])


def admin_main_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👤 Исполнители", callback_data="admin:executors")],
        [InlineKeyboardButton(text="💰 Балансы", callback_data="admin:balances")],
        [InlineKeyboardButton(text="💸 Выводы", callback_data="admin:withdrawals")],
        [InlineKeyboardButton(text="📦 Задания", callback_data="admin:orders")],
    ])


def admin_orders_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🟣 На проверке", callback_data="admin:orders:new")],
        [InlineKeyboardButton(text="📋 Открыты для откликов", callback_data="admin:orders:active")],
        [InlineKeyboardButton(text="💳 Ждут оплату заказчика", callback_data="admin:orders:awaiting_payment")],
        [InlineKeyboardButton(text="🟡 В работе", callback_data="admin:orders:progress")],
        [InlineKeyboardButton(text="🕓 Ждут клиента", callback_data="admin:orders:waiting_client")],
        [InlineKeyboardButton(text="🟣 Ручная проверка", callback_data="admin:orders:manual_review")],
        [InlineKeyboardButton(text="🔎 Поиск по номеру", callback_data="admin:orders:search")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:main")],
    ])


def admin_order_actions_kb(order_id: int, status: str) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if status == "pending_review":
        rows.extend([
            [InlineKeyboardButton(text="✅ Одобрить и открыть отклики", callback_data=f"admin:publish:{order_id}")],
            [InlineKeyboardButton(text="❌ Отклонить без комментария", callback_data=f"admin:reject:no:{order_id}")],
            [InlineKeyboardButton(text="📝 Отклонить с комментарием", callback_data=f"admin:reject:comment:{order_id}")],
        ])
    elif status in {"approved_open", "waiting_customer_payment"}:
        rows.append([InlineKeyboardButton(text="❌ Отменить", callback_data=f"admin:cancel:{order_id}")])
    elif status == "in_progress":
        rows.extend([
            [InlineKeyboardButton(text="🔁 Вернуть в пул", callback_data=f"admin:returnpool:{order_id}")],
            [InlineKeyboardButton(text="✅ Завершить вручную", callback_data=f"admin:forcedone:{order_id}")],
            [InlineKeyboardButton(text="❌ Отменить", callback_data=f"admin:cancel:{order_id}")],
        ])
    elif status in {"done_waiting_confirmation", "manual_review"}:
        rows.extend([
            [InlineKeyboardButton(text="✅ Завершить вручную", callback_data=f"admin:forcedone:{order_id}")],
            [InlineKeyboardButton(text="❌ Отменить", callback_data=f"admin:cancel:{order_id}")],
        ])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:orders")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_withdrawals_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🕓 Новые запросы", callback_data="admin:withdrawals:pending")],
        [InlineKeyboardButton(text="✅ Выплаченные", callback_data="admin:withdrawals:paid")],
        [InlineKeyboardButton(text="❌ Отклонённые", callback_data="admin:withdrawals:rejected")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:main")],
    ])


def admin_withdrawal_actions_kb(withdrawal_id: int, status: str) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if status == "pending":
        rows.extend([
            [InlineKeyboardButton(text="✅ Подтвердить выплату", callback_data=f"admin:withdraw:pay:{withdrawal_id}")],
            [InlineKeyboardButton(text="❌ Отклонить без комментария", callback_data=f"admin:withdraw:reject:no:{withdrawal_id}")],
            [InlineKeyboardButton(text="📝 Отклонить с комментарием", callback_data=f"admin:withdraw:reject:comment:{withdrawal_id}")],
        ])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:withdrawals")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_executors_list_kb(ids: list[int]) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text=f"👤 Исполнитель #{i}", callback_data=f"admin:executor:view:{i}")] for i in ids]
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_executor_actions_kb(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Одобрить", callback_data=f"admin:executor:approve:{user_id}")],
        [InlineKeyboardButton(text="❌ Отклонить", callback_data=f"admin:executor:reject:{user_id}")],
        [InlineKeyboardButton(text="🚫 Заблокировать", callback_data=f"admin:executor:block:{user_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:executors")],
    ])


def admin_balances_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Пополнить", callback_data="admin:balances:add")],
        [InlineKeyboardButton(text="➖ Списать", callback_data="admin:balances:sub")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:main")],
    ])


def insufficient_funds_kb(need: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Пополнить баланс", callback_data="deposit:start")],
        [InlineKeyboardButton(text="💬 Связаться с оператором", url=support_url())],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="global:cancel")],
    ])


# ------------------- HELPER ФУНКЦИИ ДЛЯ ОТОБРАЖЕНИЯ -------------------
def render_profile_text(user: dict[str, Any]) -> str:
    text = (
        "👤 Ваш профиль\n\n"
        f"Имя: {escape_html(user.get('name') or '—')}\n"
        f"Телефон: {escape_html(user.get('phone') or '—')}\n"
        f"Баланс: {format_price(user['balance'])}"
    )
    if user.get("is_executor_profile_created"):
        status = "✅ Подключён" if user.get("is_executor_approved") else "⏳ На проверке"
        text += f"\n\nРежим исполнителя: {status}"
    return text


def route_text(order: dict[str, Any]) -> str:
    return f"{order['pickup_text']} → {order['dropoff_text']}"


def order_details_text(order: dict[str, Any]) -> str:
    text = (
        f"📦 Задание #{order['id']}\n\n"
        f"{escape_html(order['category_label'])}\n\n"
        f"📝 Что нужно сделать:\n{escape_html(order['title'])}\n\n"
        f"📍 Откуда:\n{escape_html(order['pickup_text'])}\n\n"
        f"📍 Куда:\n{escape_html(order['dropoff_text'])}\n\n"
        f"⏰ Когда:\n{escape_html(order['when_text'])}\n\n"
    )
    if order.get("details_text"):
        text += f"📌 Детали:\n{escape_html(order['details_text'])}\n\n"
    text += (
        f"💰 Оплата:\n{format_price(order['price_amount'])}\n\n"
        f"Статус: {ORDER_STATUS_LABELS.get(order['status'], order['status'])}"
    )
    if order.get("payment_deadline_at") and order.get("status") == "waiting_customer_payment":
        text += f"\n\n⏳ Оплатить до: {escape_html(format_deadline(order.get('payment_deadline_at')))}"
    return text


async def render_order_confirm(target: Message | CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get("category_label") or not data.get("price_amount"):
        if isinstance(target, CallbackQuery):
            await target.message.edit_text("❌ Сценарий устарел. Начните заново.", reply_markup=main_menu_kb())
            await target.answer()
        else:
            await target.answer("❌ Сценарий устарел. Начните заново.", reply_markup=main_menu_kb())
        await state.clear()
        return

    text = (
        "📦 Новое задание\n\n"
        f"{escape_html(data['category_label'])}\n"
        f"📝 {escape_html(data['title'])}\n"
        f"📍 {escape_html(data['pickup_text'])} → {escape_html(data['dropoff_text'])}\n"
        f"⏰ {escape_html(data['when_text'])}\n"
        f"💰 {format_price(int(data['price_amount']))}\n"
    )
    if data.get("details_text"):
        text += f"\n📌 {escape_html(data['details_text'])}"
    text += "\n\n👇 Всё верно?"
    if isinstance(target, Message):
        await target.answer(text, reply_markup=order_confirm_kb())
    else:
        await target.message.edit_text(text, reply_markup=order_confirm_kb())
        await target.answer()


async def maybe_finish_order_edit(target: Message | CallbackQuery, state: FSMContext, field_name: str) -> bool:
    data = await state.get_data()
    if data.get("editing_field") != field_name:
        return False
    await state.update_data(editing_field=None)
    await state.set_state(CreateOrder.confirm)
    await render_order_confirm(target, state)
    return True


# ------------------- БИЗНЕС-ЛОГИКА ЗАКАЗОВ -------------------
async def create_order_for_user(tg_id: int, data: dict[str, Any]) -> Optional[int]:
    user = await ensure_user(tg_id)
    async with db_pool.acquire() as conn:
        await conn.execute("BEGIN IMMEDIATE")
        try:
            cur = await conn.execute(
                """
                INSERT INTO orders (
                    customer_id, category_code, category_label, title,
                    pickup_text, pickup_lat, pickup_lon,
                    dropoff_text, dropoff_lat, dropoff_lon, dropoff_required,
                    when_type, when_text, details_text,
                    price_amount, hold_amount, actual_hold_amount, payment_deadline_at, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, NULL, 'pending_review')
                """,
                (
                    user["id"], data["category_code"], data["category_label"], data["title"],
                    data["pickup_text"], data.get("pickup_lat"), data.get("pickup_lon"),
                    data["dropoff_text"], data.get("dropoff_lat"), data.get("dropoff_lon"), int(data["dropoff_required"]),
                    data["when_type"], data["when_text"], data.get("details_text") or "",
                    int(data["price_amount"]),
                ),
            )
            order_id = cur.lastrowid
            await conn.commit()
            return order_id
        except Exception:
            await conn.rollback()
            raise

async def reject_order(order_id: int, comment: str = "") -> bool:
    async with db_pool.acquire() as conn:
        order = await conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,))
        order_row = await order.fetchone()
        if not order_row or order_row["status"] != "pending_review":
            return False
        order_dict = dict(order_row)
        try:
            ensure_order_transition(order_dict["status"], "rejected")
        except InvalidOrderTransitionError:
            return False
        customer = await get_user_by_id(order_dict["customer_id"])
        refund_amount = int(order_dict["actual_hold_amount"] or 0)

        await conn.execute("BEGIN IMMEDIATE")
        try:
            cur = await conn.execute(
                """
                UPDATE orders
                SET status = 'rejected',
                    hold_amount = 0,
                    actual_hold_amount = 0,
                    candidate_executor_id = NULL,
                    payment_deadline_at = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ? AND status = 'pending_review'
                """,
                (order_id,),
            )
            if cur.rowcount != 1:
                await conn.rollback()
                return False

            if refund_amount > 0:
                await conn.execute(
                    "UPDATE users SET balance = balance + ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (refund_amount, customer["id"]),
                )
                await conn.execute(
                    "INSERT INTO transactions (user_id, order_id, amount, type, comment) VALUES (?, ?, ?, 'refund', ?)",
                    (customer["id"], order_id, refund_amount, "Отклонение задания"),
                )
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise

    text = f"📦 Задание #{order_id}\n\nЗадание не прошло проверку"
    if comment:
        text += f"\n\nПричина:\n{escape_html(comment)}"
    if refund_amount > 0:
        text += f"\n\nНа баланс возвращено: {format_price(refund_amount)}"

    await notify_user(
        customer["telegram_id"],
        text,
        InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📝 Разместить заново", callback_data="customer:create_order")],
            [InlineKeyboardButton(text="💬 Связаться с оператором", url=support_url())],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="customer:orders")],
        ]),
    )
    return True

async def approve_order_and_wait_payment(order_id: int) -> tuple[bool, str]:
    async with db_pool.acquire() as conn:
        await conn.execute("BEGIN IMMEDIATE")
        try:
            cur = await conn.execute(
                """
                UPDATE orders
                SET status = 'approved_open',
                    candidate_executor_id = NULL,
                    payment_deadline_at = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ? AND status = 'pending_review'
                """,
                (order_id,),
            )
            if cur.rowcount != 1:
                await conn.rollback()
                return False, "bad_status"
            await conn.commit()
            return True, "ok"
        except Exception:
            await conn.rollback()
            raise

async def pay_order_from_balance(tg_id: int, order_id: int) -> tuple[bool, str, int]:
    user = await ensure_user(tg_id)
    async with db_pool.acquire() as conn:
        await conn.execute("BEGIN IMMEDIATE")
        try:
            cur = await conn.execute(
                "SELECT * FROM orders WHERE id = ? AND customer_id = ?",
                (order_id, user["id"])
            )
            order_row = await cur.fetchone()
            if not order_row:
                await conn.rollback()
                return False, "not_found", 0

            order_dict = dict(order_row)
            if order_dict["status"] != "waiting_customer_payment":
                await conn.rollback()
                return False, "bad_status", 0

            candidate_executor_id = order_dict.get("candidate_executor_id")
            if not candidate_executor_id:
                await conn.rollback()
                return False, "candidate_missing", 0

            deadline = order_dict.get("payment_deadline_at")
            if deadline:
                try:
                    if datetime.now() > datetime.fromisoformat(deadline):
                        await conn.rollback()
                        return False, "deadline_expired", 0
                except ValueError:
                    pass

            amount = int(order_dict["price_amount"])

            cur = await conn.execute(
                "SELECT * FROM users WHERE id = ?",
                (candidate_executor_id,),
            )
            executor_row = await cur.fetchone()
            if not executor_row:
                await conn.execute(
                    """
                    UPDATE orders
                    SET status = 'approved_open',
                        candidate_executor_id = NULL,
                        payment_deadline_at = NULL,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ? AND status = 'waiting_customer_payment'
                    """,
                    (order_id,),
                )
                await conn.commit()
                return False, "candidate_missing", amount

            executor_dict = dict(executor_row)
            if (
                executor_dict.get("is_blocked")
                or not executor_dict.get("is_executor_profile_created")
                or not executor_dict.get("is_executor_approved")
                or executor_dict.get("active_order_id")
            ):
                await conn.execute(
                    """
                    UPDATE orders
                    SET status = 'approved_open',
                        candidate_executor_id = NULL,
                        payment_deadline_at = NULL,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ? AND status = 'waiting_customer_payment'
                    """,
                    (order_id,),
                )
                await conn.commit()
                return False, "candidate_unavailable", amount

            upd = await conn.execute(
                "UPDATE users SET balance = balance - ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? AND balance >= ?",
                (amount, user["id"], amount),
            )
            if upd.rowcount != 1:
                await conn.rollback()
                return False, "no_balance", amount

            upd_executor = await conn.execute(
                "UPDATE users SET active_order_id = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? AND active_order_id IS NULL",
                (order_id, candidate_executor_id),
            )
            if upd_executor.rowcount != 1:
                await conn.execute(
                    """
                    UPDATE orders
                    SET status = 'approved_open',
                        candidate_executor_id = NULL,
                        payment_deadline_at = NULL,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ? AND status = 'waiting_customer_payment'
                    """,
                    (order_id,),
                )
                await conn.commit()
                return False, "candidate_unavailable", amount


            try:
                ensure_order_transition(order_dict["status"], "in_progress")
            except InvalidOrderTransitionError:
                await conn.rollback()
                return False, "bad_status", amount

            upd_order = await conn.execute(
                """
                UPDATE orders
                SET status = 'in_progress',
                    executor_id = candidate_executor_id,
                    candidate_executor_id = NULL,
                    payment_deadline_at = NULL,
                    hold_amount = ?,
                    actual_hold_amount = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ? AND status = 'waiting_customer_payment'
                """
                ,
                (amount, amount, order_id),
            )
            if upd_order.rowcount != 1:
                await conn.execute(
                    "UPDATE users SET active_order_id = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ? AND active_order_id = ?",
                    (candidate_executor_id, order_id),
                )
                await conn.rollback()
                return False, "bad_status", amount

            await conn.execute(
                "INSERT INTO transactions (user_id, order_id, amount, type, comment) VALUES (?, ?, ?, 'hold', ?)",
                (user["id"], order_id, amount, "Оплата задания после отклика исполнителя"),
            )
            await conn.commit()
            return True, "ok", amount
        except Exception:
            await conn.rollback()
            raise

async def cancel_order_by_customer(tg_id: int, order_id: int) -> tuple[bool, str]:
    user = await ensure_user(tg_id)
    async with db_pool.acquire() as conn:
        await conn.execute("BEGIN IMMEDIATE")
        try:
            order = await conn.execute(
                "SELECT * FROM orders WHERE id = ? AND customer_id = ?",
                (order_id, user["id"])
            )
            order_row = await order.fetchone()
            if not order_row:
                await conn.rollback()
                return False, "not_found"

            order_dict = dict(order_row)
            status = order_dict["status"]
            refund_amount = int(order_dict["actual_hold_amount"] or 0)

            if status in {"pending_review", "approved_open", "waiting_customer_payment"}:
                cur = await conn.execute(
                    """
                    UPDATE orders
                    SET status = 'cancelled',
                        hold_amount = 0,
                        actual_hold_amount = 0,
                        candidate_executor_id = NULL,
                        payment_deadline_at = NULL,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                      AND status IN ('pending_review', 'approved_open', 'waiting_customer_payment')
                    """,
                    (order_id,),
                )
                if cur.rowcount != 1:
                    await conn.rollback()
                    return False, "already_cancelled"
                await conn.commit()
                return True, "ok_no_refund"

            if status == "in_progress" and order_dict["executor_id"] is None:
                cur = await conn.execute(
                    "UPDATE orders SET status = 'cancelled', hold_amount = 0, actual_hold_amount = 0, updated_at = CURRENT_TIMESTAMP WHERE id = ? AND status = 'in_progress' AND executor_id IS NULL",
                    (order_id,),
                )
                if cur.rowcount != 1:
                    await conn.rollback()
                    return False, "already_cancelled"

                if refund_amount > 0:
                    await conn.execute(
                        "UPDATE users SET balance = balance + ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                        (refund_amount, user["id"]),
                    )
                    await conn.execute(
                        "INSERT INTO transactions (user_id, order_id, amount, type, comment) VALUES (?, ?, ?, 'refund', ?)",
                        (user["id"], order_id, refund_amount, "Отмена оплаченного задания до взятия"),
                    )
                await conn.commit()
                return True, "ok_with_refund"

            await conn.rollback()
            return False, "support_only"
        except Exception:
            await conn.rollback()
            raise

async def change_price_for_customer(tg_id: int, order_id: int, new_price: int) -> tuple[bool, str]:
    user = await ensure_user(tg_id)
    async with db_pool.acquire() as conn:
        await conn.execute("BEGIN IMMEDIATE")
        try:
            order = await conn.execute(
                "SELECT * FROM orders WHERE id = ? AND customer_id = ?",
                (order_id, user["id"])
            )
            order_row = await order.fetchone()
            if not order_row:
                await conn.rollback()
                return False, "not_found"
            order_dict = dict(order_row)
            if order_dict["status"] != "approved_open" or order_dict["executor_id"] is not None or order_dict.get("candidate_executor_id") is not None:
                await conn.rollback()
                return False, "bad_status"

            old_price = int(order_dict["price_amount"])
            await conn.execute(
                "UPDATE orders SET price_amount = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? AND status = 'approved_open' AND candidate_executor_id IS NULL",
                (new_price, order_id),
            )
            await conn.commit()
            return True, "ok"
        except Exception:
            await conn.rollback()
            raise

async def take_order(executor_tg_id: int, order_id: int) -> tuple[bool, str]:
    executor = await ensure_user(executor_tg_id)
    if not executor["is_executor_profile_created"] or not executor["is_executor_approved"] or executor["is_blocked"]:
        return False, "not_allowed"

    async with db_pool.acquire() as conn:
        await conn.execute("BEGIN IMMEDIATE")
        try:
            if executor.get("active_order_id"):
                await conn.rollback()
                return False, "busy"

            cur = await conn.execute(
                """
                SELECT id FROM orders
                WHERE candidate_executor_id = ? AND status = 'waiting_customer_payment'
                LIMIT 1
                """,
                (executor["id"],),
            )
            existing_candidate = await cur.fetchone()
            if existing_candidate:
                await conn.rollback()
                return False, "waiting_payment"

            cur = await conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,))
            order_row = await cur.fetchone()
            if not order_row:
                await conn.rollback()
                return False, "not_found"

            order_dict = dict(order_row)
            if order_dict["status"] != "approved_open" or order_dict["executor_id"] is not None or order_dict.get("candidate_executor_id") is not None:
                await conn.rollback()
                return False, "taken"

            try:
                ensure_order_transition(order_dict["status"], "waiting_customer_payment")
            except InvalidOrderTransitionError:
                await conn.rollback()
                return False, "taken"

            deadline_at = (datetime.now() + timedelta(minutes=CANDIDATE_PAYMENT_TIMEOUT_MINUTES)).isoformat(timespec="seconds")
            cur_order = await conn.execute(
                """
                UPDATE orders
                SET status = 'waiting_customer_payment',
                    candidate_executor_id = ?,
                    payment_deadline_at = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                  AND status = 'approved_open'
                  AND executor_id IS NULL
                  AND candidate_executor_id IS NULL
                """,
                (executor["id"], deadline_at, order_id),
            )
            if cur_order.rowcount != 1:
                await conn.rollback()
                return False, "taken"

            await conn.commit()
        except Exception:
            await conn.rollback()
            raise

    customer = await get_user_by_id(order_dict["customer_id"])
    if customer:
        await notify_user(
            customer["telegram_id"],
            f"📦 По заданию #{order_id} есть исполнитель, готовый взять заказ.\n\n"
            f"Чтобы закрепить его, оплатите задание в течение {CANDIDATE_PAYMENT_TIMEOUT_MINUTES} минут.\n"
            f"Пока оплата не внесена, задание не подтверждено.\n"
            f"Дедлайн оплаты: {format_deadline(deadline_at)}.",
            InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💳 Оплатить заказ", callback_data=f"cust:pay:{order_id}")],
                [InlineKeyboardButton(text="❌ Отменить заказ", callback_data=f"cust:cancel:ask:{order_id}")],
                [InlineKeyboardButton(text="⚠️ Проблема по заданию", callback_data=f"support:order:{order_id}")],
            ]),
        )

    await notify_user(
        executor["telegram_id"],
        f"📦 Вы выбрали задание #{order_id}.\n\n"
        "Сейчас ждём оплату от заказчика.\n"
        "Заказ ещё не закреплён за вами.\n"
        "Начинать выполнение пока нельзя.",
        InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📌 Моё задание", callback_data="executor:my_order")],
            [InlineKeyboardButton(text="⚠️ Проблема по заданию", callback_data=f"support:order:{order_id}")],
            [InlineKeyboardButton(text="💬 Связаться с оператором", url=support_url())],
        ])
    )
    return True, "ok"

async def executor_has_result_proof(executor_user_id: int, order_id: int) -> bool:
    proof = await get_latest_result_proof(order_id, executor_user_id)
    return proof is not None


async def mark_result_sent(executor_tg_id: int, order_id: int) -> tuple[bool, str]:
    executor = await ensure_user(executor_tg_id)
    async with db_pool.acquire() as conn:
        order = await conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,))
        order_row = await order.fetchone()
        if not order_row or order_row["executor_id"] != executor["id"]:
            return False, "not_found"
        if order_row["status"] != "in_progress":
            return False, "bad_status"

    has_proof = await executor_has_result_proof(executor["id"], order_id)
    if not has_proof:
        return False, "no_proof"

    try:
        ensure_order_transition("in_progress", "done_waiting_confirmation")
    except InvalidOrderTransitionError:
        return False, "bad_transition"

    async with db_pool.acquire() as conn:
        changed = await conn.execute(
            "UPDATE orders SET status = 'done_waiting_confirmation', result_sent_to_customer_at = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ? AND status = 'in_progress'",
            (order_id,),
        )
        await conn.commit()
        if changed.rowcount != 1:
            return False, "bad_status"
    return True, "ok"

async def send_result_to_customer(order_id: int) -> bool:
    async with db_pool.acquire() as conn:
        order = await conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,))
        order_row = await order.fetchone()
        if not order_row or order_row["status"] != "done_waiting_confirmation":
            return False
        order_dict = dict(order_row)

    executor_id = order_dict.get("executor_id")
    if not executor_id:
        return False

    proof = await get_latest_result_proof(order_id, executor_id)
    if not proof:
        return False

    customer = await get_user_by_id(order_dict["customer_id"])
    if not customer:
        return False

    proof_text = (proof.get("text") or "").strip()
    proof_file_id = proof.get("file_id")

    intro_text = f"📦 Результат по заданию #{order_id}"
    if proof_text:
        intro_text += f"\n\nКомментарий исполнителя:\n{escape_html(proof_text)}"

    try:
        if proof_file_id:
            await bot.send_photo(customer["telegram_id"], proof_file_id, caption=intro_text)
        else:
            await bot.send_message(customer["telegram_id"], intro_text)
    except Exception as e:
        print(f"Не удалось отправить proof заказчику по заданию {order_id}: {e}")
        return False

    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE orders
            SET result_sent_to_customer_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ? AND status = 'done_waiting_confirmation'
            """,
            (order_id,),
        )
        await conn.commit()

    await notify_user(
        customer["telegram_id"],
        "Проверьте proof результата выше.\n\n"
        "Если всё в порядке — подтвердите выполнение.\n"
        "После подтверждения задание закроется, а деньги из холда уйдут исполнителю.\n\n"
        "Если что-то не так — нажмите «⚠️ Проблема по заданию». Тогда деньги останутся в холде до решения поддержки.",
        InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"cust:done:ask:{order_id}")],
            [InlineKeyboardButton(text="⚠️ Проблема по заданию", callback_data=f"support:order:{order_id}")],
        ])
    )
    return True


async def finalize_order(order_id: int, actor: str, actor_tg_id: Optional[int] = None) -> tuple[bool, str]:
    if actor == "customer":
        if actor_tg_id is None:
            return False, "forbidden"
        try:
            await assert_customer_order_access(order_id, actor_tg_id)
        except AccessDeniedError as e:
            return False, str(e)

    async with db_pool.acquire() as conn:
        order = await conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,))
        order_row = await order.fetchone()
        if not order_row:
            return False, "order_not_found"
        order_dict = dict(order_row)
        if order_dict["status"] != "done_waiting_confirmation":
            return False, f"wrong_status_{order_dict['status']}"
        if not order_dict["executor_id"]:
            return False, "no_executor"
        if actor == "customer" and not order_dict.get("result_sent_to_customer_at"):
            return False, "proof_not_sent"

        executor = await get_user_by_id(order_dict["executor_id"])
        if not executor:
            return False, "executor_not_found"

        payout = int(order_dict["actual_hold_amount"] or 0)
        if payout <= 0:
            return False, "zero_payout"

        try:
            ensure_order_transition(order_dict["status"], "completed")
        except InvalidOrderTransitionError:
            return False, "bad_transition"

        await conn.execute("BEGIN IMMEDIATE")
        try:
            cur = await conn.execute(
                "UPDATE orders SET status = 'completed', hold_amount = 0, actual_hold_amount = 0, updated_at = CURRENT_TIMESTAMP WHERE id = ? AND status = 'done_waiting_confirmation'",
                (order_id,),
            )
            if cur.rowcount != 1:
                await conn.rollback()
                return False, "already_processed"

            await conn.execute(
                "UPDATE users SET balance = balance + ?, active_order_id = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (payout, executor["id"]),
            )
            await conn.execute(
                "INSERT INTO transactions (user_id, order_id, amount, type, comment) VALUES (?, ?, ?, 'payout', ?)",
                (executor["id"], order_id, payout, f"Завершение задания ({actor})"),
            )
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise

    await notify_user(
        executor["telegram_id"],
        f"💰 Деньги за задание #{order_id} зачислены\n\nСумма: {format_price(payout)}",
        main_back_kb(),
    )
    return True, "ok"


async def force_finalize_order(order_id: int, actor: str) -> tuple[bool, str]:
    async with db_pool.acquire() as conn:
        order = await conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,))
        order_row = await order.fetchone()
        if not order_row:
            return False, "order_not_found"

        allowed_statuses = {"in_progress", "done_waiting_confirmation", "manual_review"}
        if order_row["status"] not in allowed_statuses:
            return False, f"wrong_status_{order_row['status']}"
        if not order_row["executor_id"]:
            return False, "no_executor"

        executor = await get_user_by_id(order_row["executor_id"])
        if not executor:
            return False, "executor_not_found"

        payout = int(order_row["actual_hold_amount"] or 0)
        if payout <= 0:
            return False, "zero_payout"

        await conn.execute("BEGIN IMMEDIATE")
        try:
            cur = await conn.execute(
                "UPDATE orders SET status = 'completed', hold_amount = 0, actual_hold_amount = 0, executor_id = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? AND status IN ('in_progress', 'done_waiting_confirmation', 'manual_review')",
                (order_row["executor_id"], order_id),
            )
            if cur.rowcount != 1:
                await conn.rollback()
                return False, "already_processed"

            await conn.execute(
                "UPDATE users SET balance = balance + ?, active_order_id = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (payout, executor["id"]),
            )
            await conn.execute(
                "INSERT INTO transactions (user_id, order_id, amount, type, comment) VALUES (?, ?, ?, 'payout', ?)",
                (executor["id"], order_id, payout, f"Принудительное завершение ({actor})"),
            )
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise

    await notify_user(
        executor["telegram_id"],
        f"💰 Деньги за задание #{order_id} зачислены\n\nСумма: {format_price(payout)}",
        main_back_kb(),
    )
    customer = await get_user_by_id(order_row["customer_id"])
    await notify_user(
        customer["telegram_id"],
        f"📦 Задание #{order_id}\n\nЗадание завершено администратором",
        main_back_kb(),
    )
    return True, "ok"

async def return_order_to_pool(order_id: int) -> bool:
    async with db_pool.acquire() as conn:
        order = await conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,))
        order_row = await order.fetchone()
        if not order_row or order_row["status"] != "in_progress":
            return False

        await conn.execute("BEGIN IMMEDIATE")
        try:
            if order_row["executor_id"]:
                await conn.execute(
                    "UPDATE users SET active_order_id = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ? AND active_order_id = ?",
                    (order_row["executor_id"], order_id),
                )
                await conn.execute(
                    "INSERT INTO transactions (user_id, order_id, amount, type, comment) VALUES (?, ?, 0, 'admin_return_pool', ?)",
                    (order_row["executor_id"], order_id, f"Возврат задания #{order_id} в пул"),
                )

            cur = await conn.execute(
                """
                UPDATE orders
                SET executor_id = NULL,
                    candidate_executor_id = NULL,
                    payment_deadline_at = NULL,
                    status = 'approved_open',
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ? AND status = 'in_progress'
                """,
                (order_id,),
            )
            if cur.rowcount != 1:
                await conn.rollback()
                return False

            await conn.commit()
        except Exception:
            await conn.rollback()
            raise

    customer = await get_user_by_id(order_row["customer_id"])
    await notify_user(
        customer["telegram_id"],
        f"📦 Задание #{order_id}\n\nИсполнитель больше не выполняет задание.\n"
        "Деньги остаются в холде.\n\n👇 Ищем нового исполнителя",
        main_back_kb(),
    )
    return True

async def create_withdrawal(tg_id: int, amount: int, phone: str, bank_name: str) -> tuple[bool, str]:
    user = await ensure_user(tg_id)
    async with db_pool.acquire() as conn:
        existing = await conn.execute(
            "SELECT id FROM withdrawal_requests WHERE user_id = ? AND status = 'pending'",
            (user["id"],)
        )
        if await existing.fetchone():
            return False, "pending_exists"

        await conn.execute("BEGIN IMMEDIATE")
        try:
            cur = await conn.execute(
                "UPDATE users SET balance = balance - ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? AND balance >= ?",
                (amount, user["id"], amount),
            )
            if cur.rowcount != 1:
                await conn.rollback()
                return False, "no_balance"

            await conn.execute(
                "INSERT INTO withdrawal_requests (user_id, requested_amount, phone, bank_name, status) VALUES (?, ?, ?, ?, 'pending')",
                (user["id"], amount, phone, bank_name),
            )
            await conn.execute(
                "INSERT INTO transactions (user_id, amount, type, comment) VALUES (?, ?, 'withdrawal_hold', ?)",
                (user["id"], amount, "Создание заявки на вывод"),
            )
            await conn.commit()
            return True, "ok"
        except Exception:
            await conn.rollback()
            raise


async def process_withdrawal(withdrawal_id: int, approve: bool, comment: str = "") -> tuple[bool, str]:
    async with db_pool.acquire() as conn:
        req = await conn.execute("SELECT * FROM withdrawal_requests WHERE id = ?", (withdrawal_id,))
        req_row = await req.fetchone()
        if not req_row or req_row["status"] != "pending":
            return False, "bad_status"

        user = await get_user_by_id(req_row["user_id"])
        await conn.execute("BEGIN IMMEDIATE")
        try:
            if approve:
                cur = await conn.execute(
                    "UPDATE withdrawal_requests SET status = 'paid', comment = ?, processed_at = CURRENT_TIMESTAMP WHERE id = ? AND status = 'pending'",
                    (comment, withdrawal_id),
                )
                if cur.rowcount != 1:
                    await conn.rollback()
                    return False, "bad_status"

                await conn.execute(
                    "INSERT INTO transactions (user_id, amount, type, comment) VALUES (?, ?, 'withdrawal_paid', ?)",
                    (user["id"], req_row["requested_amount"], f"Выплата заявки #{withdrawal_id}"),
                )
                await conn.commit()
                await notify_user(
                    user["telegram_id"],
                    f"💸 Заявка на вывод #{withdrawal_id} обработана\n\nСумма: {format_price(req_row['requested_amount'])}",
                    main_back_kb(),
                )
                return True, "ok"
            else:
                cur = await conn.execute(
                    "UPDATE withdrawal_requests SET status = 'rejected', comment = ?, processed_at = CURRENT_TIMESTAMP WHERE id = ? AND status = 'pending'",
                    (comment, withdrawal_id),
                )
                if cur.rowcount != 1:
                    await conn.rollback()
                    return False, "bad_status"

                await conn.execute(
                    "UPDATE users SET balance = balance + ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (req_row["requested_amount"], user["id"]),
                )
                await conn.execute(
                    "INSERT INTO transactions (user_id, amount, type, comment) VALUES (?, ?, 'withdrawal_refund', ?)",
                    (user["id"], req_row["requested_amount"], "Отклонение заявки на вывод"),
                )
                await conn.commit()
        except Exception:
            await conn.rollback()
            raise

    text = f"💸 Заявка на вывод #{withdrawal_id} отклонена"
    if comment:
        text += f"\n\nПричина:\n{escape_html(comment)}"
    await notify_user(user["telegram_id"], text, main_back_kb())
    return True, "ok"


# ------------------- SUPPORT FLOW -------------------
async def get_open_ticket(user_id: int, order_id: Optional[int] = None) -> Optional[dict[str, Any]]:
    async with db_pool.acquire() as conn:
        if order_id is not None:
            cur = await conn.execute(
                "SELECT * FROM support_tickets WHERE user_id = ? AND order_id = ? AND status = 'open' ORDER BY created_at DESC LIMIT 1",
                (user_id, order_id)
            )
        else:
            cur = await conn.execute(
                "SELECT * FROM support_tickets WHERE user_id = ? AND order_id IS NULL AND status = 'open' ORDER BY created_at DESC LIMIT 1",
                (user_id,)
            )
        row = await cur.fetchone()
        return dict(row) if row else None


async def get_or_create_open_ticket(user_id: int, telegram_id: int, role: str, order_id: Optional[int] = None) -> int:
    async with support_ticket_lock(user_id, order_id):
        async with db_pool.acquire() as conn:
            await conn.execute("BEGIN IMMEDIATE")
            try:
                if order_id is not None:
                    cur = await conn.execute(
                        "SELECT id FROM support_tickets WHERE user_id = ? AND order_id = ? AND status = 'open' ORDER BY created_at DESC LIMIT 1",
                        (user_id, order_id)
                    )
                    row = await cur.fetchone()
                    if row:
                        await conn.execute(
                            "UPDATE support_tickets SET telegram_id = ?, role = ? WHERE id = ?",
                            (telegram_id, role, row["id"])
                        )
                        await conn.commit()
                        return row["id"]

                    cur = await conn.execute(
                        "INSERT INTO support_tickets (user_id, telegram_id, role, order_id, status) VALUES (?, ?, ?, ?, 'open')",
                        (user_id, telegram_id, role, order_id)
                    )
                    ticket_id = cur.lastrowid
                    await conn.commit()
                    return ticket_id

                cur = await conn.execute(
                    "SELECT id FROM support_tickets WHERE user_id = ? AND order_id IS NULL AND status = 'open' ORDER BY created_at DESC LIMIT 1",
                    (user_id,)
                )
                row = await cur.fetchone()
                if row:
                    await conn.execute(
                        "UPDATE support_tickets SET telegram_id = ?, role = ? WHERE id = ?",
                        (telegram_id, role, row["id"])
                    )
                    await conn.commit()
                    return row["id"]

                cur = await conn.execute(
                    "INSERT INTO support_tickets (user_id, telegram_id, role, order_id, status) VALUES (?, ?, ?, NULL, 'open')",
                    (user_id, telegram_id, role)
                )
                ticket_id = cur.lastrowid
                await conn.commit()
                return ticket_id
            except Exception:
                await conn.rollback()
                raise


async def add_support_message(
    ticket_id: int,
    sender_type: str,
    sender_id: int,
    text: Optional[str] = None,
    file_id: Optional[str] = None,
    message_type: str = "support",
):
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO support_messages (ticket_id, sender_type, sender_id, text, file_id, message_type) VALUES (?, ?, ?, ?, ?, ?)",
            (ticket_id, sender_type, sender_id, text, file_id, message_type)
        )
        await conn.commit()


async def notify_admins_about_ticket(
    ticket_id: int,
    user_telegram_id: int,
    role: str,
    order_id: Optional[int] = None,
    message_text: str = "",
    file_id: Optional[str] = None,
    message_type: str = "support",
):
    text = build_support_admin_text(
        ticket_id=ticket_id,
        user_telegram_id=user_telegram_id,
        role=role,
        order_id=order_id,
        message_text=message_text,
        has_photo=bool(file_id),
        message_type=message_type,
    )
    for admin_id in SETTINGS.admin_ids:
        if file_id:
            try:
                await bot.send_photo(admin_id, file_id, caption=text)
            except Exception as e:
                print(f"Не удалось отправить фото админу {admin_id}: {e}")
                await notify_user(admin_id, text + f"\n\nФото ID: {escape_html(file_id)}")
        else:
            await notify_user(admin_id, text)


# ------------------- WATCHDOG ЗАВИСШИХ СТАТУСОВ -------------------
async def expire_waiting_customer_payment_order(order_id: int) -> bool:
    async with order_lock(order_id):
        async with db_pool.acquire() as conn:
            await conn.execute("BEGIN IMMEDIATE")
            try:
                cur = await conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,))
                order_row = await cur.fetchone()
                if not order_row or order_row["status"] != "waiting_customer_payment":
                    await conn.rollback()
                    return False

                order_dict = dict(order_row)
                deadline = order_dict.get("payment_deadline_at")
                if not deadline:
                    await conn.rollback()
                    return False
                try:
                    expired = datetime.now() >= datetime.fromisoformat(deadline)
                except ValueError:
                    expired = True
                if not expired:
                    await conn.rollback()
                    return False

                upd = await conn.execute(
                    """
                    UPDATE orders
                    SET status = 'approved_open',
                        candidate_executor_id = NULL,
                        payment_deadline_at = NULL,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ? AND status = 'waiting_customer_payment'
                    """,
                    (order_id,),
                )
                if upd.rowcount != 1:
                    await conn.rollback()
                    return False
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise

    customer = await get_user_by_id(order_dict["customer_id"])
    candidate = await get_user_by_id(order_dict["candidate_executor_id"]) if order_dict.get("candidate_executor_id") else None
    if customer:
        await notify_user(
            customer["telegram_id"],
            f"📦 Задание #{order_id}\n\n"
            "Время на оплату истекло.\n"
            "Исполнитель больше не закреплён.\n"
            "Заказ снова открыт для поиска исполнителя.",
            InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📜 Мои задания", callback_data="customer:orders")],
                [InlineKeyboardButton(text="⚠️ Проблема по заданию", callback_data=f"support:order:{order_id}")],
            ]),
        )
    if candidate:
        await notify_user(
            candidate["telegram_id"],
            f"📦 Задание #{order_id}\n\n"
            "Заказчик не подтвердил оплату вовремя.\n"
            "Бронь снята.\n"
            "Вы можете взять другое задание.",
            InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📋 Доступные задания", callback_data="executor:available")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="profile:open")],
            ]),
        )
    return True

async def expire_approved_open_order(order_id: int) -> bool:
    async with order_lock(order_id):
        async with db_pool.acquire() as conn:
            await conn.execute("BEGIN IMMEDIATE")
            try:
                cur = await conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,))
                order_row = await cur.fetchone()
                if not order_row or order_row["status"] != "approved_open":
                    await conn.rollback()
                    return False

                order_dict = dict(order_row)
                updated_at = order_dict.get("updated_at") or order_dict.get("created_at")
                if not updated_at:
                    await conn.rollback()
                    return False

                try:
                    expired = datetime.now() >= datetime.fromisoformat(updated_at) + timedelta(hours=SETTINGS.active_order_timeout_hours)
                except ValueError:
                    expired = True

                if not expired:
                    await conn.rollback()
                    return False

                upd = await conn.execute(
                    """
                    UPDATE orders
                    SET status = 'expired_unassigned',
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                      AND status = 'approved_open'
                      AND executor_id IS NULL
                      AND candidate_executor_id IS NULL
                    """,
                    (order_id,),
                )
                if upd.rowcount != 1:
                    await conn.rollback()
                    return False

                await conn.commit()
            except Exception:
                await conn.rollback()
                raise

    customer = await get_user_by_id(order_dict["customer_id"])
    if customer:
        await notify_user(
            customer["telegram_id"],
            f"📦 Задание #{order_id}\n\n"
            "По заданию не нашёлся исполнитель вовремя.\n"
            "Заказ автоматически закрыт.\n"
            "Вы можете разместить его заново или изменить условия.",
            InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📝 Разместить заново", callback_data="customer:create_order")],
                [InlineKeyboardButton(text="💬 Связаться с оператором", url=support_url())],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="customer:orders")],
            ]),
        )
    return True

async def requeue_waiting_client_confirmation(order_id: int) -> bool:
    async with order_lock(order_id):
        async with db_pool.acquire() as conn:
            await conn.execute("BEGIN IMMEDIATE")
            try:
                cur = await conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,))
                order_row = await cur.fetchone()
                if not order_row or order_row["status"] != "done_waiting_confirmation":
                    await conn.rollback()
                    return False

                try:
                    ensure_order_transition(order_row["status"], "manual_review")
                except InvalidOrderTransitionError:
                    await conn.rollback()
                    return False

                upd = await conn.execute(
                    """
                    UPDATE orders
                    SET status = 'manual_review',
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ? AND status = 'done_waiting_confirmation'
                    """,
                    (order_id,),
                )
                if upd.rowcount != 1:
                    await conn.rollback()
                    return False

                await conn.commit()
                order_dict = dict(order_row)
            except Exception:
                await conn.rollback()
                raise

    customer = await get_user_by_id(order_dict["customer_id"])
    executor = await get_user_by_id(order_dict["executor_id"]) if order_dict["executor_id"] else None

    if customer:
        await notify_user(
            customer["telegram_id"],
            f"📦 Задание #{order_id}\n\n"
            "Мы не получили вовремя подтверждение, поэтому задание передано на ручную проверку в поддержку.\n"
            "Деньги пока остаются в холде до решения.",
            main_back_kb(),
        )

    if executor:
        await notify_user(
            executor["telegram_id"],
            f"📦 Задание #{order_id}\n\n"
            "Заказчик долго не подтвердил результат, поэтому задание передано на ручную проверку.\n"
            "Деньги пока остаются в холде до решения поддержки.",
            main_back_kb(),
        )

    for admin_id in SETTINGS.admin_ids:
        await notify_user(
            admin_id,
            f"📦 Задание #{order_id}\n\n"
            "Заказчик не подтвердил результат вовремя.\n"
            "Статус переведён в ручную проверку.",
            admin_main_kb(),
        )

    return True

async def process_expired_orders_once():
    async with db_pool.acquire() as conn:
        approved_open_modifier = f"-{SETTINGS.active_order_timeout_hours} hours"
        confirm_modifier = f"-{SETTINGS.client_confirmation_timeout_hours} hours"

        cur = await conn.execute(
            "SELECT id FROM orders WHERE status = 'approved_open' AND executor_id IS NULL AND candidate_executor_id IS NULL AND updated_at <= datetime('now', ?)",
            (approved_open_modifier,),
        )
        approved_open_rows = await cur.fetchall()

        cur = await conn.execute(
            "SELECT id FROM orders WHERE status = 'waiting_customer_payment' AND payment_deadline_at IS NOT NULL"
        )
        waiting_payment_rows = await cur.fetchall()

        cur = await conn.execute(
            "SELECT id FROM orders WHERE status = 'done_waiting_confirmation' AND result_sent_to_customer_at IS NOT NULL AND result_sent_to_customer_at <= datetime('now', ?)",
            (confirm_modifier,)
        )
        waiting_rows = await cur.fetchall()

    for row in approved_open_rows:
        try:
            await expire_approved_open_order(row["id"])
        except Exception as e:
            print(f"Ошибка expire_approved_open_order({row['id']}): {e}")

    for row in waiting_payment_rows:
        try:
            await expire_waiting_customer_payment_order(row["id"])
        except Exception as e:
            print(f"Ошибка expire_waiting_customer_payment_order({row['id']}): {e}")

    for row in waiting_rows:
        try:
            await requeue_waiting_client_confirmation(row["id"])
        except Exception as e:
            print(f"Ошибка requeue_waiting_client_confirmation({row['id']}): {e}")

async def status_watchdog_loop():
    while True:
        try:
            await process_expired_orders_once()
        except Exception as e:
            print(f"Ошибка status_watchdog_loop: {e}")
        await asyncio.sleep(60)

# ------------------- ОБЩАЯ НАВИГАЦИЯ -------------------
@router.message(CommandStart())
async def start_cmd(message: Message, state: FSMContext):
    await ensure_user(message.from_user.id)
    await state.clear()
    await message.answer("🤝 НаВсеРуки\n\n👇 Выберите действие", reply_markup=main_menu_kb())


@router.callback_query(F.data == "common:main")
async def common_main(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await ensure_user(callback.from_user.id)
    await callback.message.edit_text("🤝 НаВсеРуки\n\n👇 Выберите действие", reply_markup=main_menu_kb())
    await callback.answer()


@router.callback_query(F.data == "common:how")
async def common_how(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text(
        "❓ Как это работает\n\n"
        "1. Заказчик создаёт задание\n"
        "2. Мы проверяем и одобряем его\n"
        "3. После одобрения задание видят исполнители\n"
        "4. Исполнитель нажимает «Готов взять»\n"
        "5. Заказчик оплачивает задание, чтобы закрепить исполнителя\n"
        "6. После выполнения заказчик подтверждает результат\n"
        "7. Деньги уходят исполнителю\n\n"
        "Если возникает спор, деньги остаются в холде до решения поддержки.",
        reply_markup=how_it_works_kb(),
    )
    await callback.answer()


@router.callback_query(F.data == "global:cancel")
async def global_cancel(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("Действие отменено.\n\n👇 Выберите действие", reply_markup=main_menu_kb())
    await callback.answer()


@router.callback_query(F.data == "profile:open")
async def profile_open(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    user = await ensure_user(callback.from_user.id)
    await callback.message.edit_text(render_profile_text(user), reply_markup=profile_keyboard(user))
    await callback.answer()


@router.callback_query(F.data == "balance:open")
async def balance_open(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    user = await ensure_user(callback.from_user.id)
    await callback.message.edit_text(
        f"💰 Баланс\n\nТекущий баланс: {format_price(user['balance'])}",
        reply_markup=balance_kb(),
    )
    await callback.answer()


# ------------------- SUPPORT CALLBACK -------------------
@router.callback_query(F.data.startswith("support:"))
async def support_callback(callback: CallbackQuery, state: FSMContext):
    parts = (callback.data or "").split(":")
    user = await ensure_user(callback.from_user.id)

    role = "customer"
    if user.get("is_executor_profile_created") and user.get("is_executor_approved"):
        role = "executor"
    elif user.get("is_executor_profile_created") and not user.get("is_executor_approved"):
        role = "executor_pending"

    order_id: Optional[int] = None
    support_message_type = "support"
    prompt_text = build_support_prompt_text(None, None)

    if len(parts) >= 3 and parts[1] in {"order", "proof"} and parts[2].isdigit():
        order_id = int(parts[2])

    if parts[1] == "proof":
        if order_id is None:
            await callback.answer("Некорректный заказ", show_alert=True)
            return
        async with db_pool.acquire() as conn:
            cur = await conn.execute(
                """
                SELECT * FROM orders
                WHERE id = ?
                  AND executor_id = ?
                  AND status IN ('in_progress', 'done_waiting_confirmation', 'manual_review')
                """,
                (order_id, user["id"]),
            )
            if not await cur.fetchone():
                await callback.answer("Proof можно отправить только по своему активному заданию", show_alert=True)
                return
        support_message_type = "result_proof"
        existing_ticket = await get_open_ticket(user["id"], order_id)
        prompt_text = (
            f"📤 Proof результата по заданию #{order_id}\n\n"
            "Отправьте текст, фото или текст с фото.\n"
            "Это сообщение увидит поддержка, а затем оно будет показано заказчику перед подтверждением."
        )
    else:
        if order_id is not None:
            participant_order = await get_order_participant_for_user(user["id"], order_id)
            if not participant_order:
                await callback.answer("Нельзя открыть обращение по чужому заданию", show_alert=True)
                return
        existing_ticket = await get_open_ticket(user["id"], order_id)
        prompt_text = build_support_prompt_text(order_id, existing_ticket["id"] if existing_ticket else None)

    await state.clear()
    await state.set_state(SupportFlow.waiting_for_message)
    await state.update_data(
        order_id=order_id,
        support_ticket_id=existing_ticket["id"] if existing_ticket else None,
        support_role=role,
        support_message_type=support_message_type,
    )

    await callback.message.edit_text(
        prompt_text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="global:cancel")]
        ])
    )
    await callback.answer()


@router.message(SupportFlow.waiting_for_message)
async def support_message_received(message: Message, state: FSMContext):
    data = await state.get_data()
    order_id = data.get("order_id")
    message_type = data.get("support_message_type", "support")
    user = await ensure_user(message.from_user.id)

    role = "customer"
    if user.get("is_executor_profile_created") and user.get("is_executor_approved"):
        role = "executor"
    elif user.get("is_executor_profile_created") and not user.get("is_executor_approved"):
        role = "executor_pending"

    text_value = (message.text or message.caption or "").strip()
    file_id = message.photo[-1].file_id if message.photo else None

    if not text_value and not file_id:
        await message.answer("Нужно отправить текст или фото.")
        return

    if message_type == "result_proof":
        async with db_pool.acquire() as conn:
            cur = await conn.execute(
                """
                SELECT id FROM orders
                WHERE id = ?
                  AND executor_id = ?
                  AND status IN ('in_progress', 'done_waiting_confirmation', 'manual_review')
                """,
                (order_id, user["id"]),
            )
            if not await cur.fetchone():
                await state.clear()
                await message.answer("Proof можно отправить только по своему активному заданию.", reply_markup=main_menu_kb())
                return

    ticket_id = await get_or_create_open_ticket(user["id"], message.from_user.id, role, order_id)
    await add_support_message(ticket_id, "user", user["id"], text_value, file_id, message_type=message_type)
    await notify_admins_about_ticket(
        ticket_id,
        message.from_user.id,
        role,
        order_id,
        text_value,
        file_id,
        message_type=message_type,
    )

    await state.clear()

    if message_type == "result_proof":
        await message.answer(
            f"✅ Proof результата по заданию #{order_id} отправлен.\n\n"
            "Теперь нажмите «✅ Я отправил результат».",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📌 Моё задание", callback_data="executor:my_order")]
            ])
        )
        return

    await message.answer(
        build_support_sent_text(ticket_id, order_id),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="common:main")]
        ])
    )


@router.message(Command("reply"))
async def admin_reply_command(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("Нет прав")
        return

    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 3:
        await message.answer("Использование: /reply <ticket_id> <текст ответа>")
        return

    try:
        ticket_id = int(parts[1])
    except ValueError:
        await message.answer("Некорректный ID тикета")
        return

    reply_text = parts[2].strip()
    if not reply_text:
        await message.answer("Текст ответа пустой")
        return

    async with db_pool.acquire() as conn:
        cur = await conn.execute("SELECT * FROM support_tickets WHERE id = ? AND status = 'open'", (ticket_id,))
        ticket_row = await cur.fetchone()
        if not ticket_row:
            await message.answer("Тикет не найден или уже закрыт")
            return

    await add_support_message(ticket_id, "admin", message.from_user.id, reply_text)

    user = await get_user_by_id(ticket_row["user_id"])
    if not user:
        await message.answer("Пользователь не найден")
        return

    response_text = f"💬 Ответ поддержки по обращению #{ticket_id}"
    if ticket_row["order_id"] is not None:
        response_text += f" по заданию #{ticket_row['order_id']}"
    response_text += f":\n\n{escape_html(reply_text)}"

    await notify_user(user["telegram_id"], response_text)
    await message.answer(
        f"Ответ отправлен.\n\n"
        f"Ticket ID: #{ticket_id}\n"
        f"Telegram ID: {user['telegram_id']}"
    )


@router.message(Command("close_ticket"))
async def admin_close_ticket(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("Нет прав")
        return

    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Использование: /close_ticket <ticket_id>")
        return

    try:
        ticket_id = int(parts[1])
    except ValueError:
        await message.answer("Некорректный ID тикета")
        return

    async with db_pool.acquire() as conn:
        cur = await conn.execute("SELECT * FROM support_tickets WHERE id = ? AND status = 'open'", (ticket_id,))
        ticket_row = await cur.fetchone()
        if not ticket_row:
            await message.answer("Тикет не найден или уже закрыт")
            return

        updated = await conn.execute(
            "UPDATE support_tickets SET status = 'closed', closed_at = CURRENT_TIMESTAMP WHERE id = ? AND status = 'open'",
            (ticket_id,)
        )
        await conn.commit()
        if updated.rowcount == 0:
            await message.answer("Тикет не найден или уже закрыт")
            return

    user = await get_user_by_id(ticket_row["user_id"])
    if user:
        close_text = f"🛠 Обращение #{ticket_id}"
        if ticket_row["order_id"] is not None:
            close_text += f" по заданию #{ticket_row['order_id']}"
        close_text += (
            " закрыто.\n\n"
            "Если у вас остались вопросы, вы можете снова нажать кнопку «📝 Написать в поддержку»."
        )
        await notify_user(user["telegram_id"], close_text)

    admin_text = f"Тикет #{ticket_id} закрыт."
    if ticket_row["order_id"] is not None:
        admin_text += f"\nOrder ID: #{ticket_row['order_id']}"
    admin_text += f"\nTelegram ID: {ticket_row['telegram_id']}"
    await message.answer(admin_text)


# ------------------- DEPOSIT FLOW -------------------
async def create_yookassa_payment(amount: int, user_telegram_id: int) -> tuple[str, str]:
    async with db_pool.acquire() as conn:
        cutoff = datetime.now() - timedelta(minutes=30)
        cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")
        temp_cutoff = datetime.now() - timedelta(minutes=5)
        temp_cutoff_str = temp_cutoff.strftime("%Y-%m-%d %H:%M:%S")
        cur = await conn.execute(
            """
            SELECT external_payment_id, payment_url
            FROM payments
            WHERE user_id = ?
              AND status = 'pending'
              AND amount = ?
              AND created_at > ?
              AND payment_url != ''
              AND (external_payment_id NOT LIKE 'temp_%' OR created_at > ?)
            ORDER BY id DESC
            LIMIT 1
            """,
            (user_telegram_id, amount, cutoff_str, temp_cutoff_str)
        )
        row = await cur.fetchone()
        if row:
            return row["external_payment_id"], row["payment_url"]

    async with user_lock(user_telegram_id):
        async with db_pool.acquire() as conn:
            cutoff = datetime.now() - timedelta(minutes=30)
            cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")
            temp_cutoff = datetime.now() - timedelta(minutes=5)
            temp_cutoff_str = temp_cutoff.strftime("%Y-%m-%d %H:%M:%S")
            cur = await conn.execute(
                """
                SELECT external_payment_id, payment_url
                FROM payments
                WHERE user_id = ?
                  AND status = 'pending'
                  AND amount = ?
                  AND created_at > ?
                  AND payment_url != ''
                  AND (external_payment_id NOT LIKE 'temp_%' OR created_at > ?)
                ORDER BY id DESC
                LIMIT 1
                """,
                (user_telegram_id, amount, cutoff_str, temp_cutoff_str)
            )
            row = await cur.fetchone()
            if row:
                return row["external_payment_id"], row["payment_url"]

            idempotence_key = str(uuid.uuid4())
            temp_payment_id = f"temp_{idempotence_key}"
            await conn.execute(
                "INSERT INTO payments (user_id, amount, provider, external_payment_id, idempotence_key, status, payment_url, raw_payload) VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)",
                (user_telegram_id, amount, "yookassa", temp_payment_id, idempotence_key, "", json.dumps({"idempotence_key": idempotence_key}))
            )
            await conn.commit()

        payload = {
            "amount": {"value": f"{amount}.00", "currency": "RUB"},
            "confirmation": {
                "type": "redirect",
                "return_url": SETTINGS.payments_return_url
            },
            "capture": True,
            "description": f"Пополнение баланса #{user_telegram_id}",
            "metadata": {
                "user_telegram_id": str(user_telegram_id),
                "amount": str(amount),
                "idempotence_key": idempotence_key
            }
        }
        auth = aiohttp.BasicAuth(SETTINGS.yookassa_shop_id, SETTINGS.yookassa_secret_key)
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://api.yookassa.ru/v3/payments",
                json=payload,
                auth=auth,
                headers={"Idempotence-Key": idempotence_key}
            ) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    async with db_pool.acquire() as conn:
                        await conn.execute(
                            "UPDATE payments SET status = 'failed', raw_payload = ? WHERE idempotence_key = ? AND status = 'pending'",
                            (json.dumps(payload), idempotence_key)
                        )
                        await conn.commit()
                    raise Exception(f"YooKassa error: {resp.status} {text}")

                data = await resp.json()
                external_payment_id = data["id"]
                payment_url = data["confirmation"]["confirmation_url"]

                async with db_pool.acquire() as conn:
                    cur = await conn.execute(
                        "UPDATE payments SET external_payment_id = ?, payment_url = ?, raw_payload = ? WHERE idempotence_key = ? AND status = 'pending'",
                        (external_payment_id, payment_url, json.dumps(payload), idempotence_key)
                    )
                    if cur.rowcount != 1:
                        await conn.execute(
                            "INSERT INTO payments (user_id, amount, provider, external_payment_id, idempotence_key, status, payment_url, raw_payload) VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)",
                            (user_telegram_id, amount, "yookassa", external_payment_id, idempotence_key, payment_url, json.dumps(payload))
                        )
                    await conn.commit()
                return external_payment_id, payment_url


@router.callback_query(F.data == "deposit:start")
async def deposit_start(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await state.set_state(DepositFlow.waiting_for_amount)
    await callback.message.edit_text(
        "💰 Введите сумму пополнения (от 50 ₽)\n\n"
        "Или выберите одну из кнопок:",
        reply_markup=deposit_amount_kb()
    )
    await callback.answer()


@router.callback_query(DepositFlow.waiting_for_amount, F.data.startswith("deposit:amount:"))
async def deposit_amount_preset(callback: CallbackQuery, state: FSMContext):
    amount_str = callback.data.split(":")[-1]
    if amount_str == "other":
        await callback.message.edit_text("Введите сумму цифрами (от 50 ₽)")
        await callback.answer()
        return
    amount = int(amount_str)
    if amount < 50:
        await callback.answer("Минимальная сумма 50 ₽", show_alert=True)
        return
    await process_deposit(callback, state, amount)


@router.message(DepositFlow.waiting_for_amount)
async def deposit_amount_custom(message: Message, state: FSMContext):
    if not message.text or not message.text.isdigit():
        await message.answer("Введите сумму числом")
        return
    amount = int(message.text)
    if amount < 50:
        await message.answer("Минимальная сумма 50 ₽")
        return
    user = await ensure_user(message.from_user.id)
    try:
        _, payment_url = await create_yookassa_payment(amount, user["id"])
        await state.clear()
        await message.answer(
            f"💳 Пополнение на {format_price(amount)}\n\n"
            "Нажмите кнопку ниже, чтобы перейти к оплате.\n"
            "После успешной оплаты мы автоматически обновим баланс и пришлём сообщение в этот чат.\n\n"
            "Если оплата прошла, а баланс не изменился, нажмите «⚠️ Проблема с оплатой».",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💳 Оплатить", url=payment_url)],
                [InlineKeyboardButton(text="⚠️ Проблема с оплатой", callback_data="support:general")],
                [InlineKeyboardButton(text="⬅️ Назад к балансу", callback_data="balance:open")],
            ])
        )
    except Exception as e:
        print(f"Ошибка создания платежа: {e}")
        await message.answer("Ошибка при создании платежа. Попробуйте позже.")


async def process_deposit(callback: CallbackQuery, state: FSMContext, amount: int):
    user = await ensure_user(callback.from_user.id)
    try:
        _, payment_url = await create_yookassa_payment(amount, user["id"])
        await state.clear()
        await callback.message.edit_text(
            f"💳 Пополнение на {format_price(amount)}\n\n"
            "Нажмите кнопку ниже, чтобы перейти к оплате.\n"
            "После успешной оплаты мы автоматически обновим баланс и пришлём сообщение в этот чат.\n\n"
            "Если оплата прошла, а баланс не изменился, нажмите «⚠️ Проблема с оплатой».",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💳 Оплатить", url=payment_url)],
                [InlineKeyboardButton(text="⚠️ Проблема с оплатой", callback_data="support:general")],
                [InlineKeyboardButton(text="⬅️ Назад к балансу", callback_data="balance:open")],
            ])
        )
        await callback.answer()
    except Exception as e:
        print(f"Ошибка создания платежа: {e}")
        await callback.message.edit_text("Ошибка при создании платежа. Попробуйте позже.")


async def yookassa_webhook(request: web.Request):
    body = await request.text()
    signature = request.headers.get("X-Yookassa-Signature", "")
    expected_signature = hmac.new(
        SETTINGS.payments_webhook_secret.encode(),
        body.encode(),
        hashlib.sha256
    ).hexdigest()
    if not hmac.compare_digest(signature, expected_signature):
        return web.Response(status=403, text="Invalid signature")

    data = json.loads(body)
    event = data.get("event")
    if event == "payment.succeeded":
        payment_id = data["object"]["id"]
        async with db_pool.acquire() as conn:
            payment = await conn.execute("SELECT * FROM payments WHERE external_payment_id = ?", (payment_id,))
            payment_row = await payment.fetchone()
            if not payment_row:
                metadata = data["object"].get("metadata", {})
                idempotence_key = metadata.get("idempotence_key")
                if idempotence_key:
                    payment = await conn.execute("SELECT * FROM payments WHERE idempotence_key = ?", (idempotence_key,))
                    payment_row = await payment.fetchone()
            if not payment_row:
                return web.Response(status=200, text="OK")
            if payment_row["status"] == "succeeded":
                return web.Response(status=200, text="OK")

            await conn.execute("BEGIN IMMEDIATE")
            try:
                cur = await conn.execute(
                    "UPDATE payments SET status = 'succeeded', paid_at = CURRENT_TIMESTAMP, raw_payload = ?, external_payment_id = ? WHERE id = ? AND status = 'pending'",
                    (json.dumps(data), payment_id, payment_row["id"])
                )
                if cur.rowcount == 0:
                    await conn.rollback()
                    return web.Response(status=200, text="OK")

                user_id = payment_row["user_id"]
                amount = payment_row["amount"]
                await conn.execute(
                    "UPDATE users SET balance = balance + ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (amount, user_id)
                )
                await conn.execute(
                    "INSERT INTO transactions (user_id, amount, type, comment) VALUES (?, ?, 'deposit', ?)",
                    (user_id, amount, "Пополнение через ЮKassa")
                )
                await conn.commit()
            except Exception:
                await conn.rollback()
                return web.Response(status=500, text="Internal error")

        user = await get_user_by_id(user_id)
        if user:
            await notify_user(
                user["telegram_id"],
                f"✅ Баланс пополнен на {format_price(amount)}"
            )
    return web.Response(status=200, text="OK")


# ------------------- ОСНОВНЫЕ ХЕНДЛЕРЫ БОТА -------------------
async def start_customer_registration(message_or_callback: Message | CallbackQuery, state: FSMContext):
    await state.clear()
    await state.set_state(CustomerReg.name)
    if isinstance(message_or_callback, CallbackQuery):
        await message_or_callback.message.edit_text(
            "👤 Как вас зовут?\n\nТак к вам будут обращаться в сервисе",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="common:main")],
                [InlineKeyboardButton(text="❌ Отмена", callback_data="global:cancel")],
            ]),
        )
        await message_or_callback.answer()
    else:
        await message_or_callback.answer("👤 Как вас зовут?\n\nТак к вам будут обращаться в сервисе")


@router.callback_query(F.data == "customer:create_order")
async def customer_create_order(callback: CallbackQuery, state: FSMContext):
    user = await ensure_user(callback.from_user.id)
    if not user.get("name") or not user.get("phone"):
        await start_customer_registration(callback, state)
        return
    await state.clear()
    await state.set_state(CreateOrder.category)
    await callback.message.edit_text("📝 Чем помочь?", reply_markup=category_kb())
    await callback.answer()


@router.message(CustomerReg.name)
async def customer_reg_name(message: Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state != CustomerReg.name.state:
        await message.answer("❌ Сценарий устарел. Начните заново.", reply_markup=main_menu_kb())
        return
    name = (message.text or "").strip()
    if len(name) < 2 or name.isdigit() or len(name) > 40:
        await message.answer("Введите имя")
        return
    await state.update_data(customer_name=name)
    await state.set_state(CustomerReg.phone)
    await message.answer(
        "📱 Номер телефона\n\nОн нужен для связи по заданию\n\nВведите номер или отправьте контакт",
        reply_markup=send_contact_kb(),
    )


@router.message(CustomerReg.phone)
async def customer_reg_phone(message: Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state != CustomerReg.phone.state:
        await message.answer("❌ Сценарий устарел. Начните заново.", reply_markup=main_menu_kb())
        return
    raw = message.contact.phone_number if message.contact else (message.text or "")
    phone = normalize_phone(raw)
    if not phone:
        await message.answer("Введите корректный номер", reply_markup=send_contact_kb())
        return
    data = await state.get_data()
    user = await ensure_user(message.from_user.id)
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET name = ?, phone = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (data["customer_name"], phone, user["id"]),
        )
        await conn.commit()
    await state.clear()
    await message.answer("Профиль сохранён", reply_markup=ReplyKeyboardRemove())
    await state.set_state(CreateOrder.category)
    await message.answer("📝 Чем помочь?", reply_markup=category_kb())


@router.callback_query(CreateOrder.category, F.data.startswith("order:cat:"))
async def order_cat(callback: CallbackQuery, state: FSMContext):
    code = callback.data.split(":")[-1]
    label = next((label for c, label in CATEGORY_OPTIONS if c == code), None)
    if not label:
        await callback.answer("Категория не найдена", show_alert=True)
        return
    await state.update_data(category_code=code, category_label=label)
    await state.set_state(CreateOrder.title)
    await callback.message.edit_text(
        "✏️ Что нужно сделать?\n\nНапример: купить воду",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="global:cancel")]]),
    )
    await callback.answer()


@router.message(CreateOrder.title)
async def order_title(message: Message, state: FSMContext):
    title = (message.text or "").strip()
    if len(title) < 3 or len(title) > 120:
        await message.answer("Введите короткое понятное описание")
        return

    await state.update_data(title=title)

    if await maybe_finish_order_edit(message, state, "title"):
        return

    await state.set_state(CreateOrder.pickup)
    await message.answer("📍 Откуда?\n\nВведите адрес", reply_markup=ReplyKeyboardRemove())


@router.message(CreateOrder.pickup)
async def order_pickup(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    if len(text) < 3 or len(text) > 200:
        await message.answer("Введите адрес")
        return

    await state.update_data(pickup_text=text, pickup_lat=None, pickup_lon=None)

    if await maybe_finish_order_edit(message, state, "pickup"):
        return

    await state.set_state(CreateOrder.dropoff)
    await message.answer("📍 Куда?\n\nЕсли никуда — выберите ниже")
    await message.answer("Выберите вариант", reply_markup=dropoff_choice_kb())


@router.callback_query(CreateOrder.dropoff, F.data == "order:dropoff:onsite")
async def order_dropoff_onsite(callback: CallbackQuery, state: FSMContext):
    await state.update_data(dropoff_required=False, dropoff_text="Только по месту", dropoff_lat=None, dropoff_lon=None)

    if await maybe_finish_order_edit(callback, state, "dropoff"):
        return

    await state.set_state(CreateOrder.when_choice)
    await callback.message.edit_text("⏰ Когда нужно?", reply_markup=when_kb())
    await callback.answer()


@router.message(CreateOrder.dropoff)
async def order_dropoff_text(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    if len(text) < 3 or len(text) > 200:
        await message.answer("Введите адрес")
        return

    await state.update_data(dropoff_required=True, dropoff_text=text, dropoff_lat=None, dropoff_lon=None)

    if await maybe_finish_order_edit(message, state, "dropoff"):
        return

    await state.set_state(CreateOrder.when_choice)
    await message.answer("⏰ Когда нужно?", reply_markup=ReplyKeyboardRemove())
    await message.answer("Выберите вариант", reply_markup=when_kb())


@router.callback_query(CreateOrder.when_choice, F.data.startswith("order:when:"))
async def order_when(callback: CallbackQuery, state: FSMContext):
    code = callback.data.split(":")[-1]
    label = next((label for c, label in WHEN_OPTIONS if c == code), None)
    if not label:
        await callback.answer("Не найдено", show_alert=True)
        return

    if code == "manual":
        await state.set_state(CreateOrder.when_manual)
        await callback.message.edit_text("⏰ Когда нужно?\n\nВведите время или дату")
        await callback.answer()
        return

    await state.update_data(when_type=code, when_text=label)

    if await maybe_finish_order_edit(callback, state, "when"):
        return

    await state.set_state(CreateOrder.details)
    await callback.message.edit_text(
        "📌 Детали (необязательно)\n\nНапишите, если есть важное",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⏭ Пропустить", callback_data="order:skipdetails")]]),
    )
    await callback.answer()

@router.message(CreateOrder.when_manual)
async def order_when_manual(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    if len(text) < 2 or len(text) > 100:
        await message.answer("Введите время или дату")
        return

    await state.update_data(when_type="manual", when_text=text)

    if await maybe_finish_order_edit(message, state, "when"):
        return

    await state.set_state(CreateOrder.details)
    await message.answer(
        "📌 Детали (необязательно)\n\nНапишите, если есть важное",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⏭ Пропустить", callback_data="order:skipdetails")]]),
    )


@router.callback_query(CreateOrder.details, F.data == "order:skipdetails")
async def order_skip_details(callback: CallbackQuery, state: FSMContext):
    await state.update_data(details_text="")

    if await maybe_finish_order_edit(callback, state, "details"):
        return

    await state.set_state(CreateOrder.price)
    await callback.message.edit_text("💰 Оплата\n\nВведите сумму, например 300")
    await callback.answer()


@router.message(CreateOrder.details)
async def order_details(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    if len(text) > 500:
        await message.answer("Напишите короче")
        return

    await state.update_data(details_text=text)

    if await maybe_finish_order_edit(message, state, "details"):
        return

    await state.set_state(CreateOrder.price)
    await message.answer("💰 Оплата\n\nВведите сумму, например 300")


@router.message(CreateOrder.price)
async def order_price(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    if not text.isdigit():
        await message.answer("Введите сумму числом")
        return
    price = int(text)
    if price < SETTINGS.min_order_price or price > SETTINGS.max_order_price:
        await message.answer(f"Введите сумму от {SETTINGS.min_order_price} до {SETTINGS.max_order_price}")
        return

    await state.update_data(price_amount=price)
    await state.set_state(CreateOrder.confirm)
    await render_order_confirm(message, state)


@router.callback_query(F.data == "order:edit")
async def order_edit(callback: CallbackQuery, state: FSMContext):
    await state.update_data(editing_field=None)
    await callback.message.edit_text("✏️ Что изменить?", reply_markup=order_edit_kb())
    await callback.answer()


@router.callback_query(F.data.startswith("order:edit:"))
async def order_edit_pick_field(callback: CallbackQuery, state: FSMContext):
    field = callback.data.split(":")[-1]
    if field not in {"title", "pickup", "dropoff", "when", "details", "price"}:
        await callback.answer("Поле не найдено", show_alert=True)
        return

    await state.update_data(editing_field=field)

    if field == "title":
        await state.set_state(CreateOrder.title)
        await callback.message.edit_text("✏️ Что нужно сделать?\n\nВведите новое описание")
    elif field == "pickup":
        await state.set_state(CreateOrder.pickup)
        await callback.message.edit_text("📍 Откуда?\n\nВведите новый адрес")
    elif field == "dropoff":
        await state.set_state(CreateOrder.dropoff)
        await callback.message.edit_text(
            "📍 Куда?\n\nВведите новый адрес или выберите вариант ниже",
            reply_markup=dropoff_choice_kb()
        )
    elif field == "when":
        await state.set_state(CreateOrder.when_choice)
        await callback.message.edit_text("⏰ Когда нужно?", reply_markup=when_kb())
    elif field == "details":
        await state.set_state(CreateOrder.details)
        await callback.message.edit_text(
            "📌 Детали (необязательно)\n\nНапишите новые детали",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⏭ Пропустить", callback_data="order:skipdetails")]
            ]),
        )
    elif field == "price":
        await state.set_state(CreateOrder.price)
        await callback.message.edit_text("💰 Оплата\n\nВведите новую сумму")
    await callback.answer()


@router.callback_query(F.data == "order:pay")
async def order_pay(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "📨 Отправка на проверку\n\n"
        "Сейчас деньги не списываются.\n"
        "Если задание одобрят, оно станет доступно исполнителям для отклика.\n"
        "Оплата потребуется только когда исполнитель нажмёт «Готов взять».",
        reply_markup=order_pay_kb()
    )
    await callback.answer()


@router.callback_query(F.data == "order:submit")
async def order_submit(callback: CallbackQuery, state: FSMContext):
    async with user_lock(callback.from_user.id):
        data = await state.get_data()
        if not data.get("price_amount"):
            await callback.answer("Сценарий сброшен. Начните заново", show_alert=True)
            return

        order_id = await create_order_for_user(callback.from_user.id, data)
        await state.clear()
        if not order_id:
            await callback.message.edit_text("Не удалось создать задание", reply_markup=main_back_kb())
            await callback.answer()
            return

        for admin_id in SETTINGS.admin_ids:
            await notify_user(admin_id, f"📦 Новое задание #{order_id} ждёт проверки", admin_main_kb())

        await callback.message.edit_text(
            f"✅ Задание отправлено на проверку\n\n"
            "Что дальше:\n"
            "• мы проверим задание\n"
            "• если всё ок — попросим вас оплатить его полностью\n"
            "• до оплаты исполнители задание не увидят\n\n"
            "Прямо сейчас деньги не списаны.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📜 Мои задания", callback_data="customer:orders")],
                [InlineKeyboardButton(text="⚠️ Проблема по заданию", callback_data=f"support:order:{order_id}")],
                [InlineKeyboardButton(text="🏠 Главное меню", callback_data="common:main")],
            ])
        )
        await callback.answer()


@router.callback_query(F.data == "customer:orders")
async def customer_orders(callback: CallbackQuery):
    user = await ensure_user(callback.from_user.id)
    async with db_pool.acquire() as conn:
        cur = await conn.execute("SELECT id FROM orders WHERE customer_id = ? LIMIT 1", (user["id"],))
        rows = await cur.fetchall()
        if not rows:
            await callback.message.edit_text(
                "📜 У вас пока нет заданий\n\n👇 Создайте первое",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="📝 Разместить задание", callback_data="customer:create_order")],
                    [InlineKeyboardButton(text="⬅️ Назад", callback_data="profile:open")],
                ]),
            )
            await callback.answer()
            return
    await callback.message.edit_text("📜 Мои задания", reply_markup=customer_orders_menu_kb())
    await callback.answer()


@router.callback_query(F.data.in_({"customer:orders:active", "customer:orders:done"}))
async def customer_orders_list(callback: CallbackQuery):
    user = await ensure_user(callback.from_user.id)
    done_mode = callback.data.endswith(":done")
    if done_mode:
        statuses = ("completed", "cancelled", "rejected")
        title = "✅ Завершённые задания"
    else:
        statuses = (
            "pending_review",
            "approved_open",
            "waiting_customer_payment",
            "in_progress",
            "done_waiting_confirmation",
            "manual_review",
        )
        title = "⌛ Активные задания"

    placeholders = ",".join(["?"] * len(statuses))
    async with db_pool.acquire() as conn:
        rows = await conn.execute(
            f"SELECT * FROM orders WHERE customer_id = ? AND status IN ({placeholders}) ORDER BY id DESC LIMIT 20",
            (user["id"], *statuses),
        )
        rows = await rows.fetchall()
        if not rows:
            await callback.message.edit_text(title + "\n\nПока пусто", reply_markup=customer_orders_menu_kb())
            await callback.answer()
            return
        buttons = []
        for row in rows:
            row_dict = dict(row)
            buttons.append([InlineKeyboardButton(text=f"📦 #{row_dict['id']} • {ORDER_STATUS_LABELS[row_dict['status']]}", callback_data=f"cust:view:{row_dict['id']}")])
        buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="customer:orders")])
        await callback.message.edit_text(title, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
        await callback.answer()


@router.callback_query(F.data.startswith("cust:view:"))
async def customer_view_order(callback: CallbackQuery):
    order_id = parse_callback_tail_int(callback.data, "cust:view:")
    if order_id is None:
        await callback.answer("Некорректный заказ", show_alert=True)
        return
    try:
        order_dict = await assert_customer_order_access(order_id, callback.from_user.id)
    except AccessDeniedError:
        await callback.answer("Задание не найдено", show_alert=True)
        return

    status_text = {
        "pending_review": "Проверяем задание перед одобрением",
        "approved_open": "Задание одобрено и открыто для откликов исполнителей",
        "waiting_customer_payment": "Есть исполнитель, готовый взять заказ. Ждём вашу оплату, чтобы закрепить его",
        "in_progress": "Исполнитель закреплён и выполняет задание. Деньги остаются в холде",
        "done_waiting_confirmation": "Ждём вашего подтверждения. Деньги пока в холде",
        "manual_review": "Идёт ручная проверка поддержки. Деньги пока остаются в холде",
        "completed": "Задание завершено",
        "cancelled": "Задание отменено",
        "rejected": "Задание отклонено",
    }.get(order_dict["status"], "Статус обновляется")

    text_with_status = f"Что сейчас происходит:\n{status_text}\n\n" + order_details_text(order_dict)
    await callback.message.edit_text(text_with_status, reply_markup=customer_order_actions_kb(order_id, order_dict["status"]))
    await callback.answer()


@router.callback_query(F.data.startswith("cust:pay:"))
async def customer_pay_order(callback: CallbackQuery):
    order_id = parse_callback_tail_int(callback.data, "cust:pay:")
    if order_id is None:
        await callback.answer("Некорректный заказ", show_alert=True)
        return

    user = await ensure_user(callback.from_user.id)
    order_row = await get_customer_order_for_user(user["id"], order_id)
    if not order_row:
        await callback.answer("Чужой заказ недоступен", show_alert=True)
        return

    async with order_lock(order_id):
        async with user_lock(callback.from_user.id):
            ok, reason, amount = await pay_order_from_balance(callback.from_user.id, order_id)
            if not ok:
                if reason == "no_balance":
                    user = await ensure_user(callback.from_user.id)
                    need = amount - int(user["balance"])
                    await callback.message.edit_text(
                        f"Недостаточно средств для оплаты задания.\n\n"
                        f"Нужно пополнить баланс минимум на {format_price(need)}.",
                        reply_markup=insufficient_funds_kb(need),
                    )
                    await callback.answer()
                    return

                if reason in {"bad_status", "deadline_expired", "candidate_missing"}:
                    await callback.answer("Бронь уже истекла или задание недоступно для оплаты", show_alert=True)
                    return

                if reason in {"executor_busy", "candidate_unavailable", "candidate_missing"}:
                    await callback.answer("Исполнитель уже недоступен. Заказ снова можно открыть для откликов через поддержку.", show_alert=True)
                    return

                await callback.answer("Не удалось оплатить задание", show_alert=True)
                return

    for admin_id in SETTINGS.admin_ids:
        await notify_user(admin_id, f"💳 Задание #{order_id} оплачено. Исполнитель закреплён", admin_main_kb())

    order = await get_order_by_id(order_id)
    executor = await get_user_by_id(order["executor_id"]) if order and order.get("executor_id") else None
    if executor:
        await notify_user(
            executor["telegram_id"],
            f"📦 Заказ #{order_id} закреплён за вами.\n\nТеперь можно выполнять задание.",
            InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📌 Моё задание", callback_data="executor:my_order")],
                [InlineKeyboardButton(text="⚠️ Проблема по заданию", callback_data=f"support:order:{order_id}")],
                [InlineKeyboardButton(text="💬 Связаться с оператором", url=support_url())],
            ]),
        )

    await callback.message.edit_text(
        f"✅ Оплата получена\n\n"
        f"С баланса списано: {format_price(amount)}\n"
        f"Исполнитель закреплён за заданием.\n"
        f"Теперь он может приступить к выполнению.\n\n"
        f"Деньги остаются в холде до завершения.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📜 Мои задания", callback_data="customer:orders")],
            [InlineKeyboardButton(text="⚠️ Проблема по заданию", callback_data=f"support:order:{order_id}")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="common:main")],
        ]),
    )
    await callback.answer()

@router.callback_query(F.data.startswith("cust:cancel:ask:"))
async def customer_cancel_ask(callback: CallbackQuery):
    order_id = parse_callback_tail_int(callback.data, "cust:cancel:ask:")
    if order_id is None:
        await callback.answer("Некорректный заказ", show_alert=True)
        return
    user = await ensure_user(callback.from_user.id)
    order_row = await get_customer_order_for_user(user["id"], order_id)
    if not order_row:
        await callback.answer("Чужой заказ недоступен", show_alert=True)
        return
    await callback.message.edit_text(f"📦 Задание #{order_id}\n\nОтменить задание?", reply_markup=customer_cancel_confirm_kb(order_id))
    await callback.answer()


@router.callback_query(F.data.startswith("cust:cancel:"))
async def customer_cancel(callback: CallbackQuery):
    parts = callback.data.split(":")
    if parts[2] == "ask":
        return
    order_id = int(parts[-1])

    async with order_lock(order_id):
        async with user_lock(callback.from_user.id):
            ok, reason = await cancel_order_by_customer(callback.from_user.id, order_id)
            if not ok:
                if reason == "support_only":
                    await callback.answer(
                        "После взятия задания или отправки результата отмена идёт только через поддержку",
                        show_alert=True
                    )
                    return
                if reason == "already_cancelled":
                    await callback.answer("Задание уже отменено", show_alert=True)
                    return
                await callback.answer("Отменить нельзя", show_alert=True)
                return

    if reason == "ok_with_refund":
        text = "📦 Задание отменено\n\nДеньги возвращены на баланс"
    else:
        text = "📦 Задание отменено\n\nОплата не списывалась, поэтому возврат не требовался"

    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📝 Разместить задание", callback_data="customer:create_order")],
            [InlineKeyboardButton(text="📜 Мои задания", callback_data="customer:orders")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="profile:open")],
        ]),
    )
    await callback.answer()


@router.callback_query(F.data == "cust:changeprice:save")
async def customer_change_price_save(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    order_id = data.get("change_order_id")
    new_price = data.get("new_price")
    if not isinstance(order_id, int) or not isinstance(new_price, int):
        await state.clear()
        await callback.message.edit_text("Сценарий изменения суммы истёк. Откройте его заново.", reply_markup=main_back_kb())
        return
    user = await ensure_user(callback.from_user.id)
    order_row = await get_customer_order_for_user(user["id"], order_id)
    if not order_row:
        await state.clear()
        await callback.message.edit_text("Чужой заказ недоступен", reply_markup=main_back_kb())
        return
    async with user_lock(callback.from_user.id):
        ok, reason = await change_price_for_customer(callback.from_user.id, order_id, new_price)
        await state.clear()
        if not ok:
            msg = "Не удалось изменить сумму"
            if reason == "no_balance":
                msg = "Недостаточно средств"
            elif reason == "bad_status":
                msg = "Заказ уже недоступен для изменения"
            await callback.message.edit_text(msg, reply_markup=main_back_kb())
            await callback.answer()
            return
    await callback.message.edit_text("✅ Вознаграждение обновлено", reply_markup=main_back_kb())
    await callback.answer()


@router.callback_query(F.data.func(lambda d: isinstance(d, str) and d.startswith("cust:changeprice:") and d.split(":")[-1].isdigit()))
async def customer_change_price_start(callback: CallbackQuery, state: FSMContext):
    order_id = parse_callback_tail_int(callback.data, "cust:changeprice:")
    if order_id is None:
        await callback.answer("Некорректный заказ", show_alert=True)
        return
    user = await ensure_user(callback.from_user.id)
    async with db_pool.acquire() as conn:
        order = await conn.execute("SELECT * FROM orders WHERE id = ? AND customer_id = ?", (order_id, user["id"]))
        order_row = await order.fetchone()
        if not order_row:
            await callback.answer("Не найдено", show_alert=True)
            return
        order_dict = dict(order_row)
        if order_dict["status"] != "approved_open" or order_dict["executor_id"] is not None or order_dict.get("candidate_executor_id") is not None:
            await callback.answer("Нельзя изменить сумму", show_alert=True)
            return
    await state.set_state(ChangePriceFlow.amount)
    await state.update_data(change_order_id=order_id)
    await callback.message.edit_text(
        f"💰 Новое вознаграждение\n\n"
        f"Текущее вознаграждение: {format_price(order_dict['price_amount'])}\n\n"
        f"Введите новую сумму"
    )
    await callback.answer()


@router.message(ChangePriceFlow.amount)
async def customer_change_price_amount(message: Message, state: FSMContext):
    data = await state.get_data()
    order_id = data.get("change_order_id")
    if not isinstance(order_id, int):
        await state.clear()
        await message.answer("Сценарий изменения суммы истёк. Начните заново.", reply_markup=main_back_kb())
        return
    text = (message.text or "").strip()
    if not text.isdigit():
        await message.answer("Введите сумму числом")
        return
    new_price = int(text)
    if new_price < SETTINGS.min_order_price or new_price > SETTINGS.max_order_price:
        await message.answer(f"Введите сумму от {SETTINGS.min_order_price} до {SETTINGS.max_order_price}")
        return
    async with db_pool.acquire() as conn:
        order = await conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,))
        order_row = await order.fetchone()
        if not order_row or order_row["status"] != "approved_open" or order_row["executor_id"] is not None or order_row["candidate_executor_id"] is not None:
            await message.answer("Нельзя изменить сумму", reply_markup=main_back_kb())
            await state.clear()
            return
        order_dict = dict(order_row)
        diff = new_price - int(order_dict["price_amount"])

    await state.update_data(new_price=new_price)
    await state.set_state(ChangePriceFlow.confirm)

    confirm_text = (
        f"💰 Изменение вознаграждения\n\n"
        f"Текущее вознаграждение: {format_price(order_dict['price_amount'])}\n"
        f"Новое вознаграждение: {format_price(new_price)}"
    )
    if diff > 0:
        confirm_text += f"\nС баланса спишется ещё {format_price(diff)}"
    elif diff < 0:
        confirm_text += f"\nНа баланс вернётся {format_price(-diff)}"
    confirm_text += "\n\n👇 Всё верно?"

    await message.answer(confirm_text, reply_markup=change_price_confirm_kb(diff))


@router.callback_query(F.data.startswith("cust:done:ask:"))
async def customer_done_ask(callback: CallbackQuery):
    order_id = parse_callback_tail_int(callback.data, "cust:done:ask:")
    if order_id is None:
        await callback.answer("Некорректный заказ", show_alert=True)
        return
    try:
        order_row = await assert_customer_order_access(order_id, callback.from_user.id)
    except AccessDeniedError:
        await callback.answer("Задание не найдено", show_alert=True)
        return
    if order_row["status"] != "done_waiting_confirmation":
        await callback.answer("Задание не ожидает подтверждения", show_alert=True)
        return
    if not order_row["result_sent_to_customer_at"]:
        await callback.answer("Сначала заказчику должен быть показан результат", show_alert=True)
        return
    await callback.message.edit_text(
        f"📦 Задание #{order_id}\n\n"
        "Всё в порядке с заданием?\n\n"
        "После подтверждения:\n"
        "• задание закроется\n"
        "• деньги из холда уйдут исполнителю\n\n"
        "Если что-то не так — нажмите «⚠️ Проблема по заданию». Тогда деньги останутся в холде до решения поддержки.",
        reply_markup=customer_done_confirm_kb(order_id),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("cust:done:"))
async def customer_done(callback: CallbackQuery):
    if callback.data.startswith("cust:done:ask"):
        return
    order_id = parse_callback_tail_int(callback.data, "cust:done:")
    if order_id is None:
        await callback.answer("Некорректный заказ", show_alert=True)
        return
    user = await ensure_user(callback.from_user.id)
    order_row = await get_customer_order_for_user(user["id"], order_id)
    if not order_row:
        await callback.answer("Чужой заказ недоступен", show_alert=True)
        return
    async with order_lock(order_id):
        async with user_lock(callback.from_user.id):
            ok, reason = await finalize_order(order_id, actor="customer", actor_tg_id=callback.from_user.id)
            if not ok:
                if reason == "proof_not_sent":
                    await callback.answer("Сначала заказчику нужно показать результат", show_alert=True)
                    return
                if reason == "forbidden":
                    await callback.answer("Чужой заказ подтверждать нельзя", show_alert=True)
                    return
                await callback.answer("Уже обработано или заказ не в статусе ожидания", show_alert=True)
                return
    await callback.message.edit_text(
        "📦 Задание завершено\n\nСпасибо, что пользуетесь НаВсеРуки 🤝",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📝 Разместить задание", callback_data="customer:create_order")],
            [InlineKeyboardButton(text="📜 Мои задания", callback_data="customer:orders")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="common:main")],
        ]),
    )
    await callback.answer()



@router.callback_query(F.data == "withdraw:start")
async def withdraw_start(callback: CallbackQuery, state: FSMContext):
    user = await ensure_user(callback.from_user.id)
    await state.clear()
    await state.set_state(WithdrawFlow.amount)
    await callback.message.edit_text(f"💸 Вывод денег\n\nДоступно: {format_price(user['balance'])}\n\nВведите сумму")
    await callback.answer()


@router.message(WithdrawFlow.amount)
async def withdraw_amount(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    if not text.isdigit():
        await message.answer("Введите сумму числом")
        return
    amount = int(text)
    user = await ensure_user(message.from_user.id)
    if amount < SETTINGS.min_withdrawal_amount:
        await message.answer(f"Минимум: {format_price(SETTINGS.min_withdrawal_amount)}")
        return
    if amount > user["balance"]:
        await message.answer(f"Недостаточно средств\n\nДоступно: {format_price(user['balance'])}")
        return
    await state.update_data(withdraw_amount=amount)
    await state.set_state(WithdrawFlow.phone)
    await message.answer(
        "📱 Номер телефона\n\nВведите номер для перевода",
        reply_markup=send_contact_kb(),
    )


@router.message(WithdrawFlow.phone)
async def withdraw_phone(message: Message, state: FSMContext):
    raw = message.contact.phone_number if message.contact else (message.text or "")
    phone = normalize_phone(raw)
    if not phone:
        await message.answer("Введите корректный номер", reply_markup=send_contact_kb())
        return
    await state.update_data(withdraw_phone=phone)
    await state.set_state(WithdrawFlow.bank)
    await message.answer("🏦 Банк\n\nВыберите или введите банк", reply_markup=ReplyKeyboardRemove())
    await message.answer("Выберите банк", reply_markup=withdraw_banks_kb())


@router.callback_query(WithdrawFlow.bank, F.data.startswith("withdraw:bank:"))
async def withdraw_bank_pick(callback: CallbackQuery, state: FSMContext):
    bank = callback.data.split(":")[-1]
    if bank == "manual":
        await callback.message.edit_text("🏦 Банк\n\nВведите банк")
        await callback.answer()
        return
    await state.update_data(withdraw_bank=bank)
    await state.set_state(WithdrawFlow.confirm)
    data = await state.get_data()
    await callback.message.edit_text(
        "💸 Подтверждение\n\n"
        f"Сумма: {format_price(data['withdraw_amount'])}\n"
        f"Телефон: {escape_html(data['withdraw_phone'])}\n"
        f"Банк: {escape_html(data['withdraw_bank'])}\n\n"
        "👇 Всё верно?",
        reply_markup=withdraw_confirm_kb(),
    )
    await callback.answer()


@router.message(WithdrawFlow.bank)
async def withdraw_bank_manual(message: Message, state: FSMContext):
    bank = (message.text or "").strip()
    if len(bank) < 2 or len(bank) > 50:
        await message.answer("Введите банк")
        return
    await state.update_data(withdraw_bank=bank)
    await state.set_state(WithdrawFlow.confirm)
    data = await state.get_data()
    await message.answer(
        "💸 Подтверждение\n\n"
        f"Сумма: {format_price(data['withdraw_amount'])}\n"
        f"Телефон: {escape_html(data['withdraw_phone'])}\n"
        f"Банк: {escape_html(data['withdraw_bank'])}\n\n"
        "👇 Всё верно?",
        reply_markup=withdraw_confirm_kb(),
    )


@router.callback_query(F.data == "withdraw:create")
async def withdraw_create(callback: CallbackQuery, state: FSMContext):
    async with user_lock(callback.from_user.id):
        data = await state.get_data()
        amount = data.get("withdraw_amount")
        phone = data.get("withdraw_phone")
        bank = data.get("withdraw_bank")
        if not isinstance(amount, int) or not phone or not bank:
            await state.clear()
            await callback.message.edit_text("Сценарий вывода истёк. Начните заново.", reply_markup=main_back_kb())
            return
        ok, reason = await create_withdrawal(callback.from_user.id, amount, phone, bank)
        await state.clear()
        if not ok:
            msg = "Не удалось создать заявку"
            if reason == "pending_exists":
                msg = "У вас уже есть активная заявка на вывод"
            await callback.message.edit_text(msg, reply_markup=main_back_kb())
            await callback.answer()
            return
        for admin_id in SETTINGS.admin_ids:
            await notify_user(admin_id, "💸 Новый запрос на вывод", admin_main_kb())
        await callback.message.edit_text(
            f"✅ Заявка на вывод создана\n\n"
            f"Сумма: {format_price(data['withdraw_amount'])}\n"
            f"Деньги зарезервированы до обработки заявки.\n\n"
            f"Мы напишем в этот чат, когда заявка будет выполнена или если понадобится уточнение.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⚠️ Проблема с выводом", callback_data="support:general")],
                [InlineKeyboardButton(text="💰 К балансу", callback_data="balance:open")],
                [InlineKeyboardButton(text="🏠 Главное меню", callback_data="common:main")],
            ]),
        )
        await callback.answer()


# ------------------- ХЕНДЛЕРЫ ИСПОЛНИТЕЛЯ -------------------
@router.callback_query(F.data == "executor:start")
async def executor_start(callback: CallbackQuery):
    user = await ensure_user(callback.from_user.id)
    if user["is_executor_profile_created"] and not user["is_executor_approved"]:
        text = "💼 Хочу заработать\n\nПрофиль уже заполнен\nСейчас проверяем данные\n\n👇 доступ к заданиям откроется после проверки"
    elif user["is_executor_approved"]:
        text = "💼 Хочу заработать\n\nВы подключены\n\n👇 можно брать задания"
    else:
        text = "💼 Хочу заработать\n\nЗадачи есть — можно зарабатывать\n\n👇 чтобы начать, нужно заполнить профиль"
    await callback.message.edit_text(text, reply_markup=executor_start_kb(user))
    await callback.answer()


@router.callback_query(F.data == "executor:reg:start")
async def executor_reg_start(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await state.set_state(ExecutorReg.name)
    await callback.message.edit_text(
        "👤 Как вас зовут?\n\nНапишите имя, которое будет в сервисе",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="executor:start")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="global:cancel")],
        ]),
    )
    await callback.answer()


@router.message(ExecutorReg.name)
async def executor_reg_name(message: Message, state: FSMContext):
    name = (message.text or "").strip()
    if len(name) < 2 or name.isdigit() or len(name) > 40:
        await message.answer("Введите имя")
        return
    await state.update_data(exec_name=name)
    await state.set_state(ExecutorReg.age)
    await message.answer("🎂 Сколько вам лет?\n\nУкажите реальный возраст")


@router.message(ExecutorReg.age)
async def executor_reg_age(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    if not text.isdigit():
        await message.answer("Введите возраст числом")
        return
    age = int(text)
    if age < 16 or age > 99:
        await message.answer("Введите корректный возраст")
        return
    await state.update_data(exec_age=age)
    await state.set_state(ExecutorReg.phone)
    await message.answer(
        "📱 Номер телефона\n\nВведите номер или отправьте контакт",
        reply_markup=send_contact_kb(),
    )


@router.message(ExecutorReg.phone)
async def executor_reg_phone(message: Message, state: FSMContext):
    raw = message.contact.phone_number if message.contact else (message.text or "")
    phone = normalize_phone(raw)
    if not phone:
        await message.answer("Введите корректный номер", reply_markup=send_contact_kb())
        return
    await state.update_data(exec_phone=phone)
    data = await state.get_data()
    await state.set_state(ExecutorReg.confirm)
    await message.answer(
        "📌 Проверьте данные\n\n"
        f"Имя: {escape_html(data['exec_name'])}\n"
        f"Возраст: {data['exec_age']}\n"
        f"Телефон: {escape_html(data['exec_phone'])}",
        reply_markup=executor_reg_confirm_kb(),
    )


@router.callback_query(F.data == "executor:reg:save")
async def executor_reg_save(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    name = data.get("exec_name")
    age = data.get("exec_age")
    phone = data.get("exec_phone")
    if not name or not isinstance(age, int) or not phone:
        await state.clear()
        await callback.message.edit_text("Сценарий регистрации истёк. Заполните профиль заново.", reply_markup=main_back_kb())
        return
    user = await ensure_user(callback.from_user.id)
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET name = ?, age = ?, phone = ?, is_executor_profile_created = 1, is_executor_approved = 0, is_blocked = 0, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (name, age, phone, user["id"]),
        )
        await conn.commit()
    await state.clear()
    for admin_id in SETTINGS.admin_ids:
        await notify_user(admin_id, "👤 Новый исполнитель ждёт проверки", admin_main_kb())
    await callback.message.edit_text(
        "✅ Профиль отправлен на проверку\n\n"
        "Мы напишем в этот чат, когда доступ к заданиям откроется.\n"
        "Пока ничего делать не нужно.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="common:main")],
            [InlineKeyboardButton(text="💬 Связаться с оператором", url=support_url())],
        ])
    )
    await callback.answer()


@router.callback_query(F.data == "executor:available")
async def executor_available(callback: CallbackQuery):
    user = await ensure_user(callback.from_user.id)
    if not user["is_executor_profile_created"] or not user["is_executor_approved"] or user["is_blocked"]:
        await callback.answer("Доступ откроется после проверки", show_alert=True)
        return
    async with db_pool.acquire() as conn:
        rows = await conn.execute(
            "SELECT id, category_label, price_amount, pickup_text, dropoff_text, when_text "
            "FROM orders WHERE status = 'approved_open' AND executor_id IS NULL AND candidate_executor_id IS NULL ORDER BY id DESC LIMIT 30"
        )
        rows = await rows.fetchall()
        if not rows:
            await callback.message.edit_text(
                "📋 Доступные задания\n\nПока пусто",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="profile:open")]]),
            )
            await callback.answer()
            return
        orders = [dict(r) for r in rows]
    await callback.message.edit_text("📋 Доступные задания", reply_markup=available_orders_list_kb(orders))
    await callback.answer()


@router.callback_query(F.data.startswith("exec:viewavailable:"))
async def executor_view_available(callback: CallbackQuery):
    order_id = parse_callback_tail_int(callback.data, "admin:orderview:")
    if order_id is None:
        await callback.answer("Некорректный заказ", show_alert=True)
        return
    async with db_pool.acquire() as conn:
        order = await conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,))
        order_row = await order.fetchone()
        if not order_row or order_row["status"] != "approved_open" or order_row["executor_id"] is not None or order_row["candidate_executor_id"] is not None:
            await callback.message.edit_text(
                "📦 Упс\n\nЗадание уже недоступно",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔄 Обновить", callback_data="executor:available")],
                    [InlineKeyboardButton(text="📋 Другие задания", callback_data="executor:available")],
                ]),
            )
            await callback.answer()
            return
        order_dict = dict(order_row)
    await callback.message.edit_text(order_details_text(order_dict), reply_markup=available_order_kb(order_id))
    await callback.answer()


@router.callback_query(F.data.startswith("exec:take:"))
async def executor_take(callback: CallbackQuery):
    order_id = parse_callback_tail_int(callback.data, "exec:take:")
    if order_id is None:
        await callback.answer("Некорректный заказ", show_alert=True)
        return
    async with order_lock(order_id):
        async with user_lock(callback.from_user.id):
            ok, reason = await take_order(callback.from_user.id, order_id)
            if not ok:
                if reason == "taken":
                    await callback.message.edit_text(
                        "📦 Упс\n\nЗадание уже взял другой исполнитель",
                        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(text="🔄 Обновить", callback_data="executor:available")],
                            [InlineKeyboardButton(text="📋 Другие задания", callback_data="executor:available")],
                        ]),
                    )
                    await callback.answer()
                    return
                if reason in {"busy", "waiting_payment"}:
                    await callback.message.edit_text(
                        "📌 У вас уже есть активное задание или бронь, ожидающая оплату\n\nСначала дождитесь завершения текущего сценария",
                        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(text="📌 Моё задание", callback_data="executor:my_order")],
                            [InlineKeyboardButton(text="⬅️ Назад", callback_data="executor:available")],
                        ]),
                    )
                    await callback.answer()
                    return
                await callback.answer("Нельзя взять задание", show_alert=True)
                return
    order = await get_order_by_id(order_id)
    await callback.message.edit_text(
        f"✅ Вы выбрали задание #{order_id}.\n\n"
        f"Сейчас ждём оплату от заказчика.\n"
        f"Заказ ещё не закреплён за вами.\n"
        f"Начинать выполнение пока нельзя.\n\n"
        f"Если оплата пройдёт, мы отдельно напишем вам.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📌 Моё задание", callback_data="executor:my_order")],
            [InlineKeyboardButton(text="⚠️ Проблема по заданию", callback_data=f"support:order:{order_id}")],
            [InlineKeyboardButton(text="💬 Связаться с оператором", url=support_url())],
        ]),
    )
    await callback.answer()


@router.callback_query(F.data == "executor:my_order")
async def executor_my_order(callback: CallbackQuery):
    user = await ensure_user(callback.from_user.id)
    if not user["is_executor_profile_created"]:
        await callback.message.edit_text(
            "📌 У вас нет активного задания\n\nЧтобы брать задания, подключите режим исполнителя",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💼 Хочу заработать", callback_data="executor:start")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="profile:open")],
            ]),
        )
        await callback.answer()
        return

    order_row = None
    async with db_pool.acquire() as conn:
        if user["active_order_id"]:
            order = await conn.execute("SELECT * FROM orders WHERE id = ?", (user["active_order_id"],))
            order_row = await order.fetchone()
            if order_row and order_row["executor_id"] != user["id"]:
                order_row = None

        if not order_row:
            candidate = await conn.execute(
                "SELECT * FROM orders WHERE candidate_executor_id = ? AND status = 'waiting_customer_payment' ORDER BY id DESC LIMIT 1",
                (user["id"],),
            )
            order_row = await candidate.fetchone()

        if not order_row:
            await callback.message.edit_text(
                "📌 У вас нет активного задания\n\n👇 возьмите задание из списка",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="📋 Доступные задания", callback_data="executor:available")],
                    [InlineKeyboardButton(text="⬅️ Назад", callback_data="profile:open")],
                ]),
            )
            await callback.answer()
            return

        order_dict = dict(order_row)
        if order_dict["status"] not in {"waiting_customer_payment", "in_progress", "done_waiting_confirmation", "manual_review"}:
            if user["active_order_id"] == order_dict["id"]:
                await conn.execute("UPDATE users SET active_order_id = NULL WHERE id = ?", (user["id"],))
                await conn.commit()
            await callback.message.edit_text(
                "📌 Задание уже завершено\n\n👇 возьмите новое задание",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="📋 Доступные задания", callback_data="executor:available")],
                    [InlineKeyboardButton(text="⬅️ Назад", callback_data="profile:open")],
                ]),
            )
            await callback.answer()
            return

    status_text = {
        "waiting_customer_payment": "Сейчас ждём оплату от заказчика. Заказ ещё не закреплён. Начинать выполнение пока нельзя",
        "in_progress": "Задание закреплено за вами и находится в работе. Деньги по нему в холде",
        "done_waiting_confirmation": "Результат отправлен. Ждём подтверждение заказчика, деньги ещё в холде",
        "manual_review": "Идёт ручная проверка поддержки. Деньги пока ещё в холде",
    }.get(order_dict["status"], "Статус обновляется")
    text_with_status = f"Что сейчас происходит:\n{status_text}\n\n" + order_details_text(order_dict)
    await callback.message.edit_text(text_with_status, reply_markup=my_order_kb(order_dict["id"], order_dict["status"]))
    await callback.answer()

@router.callback_query(F.data.startswith("exec:sendresult:"))
async def executor_send_result(callback: CallbackQuery):
    order_id = parse_callback_tail_int(callback.data, "exec:sendresult:")
    if order_id is None:
        await callback.answer("Некорректный заказ", show_alert=True)
        return
    async with db_pool.acquire() as conn:
        order = await conn.execute("SELECT id, status, executor_id FROM orders WHERE id = ?", (order_id,))
        order_row = await order.fetchone()
        if not order_row:
            await callback.answer("Задание не найдено", show_alert=True)
            return
        order_dict = dict(order_row)
        user = await ensure_user(callback.from_user.id)
        if order_dict["executor_id"] != user["id"]:
            await callback.answer("Это не ваше задание", show_alert=True)
            return
        if order_dict["status"] != "in_progress":
            await callback.answer("Нельзя отправить результат", show_alert=True)
            return
    await callback.message.edit_text(
        f"📤 Отправка результата по заданию #{order_id}\n\n"
        "1. Отправьте в поддержку фото и/или комментарий по результату\n"
        "2. Если это покупка — приложите чек / доказательство покупки\n"
        f"3. Укажите номер задания: #{order_id}\n"
        "4. После этого нажмите «Я отправил результат»\n\n"
        "Без доказательства результат не будет принят на проверку.",
        reply_markup=send_result_kb(order_id),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("exec:sent:"))
async def executor_sent(callback: CallbackQuery):
    order_id = parse_callback_tail_int(callback.data, "exec:sent:")
    if order_id is None:
        await callback.answer("Некорректный заказ", show_alert=True)
        return
    ok, reason = await mark_result_sent(callback.from_user.id, order_id)
    if not ok:
        if reason == "no_proof":
            await callback.answer(
                "Сначала отправьте в поддержку хотя бы фото или комментарий по результату",
                show_alert=True
            )
            return
        await callback.answer("Сейчас нельзя", show_alert=True)
        return

    for admin_id in SETTINGS.admin_ids:
        await notify_user(admin_id, f"📨 Задание #{order_id} ждёт проверки результата", admin_main_kb())

    await callback.message.edit_text(
        "✅ Результат отправлен на проверку\n\n"
        "Мы напишем сюда, когда статус изменится.\n"
        "Деньги пока остаются в холде. После подтверждения заказчиком они будут зачислены на ваш баланс.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📌 Моё задание", callback_data="executor:my_order")],
            [InlineKeyboardButton(text="⚠️ Проблема по заданию", callback_data=f"support:order:{order_id}")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="common:main")],
        ]),
    )
    await callback.answer()


# ------------------- АДМИН-ХЕНДЛЕРЫ -------------------
@router.message(Command("admin"))
async def admin_cmd(message: Message):
    if not is_admin(message.from_user.id):
        return
    await message.answer("🛠 Админ-панель", reply_markup=admin_main_kb())


@router.callback_query(F.data == "admin:main")
async def admin_main(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer()
        return
    await callback.message.edit_text("🛠 Админ-панель", reply_markup=admin_main_kb())
    await callback.answer()


@router.callback_query(F.data == "admin:orders")
async def admin_orders(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.edit_text("📦 Задания", reply_markup=admin_orders_kb())
    await callback.answer()



@router.callback_query(F.data.startswith("admin:orders:"))
async def admin_orders_section(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    section = callback.data.split(":")[-1]
    if section == "search":
        await state.set_state(AdminSearchOrder.order_id)
        await callback.message.edit_text("Введите номер задания")
        await callback.answer()
        return

    titles = {
        "new": "🟣 На проверке",
        "awaiting_payment": "💳 Ждут оплату заказчика",
        "active": "📋 Открыты для откликов",
        "progress": "🟡 В работе",
        "waiting_client": "🕓 Ждут клиента",
        "manual_review": "🟣 Ручная проверка",
    }

    async with db_pool.acquire() as conn:
        mapping = {
            "new": "pending_review",
            "awaiting_payment": "waiting_customer_payment",
            "active": "approved_open",
            "progress": "in_progress",
            "waiting_client": "done_waiting_confirmation",
            "manual_review": "manual_review",
        }
        status = mapping[section]
        rows = await conn.execute("SELECT id FROM orders WHERE status = ? ORDER BY id DESC LIMIT 30", (status,))
        rows = await rows.fetchall()

    buttons = [[InlineKeyboardButton(text=f"📦 Задание #{r['id']}", callback_data=f"admin:order:view:{r['id']}")] for r in rows]
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:orders")])
    await callback.message.edit_text(titles.get(section, "📦 Задания"), reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    await callback.answer()

@router.message(AdminSearchOrder.order_id)
async def admin_search_order(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    text = (message.text or "").strip()
    if not text.isdigit():
        await message.answer("Введите номер задания")
        return
    await state.clear()
    async with db_pool.acquire() as conn:
        order = await conn.execute("SELECT * FROM orders WHERE id = ?", (int(text),))
        order_row = await order.fetchone()
        if not order_row:
            await message.answer("Задание не найдено", reply_markup=admin_main_kb())
            return
        order_dict = dict(order_row)
    await message.answer(order_details_text(order_dict), reply_markup=admin_order_actions_kb(order_dict["id"], order_dict["status"]))


@router.callback_query(F.data.startswith("admin:order:view:"))
async def admin_order_view(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    order_id = parse_callback_tail_int(callback.data, "admin:orderview:")
    if order_id is None:
        await callback.answer("Некорректный заказ", show_alert=True)
        return
    async with db_pool.acquire() as conn:
        order = await conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,))
        order_row = await order.fetchone()
        if not order_row:
            await callback.answer("Не найдено", show_alert=True)
            return
        order_dict = dict(order_row)
    await callback.message.edit_text(order_details_text(order_dict), reply_markup=admin_order_actions_kb(order_id, order_dict["status"]))
    await callback.answer()


@router.callback_query(F.data.startswith("admin:publish:"))
async def admin_publish(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    order_id = parse_callback_tail_int(callback.data, "admin:publish:")
    if order_id is None:
        await callback.answer("Некорректный заказ", show_alert=True)
        return

    async with order_lock(order_id):
        ok, reason = await approve_order_and_wait_payment(order_id)
        if not ok:
            await callback.answer("Задание уже обработано или не может быть одобрено", show_alert=True)
            return

    order = await get_order_by_id(order_id)
    customer = await get_user_by_id(order["customer_id"])

    await notify_user(
        customer["telegram_id"],
        f"📦 Задание #{order_id} одобрено\n\n"
        "Теперь оно открыто для откликов исполнителей.\n"
        "Оплата потребуется только когда исполнитель нажмёт «Готов взять».",
        InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💳 Оплатить задание", callback_data=f"cust:pay:{order_id}")],
            [InlineKeyboardButton(text="➕ Пополнить баланс", callback_data="deposit:start")],
            [InlineKeyboardButton(text="⚠️ Проблема по заданию", callback_data=f"support:order:{order_id}")],
        ])
    )
    await callback.message.edit_text("Задание одобрено. Ждём оплату заказчика", reply_markup=admin_orders_kb())
    await callback.answer()


@router.callback_query(F.data.startswith("admin:reject:no:"))
async def admin_reject_no(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    order_id = parse_callback_tail_int(callback.data, "admin:reject:")
    if order_id is None:
        await callback.answer("Некорректный заказ", show_alert=True)
        return
    ok = await reject_order(order_id)
    if not ok:
        await callback.answer("Уже обработано", show_alert=True)
        return
    await callback.message.edit_text("Задание отклонено", reply_markup=admin_orders_kb())
    await callback.answer()


@router.callback_query(F.data.startswith("admin:reject:comment:"))
async def admin_reject_comment_start(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    order_id = parse_callback_tail_int(callback.data, "admin:rejectcomment:")
    if order_id is None:
        await callback.answer("Некорректный заказ", show_alert=True)
        return
    await state.set_state(AdminRejectOrderComment.comment)
    await state.update_data(admin_reject_order_id=order_id)
    await callback.message.edit_text("Введите комментарий для заказчика")
    await callback.answer()


@router.message(AdminRejectOrderComment.comment)
async def admin_reject_comment_save(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    data = await state.get_data()
    ok = await reject_order(data["admin_reject_order_id"], (message.text or "").strip())
    await state.clear()
    if not ok:
        await message.answer("Задание уже обработано или недоступно для отклонения", reply_markup=admin_main_kb())
        return
    await message.answer("Задание отклонено", reply_markup=admin_main_kb())


@router.callback_query(F.data.startswith("admin:returnpool:"))
async def admin_return_pool(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    order_id = parse_callback_tail_int(callback.data, "exec:take:")
    if order_id is None:
        await callback.answer("Некорректный заказ", show_alert=True)
        return
    async with order_lock(order_id):
        ok = await return_order_to_pool(order_id)
        if not ok:
            await callback.answer("Нельзя вернуть", show_alert=True)
            return
    await callback.message.edit_text("Задание возвращено в пул", reply_markup=admin_orders_kb())
    await callback.answer()


@router.callback_query(F.data.startswith("admin:sendcustomer:"))
async def admin_send_customer(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    order_id = parse_callback_tail_int(callback.data, "admin:sendcustomer:")
    if order_id is None:
        await callback.answer("Некорректный заказ", show_alert=True)
        return
    ok = await send_result_to_customer(order_id)
    if not ok:
        await callback.answer("Уже обработано", show_alert=True)
        return
    await callback.message.edit_text("Заказчику отправлено напоминание проверить результат", reply_markup=admin_orders_kb())
    await callback.answer()


@router.callback_query(F.data.startswith("admin:rework:"))
async def admin_rework(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    order_id = parse_callback_tail_int(callback.data, "admin:rework:")
    if order_id is None:
        await callback.answer("Некорректный заказ", show_alert=True)
        return
    async with db_pool.acquire() as conn:
        changed = await conn.execute(
            "UPDATE orders SET status = 'in_progress', updated_at = CURRENT_TIMESTAMP WHERE id = ? AND status = 'result_pending_review'",
            (order_id,),
        )
        if changed.rowcount != 1:
            await callback.answer("Уже обработано", show_alert=True)
            return
        await conn.commit()
    order = await get_order_by_id(order_id)
    executor = await get_user_by_id(order["executor_id"])
    await notify_user(
        executor["telegram_id"],
        f"📦 Задание #{order_id}\n\nЗадание нужно доработать\n\n👇 напишите в поддержку, чтобы уточнить детали",
        InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="💬 Связаться с оператором", url=support_url())]]),
    )
    await callback.message.edit_text("Возвращено на доработку", reply_markup=admin_orders_kb())
    await callback.answer()


@router.callback_query(F.data.startswith("admin:forcedone:"))
async def admin_force_done(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    order_id = parse_callback_tail_int(callback.data, "exec:take:")
    if order_id is None:
        await callback.answer("Некорректный заказ", show_alert=True)
        return
    async with order_lock(order_id):
        ok, msg = await force_finalize_order(order_id, actor="admin")
        if not ok:
            if msg == "already_processed":
                await callback.answer("Задание уже завершено", show_alert=True)
            else:
                await callback.answer("Нельзя завершить", show_alert=True)
            return
    await callback.message.edit_text("Задание завершено вручную", reply_markup=admin_orders_kb())
    await callback.answer()


@router.callback_query(F.data.startswith("admin:cancel:"))
async def admin_cancel_order(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    order_id = parse_callback_tail_int(callback.data, "admin:cancel:")
    if order_id is None:
        await callback.answer("Некорректный заказ", show_alert=True)
        return

    async with order_lock(order_id):
        async with db_pool.acquire() as conn:
            await conn.execute("BEGIN IMMEDIATE")
            try:
                order = await conn.execute(
                    "SELECT * FROM orders WHERE id = ? AND status NOT IN ('completed', 'cancelled', 'rejected')",
                    (order_id,)
                )
                order_row = await order.fetchone()
                if not order_row:
                    await conn.rollback()
                    await callback.answer("Задание не найдено или уже завершено", show_alert=True)
                    return

                order_dict = dict(order_row)
                refund_amount = int(order_dict["actual_hold_amount"] or 0)

                cur = await conn.execute(
                    """
                    UPDATE orders
                    SET status = 'cancelled',
                        hold_amount = 0,
                        actual_hold_amount = 0,
                        candidate_executor_id = NULL,
                        payment_deadline_at = NULL,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                      AND status NOT IN ('completed', 'cancelled', 'rejected')
                    """,
                    (order_id,),
                )
                if cur.rowcount != 1:
                    await conn.rollback()
                    await callback.answer("Не удалось отменить задание", show_alert=True)
                    return

                if refund_amount > 0:
                    await conn.execute(
                        "UPDATE users SET balance = balance + ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                        (refund_amount, order_dict["customer_id"]),
                    )
                    await conn.execute(
                        "INSERT INTO transactions (user_id, order_id, amount, type, comment) VALUES (?, ?, ?, 'refund', ?)",
                        (order_dict["customer_id"], order_id, refund_amount, "Отмена задания администратором"),
                    )

                if order_dict["executor_id"]:
                    await conn.execute(
                        "UPDATE users SET active_order_id = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                        (order_dict["executor_id"],),
                    )

                await conn.commit()
            except Exception:
                await conn.rollback()
                await callback.answer("Ошибка при отмене", show_alert=True)
                raise

    customer = await get_user_by_id(order_dict["customer_id"])
    if customer:
        text = f"📦 Задание #{order_id}\n\nЗадание отменено администратором"
        if refund_amount > 0:
            text += "\n\nДеньги возвращены на баланс"
        await notify_user(customer["telegram_id"], text, main_back_kb())

    if order_dict.get("executor_id"):
        executor = await get_user_by_id(order_dict["executor_id"])
        if executor:
            await notify_user(
                executor["telegram_id"],
                f"📦 Задание #{order_id}\n\nЗадание отменено администратором",
                main_back_kb(),
            )

    final_text = "Задание отменено"
    if refund_amount > 0:
        final_text += ", деньги возвращены"
    await callback.message.edit_text(final_text, reply_markup=admin_orders_kb())
    await callback.answer()


@router.callback_query(F.data == "admin:executors")
async def admin_executors(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    async with db_pool.acquire() as conn:
        rows = await conn.execute("SELECT id FROM users WHERE is_executor_profile_created = 1 AND is_executor_approved = 0 ORDER BY id DESC LIMIT 30")
        rows = await rows.fetchall()
        await callback.message.edit_text("👤 Исполнители", reply_markup=admin_executors_list_kb([r["id"] for r in rows]))
        await callback.answer()


@router.callback_query(F.data.startswith("admin:executor:view:"))
async def admin_executor_view(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    user_id = parse_callback_tail_int(callback.data, "admin:executor:view:")
    if user_id is None:
        await callback.answer("Некорректный исполнитель", show_alert=True)
        return
    user = await get_user_by_id(user_id)
    if not user:
        await callback.answer("Не найдено", show_alert=True)
        return
    text = (
        f"👤 Исполнитель #{user_id}\n\n"
        f"Имя: {escape_html(user.get('name') or '—')}\n"
        f"Возраст: {user.get('age') or '—'}\n"
        f"Телефон: {escape_html(user.get('phone') or '—')}\n"
        f"Telegram ID: {user['telegram_id']}"
    )
    await callback.message.edit_text(text, reply_markup=admin_executor_actions_kb(user_id))
    await callback.answer()


@router.callback_query(F.data.startswith("admin:executor:approve:"))
async def admin_executor_approve(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    user_id = parse_callback_tail_int(callback.data, "admin:executor:approve:")
    if user_id is None:
        await callback.answer("Некорректный исполнитель", show_alert=True)
        return
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET is_executor_approved = 1, is_blocked = 0, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (user_id,),
        )
        await conn.commit()
    user = await get_user_by_id(user_id)
    await notify_user(user["telegram_id"], "💼 Профиль одобрен\n\nТеперь можно брать задания", main_back_kb())
    await callback.message.edit_text("Исполнитель одобрен", reply_markup=admin_main_kb())
    await callback.answer()


@router.callback_query(F.data.startswith("admin:executor:reject:"))
async def admin_executor_reject(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    user_id = parse_callback_tail_int(callback.data, "admin:executor:reject:")
    if user_id is None:
        await callback.answer("Некорректный исполнитель", show_alert=True)
        return
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET is_executor_profile_created = 0, is_executor_approved = 0, age = NULL, active_order_id = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (user_id,),
        )
        await conn.commit()
    user = await get_user_by_id(user_id)
    await notify_user(user["telegram_id"], "💼 Заявка исполнителя отклонена", main_back_kb())
    await callback.message.edit_text("Исполнитель отклонён", reply_markup=admin_main_kb())
    await callback.answer()


@router.callback_query(F.data.startswith("admin:executor:block:"))
async def admin_executor_block(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    user_id = parse_callback_tail_int(callback.data, "admin:executor:block:")
    if user_id is None:
        await callback.answer("Некорректный исполнитель", show_alert=True)
        return
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET is_blocked = 1, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (user_id,),
        )
        await conn.commit()
    await callback.message.edit_text("Исполнитель заблокирован", reply_markup=admin_main_kb())
    await callback.answer()


@router.callback_query(F.data == "admin:withdrawals")
async def admin_withdrawals(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.edit_text("💸 Выводы", reply_markup=admin_withdrawals_kb())
    await callback.answer()


@router.callback_query(F.data.startswith("admin:withdrawals:"))
async def admin_withdrawals_section(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    status = callback.data.split(":")[-1]
    async with db_pool.acquire() as conn:
        db_status = "paid" if status == "paid" else ("rejected" if status == "rejected" else "pending")
        rows = await conn.execute("SELECT id, requested_amount FROM withdrawal_requests WHERE status = ? ORDER BY id DESC LIMIT 30", (db_status,))
        rows = await rows.fetchall()
        buttons = [[InlineKeyboardButton(text=f"💸 Заявка #{r['id']} • {format_price(r['requested_amount'])}", callback_data=f"admin:withdraw:view:{r['id']}")] for r in rows]
        buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin:withdrawals")])
        await callback.message.edit_text("💸 Выводы", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
        await callback.answer()


@router.callback_query(F.data.startswith("admin:withdraw:view:"))
async def admin_withdraw_view(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    wid = parse_callback_tail_int(callback.data, "admin:withdraw:view:")
    if wid is None:
        await callback.answer("Некорректная заявка", show_alert=True)
        return
    async with db_pool.acquire() as conn:
        req = await conn.execute("SELECT * FROM withdrawal_requests WHERE id = ?", (wid,))
        req_row = await req.fetchone()
        if not req_row:
            await callback.answer("Не найдено", show_alert=True)
            return
        req_dict = dict(req_row)
        user = await get_user_by_id(req_dict["user_id"])
        text = (
            f"💸 Заявка на вывод #{wid}\n\n"
            f"Пользователь: {escape_html(user.get('name') or '—')}\n"
            f"Telegram ID: {user['telegram_id']}\n"
            f"Телефон: {escape_html(req_dict['phone'])}\n"
            f"Банк: {escape_html(req_dict['bank_name'])}\n"
            f"Сумма: {format_price(req_dict['requested_amount'])}\n"
            f"Статус: {escape_html(req_dict['status'])}"
        )
        await callback.message.edit_text(text, reply_markup=admin_withdrawal_actions_kb(wid, req_dict["status"]))
        await callback.answer()


@router.callback_query(F.data.startswith("admin:withdraw:pay:"))
async def admin_withdraw_pay(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    wid = parse_callback_tail_int(callback.data, "admin:withdraw:pay:")
    if wid is None:
        await callback.answer("Некорректная заявка", show_alert=True)
        return
    async with withdraw_lock(wid):
        ok, _ = await process_withdrawal(wid, approve=True)
        if not ok:
            await callback.answer("Уже обработано", show_alert=True)
            return
    await callback.message.edit_text("Выплата подтверждена", reply_markup=admin_withdrawals_kb())
    await callback.answer()


@router.callback_query(F.data.startswith("admin:withdraw:reject:no:"))
async def admin_withdraw_reject_no(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    wid = parse_callback_tail_int(callback.data, "admin:withdraw:reject:no:")
    if wid is None:
        await callback.answer("Некорректная заявка", show_alert=True)
        return
    async with withdraw_lock(wid):
        ok, _ = await process_withdrawal(wid, approve=False, comment="")
        if not ok:
            await callback.answer("Уже обработано", show_alert=True)
            return
    await callback.message.edit_text("Заявка отклонена", reply_markup=admin_withdrawals_kb())
    await callback.answer()


@router.callback_query(F.data.startswith("admin:withdraw:reject:comment:"))
async def admin_withdraw_reject_comment_start(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    wid = parse_callback_tail_int(callback.data, "admin:withdraw:reject:comment:")
    if wid is None:
        await callback.answer("Некорректная заявка", show_alert=True)
        return
    await state.set_state(AdminRejectWithdrawalComment.comment)
    await state.update_data(admin_withdraw_id=wid)
    await callback.message.edit_text("Введите комментарий для пользователя")
    await callback.answer()


@router.message(AdminRejectWithdrawalComment.comment)
async def admin_withdraw_reject_comment_save(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    data = await state.get_data()
    async with withdraw_lock(data["admin_withdraw_id"]):
        ok, _ = await process_withdrawal(
            data["admin_withdraw_id"],
            approve=False,
            comment=(message.text or "").strip()
        )
    await state.clear()
    if not ok:
        await message.answer("Заявка уже обработана или недоступна для отклонения", reply_markup=admin_main_kb())
        return
    await message.answer("Заявка отклонена", reply_markup=admin_main_kb())


@router.callback_query(F.data == "admin:balances")
async def admin_balances(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.edit_text("💰 Балансы", reply_markup=admin_balances_kb())
    await callback.answer()


@router.callback_query(F.data.in_({"admin:balances:add", "admin:balances:sub"}))
async def admin_balances_start(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    mode = "add" if callback.data.endswith(":add") else "sub"
    await state.set_state(AdminBalanceAdjust.user_tg_id)
    await state.update_data(balance_mode=mode)
    await callback.message.edit_text("Введите Telegram ID пользователя")
    await callback.answer()


@router.message(AdminBalanceAdjust.user_tg_id)
async def admin_balance_user_id(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    text = (message.text or "").strip()
    if not text.isdigit():
        await message.answer("Введите Telegram ID числом")
        return
    await state.update_data(balance_target_tg=int(text))
    await state.set_state(AdminBalanceAdjust.amount)
    await message.answer("Введите сумму")


@router.message(AdminBalanceAdjust.amount)
async def admin_balance_amount(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    text = (message.text or "").strip()
    if not text.isdigit():
        await message.answer("Введите сумму числом")
        return
    amount = int(text)
    data = await state.get_data()
    async with db_pool.acquire() as conn:
        user = await conn.execute("SELECT * FROM users WHERE telegram_id = ?", (data["balance_target_tg"],))
        user_row = await user.fetchone()
        if not user_row:
            await state.clear()
            await message.answer("Пользователь не найден", reply_markup=admin_main_kb())
            return
        user_dict = dict(user_row)
        if data["balance_mode"] == "sub" and user_dict["balance"] < amount:
            await message.answer("Недостаточно средств у пользователя")
            return
        await conn.execute("BEGIN IMMEDIATE")
        try:
            if data["balance_mode"] == "add":
                cur = await conn.execute(
                    "UPDATE users SET balance = balance + ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (amount, user_dict["id"])
                )
                if cur.rowcount != 1:
                    await conn.rollback()
                    await message.answer("Ошибка обновления баланса")
                    return
                await conn.execute(
                    "INSERT INTO transactions (user_id, amount, type, comment) VALUES (?, ?, 'manual_adjustment', ?)",
                    (user_dict["id"], amount, "Ручное пополнение"),
                )
            else:
                cur = await conn.execute(
                    "UPDATE users SET balance = balance - ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? AND balance >= ?",
                    (amount, user_dict["id"], amount)
                )
                if cur.rowcount != 1:
                    await conn.rollback()
                    await message.answer("Недостаточно средств или ошибка обновления")
                    return
                await conn.execute(
                    "INSERT INTO transactions (user_id, amount, type, comment) VALUES (?, ?, 'manual_adjustment', ?)",
                    (user_dict["id"], -amount, "Ручное списание"),
                )
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise
    await state.clear()
    await message.answer("Баланс обновлён", reply_markup=admin_main_kb())


# ------------------- FALLBACK -------------------
@router.message()
async def fallback(message: Message):
    await ensure_user(message.from_user.id)
    await message.answer("🤝 НаВсеРуки\n\n👇 Выберите действие", reply_markup=main_menu_kb())


# ------------------- ЗАПУСК -------------------
async def main():
    await db_pool.initialize()

    app = web.Application()
    app.router.add_post("/yookassa-webhook", yookassa_webhook)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8080)
    await site.start()
    print("Webhook server started on port 8080")

    watchdog_task = asyncio.create_task(status_watchdog_loop())

    try:
        await dp.start_polling(bot)
    finally:
        watchdog_task.cancel()
        with suppress(asyncio.CancelledError):
            await watchdog_task
        await runner.cleanup()
        await db_pool.close()


if __name__ == "__main__":
    asyncio.run(main())
