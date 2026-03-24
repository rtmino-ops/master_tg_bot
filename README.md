# НаВсеРуки — Telegram Bot

MVP-бот для бытовых поручений. Python + aiogram 3 + SQLite.

---

## 🚀 Быстрый старт (локально, Windows)

### 1. Установи Python 3.11+
Скачай с [python.org](https://python.org) — при установке отметь **"Add to PATH"**

### 2. Создай виртуальное окружение
```bash
python -m venv venv
venv\Scripts\activate
```

### 3. Установи зависимости
```bash
pip install -r requirements.txt
```

### 4. Настрой переменные окружения
```bash
copy .env.example .env
```
Открой `.env` в блокноте и заполни:
- `BOT_TOKEN` — получи у @BotFather в Telegram
- `ADMIN_IDS` — свой Telegram ID (узнать у @userinfobot)
- `SUPPORT_USERNAME` — username саппорта без @

### 5. Создай папку для базы
```bash
mkdir app\data
```

### 6. Запусти
```bash
python botv1_fixed.py
```

---

## 🐳 Запуск через Docker (рекомендуется для сервера)

```bash
# Скопируй и заполни .env
copy .env.example .env

# Запусти
docker-compose up -d

# Логи
docker-compose logs -f

# Остановить
docker-compose down
```

---

## 📁 Структура проекта

```
navseruki-bot/
├── botv1_fixed.py       # основной код бота
├── requirements.txt     # зависимости
├── Dockerfile           # для деплоя
├── docker-compose.yml   # для деплоя с Docker
├── .env.example         # шаблон настроек
├── .env                 # твои настройки (не пушить!)
├── .gitignore
├── README.md
└── app/
    └── data/
        └── navseruki.db # база данных (создаётся автоматически)
```

---

## ⚙️ Переменные окружения

| Переменная | Обязательно | Описание |
|---|---|---|
| `BOT_TOKEN` | ✅ | Токен от @BotFather |
| `ADMIN_IDS` | ✅ | Telegram ID админов через запятую |
| `SUPPORT_USERNAME` | ✅ | Username саппорта (без @) |
| `DATABASE_PATH` | — | Путь к БД (по умолчанию `app/data/navseruki.db`) |
| `YOOKASSA_SHOP_ID` | — | Для приёма платежей |
| `YOOKASSA_SECRET_KEY` | — | Для приёма платежей |
| `REDIS_URL` | — | Для сохранения FSM между перезапусками |

---

## 🛠 Исправленные баги (v1 → fixed)

1. `admin:withdraw:reject:no:` — неверный префикс парсинга callback (кнопка не работала)
2. `admin:withdraw:reject:comment:` — неверный префикс парсинга callback (кнопка не работала)
3. `admin:orders:manual_review` — отсутствовал в `ACTION_CALLBACKS_EXACT` (кнопка игнорировалась)

---

## 🚢 Деплой на сервер

**Рекомендуемые платформы:**
- [Railway.app](https://railway.app) — проще всего, бесплатный тариф есть
- [VPS на Timeweb / Beget](https://timeweb.cloud) — полный контроль, от 200₽/мес
- [Render.com](https://render.com) — бесплатно, но засыпает при простое

**Не подходит:**
- Vercel — только для веб-приложений, не для ботов
- GitHub Pages — только статика

---

## ⚠️ Важно

- Файл `.env` **никогда не пушить** в GitHub — там токены и секреты
- База данных `app/data/` тоже в `.gitignore`
- Для продакшена рекомендуется VPS + Docker
