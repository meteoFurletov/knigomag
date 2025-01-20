import os
import logging
import subprocess
import pandas as pd
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import (
    Updater,
    CommandHandler,
    CallbackContext,
    MessageHandler,
    Filters,
)
from datetime import datetime, timedelta
import warnings

warnings.filterwarnings("ignore")

# Настройка логирования
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# Путь к файлу с данными
DATA_FILE = os.getenv("DATA_FILE", "data/shop_data.parquet")

# Токен Telegram-бота из переменной окружения
BOT_TOKEN = os.getenv("BOT_TOKEN")


# Загрузка данных из файла Parquet
def load_data():
    try:
        data = pd.read_parquet(DATA_FILE)
        data["date"] = pd.to_datetime(
            data["date"], errors="coerce"
        )  # Handle invalid dates
        logger.info("Данные успешно загружены")
        return data
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных из файла: {e}")
        return pd.DataFrame()


data = load_data()


# Команда для запуска бота
def start(update: Update, context: CallbackContext) -> None:
    keyboard = [
        ["Выручка", "Остаток на конец дня"],
        ["Выручка сегодня", "Остаток на конец дня сегодня"],
        ["Выручка вчера", "Остаток на конец дня вчера"],
        ["Загрузка данных", "Расчёт зарплат"],  # Кнопки для запуска скриптов
    ]
    reply_markup = ReplyKeyboardMarkup(
        keyboard, one_time_keyboard=False, resize_keyboard=True
    )
    update.message.reply_text("Выберите опцию:", reply_markup=reply_markup)


# Функция для запуска скрипта загрузки данных (db_loader)
def run_db_loader(update: Update, context: CallbackContext) -> None:
    try:
        update.message.reply_text("Запуск скрипта загрузки данных...")
        subprocess.Popen(["python", "db_loader.py", "--no-schedule"])
        update.message.reply_text("Скрипт загрузки данных успешно запущен.")
    except Exception as e:
        logger.error(f"Ошибка при запуске скрипта загрузки данных: {e}")
        update.message.reply_text(f"Не удалось запустить скрипт: {e}")


# Функция для запуска скрипта расчёта зарплат (salary)
def run_salary(update: Update, context: CallbackContext) -> None:
    try:
        update.message.reply_text("Запуск скрипта расчёта зарплат...")
        subprocess.Popen(["python", "salary.py", "--no-schedule"])
        update.message.reply_text("Скрипт расчёта зарплат успешно запущен.")
    except Exception as e:
        logger.error(f"Ошибка при запуске скрипта расчёта зарплат: {e}")
        update.message.reply_text(f"Не удалось запустить скрипт: {e}")


def handle_text(update: Update, context: CallbackContext) -> None:
    text = update.message.text
    now = datetime.now()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_start = today_start - timedelta(days=1)

    if text == "Выручка":
        update.message.reply_text("Введите дату в формате дд-мм-гг:")
        context.user_data["query"] = "revenue_all"
    elif text == "Остаток на конец дня":
        update.message.reply_text("Введите дату в формате дд-мм-гг:")
        context.user_data["query"] = "balance_all"
    elif text == "Выручка сегодня":
        result = query_data("revenue", today_start, None)
        update.message.reply_text(result)
    elif text == "Остаток на конец дня сегодня":
        result = query_data("balance", today_start, None)
        update.message.reply_text(result)
    elif text == "Выручка вчера":
        result = query_data("revenue", yesterday_start, today_start)
        update.message.reply_text(result)
    elif text == "Остаток на конец дня вчера":
        result = query_data("balance", yesterday_start, today_start)
        update.message.reply_text(result)
    elif text == "Загрузка данных":
        run_db_loader(update, context)
    elif text == "Расчёт зарплат":
        run_salary(update, context)
    else:
        handle_date_input(update, context)


def query_data(query_type: str, start_date: datetime, end_date: datetime) -> str:
    try:
        # Reload data from file each time a query is made
        data = load_data()

        # Ensure date is consistently datetime
        data["date"] = pd.to_datetime(data["date"], errors="coerce")

        # Ensure store_id is consistently a string
        data["store_id"] = data["store_id"].astype(str)

        logger.debug(
            f"Query Type: {query_type}, Start Date: {start_date}, End Date: {end_date}"
        )

        if end_date:
            filtered_data = data[
                (data["date"] >= start_date) & (data["date"] < end_date)
            ]
        else:
            filtered_data = data[data["date"] >= start_date]

        logger.debug(f"Filtered Data:\n{filtered_data}")

        if query_type == "revenue":
            results = filtered_data[["revenue", "seller_name", "store_id"]]
            total_revenue = df.drop_duplicates("store_id")["revenue"].sum()
            summary_row = pd.DataFrame(
                {
                    "store_id": [" "],
                    "seller_name": ["Сумма"],
                    "revenue": [total_revenue],
                }
            )
            results = pd.concat([results, summary_row], ignore_index=True)
        elif query_type == "balance":
            results = filtered_data[["balance", "seller_name", "store_id"]]
            summary_row = pd.DataFrame(
                {
                    "store_id": [" "],
                    "seller_name": ["Сумма"],
                    "balance": [results["balance"].sum()],
                }
            )
            results = pd.concat([results, summary_row], ignore_index=True)

        # Sort by store_id (which is now consistently a string)
        results = results.sort_values(by="store_id")
        formatted_results = "\n".join(
            [
                f"{row['store_id']}  {row['seller_name']}  {row[query_type]}"
                for _, row in results.iterrows()
            ]
        )
        return formatted_results if not results.empty else "No data found"
    except Exception as e:
        logger.error(f"Error querying the data: {e}", exc_info=True)
        return "An error occurred while querying the data."


def handle_date_input(update: Update, context: CallbackContext) -> None:
    user_data = context.user_data
    query_type = user_data.get("query")

    try:
        date_str = update.message.text
        date_obj = datetime.strptime(date_str, "%d-%m-%y")
        date_start = date_obj.replace(hour=0, minute=0, second=0, microsecond=0)
        date_end = date_start + timedelta(days=1)

        if query_type == "revenue_all":
            result = query_data("revenue", date_start, date_end)
        elif query_type == "balance_all":
            result = query_data("balance", date_start, date_end)
        else:
            result = "Unknown command"

        update.message.reply_text(result)

    except ValueError:
        update.message.reply_text(
            "Invalid date format. Please enter the date in format dd-mm-yy:"
        )


def main() -> None:
    updater = Updater(BOT_TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_text))

    updater.start_polling()
    logger.info("Бот запущен")
    updater.idle()


if __name__ == "__main__":
    main()
