# Online Cinema: Event Streaming + Analytics Pipeline

Система собирает события онлайн-кинотеатра, хранит их в ClickHouse, рассчитывает бизнес-метрики и выгружает результаты в PostgreSQL. Реализован функционал задания на 1–7 баллов.

## Архитектура

- **Movie Service** — валидация, генерация и публикация событий в Kafka.
- **Kafka и Schema Registry** — топик `movie-events`, три партиции, версионируемая Avro-схема (`schemas/movie_event.avsc`). В качестве ключа партиционирования выбран `user_id`. Благодаря этому все события одного пользователя при неизменном количестве партиций попадают в одну партицию Kafka, что позволяет сохранять порядок их публикации. Это важно для анализа последовательностей пользовательских действий, сессий просмотра и Retention. В отличие от movie_id, выбранный ключ позволяет сохранять порядок событий пользователя даже при взаимодействии с разными фильмами. Producer использует `acks=all`, идемпотентность, retry с exponential backoff и логирование доставки.
- **ClickHouse** — Kafka Engine и Materialized View переносят события в MergeTree. Партиционирование по месяцу (`toYYYYMM(timestamp)`), сортировка по `(user_id, timestamp, event_id)` для анализа истории пользователей.
- **Aggregation Service** — отдельный сервис, вычисляющий дневные метрики по расписанию и по HTTP-запросу.
- **PostgreSQL** — хранение готовых бизнес-метрик.

Стек: Python, Flask, Kafka, Avro, ClickHouse, PostgreSQL, APScheduler, pytest, Docker Compose.

## Запуск

Необходимы Docker и Docker Compose v2. Для тестов также нужен Python 3.12+.

```bash
docker compose up --build -d
docker compose ps -a
```

Топик, Avro-схема и таблицы БД создаются автоматически. Контейнеры `kafka-init` и `schema-init` завершаются с кодом 0 после инициализации.

| Компонент | Адрес с хоста |
|---|---|
| Movie Service | `localhost:5000` |
| Aggregation Service | `localhost:5001` |
| Kafka | `localhost:9092` |
| Schema Registry | `localhost:8081` |
| ClickHouse | `localhost:8123`, `localhost:9000` |
| PostgreSQL | `localhost:5432` |

## Генерация и публикация событий

`POST /events` принимает JSON-событие, возвращает HTTP 202 и идентификатор при успешной публикации, HTTP 400 при ошибке валидации. `GET /health` проверяет доступность Movie Service.

Поддерживаются `VIEW_STARTED`, `VIEW_PAUSED`, `VIEW_RESUMED`, `VIEW_FINISHED`, `LIKED`, `SEARCHED`. Для событий просмотра обязателен неотрицательный целочисленный `progress_seconds`. Время передаётся в миллисекундах Unix (UTC).

Пример запуска генератора (50 пользовательских сессий с реалистичными последовательностями действий):

```bash
docker compose exec movie-service python app/publish_generated.py
```

Проверка поступления событий и доступных дат:

```bash
docker compose exec clickhouse clickhouse-client \
  --user cinema --password cinema_dev_password --database cinema \
  --query 'SELECT toDate(timestamp) AS day, count() AS events FROM movie_events GROUP BY day ORDER BY day DESC'
```

## Агрегация и бизнес-метрики

Scheduler запускается при старте и затем каждые 1440 минут (настраивается через `AGGREGATION_INTERVAL_MINUTES`). Автоматический цикл обрабатывает предыдущий UTC-день и обновляет когорты D0, D−1 и D−7 относительно него.

Ручной пересчёт за дату:

```bash
DAY=2026-11-23
curl -i -X POST http://localhost:5001/aggregate \
  -H 'Content-Type: application/json' \
  -d "{\"date\":\"$DAY\"}"
```

| Метрика | Расчёт |
|---|---|
| DAU | Уникальные пользователи за день (`uniqExact`) |
| Среднее время просмотра | Средний `progress_seconds` для `VIEW_FINISHED` |
| Конверсия | Отношение числа `VIEW_FINISHED` к `VIEW_STARTED` за день, % |
| Рейтинг фильмов | По числу начатых просмотров (`row_number`), при равенстве — по `movie_id` |
| Retention D1 / D7 | Доля пользователей когорты с любой активностью ровно через 1 / 7 дней после первого `VIEW_STARTED`, % |

Все расчёты выполняются в UTC. Неопределённые показатели сохраняются как `NULL`. В retention пользователи без просмотров не включаются.

Агрегаты материализуются в ClickHouse (`daily_aggregates`, `movie_aggregates`, `retention_aggregates`) и сохраняются в PostgreSQL (`daily_metrics`, `movie_metrics`, `retention_metrics`). Повторный пересчёт обновляет логические записи: `ReplacingMergeTree(computed_at)` в ClickHouse (чтение через `FINAL`) и составные первичные ключи с UPSERT в PostgreSQL.

### Проверка результатов

```bash
docker compose exec clickhouse clickhouse-client \
  --user cinema --password cinema_dev_password --database cinema \
  --query "SELECT * FROM daily_aggregates FINAL WHERE metric_date = '$DAY'"

docker compose exec postgres psql -U analytics -d cinema_analytics \
  -c "SELECT * FROM daily_metrics WHERE metric_date = '$DAY';"

docker compose exec postgres psql -U analytics -d cinema_analytics \
  -c "SELECT movie_id, metric_value AS rank FROM movie_metrics WHERE metric_date = '$DAY' AND metric_name = 'view_rank' ORDER BY metric_value LIMIT 5;"

docker compose exec postgres psql -U analytics -d cinema_analytics \
  -c "SELECT * FROM retention_metrics WHERE cohort_date = '$DAY';"
```

## Тестирование и обработка ошибок

На запущенном Compose-окружении:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-test.txt
python -m pytest tests/ -v
```

`test_e2e.py` проверяет доставку событий в ClickHouse и валидацию API. `test_aggregation_e2e.py` проверяет бизнес-метрики в обеих БД и идемпотентность пересчёта. Тестовые данные изолируются уникальными идентификаторами и свободным диапазоном дат. Последний результат: **15 passed in 8.86s**.

Aggregation Service выполняет до трёх попыток с паузой 5 секунд при временных ошибках доступа к БД. Логируются начало, завершение, количество записей агрегатов, длительность и ошибки.

## Ограничения и остановка

```bash
docker compose down
```

Для намеренного чистого развёртывания использовать `docker compose down -v`.
