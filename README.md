# DWH: расчёты с курьерами

Ильнур Аскаров · когорта 49 · API nickname `ilnur_askarov`.

Проект загружает заказы из MongoDB, данные бонусной системы PostgreSQL и доставки
из API. Слои STG/DDS/CDM формируют `cdm.dm_courier_ledger` за предыдущий месяц.
Стоимость заказа берётся из MongoDB (`cost`), рейтинг — за полный месяц,
минимальная выплата применяется к целому заказу. Повторный запуск не создаёт
дубликатов в витрине. Модель описана в [api_entities.md](api_entities.md).

## Запуск в новом тренажёре

Нужен учебный контейнер `de-pg-cr-af` с Airflow 2.3.x. Если он ещё не создан:

```bash
docker run -d --name de-project-sprint-5-server -p 3005:3000 -p 3006:3002 -p 15435:5432 cr.yandex/crp1r8pht0n0gl25aug1/de-pg-cr-af:latest
```

Airflow: <http://localhost:3005/airflow>.

1. В Airflow должны быть подключения `PG_WAREHOUSE_CONNECTION` (локальное DWH)
   и `PG_ORIGIN_BONUS_SYSTEM_CONNECTION` (облачная бонусная система, SSL verify-full).
2. Настройте Variables `MONGO_DB_HOST`, `MONGO_DB_USER`, `MONGO_DB_PASSWORD`,
   `MONGO_DB_REPLICA_SET`, `MONGO_DB_DATABASE_NAME`, `MONGO_DB_CERTIFICATE_PATH`.
   Сертификат должен находиться внутри контейнера.
3. Скопируйте DAG из каталога проекта:

```bash
docker cp src/dags/. de-project-sprint-5-server:/lessons/dags/
```

4. Настройте API доставки:

```bash
docker exec de-project-sprint-5-server airflow variables set project5_nickname ilnur_askarov
docker exec de-project-sprint-5-server airflow variables set project5_cohort 49
docker exec de-project-sprint-5-server airflow variables set project5_api_key 25c27781-8fde-4b30-a22e-524044a7580f
```

Это общий ключ из задания; личные пароли/сертификаты в репозиторий не добавляйте.

5. Проверьте импорт и включите три DAG:

```bash
docker exec de-project-sprint-5-server airflow dags list-import-errors
docker exec de-project-sprint-5-server airflow dags unpause project5_delivery_stg
docker exec de-project-sprint-5-server airflow dags unpause project5_delivery_dimensions
docker exec de-project-sprint-5-server airflow dags unpause project5_orders
```

Схемы и таблицы создаются автоматически первой задачей. Расписание основного
DAG — ежедневно в 02:00 UTC, `catchup=False`. Scheduler должен работать.
STG и измерения доставки запускаются родителем; сенсоры в режиме `reschedule`.
Основной DAG продолжает обновлять MongoDB и бонусную систему каждый день.

## Первый месячный расчёт

Загрузите полный предыдущий месяц: обычных семи дней для первого запуска мало.
Например, при запуске 8 ноября 2026 года:

```bash
docker exec de-project-sprint-5-server airflow dags trigger project5_orders --conf '{"from":"2026-10-01 00:00:00","to":"2026-11-08 00:00:00"}'
```

Замените даты на актуальные. Параметры `from` и `to` относятся к дате доставки;
месяц выплаты определяется датой заказа MongoDB. Дальше запускается обычное
ежедневное семидневное окно. Для исправления старых доставок передайте их даты
явно. Исторические полные месяцы пересчитываются вместе с месячным рейтингом.

## Существующий пайплайн спринта

В пустом тренажёре используется включённый `project5_orders_dag.py`.
Если полноценный DAG заказов уже работает, сохраните его: **не подключайте второй
загрузчик к тем же таблицам**. Не копируйте `project5_orders_dag.py`, перенесите
только пакет `project5/` и `project5_delivery_dags.py`, затем подключите расширение:

```python
from project5.integration import extend_orders_dag
extend_orders_dag(your_orders_dag)
```

Его исходные задачи выполнятся до фактов доставки и CDM. Альтернатива —
`tools/install.py --orders-dag-id ID` внутри контейнера. Проверка схемы перед
установкой: `tools/preflight.py`. Сохраните изменённый родительский DAG в репозиторий.
Исходные DAG заказов и бонусов не останавливайте.

## Проверки

```bash
python3 -m unittest discover -s tests -v
```

Дополнительно SQL на изолированном PostgreSQL-движке:

```bash
npm install --no-save @electric-sql/pglite
node tests/test_baseline_sql.mjs .
node tests/test_sql.mjs .
```

В учебном DWH выполните `tests/checks.sql` после успешного завершения всего DAG.
Проверенные сценарии и ограничения — [VALIDATION.md](VALIDATION.md).

## Структура

- `src/dags/project5_orders_dag.py` — исходные источники, заказы, факты и витрина.
- `src/dags/project5_delivery_dags.py` — STG и измерения API доставки.
- `src/dags/project5/` — загрузчики, формулы, зависимости и SQL.
- `api_entities.md` — поля и связи модели.
- `tests/` — проверки Python и SQL.
- `tools/` — диагностика и подключение к существующему DAG.
