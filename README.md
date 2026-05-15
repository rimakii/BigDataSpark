# BigDataSpark

Лабораторная работа реализует пайплайн обработки данных с использованием PostgreSQL, Apache Spark и ClickHouse.

# Инструкция по запуску

## 1. Запуск контейнеров

```powershell
docker compose up -d
```

После запуска должны быть подняты контейнеры:

```text
bigdataspark_pg
bigdataspark_spark
bigdataspark_clickhouse
```

## 2. Создание таблицы для исходных данных

```powershell
docker exec -i bigdataspark_pg psql -U postgres -d bigdataspark -f /sql/01_raw_table.sql
```

## 3. Загрузка CSV-файлов в PostgreSQL

```powershell
docker exec -i bigdataspark_pg psql -U postgres -d bigdataspark -f /sql/02_import_all_csv.sql
```

## 4. Проверка загрузки данных

```powershell
docker exec -i bigdataspark_pg psql -U postgres -d bigdataspark -f /sql/03_check.sql
```

Ожидаемый результат:

```text
raw_rows
----------
10000
```

---

## 5. Запуск Spark-трансформации в модель «звезда»

```powershell
docker exec -u root -it bigdataspark_spark /opt/spark/bin/spark-submit `
  --packages org.postgresql:postgresql:42.7.3 `
  /app/etl_to_star.py
```

В результате создаются таблицы:

```text
dim_customers
dim_products
dim_stores
dim_suppliers
dim_sellers
fact_sales
```

Ожидаемый результат выполнения Spark-джобы:

```text
fact_sales -> 10000 rows
SparkContext: Successfully stopped SparkContext
```

---

## 6. Запуск Spark-трансформации отчётов в ClickHouse

```powershell
docker exec -u root -it bigdataspark_spark /opt/spark/bin/spark-submit `
  --packages org.postgresql:postgresql:42.7.3,com.clickhouse:clickhouse-jdbc:0.6.3 `
  /app/reports_to_clickhouse.py
```

Ожидаемый результат выполнения Spark-джобы:

```text
report_product_sales -> 9 rows
report_customer_sales -> 10000 rows
report_time_sales -> 12 rows
report_store_sales -> 6000 rows
report_supplier_sales -> ... rows
report_product_quality -> 3 rows
SparkContext: Successfully stopped SparkContext
```

---

# Проверка результатов в ClickHouse

Войти в ClickHouse:

```powershell
docker exec -it bigdataspark_clickhouse clickhouse-client --user default --password postgres
```

Выбрать базу данных:

```sql
USE reports;
```

## Список созданных отчётов

```sql
SHOW TABLES;
```

Ожидаемый результат:

```text
report_customer_sales
report_product_quality
report_product_sales
report_store_sales
report_supplier_sales
report_time_sales
```

---

## Отчёт 1. Витрина продаж по продуктам

```sql
-- Топ-10 самых продаваемых продуктов
SELECT product_name, product_category, total_quantity, total_revenue
FROM report_product_sales
ORDER BY total_quantity DESC
LIMIT 10;
```

```sql
-- Общая выручка по категориям
SELECT product_category, sum(total_revenue) AS category_revenue
FROM report_product_sales
GROUP BY product_category
ORDER BY category_revenue DESC;
```

```sql
-- Топ-1 продукт в каждой категории по выручке
SELECT product_name, product_category, total_revenue
FROM report_product_sales
WHERE category_rank = 1
ORDER BY total_revenue DESC;
```

---

## Отчёт 2. Витрина продаж по клиентам


```sql
-- Топ-10 клиентов с наибольшей суммой покупок
SELECT customer_email, customer_country, total_spent, orders
FROM report_customer_sales
ORDER BY total_spent DESC
LIMIT 10;
```

```sql
-- Распределение клиентов по странам
SELECT customer_country, count(*) AS customers, sum(total_spent) AS revenue
FROM report_customer_sales
GROUP BY customer_country
ORDER BY revenue DESC;
```

```sql
-- Средний чек по клиентам
SELECT customer_email, customer_country, avg_order
FROM report_customer_sales
ORDER BY avg_order DESC
LIMIT 10;
```

---

## Отчёт 3. Витрина продаж по времени


```sql
-- Месячные тренды продаж
SELECT year, month, revenue, orders, avg_order
FROM report_time_sales
ORDER BY year, month;
```

```sql
-- Годовая выручка
SELECT year, sum(revenue) AS annual_revenue, sum(orders) AS annual_orders
FROM report_time_sales
GROUP BY year
ORDER BY year;
```

```sql
-- Средний размер заказа по месяцам
SELECT month, avg(avg_order) AS avg_monthly_order
FROM report_time_sales
GROUP BY month
ORDER BY month;
```

---

## Отчёт 4. Витрина продаж по магазинам


```sql
-- Топ-5 магазинов по выручке
SELECT store_name, store_city, store_country, revenue, orders
FROM report_store_sales
ORDER BY revenue DESC
LIMIT 5;
```

```sql
-- Распределение продаж по странам
SELECT store_country, sum(revenue) AS revenue, sum(orders) AS orders
FROM report_store_sales
GROUP BY store_country
ORDER BY revenue DESC;
```

```sql
-- Средний чек для каждого магазина
SELECT store_name, store_city, avg_check
FROM report_store_sales
ORDER BY avg_check DESC
LIMIT 10;
```

---

## Отчёт 5. Витрина продаж по поставщикам


```sql
-- Топ-5 поставщиков по выручке
SELECT supplier_name, supplier_country, revenue, sales
FROM report_supplier_sales
ORDER BY revenue DESC
LIMIT 5;
```

```sql
-- Средняя цена товаров от каждого поставщика
SELECT supplier_name, avg_product_price
FROM report_supplier_sales
ORDER BY avg_product_price DESC;
```

```sql
-- Распределение выручки по странам поставщиков
SELECT supplier_country, sum(revenue) AS revenue
FROM report_supplier_sales
GROUP BY supplier_country
ORDER BY revenue DESC;
```

---

## Отчёт 6. Витрина качества продукции


```sql
-- Продукты с наивысшим рейтингом
SELECT product_name, product_category, avg_rating, total_sales
FROM report_product_quality
ORDER BY avg_rating DESC
LIMIT 10;
```

```sql
-- Продукты с наименьшим рейтингом
SELECT product_name, product_category, avg_rating, total_sales
FROM report_product_quality
ORDER BY avg_rating ASC
LIMIT 10;
```

```sql
-- Продукты с наибольшим количеством отзывов
SELECT product_name, avg_reviews, total_sales
FROM report_product_quality
ORDER BY avg_reviews DESC
LIMIT 10;
```

```sql
-- Корреляция рейтинга с объёмом продаж
SELECT product_name, product_category, avg_rating, total_sales, rating_sales_corr
FROM report_product_quality
ORDER BY rating_sales_corr DESC;
```

---

# Описание Spark-скриптов

## `etl_to_star.py`

Трансформирует исходную таблицу `mock_data` в модель «звезда».

Таблицы измерений:

```text
dim_customers  — покупатели
dim_products   — товары
dim_stores     — магазины
dim_suppliers  — поставщики
dim_sellers    — продавцы
```

Таблица фактов:

```text
fact_sales: sale_id, customer_id, product_id, store_id,
            supplier_id, seller_id, sale_date, sale_quantity, sale_total_price
```

## `reports_to_clickhouse.py`

Формирует 6 аналитических витрин на основе таблиц модели «звезда»