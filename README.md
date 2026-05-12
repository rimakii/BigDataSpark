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
report_supplier_sales -> ...
report_product_quality -> 3 rows
SparkContext: Successfully stopped SparkContext
```

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
## Отчёт 1. Продажи по товарам

```sql
SELECT * FROM report_product_sales LIMIT 10;
```

Ожидаемый результат:

```text
product_name | product_category | total_quantity | total_revenue | avg_rating | avg_reviews
Dog Food     | Toy              | 6233           | 293793.2      | 3.0034     | 505.1066
Dog Food     | Food             | 6110           | 282948.17     | 2.9817     | 497.3078
Dog Food     | Cage             | 5893           | 264915.97     | 3.0508     | 484.8214
Bird Cage    | Toy              | 6331           | 296266.92     | 2.9805     | 491.9624
Bird Cage    | Cage             | 6081           | 282284.66     | 3.0087     | 508.7208
Bird Cage    | Food             | 5953           | 267502.54     | 3.0225     | 497.6842
Cat Toy      | Cage             | 5904           | 283824.64     | 2.9882     | 502.3360
Cat Toy      | Food             | 6072           | 276444.75     | 2.9233     | 512.9722
Cat Toy      | Toy              | 6046           | 281871.27     | 3.0994     | 499.0072
```



## Отчёт 2. Продажи по покупателям

```sql
SELECT * FROM report_customer_sales LIMIT 10;
```

Ожидаемый результат содержит поля:

```text
customer_email
customer_country
total_spent
orders
avg_order
```

Пример результата:

```text
customer_email            | customer_country | total_spent | orders | avg_order
aanwyljd@si.edu           | Honduras         | 144.36      | 1      | 144.36
hdrehernj@cyberchimps.com | Peru             | 93.65       | 1      | 93.65
pfarrar7o@businessweek.com| China            | 282.51      | 1      | 282.51
llarret8h@alexa.com       | Russia           | 269.28      | 1      | 269.28
```


## Отчёт 3. Продажи по времени

```sql
SELECT * FROM report_time_sales LIMIT 10;
```

Ожидаемый результат:

```text
year | month | revenue   | orders
2021 | 8     | 221275.78 | 897
2021 | 6     | 215042.8  | 822
2021 | 5     | 211764.86 | 828
2021 | 10    | 228743.32 | 892
2021 | 11    | 200154.69 | 801
2021 | 9     | 210623.43 | 839
2021 | 12    | 191368.86 | 770
2021 | 7     | 220496.51 | 858
2021 | 3     | 207282.2  | 843
2021 | 2     | 192348.31 | 739
```

## Отчёт 4. Продажи по магазинам

```sql
SELECT * FROM report_store_sales LIMIT 10;
```

Ожидаемый результат:

```text
store_name   | store_country  | revenue | orders
Edgeclub     | China          | 2203.39 | 7
Brainsphere  | Ukraine        | 454.21  | 1
Skipfire     | Uruguay        | 158.76  | 1
Photobean    | South Africa   | 406.57  | 1
Skajo        | Ukraine        | 776.14  | 2
Jaxnation    | Serbia         | 231.05  | 1
Jabbersphere | Czech Republic | 477.31  | 1
Bluezoom     | Portugal       | 89.74   | 1
Eazzy        | China          | 1085.93 | 4
Realfire     | Japan          | 374.25  | 2
```

## Отчёт 5. Продажи по поставщикам

```sql
SELECT * FROM report_supplier_sales LIMIT 10;
```

Ожидаемый результат содержит поля:

```text
supplier_name
revenue
sales
```

## Отчёт 6. Качество товаров

```sql
SELECT * FROM report_product_quality LIMIT 10;
```

Ожидаемый результат:

```text
product_name | rating | sales
Bird Cage    | 3.0035 | 18365
Dog Food     | 3.0115 | 18236
Cat Toy      | 3.0043 | 18022
```

# Описание Spark-скриптов

## `etl_to_star.py`

Скрипт выполняет трансформацию исходной таблицы `mock_data` в модель «звезда».

Создаются таблицы измерений:

```text
dim_customers
dim_products
dim_stores
dim_suppliers
dim_sellers
```

Создаётся таблица фактов:

```text
fact_sales
```

Таблица `fact_sales` содержит:

```text
sale_id
customer_id
product_id
store_id
supplier_id
seller_id
sale_date
sale_quantity
sale_total_price
```

## `reports_to_clickhouse.py`

Скрипт формирует аналитические отчёты на основе таблиц модели «звезда».

В ClickHouse создаются следующие витрины:

```text
report_product_sales
report_customer_sales
report_time_sales
report_store_sales
report_supplier_sales
report_product_quality
```


