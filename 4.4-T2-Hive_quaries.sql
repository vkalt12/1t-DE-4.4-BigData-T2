-- ЗАДАЧА: 
-- Аналитики хотят сводную статистику на уровне каждой компании и на уровне каждого года получить целевую возрастную группу подписчиков — то есть, возрастную группу, представители которой чаще всего совершали подписку именно в текущий год на текущую компанию

-- Включение поддержки бакетов в hive
SET hive.enforce.bucketing=true;

-- Создание временной таблицы для загрузки данных из csv-файлов. 
CREATE TEMPORARY TABLE IF NOT EXISTS customers_temp (
    index_ INT,
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    company STRING,
    city STRING,
    country STRING,
    phone_1 STRING,
    phone_2 STRING,
    email STRING,
    subscription_date DATE,
    website STRING,
    group_ INT,
    subscription_year INT
)
ROW FORMAT DELIMITED                -- указание на то, что формат данных - с разделителями
FIELDS TERMINATED BY ','            -- поля отделены друг от друга запятыми
LINES TERMINATED BY '\n'            -- строки оканчиваются символом новой строки '\n'
TBLPROPERTIES ("skip.header.line.count"="1");   -- Первая строка файла будет проигнорирована. Атрибут используется для игнорирования количества строк 'n' в верхней части файла перед загрузкой данных в Hive.

-- Загрузка данных из csv-файла в созданную временную таблицу
LOAD DATA INPATH '/user/loghue/4.4-Task2/csv_changed/customers_ch.csv' OVERWRITE INTO TABLE customers_temp;

-- Создание таблицы customers с использованием партиционирования и бакетирования (в неё будут собраны данные из временной таблицы)
CREATE TABLE IF NOT EXISTS customers (
    index_ INT,
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    company STRING,
    city STRING,
    country STRING,
    phone_1 STRING,
    phone_2 STRING,
    email STRING,
    subscription_date DATE,
    website STRING,
    group_ INT
)
PARTITIONED BY (subscription_year INT)      -- колонка, по которой производится партиционирование указывается в атрибуте PARTITION BY, но в основном запросе CREAT TABLE не указывается (иначе выдаёт ошибку дублирования)
CLUSTERED BY (first_name, last_name, email) INTO 10 BUCKETS     -- Бакетирование - разделение на 10 бакетов
STORED AS PARQUET;      -- указание на использование формата PARQUET для хранения данных (омпактный и эффективный способ хранения данных)
;

-- Вставка данных в таблицу customers (партицирование которой основано на значении столбца subscription_year) из временной таблицы. Все строки, вставленные в эту таблицу, будут в партиции, где значение subscription_year равно 2020.
INSERT INTO TABLE customers PARTITION(subscription_year = 2020)
SELECT index_, customer_id, first_name, last_name, company, city, country,
	phone_1, phone_2, email, subscription_date, website, group_
FROM customers_temp WHERE subscription_year = 2020;

-- Аналогичная вставка для года подписки (subscription_year), равного 2021.
INSERT INTO TABLE customers PARTITION(subscription_year = 2021)
SELECT index_, customer_id, first_name, last_name, company, city, country,
	phone_1, phone_2, email, subscription_date, website, group_
FROM customers_temp WHERE subscription_year = 2021;

-- Аналогичная вставка для года подписки (subscription_year), равного 2022.
INSERT INTO TABLE customers PARTITION(subscription_year = 2022)
SELECT index_, customer_id, first_name, last_name, company, city, country,
	phone_1, phone_2, email, subscription_date, website, group_
FROM customers_temp WHERE subscription_year = 2022;

-- Комплекс запросов для создания таблицы organizations и загрузки в неё данных
CREATE TEMPORARY TABLE IF NOT EXISTS organizations_temp (
	index_ INT,
	organization_id STRING,
	name STRING,
	website STRING,
	country STRING,
	description STRING,
	founded INT,
	industry STRING,
	number_of_employees INT,
	group_ INT
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH '/user/loghue/4.4-Task2/csv_changed/organizations_ch.csv' OVERWRITE INTO TABLE organizations_temp;

-- Для данных в этой таблице применяется только бакетирование, без партиционирования.
CREATE TABLE IF NOT EXISTS organizations (
 	index_ INT,
	organization_id STRING,
	name STRING,
	website STRING,
	country STRING,
	description STRING,
	founded INT,
	industry STRING,
	number_of_employees INT,
	group_ INT
)
CLUSTERED BY (name) INTO 10 BUCKETS
STORED AS PARQUET;

INSERT INTO TABLE organizations
SELECT * 
FROM organizations_temp;

-- Комплекс запросов для создания таблицы organizations и загрузки в неё данных
CREATE TEMPORARY TABLE IF NOT EXISTS people_temp (
	index_ INT,
	user_id STRING,
	first_name STRING,
	last_name STRING,
	sex STRING,
	email STRING,
	phone STRING,
	date_of_birth DATE,
	job_title STRING,
	group_ INT
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH '/user/loghue/4.4-Task2/csv_changed/people_ch.csv' OVERWRITE INTO TABLE people_temp;

CREATE TABLE IF NOT EXISTS people (
	index_ INT,
	user_id STRING,
	first_name STRING,
	last_name STRING,
	sex STRING,
	email STRING,
	phone STRING,
	date_of_birth DATE,
	job_title STRING,
	group_ INT
)
CLUSTERED BY(first_name, last_name, email) INTO 10 BUCKETS
STORED AS PARQUET;

INSERT INTO TABLE people
SELECT * 
FROM people_temp;

-- Создание витрины, которая решает поставленную задачу (содержит сводную статистику на уровне каждой компании и на уровне каждого года получить целевую возрастную группу подписчиков — то есть, возрастную группу, представители которой чаще всего совершали подписку именно в текущий год на текущую компанию)
CREATE TABLE IF NOT EXISTS stat_mart AS
WITH custumers_union AS (       -- Соединение партиционированных таблиц
	SELECT * FROM customers WHERE subscription_year = 2020
	UNION
	SELECT * FROM customers WHERE subscription_year = 2021
	UNION
	SELECT * FROM customers WHERE subscription_year = 2022
),
customers_age_range AS (        -- Таблица с возрастными группами
	SELECT cust.company, cust.subscription_year, COUNT(*) AS amount,
		CASE
			WHEN YEAR(CURRENT_DATE) - YEAR(pep.date_of_birth) <= 18 THEN '0 - 18'
			WHEN YEAR(CURRENT_DATE) - YEAR(pep.date_of_birth) <= 25 THEN '19 - 25'
			WHEN YEAR(CURRENT_DATE) - YEAR(pep.date_of_birth) <= 35 THEN '26 - 35'
			WHEN YEAR(CURRENT_DATE) - YEAR(pep.date_of_birth) <= 45 THEN '36 - 45'
			WHEN YEAR(CURRENT_DATE) - YEAR(pep.date_of_birth) <= 55 THEN '46 - 55'
			WHEN YEAR(CURRENT_DATE) - YEAR(pep.date_of_birth) <= 65 THEN '56 - 65'
			ELSE 'more than 65'
		END AS age_group
		FROM custumers_union cust
	JOIN people pep ON cust.first_name = pep.first_name AND cust.last_name = pep.last_name AND cust.email = pep.email
	JOIN organizations org ON cust.company = org.name
	GROUP BY cust.company, cust.subscription_year, pep.date_of_birth
)
SELECT company, subscription_year, age_group, MAX(amount) AS num_cust   -- Итоговая витрина
FROM customers_age_range
GROUP BY company, subscription_year, age_group
;

-- Удаление таблиц
DROP TABLE customers_temp;
DROP TABLE organizations_temp;
DROP TABLE people_temp;
