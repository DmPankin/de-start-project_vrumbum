-- Этап 1. Создание и заполнение БД
CREATE SCHEMA IF NOT EXISTS raw_data;

--создание таблицы для сырых данных
CREATE TABLE raw_data.sales (
    id integer not null,
	auto varchar NULL,
	gasoline_consumprion numeric NULL,
	price float4 NULL, -- выбираем тип float потому что есть такие значения 49587.884999999995
	"date" date NULL,
	person varchar NULL,
	phone varchar NULL,
	discount int4 NULL,
	brand_origin varchar NULL);


/*копируем сырые данные в таблицу*/

COPY raw_data.sales FROM 'C:\Dev\ProjectSQL\cars.csv' WITH CSV HEADER NULL 'null';

/* создание схемы  */

CREATE SCHEMA IF NOT EXISTS car_shop;

/* справочник цветов машин  */

CREATE TABLE car_shop.colors (
	color_id serial4 NOT NULL PRIMARY KEY,
	color_name varchar NOT NULL unique -- названия цветов в таблице должны быть уникальными
);

/*  справочник стран */

CREATE TABLE car_shop.countries (
	country_id serial4 NOT NULL PRIMARY KEY,
	country_name varchar NOT NULL UNIQUE      -- назввание страны должно быть уникальным
);

/* справочник физ.лиц  */ 

CREATE TABLE car_shop.persons (
	person_id serial4 NOT NULL PRIMARY KEY,
	first_name varchar NOT NULL,
	last_name varchar NOT NULL,
	phone varchar NOT NULL unique -- т.к. ФИО может повторяться, решил сделать телефон уникальным ключем
);


/*справочник брэндов, имеет внешний ключ  в справочнике стран   */

CREATE TABLE car_shop.brands (
	brand_id serial4 NOT NULL PRIMARY KEY,
	brand_name varchar NOT NULL UNIQUE,  -- уникальное название брэнда
	brand_origin int4 NULL REFERENCES car_shop.countries -- внешний ключ в таблице Countries
);

/* справочник моделей, содержит ссылку на справочник брэндов  */

CREATE TABLE car_shop.car_models (
	model_id serial4 NOT NULL PRIMARY KEY,
	model_name varchar NOT NULL,
	brand_id int4 NOT NULL REFERENCES car_shop.brands,  -- внешний ключ на справочник брэндов
	gasoline_consumtion numeric null,
	CONSTRAINT model_brand UNIQUE (model_name,brand_id) -- ограничение на уникальность сочетания брэнда и модели
);


/* итоговая таблица продаж  */

CREATE TABLE car_shop.car_sales (
	sale_id serial4 NOT NULL PRIMARY KEY,
	model_id int4 NOT NULL REFERENCES car_shop.car_models(model_id), -- внешний ключ на модели
	color_id int4 NOT NULL REFERENCES car_shop.colors(color_id),   -- внешний ключ на цвета
	person_id int4 NOT NULL REFERENCES car_shop.persons(person_id),  -- внешний ключ на клиентов
	sale_date timestamp NOT NULL,
	price numeric(9, 2) NOT NULL,
	discount int4 DEFAULT 0 NOT NULL
);

/* заполнение справочника цветов
   для вставки выбираем уникальные цвета из таблицы сырых данных  */

insert into car_shop.colors(color_name)
select distinct substr(auto, STRPOS(auto, ',') + 2, length(auto)) from raw_data.sales;

/* заполнение справочника стран
   выборка уникальных стран из сырых данных  */

insert into car_shop.countries(country_name)
select distinct brand_origin from raw_data.sales where brand_origin is not null;

/* заполнение справочника покупателей парсим из поля auto имя и фамилию клиента и вставляем в разные поля 
   в справочник клиентов, телефон должен быть уникальным  */

insert into car_shop.persons(first_name, last_name, phone)
select distinct substr(person, 1, STRPOS(person, ' ') -1),   substr(person, STRPOS(person, ' ') +1, length(person)), phone from raw_data.sales;

/* заполняем справочник брэндов из поля auto и подставляем ID старны из справочника стран, соединенного по полю brand_origin сырых данных   */

insert into car_shop.brands(brand_name, brand_origin)
select distinct substr(auto, 1, STRPOS(auto, ' ') - 1), country_id from raw_data.sales s left join car_shop.countries c
on s.brand_origin = c.country_name;

/* заполняем справочник моделей распарсив поле auto, добавляем brand_id из таблицы brands, присоединенной по названию брэнда из поля auto */

insert into car_shop.car_models(brand_id, model_name, gasoline_consumtion)
select distinct car_shop.brands.brand_id,
substr(auto, STRPOS(auto, ' ') + 1, STRPOS(auto, ',')-1-STRPOS(auto, ' ')), gasoline_consumprion
from raw_data.sales join car_shop.brands on substr(auto, 1, STRPOS(auto, ' ') - 1)=car_shop.brands.brand_name;


/* заполняем таблицу продаж со ссылками на справочники colors, persons, brands, car_models, присоединенных 
 к таблице сырых данных и часть данных выбираем из самой таблицы сырых данных */

insert into car_shop.car_sales(model_id, color_id, person_id, sale_date, price, discount)
select model_id, color_id, person_id, date, price, discount 
from raw_data.sales s left join car_shop.colors cl on  substr(auto, STRPOS(auto, ',') + 2, length(auto))=color_name
left join car_shop.persons p on s.phone = p.phone
left join car_shop.brands b on substr(auto, 1, STRPOS(auto, ' ') - 1)=b.brand_name
left join car_shop.car_models cm on substr(auto, STRPOS(auto, ' ') + 1, STRPOS(auto, ',')-1-STRPOS(auto, ' '))=cm.model_name 
and b.brand_id = cm.brand_id;


-- Этап 2. Создание выборок

---- Задание 1. Напишите запрос, который выведет процент моделей машин, у которых нет параметра `gasoline_consumption`.

/*делаем 2 подзапроса и из них получаем 2 значения  */

select  100*case when cnt=0 then 0 else ROUND(cnt_null/cnt::numeric, 2) end nulls_percentage_gasoline_consumption 
from (select count(*) cnt_null from car_shop.car_models cm
where cm.gasoline_consumtion is null) sub_query_2, 
(select count(*) cnt from car_shop.car_models) sub_query_1;


---- Задание 2. Напишите запрос, который покажет название бренда и среднюю цену его автомобилей в разбивке по всем годам с учётом скидки.

select brand_name, extract(year from sale_date::date) year_sale, round(avg(price)) avg_price
from car_shop.car_sales sl join car_shop.car_models cm using (model_id) join car_shop.brands br using (brand_id)
group by brand_name, extract(year from sale_date::date)
order by brand_name, extract(year from sale_date::date);

---- Задание 3. Посчитайте среднюю цену всех автомобилей с разбивкой по месяцам в 2022 году с учётом скидки.

select extract(month from sale_date::date) month_sale, extract(year from sale_date::date) year_sale, round(avg(price)) avg_price
from car_shop.car_sales sl
where extract(year from sale_date::date) = 2022
group by extract(month from sale_date::date), extract(year from sale_date::date)
order by extract(month from sale_date::date), extract(year from sale_date::date);

---- Задание 4. Напишите запрос, который выведет список купленных машин у каждого пользователя.

select first_name || ' ' || last_name person_name, STRING_AGG(brand_name || ' ' || model_name, ', ') car_name
from car_shop.car_sales sl join car_shop.persons p using(person_id) join car_shop.car_models cm using (model_id) 
join car_shop.brands br using (brand_id)
group by first_name || ' ' || last_name
order by first_name || ' ' || last_name;


---- Задание 5. Напишите запрос, который вернёт самую большую и самую маленькую цену продажи автомобиля с разбивкой по стране без учёта скидки.

select c.country_name, min(price*(1+discount/100)) min_price, max(price*(1+discount/100)) max_price
from car_shop.car_sales sl join car_shop.car_models cm using (model_id) join car_shop.brands br using (brand_id)
left join car_shop.countries c on br.brand_origin = c.country_id
group by country_name
order by country_name;

---- Задание 6. Напишите запрос, который покажет количество всех пользователей из США.

select count(person_id) persons_from_usa_count from car_shop.persons
where phone like '+1%';














