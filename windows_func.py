from sqlalchemy import func, case, and_
from core.db import SessionLocal
from core.models import Products, Categories, Orders, Order_details


# SELECT
# 	category_id,
# 	category_name,
# 	product_name,
# 	unit_price,
# 	AVG(unit_price) OVER(PARTITION BY category_id) AS avg_price
# FROM products
# JOIN categories USING(category_id)
# LIMIT 20
# Gives the average price value across category groups
def get_windows_func_1():
    with SessionLocal() as session:
        query_answer = session.query(Products.c.category_id, Categories.c.category_name, Products.c.product_name,
                                     Products.c.unit_price,
                                     func.avg(Products.c.unit_price).over(partition_by=Products.c.category_id).label(
                                         'avg_price')
                                     ).join(Categories).limit(20)

        for item in query_answer:
            print(item)
        print('-----------------------SQL-Query--------------------------------')
        return query_answer


# SELECT
# 	order_id,
# 	order_date,
# 	product_id,
# 	product_name,
# 	products.unit_price,
# 	SUM(products.unit_price) OVER(PARTITION BY order_id ORDER BY product_id) AS sale_sum
# FROM orders
# JOIN order_details USING(order_id)
# JOIN products USING(product_id)
# LIMIT 20
# Gives a subtotal of the amount of purchases ascending in each group of orders
def get_windows_func_2():
    with SessionLocal() as session:
        query_answer = session.query(Orders.c.order_id, Orders.c.order_date, Order_details.c.product_id,
                                     Products.c.product_name,
                                     Products.c.unit_price,
                                     func.sum(Products.c.unit_price).over(partition_by=Orders.c.order_id,
                                                                          order_by=Order_details.c.product_id).label(
                                         'sale_sum')
                                     ).select_from(Orders).join(Order_details).join(Products).limit(20)

        for item in query_answer:
            print(item)
        print('-----------------------SQL-Query--------------------------------')
        return query_answer


# SELECT
# 	order_id,
# 	order_date,
# 	product_id,
# 	unit_price,
# 	SUM(unit_price) OVER(ORDER BY row_id) AS sale_sum
# FROM (
# 	SELECT
# 		order_id,
# 		order_date,
# 		product_id,
# 		unit_price,
# 		row_number() OVER() AS row_id
# 	FROM orders
# 	JOIN order_details USING(order_id)
#   LIMIT 20
# ) as subquerye
# ORDER BY order_id;
#   LIMIT 20
# Gives a subtotal of the sum of each successive position
def get_windows_func_3():
    with SessionLocal() as session:
        subquery = session.query(Orders.c.order_id, Orders.c.order_date, Order_details.c.product_id,
                                 Order_details.c.unit_price,
                                 func.row_number().over().label('row_id')
                                 ).select_from(Orders).join(Order_details).limit(20).subquery()
        query_answer = session.query(subquery.c.order_id, subquery.c.order_date, subquery.c.product_id,
                                     subquery.c.unit_price,
                                     func.sum(subquery.c.unit_price).over(order_by=subquery.c.row_id).label('sale_sum')
                                     ).select_from(subquery).order_by(subquery.c.order_id).limit(20)

        for item in query_answer:
            print(item)
        print('-----------------------SQL-Query--------------------------------')
        return query_answer


# SELECT product_name, units_in_stock,
# 	RANK() OVER(ORDER BY product_id)
# FROM products;
#
# Gives successive runk for each product_id
def get_windows_func_runk():
    with SessionLocal() as session:
        query_answer = session.query(Products.c.product_name, Products.c.units_in_stock, Products.c.product_name,
                                     func.rank().over(order_by=Products.c.product_id))

        for item in query_answer:
            print(item)
        print('-----------------------SQL-Query--------------------------------')
        return query_answer


# SELECT product_name, units_in_stock,
# 	RANK() OVER(ORDER BY units_in_stock)
# FROM products;
#
# Gives a sequential rank for each unit_in_stock with gap
#   if first 4 unit_price = 0 then for each runk unit_stock = 1 but for next unit_price which is > 0 runk = 5
def get_windows_func_runk_2():
    with SessionLocal() as session:
        query_answer = session.query(Products.c.product_name, Products.c.units_in_stock, Products.c.product_name,
                                     func.rank().over(order_by=Products.c.units_in_stock))

        for item in query_answer:
            print(item)
        print('-----------------------SQL-Query--------------------------------')
        return query_answer


# SELECT product_name, units_in_stock,
# 	DENSE_RANK() OVER(ORDER BY units_in_stock)
# FROM products;
#
# Gives a sequential rank for each unit_in_stock without   gap
#   if first 4 unit_price = 0 then for each runk unit_stock = 1  for next unit_price which is > 0 runk = 2 etc.
def get_windows_func_dense_rank():
    with SessionLocal() as session:
        query_answer = session.query(Products.c.product_name, Products.c.units_in_stock, Products.c.product_name,
                                     func.dense_rank().over(order_by=Products.c.units_in_stock))

        for item in query_answer:
            print(item)
        print('-----------------------SQL-Query--------------------------------')
        return query_answer


# SELECT product_name, unit_price,
# 	DENSE_RANK() OVER(ORDER BY
# 						 CASE
# 							WHEN unit_price <= 30 THEN 1
# 							WHEN unit_price > 30 AND unit_price < 80 THEN 2
# 							ELSE 3
# 						 END
# 				) as ranking
# FROM products
# ORDER BY unit_price DESC;
# Gives a own rank for each unit_price
def get_windows_func_dense_rank_2():
    with SessionLocal() as session:
        query_answer = session.query(Products.c.product_name, Products.c.unit_price, Products.c.product_name,
                                     case(
                                         (Products.c.unit_price <= 30, 1),
                                         (and_(Products.c.unit_price > 30, Products.c.unit_price < 80), 2),
                                         else_=3
                                     ).label('ranking')).order_by(Products.c.unit_price.desc())

        for item in query_answer:
            print(item)
        print('-----------------------SQL-Query--------------------------------')
        return query_answer


# SELECT product_name, unit_price,
# 	LAG(unit_price) OVER(ORDER BY unit_price DESC) - unit_price AS price_lag
# FROM products
# ORDER BY unit_price DESC;
# Gives the difference between the current item and the previous item

def get_windows_func_lag():
    with SessionLocal() as session:
        query_answer = session.query(Products.c.product_name, Products.c.unit_price,
                                     (func.lag(Products.c.unit_price).over(
                                         order_by=Products.c.unit_price.desc()) - Products.c.unit_price).label(
                                         'price_lag')).order_by(
            Products.c.unit_price.desc())

        for item in query_answer:
            print(item)
        print('-----------------------SQL-Query--------------------------------')
        return query_answer


# SELECT product_name, unit_price,
# 	LEAD(unit_price) OVER(ORDER BY unit_price) - unit_price AS price_lead
# FROM products
# ORDER BY unit_price;
# Gives the difference between the current item and the next item
def get_windows_func_lead():
    with SessionLocal() as session:
        query_answer = session.query(Products.c.product_name, Products.c.unit_price,
                                     (func.lead(Products.c.unit_price).over(
                                         order_by=Products.c.unit_price) - Products.c.unit_price).label(
                                         'price_lead')).order_by(
            Products.c.unit_price)

        for item in query_answer:
            print(item)
        print('-----------------------SQL-Query--------------------------------')
        return query_answer


# SELECT *
# FROM (
# 	SELECT product_id, product_name, category_id, unit_price, units_in_stock,
# 		ROW_NUMBER() OVER(ORDER BY unit_price DESC) as nth
# 	FROM products
# ) as sorted_prices
# WHERE nth < 4
# ORDER BY unit_price
# Return N rows
def get_n_rows():
    with SessionLocal() as session:
        subquery = session.query(Products.c.product_id, Products.c.product_name, Products.c.category_id,
                                 Products.c.unit_price,  Products.c.units_in_stock,
                                 func.row_number().over(order_by=Products.c.unit_price.desc()).label('nth')).subquery()

        query_answer = session.query(subquery).where(subquery.c.nth < 4).order_by(subquery.c.unit_price)

        for item in query_answer:
            print(item)
        print('-----------------------SQL-Query--------------------------------')
        return query_answer


if __name__ == '__main__':
    print(get_n_rows())
