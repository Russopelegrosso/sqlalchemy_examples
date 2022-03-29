from sqlalchemy import func
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


if __name__ == '__main__':
    print(get_windows_func_3())
