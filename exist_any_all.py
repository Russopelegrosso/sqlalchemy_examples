from sqlalchemy import func, any_, all_

from core.db import SessionLocal
from core.models import Suppliers, Customers, Orders, Order_details, Products


# SELECT company_name
# FROM suppliers
# WHERE EXISTS (SELECT DISTINCT country
# 					FROM customers WHERE country == 'Germany')
def get_exist():
    with SessionLocal() as session:
        subquery_answer = session.query(Customers.c.country).where(Customers.c.country == 'Germany')
        query_answer = session.query(Suppliers.c.company_name
                                     ).where(subquery_answer.exists())
        for item in query_answer:
            print(item)
        print('-----------------------SQL-Query--------------------------------')
        return query_answer


# SELECT DISTINCT company_name
# FROM customers
# WHERE customer_id = ANY(
# 	SELECT customer_id
# 	FROM orders
# 	JOIN order_details using(order_id)
# 	WHERE quantity >40
# )


def get_any():
    with SessionLocal() as session:
        subquery_answer = session.query(Orders.c.customer_id).join(Order_details) \
            .where(Order_details.c.quantity > 40).scalar_subquery()
        query_answer = session.query(Customers.c.company_name.distinct()) \
            .where(Customers.c.customer_id == any_(subquery_answer))

        for item in query_answer:
            print(item)
        print('-----------------------SQL-Query--------------------------------')
        return query_answer


# SELECT DISTINCT product_name, quantity
# FROM products
# JOIN order_details USING(product_id)
# WHERE quantity > ALL(
# SELECT AVG(quantity)
# FROM order_details
# GROUP BY product_id
# )
# ORDER BY quantity
def get_all():
    with SessionLocal() as session:
        subquery_answer = session.query(func.avg(Order_details.c.quantity)).group_by(
            Order_details.c.product_id).scalar_subquery()
        query_answer = session.query(Products.c.product_name.distinct(), Order_details.c.quantity) \
            .join(Order_details) \
            .where(Order_details.c.quantity > all_(subquery_answer)).order_by(Order_details.c.quantity)

        for item in query_answer:
            print(item)
        print('-----------------------SQL-Query--------------------------------')
        return query_answer


if __name__ == '__main__':
    print(get_all())
