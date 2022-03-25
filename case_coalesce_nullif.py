from sqlalchemy import func, any_, all_, case, and_

from core.db import SessionLocal
from core.models import Suppliers, Customers, Orders, Order_details, Products


# SELECT product_name, unit_price, units_in_stock,
# 	CASE WHEN units_in_stock >= 100 THEN 'lots of'
# 		 WHEN units_in_stock >= 50 AND units_in_stock < 100 THEN 'average'
# 		 WHEN units_in_stock < 50 THEN 'low number'
# 		 ELSE 'unknown'
# 	END AS amount
# FROM products
# ORDER BY units_in_stock DESC;
def get_case():
    with SessionLocal() as session:
        query_answer = session.query(Products.c.product_name, Products.c.unit_price, Products.c.units_in_stock,
                                     case(
                                         (Products.c.units_in_stock >= 100, 'lots of'),
                                         (and_(Products.c.units_in_stock >= 50, Products.c.units_in_stock < 100),
                                          'average'),
                                         (Products.c.units_in_stock < 50, 'low number'),
                                         else_='unknown'
                                     )).order_by(Products.c.units_in_stock.desc())
        for item in query_answer:
            print(item)
        print('-----------------------SQL-Query--------------------------------')
        return query_answer


# SELECT order_id, order_date, COALESCE(ship_region, 'unknown') as ship_region
# FROM orders
# LIMIT 10
def get_coalesce():
    with SessionLocal() as session:
        query_answer = session.query(Orders.c.order_id, Orders.c.order_date,
                                     func.coalesce(Orders.c.ship_region, 'unknown').label('ship_region')).limit(10)
        for item in query_answer:
            print(item)
        print('-----------------------SQL-Query--------------------------------')
        return query_answer


# SELECT contact_name, COALESCE(NULLIF(city, 'n/a'), 'unknown') as city
# FROM customers
def get_nullif():
    with SessionLocal() as session:
        query_answer = session.query(Customers.c.contact_name,
                                     func.coalesce(func.nullif(Customers.c.city, 'n/a'), 'unknown').label('city'))

        for item in query_answer:
            print(item)
        print('-----------------------SQL-Query--------------------------------')
        return query_answer


if __name__ == '__main__':
    print(get_coalesce())
