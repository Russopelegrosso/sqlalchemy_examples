from sqlalchemy import func, case, and_, nullsfirst, tuple_

from core.db import SessionLocal
from core.models import Customers, Products


# SELECT supplier_id, category_id, SUM(units_in_stock)
# FROM products
# GROUP BY GROUPING SETS((supplier_id), (supplier_id, category_id))
# ORDER BY supplier_id, category_id NULLS FIRST
def get_grouping_sets():
    with SessionLocal() as session:
        query_answer = session.query(Products.c.supplier_id,
                                     Products.c.category_id,
                                     func.sum(Products.c.units_in_stock)
                                     ).group_by(func.grouping_sets(
                                                tuple_(Products.c.supplier_id, ),
                                                tuple_(Products.c.supplier_id,
                                                       Products.c.category_id), )).order_by(
                                                                nullsfirst(Products.c.supplier_id),
                                                                nullsfirst(Products.c.category_id))
        for item in query_answer:
            print(item)
        print('-----------------------SQL-Query--------------------------------')
        return query_answer


# SELECT supplier_id, category_id, SUM(units_in_stock)
# FROM products
# GROUP BY ROLLUP (supplier_id, category_id)
# ORDER BY supplier_id, category_id NULLS FIRST
def get_rollup():
    with SessionLocal() as session:
        query_answer = session.query(Products.c.supplier_id,
                                     Products.c.category_id,
                                     func.sum(Products.c.units_in_stock)
                                     ).group_by(func.rollup(Products.c.supplier_id,
                                                       Products.c.category_id)).order_by(
                                                                nullsfirst(Products.c.supplier_id),
                                                                nullsfirst(Products.c.category_id))
        for item in query_answer:
            print(item)
        print('-----------------------SQL-Query--------------------------------')
        return query_answer


# SELECT supplier_id, category_id, SUM(units_in_stock)
# FROM products
# GROUP BY CUBE (supplier_id, category_id)
# ORDER BY supplier_id, category_id NULLS FIRST
def get_cube():
    with SessionLocal() as session:
        query_answer = session.query(Products.c.supplier_id,
                                     Products.c.category_id,
                                     func.sum(Products.c.units_in_stock)
                                     ).group_by(func.cube(Products.c.supplier_id,
                                                       Products.c.category_id)).order_by(
                                                                Products.c.supplier_id,
                                                                nullsfirst(Products.c.category_id))

        for item in query_answer:
            print(item)
        print('-----------------------SQL-Query--------------------------------')
        return query_answer


if __name__ == '__main__':
    print(get_cube())
