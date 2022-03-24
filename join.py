from sqlalchemy import func

from core.db import SessionLocal
from core.models import Products, Suppliers, Categories


# SELECT product_name, suppliers.company_name, units_in_stock
# FROM products
# INNER JOIN suppliers ON products.supplier_id = suppliers.supplier_id

def get_inner_join_1():
    with SessionLocal() as session:
        query_answer = session.query(Products.c.product_name, Suppliers.c.company_name, Products.c.units_in_stock
                                     ).join(Suppliers).limit(10)

        for item in query_answer:
            print(item)
        print('-----------------------SQL-Query--------------------------------')
        return query_answer


# SELECT category_name, SUM(units_in_stock)
# FROM products
# INNER JOIN categories ON products.category_id = categories.category_id
# GROUP BY category_name
# ORDER BY SUM(units_in_stock) DESC
def get_inner_join_2():
    with SessionLocal() as session:
        query_answer = session.query(Categories.c.category_name,
                                     func.sum(Products.c.units_in_stock)
                                     ).outerjoin(Categories).group_by(Categories.c.category_name
                                                                 ).order_by(func.sum(Products.c.units_in_stock).desc())

        for item in query_answer:
            print(item)
        print('-----------------------SQL-Query--------------------------------')
        return query_answer


if __name__ == '__main__':
    print(get_inner_join_1())
    print('=======================NEXT==============================')
    print(get_inner_join_2())
