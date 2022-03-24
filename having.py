from sqlalchemy import func

from core.db import SessionLocal
from core.models import Orders, Products


# SELECT category_id, SUM(unit_price * units_in_stock)
# FROM products
# WHERE discontinued <>1
# GROUP by category_id
# HAVING SUM(unit_price * units_in_stock) > 5000
# ORDER BY SUM(unit_price * units_in_stock)

def get_having():
    with SessionLocal() as session:
        query_answer = session.query(Products.c.category_id,
                                     func.sum(Products.c.unit_price * Products.c.units_in_stock)
                                     ).where(Products.c.discontinued != 1
                                             ).group_by(Products.c.category_id
                                                        ).having(
            func.sum(Products.c.unit_price * Products.c.units_in_stock) > 5000
            ).order_by(
            func.sum(Products.c.unit_price * Products.c.units_in_stock).desc())
        for item in query_answer:
            print(item)
        print('-----------------------SQL-Query--------------------------------')
        return query_answer


if __name__ == '__main__':
    print(get_having())
