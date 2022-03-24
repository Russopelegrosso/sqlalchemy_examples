from sqlalchemy import func

from core.db import SessionLocal
from core.models import Orders


# SELECT ship_country, COUNT(*)
# FROM orders
# WHERE freight > 50
# GROUP BY ship_country
# ORDER BY COUNT(*)


def get_group_by():
    with SessionLocal() as session:
        query_answer = session.query(Orders.c.ship_country,
                                     func.count()).where(Orders.c.freight > 50).group_by(
            Orders.c.ship_country).order_by(func.count().desc())

        for item in query_answer:
            print(item)
        print('-----------------------SQL-Query--------------------------------')
        return query_answer


if __name__ == '__main__':
    print(get_group_by())
