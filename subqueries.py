from sqlalchemy import func

from core.db import SessionLocal
from core.models import Suppliers, Customers


# SELECT company_name
# FROM suppliers
# WHERE country in (SELECT DISTINCT country
# 					FROM customers)
def get_subqueries():
    with SessionLocal() as session:
        subquery_answer = session.query(Customers.c.country)
        query_answer = session.query(Suppliers.c.company_name
                                     ).where(Suppliers.c.country.in_(subquery_answer))

        for item in query_answer:
            print(item)
        print('-----------------------SQL-Query--------------------------------')
        return query_answer


if __name__ == '__main__':
    print(get_subqueries())

