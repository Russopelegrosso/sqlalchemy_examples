from sqlalchemy import func

from core.db import SessionLocal
from core.models import Customers


# SELECT address || ' / ' ||city
# FROM customers
# LIMIT 10

def get_concat():
    with SessionLocal() as session:
        query_answer = session.query(func.concat(Customers.c.address, ' / ', Customers.c.city)
                                     ).limit(10)

        for item in query_answer:
            print(item)
        print('-----------------------SQL-Query--------------------------------')
        return query_answer


# SELECT  address, char_length(address)
# FROM customers
# LIMIT 10

def get_char_length():
    with SessionLocal() as session:
        query_answer = session.query(Customers.c.address, func.char_length(Customers.c.address)
                                     ).limit(10)

        for item in query_answer:
            print(item)
        print('-----------------------SQL-Query--------------------------------')
        return query_answer


# SELECT  upper(address), lower(address)
# FROM customers
# LIMIT 10

def get_upper_and_lower():
    with SessionLocal() as session:
        query_answer = session.query(func.upper(Customers.c.address), func.lower(Customers.c.address)
                                     ).limit(10)

        for item in query_answer:
            print(item)
        print('-----------------------SQL-Query--------------------------------')
        return query_answer


# SELECT  customer_id, overlay(customer_id placing 'AA' from 1 for 2)
# FROM customers
# LIMIT 10


def get_overlay():
    with SessionLocal() as session:
        query_answer = session.query(Customers.c.customer_id, func.overlay(Customers.c.customer_id, 'AA', 1, 2)
                                     ).limit(10)

        for item in query_answer:
            print(item)
        print('-----------------------SQL-Query--------------------------------')
        return query_answer


# SELECT  customer_id, substring(customer_id placing from 2 for 3)
# FROM customers
# LIMIT 10
def get_substring():
    with SessionLocal() as session:
        query_answer = session.query(Customers.c.customer_id, func.substring(Customers.c.customer_id, 2, 3)
                                     ).limit(10)

        for item in query_answer:
            print(item)
        print('-----------------------SQL-Query--------------------------------')
        return query_answer


# SELECT  customer_id, left(customer_id, 1)
# FROM customers
# LIMIT 10
def get_left_side_of_string():
    with SessionLocal() as session:
        query_answer = session.query(Customers.c.customer_id, func.left(Customers.c.customer_id, 1)
                                     ).limit(10)

        for item in query_answer:
            print(item)
        print('-----------------------SQL-Query--------------------------------')
        return query_answer


# SELECT  customer_id, right(customer_id, 2)
# FROM customers
# LIMIT 10
def get_right_side_of_string():
    with SessionLocal() as session:
        query_answer = session.query(Customers.c.customer_id, func.right(Customers.c.customer_id, 2)
                                     ).limit(10)

        for item in query_answer:
            print(item)
        print('-----------------------SQL-Query--------------------------------')
        return query_answer


if __name__ == '__main__':
    print(get_right_side_of_string())
