from sqlalchemy import func, case, and_, update, delete, select
from core.db import SessionLocal
from core.models import Products, Customers, Orders, Order_details


# begin;
# update customers
# set city = 'Berlin-test'
# where customer_id = 'ALFKI'
#
# DELETE FROM customerds
# where customer_id = 'ALFKI'
# commit;
def get_transaction():
    with SessionLocal() as session:
        session.begin()
        try:
            # session.execute(update(Customers).where(Customers.c.customer_id == 'ALFKI').values(city='Berlin-test'))
            session.execute(update(Customers).where(Customers.c.city == 'México D.F.').values(city='test'))
            # if you try to delete, the request is get rollback, because clients have fk_orders
            # if comment out execute(delete()) the request is get commit
            savepoint = session.begin_nested()  # establish a savepoint

            session.execute(update(Customers).where(Customers.c.city == 'test').values(city='test2'))
            savepoint.rollback()
            session.execute(delete(Customers).where(Customers.c.customer_id == 'ALFKI'))
        except:
            print('rollback')
            session.rollback()
        else:
            print('commit')
            session.commit()

        print('-----------------------SQL-Query--------------------------------')


if __name__ == '__main__':
    print(get_transaction())
