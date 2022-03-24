from sqlalchemy import Table, MetaData
from sqlalchemy.orm import declarative_base

from core.db import engine

Base = declarative_base()
metadata_obj = MetaData()

Categories = Table('categories', metadata_obj, autoload_with=engine)
Customer_customer_demo = Table('customer_customer_demo', metadata_obj, autoload_with=engine)
Customer_demographics = Table('customer_demographics', metadata_obj, autoload_with=engine)
Customers = Table('customers', metadata_obj, autoload_with=engine)
Employee_territories = Table('employee_territories', metadata_obj, autoload_with=engine)
Employees = Table('employees', metadata_obj, autoload_with=engine)
Order_details = Table('order_details', metadata_obj, autoload_with=engine)
Orders = Table('orders', metadata_obj, autoload_with=engine)
Products = Table('products', metadata_obj, autoload_with=engine)
Region = Table('region', metadata_obj, autoload_with=engine)
Shippers = Table('shippers', metadata_obj, autoload_with=engine)
Suppliers = Table('suppliers', metadata_obj, autoload_with=engine)
Territories = Table('territories', metadata_obj, autoload_with=engine)
Us_states = Table('us_states', metadata_obj, autoload_with=engine)


if __name__ == '__main__':
    a = [c.name for c in Categories.columns]
    b = [c.name for c in Territories.columns]
    c = [c.name for c in Customer_customer_demo.columns]
