from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import validates, Session, relationship
from sqlalchemy.sql import *
import os

from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

Base = declarative_base()   

class DimProducts(Base):
    __tablename__ = "dim_products"
    id = Column(Integer, primary_key=True)
    product_category_id = Column(Integer)
    product_name = Column(VARCHAR)
    product_description = Column(VARCHAR)
    product_price = Column(Numeric)
    product_image_url = Column(VARCHAR)
    created_at = Column(DateTime, default=func.now())
    created_by = Column(VARCHAR, default='de1')
    modified_at = Column(DateTime, onupdate=func.now())
    modified_by = Column(VARCHAR, onupdate='de2')

    ### Validation for a car and other dimensions can be specified.
    # @validates('manufacturer')
    # def validate_dob(self, key, value):
    #     if value not in ['Honda','Toyota']:
    #         raise ValueError("Manufacturer not recognized.")
    #     return value

class DimCustomers(Base):
    __tablename__ = "dim_customers"
    id = Column(Integer, primary_key=True)
    customer_fname = Column(VARCHAR)
    customer_lname = Column(VARCHAR)
    customer_email = Column(VARCHAR) ## Probably need to implement a hashing fucntion here. skip unique=True for now.
    customer_password = Column(VARCHAR) ## Probably need to implement a hashing fucntion here.
    customer_street = Column(VARCHAR)
    customer_city = Column(VARCHAR)
    customer_state = Column(VARCHAR)
    customer_zipcode = Column(VARCHAR)

    created_at = Column(DateTime, default=func.now())
    created_by = Column(VARCHAR, default='de1')
    modified_at = Column(DateTime, onupdate=func.now())
    modified_by = Column(VARCHAR, onupdate='de2')
    # created_at = Column(DateTime, default=func.now())
    # created_by = Column(VARCHAR, server_default=func.current_user())
    # modified_at = Column(DateTime, onupdate=func.now())
    # modified_by = Column(VARCHAR, onupdate=func.current_user())

    ### Validation for a car and other dimensions can be specified.
    # @validates('manufacturer')
    # def validate_dob(self, key, value):
    #     if value not in ['Honda','Toyota']:
    #         raise ValueError("Manufacturer not recognized.")

class DimDate(Base):
    '''
    This could be imported from pre-constructed data-lake in use in the enterprise. Otherwise, for simplicity only capturing basic info.
    '''
    __tablename__ = "dim_dates"
    id = Column(Integer, primary_key=True, autoincrement=True)
    date_year = Column(Integer)
    date_month = Column(Integer)
    date_day = Column(Integer)
    date_hour = Column(Integer)
    date_isholiday = Column(Boolean)

class DimCategory(Base):
    __tablename__ = "dim_categories"
    id = Column(Integer, primary_key=True, autoincrement=True)
    category_department_id = Column(String)
    category_name = Column(String)

class DimDepartment(Base):
    __tablename__ = "dim_departments"
    id = Column(Integer, primary_key=True, autoincrement=True)
    department_name = Column(String)

class FactOrders(Base):
    __tablename__ = "fct_orders"
    id = Column(Integer, primary_key=True)
    order_date_id = Column(Integer, ForeignKey('dim_dates.id'))
    order_datetime = Column(DATETIME)
    order_customer_id = Column(Integer, ForeignKey('dim_customers.id'))
    order_status = Column(VARCHAR)

class FactOrderItems(Base):
    __tablename__ = "fct_order_items"
    id = Column(Integer, primary_key=True)
    order_item_order_id = Column(Integer, ForeignKey('fct_orders.id'))
    order_item_product_id = Column(Integer, ForeignKey('dim_products.id'))
    order_item_quantity = Column(Integer)
    order_item_subtotal = Column(Integer)
    order_item_product_price = Column(Integer)
    order_item_shipping_id = Column(Integer, ForeignKey('junk_dim_shipping.id'))

class JunkDimShipping(Base):
    __tablename__ = "junk_dim_shipping"
    id = Column(Integer, primary_key=True, autoincrement=True)
    shipping_created_date = Column(String)
    shipping_status = Column(String)
    shipping_tracking_number = Column(String)
    shipping_handler = Column(String)
    shipping_last_modified = Column(DATETIME)

def main():
    '''
    Create database.
    '''
    # postgres_pw = os.environ.get("POSTGRES_PASSWORD")
    # postgres_db = os.environ.get("POSTGRES_DB")
    # docker_container_name = os.environ.get("POSTGRES_CONTAINER")
    engine = create_engine(f'sqlite:///data/demo.db')
    # engine = create_engine(f'postgresql://postgres:{postgres_pw}@{docker_container_name}:5432/{postgres_db}')

    Base.metadata.create_all(bind=engine)

    print('Tables created.')

    ## Setting up dimdates table for BI requirements.
    with Session(engine) as s:
        mydate = datetime(2013,1,1,0)
        while mydate < datetime(2015,1,1,0):
            s.add(
                DimDate(
                    id=int(mydate.strftime('%Y%m%d%H')),
                    date_year=mydate.year,
                    date_month=mydate.month,
                    date_day=mydate.day,
                    date_hour=mydate.hour,
                    date_isholiday=False
                )
            )
            mydate += timedelta(hours=1)
        
        s.commit()

if __name__ == '__main__':
    main()
