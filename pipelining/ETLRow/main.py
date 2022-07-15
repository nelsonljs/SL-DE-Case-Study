# from .model import *
# import boto3
from smart_open import open
from sqlalchemy import create_engine
from sqlalchemy.orm import validates, Session
from sqlalchemy.sql import *
import pandas as pd
import csv

import os, sys; sys.path.append('.')
from pipelining.ETLRow.model import *

class Pipeline1:
    '''
    ETL steps for data ingestion.
    '''

    def __init__(self):
        self.engine = create_engine(f'sqlite:///data/demo.db')
        pass

    def _ingest_categories(self):
        '''
        ingesting categories
        '''
        # Read files from s3 resource.
        # s3 = boto3.client('s3')
        with Session(self.engine) as s:
            # with open('s3://bucket/key', transport_params=dict(client=client)) as f: Can use smart_open to open s3 files.
            with open('data/categories/part-00000') as f: 
                for l in csv.reader(f, quotechar='"', delimiter=',', skipinitialspace=True):
                    id, category_department_id, category_name = l
                    s.add(
                        DimCategory(
                            id=id,
                            category_department_id=category_department_id,
                            category_name=category_name
                        )
                    )
            s.commit()

    def _ingest_departments(self):
        '''
        ingesting departments
        '''
        # Read files from s3 resource.
        # s3 = boto3.client('s3')

        with Session(self.engine) as s:
            # with open('s3://bucket/key', transport_params=dict(client=client)) as f: Can use smart_open to open s3 files.
            with open('data/departments/part-00000') as f:
                for l in csv.reader(f, quotechar='"', delimiter=',', skipinitialspace=True):
                    id, department_name = l
                    s.add(
                        DimDepartment(
                            id=id,
                            department_name=department_name
                        )
                    )
            s.commit()

    def _ingest_products(self):
        '''
        ingesting products
        '''
        # Read files from s3 resource.
        # s3 = boto3.client('s3')
        with Session(self.engine) as s:
            # with open('s3://bucket/key', transport_params=dict(client=client)) as f: Can use smart_open to open s3 files.
            with open('data/products/part-00000') as f:
                for l in csv.reader(f, quotechar='"', delimiter=',', skipinitialspace=True):
                    id, product_category_id, product_name, product_description, product_price, product_image_url = l
                    s.add(
                        DimProducts(
                            id=id,
                            product_category_id=product_category_id,
                            product_name=product_name,
                            product_description=product_description,
                            product_price=product_price,
                            product_image_url=product_image_url,
                        )
                    )
            s.commit()

    def _ingest_customers(self):
        '''
        ingesting customers
        '''
        # Read files from s3 resource.
        # s3 = boto3.client('s3')
        with Session(self.engine) as s:
            # with open('s3://bucket/key', transport_params=dict(client=client)) as f: Can use smart_open to open s3 files.
            with open('data/customers/part-00000') as f:
                for l in csv.reader(f, quotechar='"', delimiter=',', skipinitialspace=True):
                    id, customer_fname, customer_lname, customer_email, customer_password, customer_street, customer_city, customer_state, customer_zipcode = \
                        l
                    s.add(
                        DimCustomers(
                            id=id,
                            customer_fname=customer_fname,
                            customer_lname=customer_lname,
                            customer_email=customer_email,
                            customer_password=customer_password,
                            customer_street=customer_street,
                            customer_city=customer_city,
                            customer_state=customer_state,
                            customer_zipcode=customer_zipcode,
                        )
                    )
            s.commit()

    def _ingest_orders(self):
        '''
        Ingesting fact orders.
        '''
        # Read files from s3 resource.
        # s3 = boto3.client('s3')
        with Session(self.engine) as s:
            # with open('s3://bucket/key', transport_params=dict(client=client)) as f: Can use smart_open to open s3 files.
            with open('data/orders/part-00000') as f:
                for l in csv.reader(f, quotechar='"', delimiter=',', skipinitialspace=True):
                    id, order_datetime, order_customer_id, order_status = \
                        l
                    order_datetime = datetime.strptime(order_datetime, '%Y-%m-%d %H:%M:%S.0')
                    s.add(
                        FactOrders(
                            id=id,
                            order_date_id=order_datetime.strftime('%Y%m%d%H'),
                            order_datetime=order_datetime,
                            order_customer_id=order_customer_id,
                            order_status=order_status
                        )
                    )
            s.commit()

    def _ingest_orders_items(self):
        '''
        Ingesting fact orders.
        '''
        # Read files from s3 resource.
        # s3 = boto3.client('s3')
        # with open('s3://bucket/key', transport_params=dict(client=client)) as f:
        with Session(self.engine) as s:
            with open('data/order_items/part-00000') as f:
                for l in csv.reader(f, quotechar='"', delimiter=',', skipinitialspace=True):
                    id, order_item_order_id, order_item_product_id, order_item_quantity, order_item_subtotal, order_item_product_price= \
                        l
                    s.add(
                        FactOrderItems(
                            id=id,
                            order_item_order_id=order_item_order_id,
                            order_item_product_id=order_item_product_id,
                            order_item_quantity=order_item_quantity,
                            order_item_subtotal=order_item_subtotal,
                            order_item_product_price=order_item_product_price,
                            order_item_shipping_id=1
                        )
                    )
            s.commit()

    def _ingest_dimensions(self):
        '''
        Ingest dimensions from raw data.
        '''
        try:
            self._ingest_categories()
            self._ingest_departments()
            self._ingest_products()
            self._ingest_customers()
        except Exception as e:
            print('oops', e)
            pass

    def _ingest_facts(self):
        '''
        Ingest facts from raw data.
        '''
        try:
            self._ingest_orders()
            self._ingest_orders_items()

        except Exception as e:
            print('oops', e)
            pass        

    def main(self):
        '''
        Driver script
        '''
        self._ingest_dimensions()
        self._ingest_facts()


if __name__=='__main__':
    a = ETLRow()
    a._ingest_facts()
    a._ingest_dimensions()