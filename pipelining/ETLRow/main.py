# import boto3
from io import TextIOWrapper
from typing import TextIO
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.sql import *
from sqlalchemy.exc import IntegrityError

import pandas as pd
import logging
import csv

import os, sys; sys.path.append('.')
from pipelining.ETLRow.model import *

class Pipeline1:
    '''
    ETL steps for data ingestion.
    '''
    error_records=[]

    def __init__(self, folder=None):
        self.engine = create_engine(f'sqlite:///data/demo.db')
        if folder is not None:
            self.folder = folder
        # self.s3 = boto3.client('s3')
        # self.engine = create_engine({postgres_connect_string})

    def _compare_and_update_audit_table(self, oldobj, new_params, audit_table_obj, session):
        '''
        compare slow changing dimensions to take note of price or detail changes.  
        '''
        for k, v in new_params.items():
            if oldobj.__dict__[k] != v:
                ### K got updated. Add new row into audittable
                session.add(
                    audit_table_obj(
                        object_id=new_params['id'],
                        field_changed=k,
                        previous_value=oldobj.__dict__[k],
                        updated_value=v,
                        modified_at=datetime.now()
                    )
                )
        
        return session

    def _validate_fact(self, oldobj, new_params, allowed_change_fields):
        '''
        validate facts. Do not allow most facts to be changed once entered.
        '''
        for k, v in new_params.items():
            if k in allowed_change_fields:
                continue
            if oldobj.__dict__[k] != v:
                ### K got updated. Add new row into audittable
                return False
        return True

    def _get_file_from_s3(self, bucket, key):
        '''
        internal function to get 
        '''
        response = self.s3.get_object(Bucket=bucket, Key=key)
        return TextIOWrapper(response['body'])

    def _ingest_categories(self, myfile):
        '''
        ingesting categories | No Audit trail, Type 1 Dimension, myfile should be an opened file object, to work with s3.
        '''
        with Session(self.engine) as s:
            with myfile as f: # Reading from local file.
                for l in csv.reader(f, quotechar='"', delimiter=',', skipinitialspace=True):
                    id, category_department_id, category_name = l
                    new_params = {
                        "id":int(id),
                        "category_department_id":category_department_id,
                        "category_name":category_name
                    }

                    category = DimCategory(**new_params)
                    try:
                        exists_result = s.query(DimCategory).filter(DimCategory.id == int(id)).first()

                        if exists_result is None:
                            s.add(category)

                        else:
                            s.query(DimCategory).filter(DimCategory.id == id).\
                                update(new_params, synchronize_session="fetch")

                    except IntegrityError as e:
                        s.rollback()

            s.commit()

    def _ingest_departments(self, myfile):
        '''
        ingesting departments | No Audit trail, Type 1 Dimension
        '''

        with Session(self.engine) as s:
            with myfile as f:
                for l in csv.reader(f, quotechar='"', delimiter=',', skipinitialspace=True):
                    id, department_name = l
                    new_params = {
                        "id":int(id),
                        "department_name":department_name
                    }

                    department = DimDepartment(**new_params)
                    try:
                        exists_result = s.query(DimDepartment).filter(DimDepartment.id == int(id)).first()

                        if exists_result is None:
                            s.add(department)

                        else:
                            s.query(DimDepartment).filter(DimDepartment.id == id).\
                                update(new_params, synchronize_session="fetch")

                    except IntegrityError as e:
                        s.rollback()

            s.commit()

    def _ingest_products(self, myfile):
        '''
        ingesting products | Yes Audit trail, Type 1 Dimension
        '''
        with Session(self.engine) as s:
            with myfile as f:
                for l in csv.reader(f, quotechar='"', delimiter=',', skipinitialspace=True):
                    id, product_category_id, product_name, product_description, product_price, product_image_url = l
                    new_params = {
                        "id":int(id),
                        "product_category_id":product_category_id,
                        "product_name":product_name,
                        "product_description":product_description,
                        "product_price":product_price,
                        "product_image_url":product_image_url
                    }

                    category = DimProducts(**new_params)
                    try:
                        exists_result = s.query(DimProducts).filter(DimProducts.id == int(id)).first()

                        if exists_result is None:
                            s.add(category)

                        else:
                            s = self._compare_and_update_audit_table(exists_result, new_params, AudPriceChanges, s)

                            s.query(DimProducts).filter(DimProducts.id == id).\
                                update(new_params, synchronize_session="fetch")

                    except IntegrityError as e:
                        s.rollback()

            s.commit()

    def _ingest_customers(self, myfile):
        '''
        ingesting customers |Yes Audit trail, Type 1 Dimension
        '''
        with Session(self.engine) as s:
            with myfile as f:
                for l in csv.reader(f, quotechar='"', delimiter=',', skipinitialspace=True):
                    id, customer_fname, customer_lname, customer_email, customer_password, customer_street, customer_city, customer_state, customer_zipcode = \
                        l
                    new_params = {
                        "id":int(id),
                        "customer_fname":customer_fname,
                        "customer_lname":customer_lname,
                        "customer_email":customer_email,
                        "customer_password":customer_password,
                        "customer_street":customer_street,
                        "customer_city":customer_city,
                        "customer_state":customer_state,
                        "customer_zipcode":customer_zipcode
                    }

                    category = DimCustomers(**new_params)
                    try:
                        exists_result = s.query(DimCustomers).filter(DimCustomers.id == int(id)).first()
                        if exists_result is None:
                            s.add(category)

                        else:
                            s = self._compare_and_update_audit_table(exists_result, new_params, AudCustomerChanges, s)

                            s.query(DimCustomers).filter(DimCustomers.id == id).\
                                update(new_params, synchronize_session="fetch")

                    except IntegrityError as e:
                        s.rollback()

            s.commit()

    def _ingest_orders(self, myfile):
        '''
        Ingesting fact orders.
        '''

        with Session(self.engine) as s:
            with myfile as f:
                for l in csv.reader(f, quotechar='"', delimiter=',', skipinitialspace=True):
                    id, order_datetime, order_customer_id, order_status = \
                        l

                    order_datetime = datetime.strptime(order_datetime, '%Y-%m-%d %H:%M:%S.0')
                    new_params = {
                        "id":int(id),
                        'order_date_id':order_datetime.strftime('%Y%m%d%H'),
                        'order_datetime':order_datetime,
                        'order_customer_id':order_customer_id,
                        'order_status':order_status
                    }
                    order = FactOrders(**new_params)
                    try:
                        exists_result = s.query(FactOrders).filter(FactOrders.id == int(id)).first()
                        if exists_result is None:
                            s.add(order)

                        else: 
                            ## Should we keep track of order status changes time? Excluding audit table here.
                            ## Only allow update on order status field.

                            if self._validate_fact(exists_result, new_params, allowed_change_fields=['order_status']):
                                s.query(FactOrders).filter(FactOrders.id == id).\
                                    update(new_params, synchronize_session="fetch")
                            
                            else:
                                ## Log fact row as error for check processing.
                                self.error_records.append(
                                    {
                                        'source': 'orders',
                                        'line': '|'.join(l)
                                    }
                                )

                    except IntegrityError as e:
                        s.rollback()

            s.commit()

    def _ingest_orders_items(self, myfile):
        '''
        Ingesting fact orders.
        '''
        with Session(self.engine) as s:
            with myfile as f:
                for l in csv.reader(f, quotechar='"', delimiter=',', skipinitialspace=True):
                    id, order_item_order_id, order_item_product_id, order_item_quantity, order_item_subtotal, order_item_product_price= \
                        l
                    new_params = {
                        "id":int(id),
                        'order_item_order_id':order_item_order_id,
                        'order_item_product_id':order_item_product_id,
                        'order_item_quantity':order_item_quantity,
                        'order_item_subtotal':order_item_subtotal,
                        'order_item_product_price':order_item_product_price,
                        'order_item_shipping_id':1
                    }
                    order = FactOrderItems(**new_params)
                    try:
                        exists_result = s.query(FactOrderItems).filter(FactOrderItems.id == int(id)).first()
                        if exists_result is None:
                            s.add(order)

                        else: 
                            ## Should we keep track of order status changes time? Excluding audit table here.
                            ## Only allow update on order status field.

                            if self._validate_fact(exists_result, new_params, allowed_change_fields=['order_item_shipping_id']):
                                s.query(FactOrderItems).filter(FactOrderItems.id == id).\
                                    update(new_params, synchronize_session="fetch")
                            
                            else:
                                ## Log fact row as error for check processing.
                                self.error_records.append(
                                    {
                                        'source': 'orders',
                                        'line': '|'.join(l)
                                    }
                                )

                    except IntegrityError as e:
                        s.rollback()
            s.commit()

    def _ingest_dimensions(self, folder=None, s3=False):
        '''
        Ingest dimensions from raw data.
        '''
        if s3:
            pass
            ## do file stuff from s3
        else:
            category_file = open(os.path.join(self.folder, 'categories','part-00000'))
            department_file = open(os.path.join(self.folder, 'departments','part-00000'))
            product_file = open(os.path.join(self.folder, 'products','part-00000'))
            customer_file = open(os.path.join(self.folder, 'customers','part-00000'))

        try:
            self._ingest_categories(category_file)
            self._ingest_departments(department_file)
            self._ingest_products(product_file)
            self._ingest_customers(customer_file)
        except Exception as e:
            logging.error(e)
            pass

    def _ingest_facts(self, s3=False):
        '''
        Ingest facts from raw data.
        '''
        if s3:
            pass
            ## do file stuff from s3
        else:
            orders_file = open(os.path.join(self.folder, 'orders','part-00000'))
            order_items_file = open(os.path.join(self.folder, 'order_items','part-00000'))

        try:
            self._ingest_orders(orders_file)
            self._ingest_orders_items(order_items_file)

        except Exception as e:
            logging.error(e)
            pass        

    def main(self, s3=False):
        '''
        Driver script
        '''
        self._ingest_dimensions()
        self._ingest_facts()

if __name__=='__main__':
    a = Pipeline1(folder='data')
    a.main()
    # a._ingest_customers()