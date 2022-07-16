import unittest
from pipelining.ETLRow import model
import os
from pipelining.ETLRow.main import Pipeline1
from sqlalchemy.sql import *
from sqlalchemy.orm import Session
from sqlalchemy import create_engine

class TestETL(unittest.TestCase):
    '''
    Not so much unit test, test entire ingestion on smaller data subset.
    '''

    def setUp(self):
        '''
        setUp script, setup dummy db based on model.
        '''
        self.engine = create_engine(f'sqlite:///test/demo.db')
        model.Base.metadata.create_all(bind=self.engine)        

        self.pipeline = Pipeline1(folder=os.path.join('test','data'))
        self.pipeline.engine = create_engine(f'sqlite:///test/demo.db')

        self.pipeline.main()

    def test_ingestion(self):
        '''
        ingest test data
        '''
        with Session(self.engine) as s:
            nrows = s.query(model.DimCustomers).count()
        self.assertEqual(nrows, 10)

    def test_audit_table_ingestion(self):
        '''
        test updates.
        '''
        update_customer_file = os.path.join(self.pipeline.folder, 'customers','part-00001')
        self.pipeline._ingest_customers(open(update_customer_file))

        with Session(self.engine) as s:
            nrows = s.query(model.AudCustomerChanges).first()

        self.assertEqual(nrows.field_changed, 'customer_password')

    def tearDown(self):
        '''
        Delete sqlite db.
        '''
        os.remove('test/demo.db')

if __name__ == '__main__':
    unittest.main()