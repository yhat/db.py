import pandas as pd
from db import DemoDB, DB
import unittest


db = DemoDB()

class PandaSQLTest(unittest.TestCase):

    def setUp(self):
        pass

    def test_query_rowsum(self):
        df = db.query("select * from Artist;")
        self.assertEqual(len(df), 275)
    
    def test_query_groupby(self):
        pass

    def test_query_from_file_rowsum(self):
        pass

    def test_add_profile(self):
        pass
    
    def test_remove_profile(self):
        pass

    def test_list_profiles(self):
        pass

    def test_table_head(self):
        pass

    def test_table_all(self):
        pass

    def test_table_select(self):
        pass

    def test_table_sample(self):
        pass

    def test_column_head(self):
        pass

    def test_column_all(self):
        pass

    def test_column_sample(self):
        pass


if __name__=="__main__":
    unittest.main()

