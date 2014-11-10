import pandas as pd
from db import DemoDB, DB, list_profiles, remove_profile
import unittest


db = DemoDB()

class PandaSQLTest(unittest.TestCase):

    def setUp(self):
        pass

    def test_query_rowsum(self):
        df = db.query("select * from Artist;")
        self.assertEqual(len(df), 275)
    
    def test_query_groupby(self):
        q = "select AlbumId, sum(1) from Track group by 1"
        df = db.query(q)
        self.assertEqual(len(df), 347)

    def test_query_from_file_rowsum(self):
        with open("/tmp/testscript.sql", "w") as f:
            f.write("select * from Artist;")
        df = db.query_from_file("/tmp/testscript.sql")
        self.assertEqual(len(df), 275)

    def test_add_profile(self):
        profiles = list_profiles()
        db.save_credentials(profile="test_profile")
        self.assertEqual(len(profiles)+1, len(list_profiles()))
        remove_profile("test_profile")
    
    def test_remove_profile(self):
        profiles = list_profiles()
        db.save_credentials(profile="test_profile")
        self.assertEqual(len(profiles)+1, len(list_profiles()))
        remove_profile("test_profile")

    def test_list_profiles(self):
        db.save_credentials(profile="test_profile")
        self.assertTrue(len(list_profiles()) > 0)
        remove_profile("test_profile")

    def test_table_head(self):
        self.assertEqual(len(db.tables.Artist.head()), 6)

    def test_table_all(self):
        self.assertEqual(len(db.tables.Artist.all()), 275)

    def test_table_select(self):
        df = db.tables.Artist.select("ArtistId", "Name")
        self.assertEqual(df.shape, (275, 2))

    def test_table_sample(self):
        df = db.tables.Artist.sample(n=10)
        self.assertEqual(len(df), 10)

    def test_column_head(self):
        col = db.tables.Track.TrackId.head()
        self.assertEqual(len(col), 6)

    def test_column_all(self):
        col = db.tables.Track.TrackId.all()
        self.assertEqual(len(col), 3503)

    def test_column_sample(self):
        col = db.tables.Track.TrackId.sample(n=10)
        self.assertEqual(len(col), 10)


if __name__=="__main__":
    unittest.main()

