import pandas as pd
from db import DB, list_profiles, remove_profile
import unittest


class PandaPostgreSQLTest(unittest.TestCase):

    def setUp(self):
        self.db = DB(username='glamp', dbname='Chinook', dbtype='postgres')

    def test_query_rowsum(self):
        df = self.db.query('select * from "Artist";')
        self.assertEqual(len(df), 244)

    def test_query_groupby(self):
        q = 'select "AlbumId", sum(1) from "Track" group by 1'
        df = self.db.query(q)
        self.assertEqual(len(df), 297)

    def test_query_from_file_rowsum(self):
        with open("db/tests/testscript.sql", "w") as f:
            f.write('select * from "Artist";')
        df = self.db.query_from_file("db/tests/testscript.sql")
        self.assertEqual(len(df), 244)

    def test_add_profile(self):
        profiles = list_profiles()
        self.db.save_credentials(profile="test_profile")
        self.assertEqual(len(profiles)+1, len(list_profiles()))
        remove_profile("test_profile")

    def test_remove_profile(self):
        profiles = list_profiles()
        self.db.save_credentials(profile="test_profile")
        self.assertEqual(len(profiles)+1, len(list_profiles()))
        remove_profile("test_profile")

    def test_list_profiles(self):
        self.db.save_credentials(profile="test_profile")
        self.assertTrue(len(list_profiles()) > 0)
        remove_profile("test_profile")

    def test_table_head(self):
        self.assertEqual(len(self.db.tables.Artist.head()), 6)

    def test_table_all(self):
        self.assertEqual(len(self.db.tables.Artist.all()), 244)

    def test_table_select(self):
        df = self.db.tables.Artist.select("ArtistId", "Name")
        self.assertEqual(df.shape, (244, 2))

    def test_table_sample(self):
        df = self.db.tables.Artist.sample(n=10)
        self.assertEqual(len(df), 10)

    def test_table_uniqe(self):
        df = self.db.tables.Track.unique("GenreId", "MediaTypeId")
        self.assertEqual(len(df), 33)

    def test_column_head(self):
        col = self.db.tables.Track.TrackId.head()
        self.assertEqual(len(col), 6)

    def test_column_all(self):
        col = self.db.tables.Track.TrackId.all()
        self.assertEqual(len(col), 2883)

    def test_column_sample(self):
        col = self.db.tables.Track.TrackId.sample(n=10)
        self.assertEqual(len(col), 10)

    def test_column_unique(self):
        col = self.db.tables.Customer.Country.unique()
        self.assertEqual(len(col), 11)

    def test_table_count_rows(self):
        count = self.db.tables.Invoice.count
        self.assertEqual(count, 251)

if __name__ == "__main__":
    unittest.main()
