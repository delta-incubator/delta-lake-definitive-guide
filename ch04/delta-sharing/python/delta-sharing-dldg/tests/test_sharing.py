import unittest
from delta_sharing_dldg.sharing import Sharing
from delta_sharing import Share, Schema, Table

class TestSharing(unittest.TestCase):

    def test_all_methods(self):
        sharing_client: Sharing = Sharing(sharing_profile="profiles/open-datasets.share")

        shares = sharing_client.list_shares()
        first_share: Share = shares[0]

        schemas = sharing_client.list_schemas(first_share)
        first_schema: Schema = schemas[0] # [Schema(name='default',share='delta_sharing')]

        tables = sharing_client.list_tables(first_schema)
        self.assertEqual(len(tables), 7)

        lending_club: Table = tables[3]

        share_url = sharing_client.share_url(lending_club)

        self.assertTrue(
            (share_url).endswith("delta-sharing/profiles/open-datasets.share#delta_sharing.default.lending_club"))




