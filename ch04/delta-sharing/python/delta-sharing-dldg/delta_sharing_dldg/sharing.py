import os
from pathlib import Path
from typing import Optional, Any, Sequence
from pyspark.sql import SparkSession
from delta_sharing import SharingClient, Share, Schema, Table


class Sharing:
    _client: SharingClient
    _profile: Path

    def __init__(self, sharing_profile: Optional[str] = "profiles/open-datasets.share"):
        """
        The sharing client requires a Sharing Profile.
        :param sharing_profile: The path to profiles/open-datasets.share
        > note: this points logically to the <chapter>/profiles/open-datasets.share
        > you can provide a Path reference just as easily with minor work
        """
        profile_file = Path(__file__).parent.parent.parent.parent.joinpath(sharing_profile)
        self._profile = profile_file
        self._client = SharingClient(profile_file.as_posix())

    def list_shares(self) -> Sequence[Share]:
        shares: Sequence[Share] = self._client.list_shares()
        print(shares)
        return shares

    def list_schemas(self, share: Share) -> Sequence[Schema]:
        schemas: Sequence[Schema] = self._client.list_schemas(share)
        print(schemas)
        return schemas

    def list_tables(self, schema: Schema) -> Sequence[Table]:
        tables: Sequence[Table] = self._client.list_tables(schema)
        print(tables)
        return tables

    def share_url(self, table: Table) -> str:
        share_uri = f"#{table.share}.{table.schema}.{table.name}"
        return f"{self._profile.as_posix()}{share_uri}"





