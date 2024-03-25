from delta_sharing.reader import DeltaSharingReader
from delta_sharing.protocol import DeltaSharingProfile, Table
from delta_sharing.rest_client import DataSharingRestClient
import pandas as pd
from dataclasses import dataclass, asdict


@dataclass
class ShareConfig:
    share: str
    schema: str
    name: str


def pandas_from_share(credentials: str, share_config: ShareConfig) -> pd.DataFrame:
    profile = DeltaSharingProfile.from_json(credentials)
    share_config_dict = asdict(share_config)

    reader = DeltaSharingReader(
        table=Table(**share_config_dict),
        rest_client=DataSharingRestClient(profile),
    )

    return reader.to_pandas()
