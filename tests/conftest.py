import intake
import pytest

from intake_esm import config
from intake_esm.core import ESMMetadataStoreCatalog

intake.registry['esm_metadatastore'] = ESMMetadataStoreCatalog
