from sams import SAMS
from .config import settings


def get_data():
    api = SAMS.with_username(settings.sams_user, settings.sams_password)

    sams_data = api.get_phenopackets()

    return sams_data
