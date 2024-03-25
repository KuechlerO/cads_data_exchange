from loguru import logger
from sodar_cli import api

from .config import settings
from io import StringIO
import csv


def parse_samplesheet(raw_data):
    all_data = []
    for study_name, study_data in raw_data["studies"].items():
        tsv_data = study_data["tsv"]
        reader = csv.DictReader(StringIO(tsv_data), delimiter="\t")
        for row in reader:
            all_data.append(row)
    return all_data


def get_data():
    logger.debug("Loading SODAR")
    sodar_data = []
    for project in settings.sodar.project:
        sodar_url = settings.sodar.url
        samplesheet = api.samplesheet.export(sodar_url=sodar_url, sodar_api_token=settings.sodar_token, project_uuid=project.id)
        samplesheet_data = parse_samplesheet(samplesheet)
        sodar_data.append({
            "uuid": project.id,
            "name": project.name,
            "data_type": project.data_type,
            "data": samplesheet_data,
        })
    logger.debug("Loaded meta data from {num_projects} SODAR projects", num_projects=len(sodar_data))
    return sodar_data
