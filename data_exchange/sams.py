from .config import settings

from typing import List
import requests
from attrs import define, field


@define
class SAMS:
    session: requests.Session = field(factory=requests.Session)

    @property
    def loggedIn(self):
        return "SAMSI-SieWarSoWeich" in self.session.cookies

    def login(self, username, password):
        data = {
            "email": username,
            "password": password
        }
        resp = self.session.post("https://www.genecascade.org/sams-cgi/login.cgi", data=data)
        resp.raise_for_status()

    def get_phenopackets(self) -> List[dict]:
        resp = self.session.get("https://www.genecascade.org/sams-cgi/export_all_phenopackets.cgi")
        resp.raise_for_status()
        all_data = resp.json()
        return all_data

    def get_phenopacket(self, patient_id: str) -> dict:
        resp = self.session.get(f"https://www.genecascade.org/sams-cgi/export_phenopacket.cgi?external_id={patient_id}")
        resp.raise_for_status()
        patient_data = resp.json()
        if patient_data["subject"]["id"] != patient_id:
            raise RuntimeError(f"Failed to obtain phenopacket for external id {patient_id}")
        return patient_data

    @classmethod
    def with_credentials_file(cls, credentials_file: str):
        with open(credentials_file) as f:
            username, password = [l.strip() for l in f.readlines()]
        s = cls()
        s.login(username, password)
        return s

    @classmethod
    def with_username(cls, username: str, password: str):
        s = cls()
        s.login(username, password)
        return s


def phenopacket_to_varfish_format(pheno: dict) -> str:
    phenotypes = pheno["phenotypicFeatures"]

    pheno_strings = []
    for feature in phenotypes:
        # check that hpo term exists
        if not feature.get("excluded", 0):
            pheno_string = f"{feature['type']['id']} - {feature['type']['label']}"
            pheno_strings.append(pheno_string)

    return "; ".join(pheno_strings)


def get_data():
    api = SAMS.with_username(settings.sams_user, settings.sams_password)

    sams_data = api.get_phenopackets()

    return sams_data
