import requests
from attrs import define, field

from .config import settings


class MyVariantAPI:
    url = settings.myvariant.url
    email = settings.myvariant.email
    size = settings.myvariant.size

    fields = ["clinvar", "snpeff"]

    def get_annotations(self, chr, pos, ref, alt):
        params = {
            "email": self.email,
            "size": self.size,
            "fields": self.fields,
        }
        var_id = f"chr{chr}:g.{pos}{ref}>{alt}"
        url = f"{self.url}/{var_id}"
        resp = requests.get(url, params)
        resp.raise_for_status()
        return resp.json()
