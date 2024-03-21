import requests
from functools import reduce
from python_baserow_simple import BaserowApi


class BaserowTemplates(BaserowApi):

    TEMPLATE_TABLE_ID = 770
    CASE_TABLE_ID = 579

    @staticmethod
    def _process_template_data(entry):
        entry["docx_url"] = reduce(lambda n, e: e.get("url", None) or n, entry["docx"], None)
        return entry

    def get_templates(self):
        data: dict = self.get_data(self.TEMPLATE_TABLE_ID, writable_only=False)
        for entry_id, entry in data.items():
            entry["id"] = entry_id
            self._process_template_data(entry)
        return data

    def get_template(self, entry_id):
         data = self.get_entry(self.TEMPLATE_TABLE_ID, entry_id)
         data["id"] = entry_id
         return self._process_template_data(data)


    def get_template_docx(self, entry_id):
        template_data = self.get_template(entry_id)

        r = requests.get(template_data["docx_url"], allow_redirects=True)
        template_content = r.content
        return template_content

    def get_case_data(self, case_id):
        case_data = self.get_entry(self.CASE_TABLE_ID, case_id, linked=True)
        return case_data

    def update_template_config(self, entry_id, config_string):
        entry_data = {
            "TemplateData": config_string
        }
        self._update_row(self.TEMPLATE_TABLE_ID, entry_id, entry_data)
