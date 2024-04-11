import os
import json
from typing import Tuple

from flask import Flask, abort, make_response, render_template, request
from flask_htmx import HTMX

from .baserow_api import BaserowTemplates
from .docx_templating import check_config, generate_docx, get_docx_schema
from .rules import RULE_FUNCTIONS

TOKEN = os.environ.get("TOKEN")
HEADERS = {"Authorization": f"Token {TOKEN}"}

BS = BaserowTemplates("https://phenotips.charite.de", token=TOKEN)

app = Flask(__name__)
htmx = HTMX(app)


def get_template_schema(template_id):
    docx_data = BS.get_template_docx(template_id)
    return get_docx_schema(docx_data)


def update_template_config(template_id, config_string):
    if errors := check_config(config_string):
        return errors

    BS.update_template_config(template_id, config_string)


def generate_docx_file(case_id, template_id) -> Tuple[str, bytes]:
    template = BS.get_template(template_id)
    template_docx = BS.get_template_docx(template_id)
    template_config = template.get("TemplateData", {})

    case_data = BS.get_case_data(case_id)

    snippet_data = BS.get_snippets()

    mapped_docx = generate_docx(template_config, case_data, template_docx, snippet_data)
    filename = f"SV-{case_id}_{template['Name'].replace(' ', '_')}.docx"
    return filename, mapped_docx


@app.route("/")
def home():
    if htmx:
        case_id = request.args.get("svid")
        templates = BS.get_templates()
        return render_template("partials/entry_generate.html", templates=templates, case_id=case_id)
    return render_template("index.html")


@app.route("/generate", methods=["GET"])
def generate():
    case_id = request.args.get("case_id")
    template_id = request.args.get("template_id")
    if case_id and template_id:
        filename, result = generate_docx_file(case_id, template_id)
        response = make_response(result)
        response.headers.set("Content-Type", "application/vnd.openxmlformats-officedocument.wordprocessingml.document")
        response.headers.set("Content-Disposition", "attachment", filename=filename)
        return response
    abort(404)


@app.route("/templates", methods=["GET", "POST"])
def templates():
    functions_data = [{"name": name, "doc": fun.__doc__} for name, fun in RULE_FUNCTIONS.items()]
    if request.method == "POST":
        form_response = request.form.to_dict()
        template_id = form_response.pop("template_id")
        template_name = form_response.pop("template_name")
        template_data = json.loads(form_response.pop("template_data"))
        template_tags = {t["tag"]: t["types"] for t in template_data}
        config_string = json.dumps(form_response)
        errors = update_template_config(template_id, config_string)
        if errors:
            template_data = [{"tag": tag, "types": template_tags.get(tag, []), "config": config}for tag, config in form_response.items()]
            return render_template("partials/template_update.html", template_data=template_data, template_name=template_name, template_id=template_id, errors=errors, template_json=json.dumps(template_data), functions=functions_data)
    elif htmx:
        template_id = htmx.trigger_name
        template_data = BS.get_template(template_id)
        template_schema = get_template_schema(template_id)
        template_config = json.loads(template_data["TemplateData"] or "{}")
        template_configuration = [
            {
                "tag": tag,
                "types": types,
                "config": template_config.get(tag, "")
            }
            for tag, types in template_schema.items()
        ]
        template_configuration.sort(key=lambda c: str.casefold(c["tag"]))
        return render_template("partials/template_update.html", template_data=template_configuration, template_name=template_data["Name"], template_id=template_data["id"], template_json=json.dumps(template_configuration), functions=functions_data)
    templates = BS.get_templates()
    return render_template("templates.html", entries=templates)
