import datetime
from data_exchange import DATA_EXCHANGE_VERSION
from data_exchange.validation import group_by_errors, RULES
from flask import Flask, render_template, request
from worker import run_data_exchange, update_cached_run
import redis
import dill
r = redis.Redis(host="localhost", port=6379, decode_responses=False)

app = Flask(__name__)


def get_error_string(err):
    return f"{err.source.name} {err.field} {err.comment}"


def get_cached_data():
    raw_result = r.get("run_data_exchange")
    last_submitted = r.get("run_data_exchange_submitted")
    last_submitted_time = dill.loads(last_submitted) if last_submitted else None
    if raw_result:
        result_package = dill.loads(raw_result)
        if (datetime.datetime.now() - result_package["time"]) > datetime.timedelta(hours=2):
            update_cached_run.delay()
        elif result_package["version"] != DATA_EXCHANGE_VERSION:
            update_cached_run.delay()
        return result_package
    elif last_submitted_time and (datetime.datetime.now() - last_submitted_time < datetime.timedelta(minutes=3)):
        return raw_result
    else:
        update_cached_run.delay()
        return raw_result

@app.route("/", methods=["GET", "POST"])
def get_home():
    results = get_cached_data()
    query_text = request.args.get("searchbox") or ""
    print(query_text)
    kw_args = [v for v in request.args.values()]
    if not kw_args:
        kw_args = [r.name for r in RULES]
    error_types = {r.name: r.name in kw_args for r in RULES}
    if results:
        result_date = results["time"]
        entries, updates_entries, updates_findings, errors = results["data"]
        errors_filtered = [e for e in errors if error_types[e.source.name]]
        errors_filtered = [e for e in errors_filtered if query_text in get_error_string(e)]
        errors_by_responsible = group_by_errors(errors_filtered, lambda e: [r['Shorthand'] for r in e.responsible])
        return render_template("main.html", result_date=result_date, errors_by_responsible=errors_by_responsible, error_types=error_types, query_text=query_text)
    else:
        result_date = "no results yet"
        return render_template("main.html", result_date=result_date, error_types=error_types, query_text=query_text)
