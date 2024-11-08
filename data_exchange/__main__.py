from data_exchange.report import to_text_report_by_responsible

from . import run
import typer


app = typer.Typer(pretty_exceptions_show_locals=False)


@app.command()
def main(dry_run: bool = False, sodar: bool = False, varfish: bool = False, sams: bool = False):
    _, _, _, errors = run(dry_run=dry_run, get_sodar=sodar, get_varfish=varfish, get_sams=sams)
    # text_vali_report = to_text_report(errors)
    report_text = to_text_report_by_responsible(errors)
    print(report_text)

app()
