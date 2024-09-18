# Export/Import for Exomdiagnostics

`data_exchange` merges information contained in Baserow.

Users in Baserow are used to generate a number of notification emails.

## General data flow

LB Data -> `pel_transfer` -> Baserow (LB-Metadata)

## Setup

Best use a conda environment to install all requirements. Setting up the project
should be easiest by using the following command from inside the repository in a
conda environment:

```
$ pip install -e .
```

### Setup secrets file

In order to run synchronizations a secrets file with the following content will
need to be created under `<proj dir>/.secrets.toml`.

```
baserow_token = ""
sams_user = ""
sams_password = ""
varfish_user = ""
varfish_password = ""
sodar_token = ""
varfish_token = ""
```

### Run synchronisation

The ETL pipeline can be run using the following command:

```
$ python -m data_exchange --sodar --varfish --sams
```

Use `--dry-run` to not change any data in the connected baserow database.

### Clinvar Upload/Download

Clinvar upload works on a per-batch basis and uses the retrieval and upload
scripts found in `./scripts/`.

Batch number can be an arbitrary string is just used to set storing locations
for retrieved data for comparisons, as well as uniquely identifying batches for
[clinvar-this](https://github.com/varfish-org/clinvar-this) operation.

Use `./scripts/retrieve_clinvar.sh <Batch number>` to download data from clinvar
on currently uploaded data.

Use `./scripts/upload_clinvar.sh <Batch number>` to upload data again.
