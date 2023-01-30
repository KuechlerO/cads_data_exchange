# Export/Import for Exomdiagnostics

## Automated Email systems

These systems run inside a cronjob periodically and are currently used for
notification and automated assignment on new batches.

Main systems include:

`./combine_tnamse.py`
  Merges PEL, SODAR Samplesheet and TNamse Data in order to generate a mail with
  assignments per batch.

`./mdb_to_mail.py`
  Extracts appointments for individual users and sends them a mail with
  appointments for the next 14 days.

## Baserow import

`./fetch_baserow_schemas.py`
  Extract baserow schemas into yaml format. This is used to manually add the
  mapping from our tnamse export.
