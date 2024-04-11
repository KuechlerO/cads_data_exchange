#!/bin/bash

# BASEROW Token needs to be provided via the TOKEN env-var!

flask --app webgui --debug run --host 0.0.0.0 --port 5151
