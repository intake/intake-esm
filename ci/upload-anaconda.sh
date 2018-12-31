#!/bin/bash

set -e
set -eo pipefail
if [ -z "$CONDA_UPLOAD_TOKEN" ]; then 
   echo "No upload key"
   exit 0 
fi 

export UPLOADFILE=`conda build conda/ --output`
echo "UPLOADFILE = ${UPLOADFILE}"

echo "[Uploading esmalb]"
anaconda -t ${CONDA_UPLOAD_TOKEN} upload -u cisl-iowa --force ${UPLOADFILE}
echo "Successfully deployed to Anaconda.org."

exit 0