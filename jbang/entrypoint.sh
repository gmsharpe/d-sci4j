#!/usr/bin/env bash

# In OpenShift, containers are run as a random high number uid
# that doesn't exist in /etc/passwd
if [ `id -u` -ge 500 ] || [ -z "${CURRENT_UID}" ]; then

  cat << EOF > /tmp/passwd
root:x:0:0:root:/root:/bin/bash
jbang:x:`id -u`:`id -g`:,,,:/scripts:/bin/bash
EOF

  cat /tmp/passwd > /etc/passwd
  rm /tmp/passwd
fi

if [[ ! -z "$INPUT_TRUST" ]]; then
  jbang trust add $INPUT_TRUST
fi

echo jbang $INPUT_JBANGARGS $INPUT_SCRIPT $INPUT_ARGS "${@}"
exec jbang $INPUT_JBANGARGS $INPUT_SCRIPT $INPUT_SCRIPTARGS "${@}"