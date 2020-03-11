#!/usr/bin/env bash

#Licensed to the Apache Software Foundation (ASF) under one or more contributor license
#agreements. See the NOTICE file distributed with this work for additional information regarding
#copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
#"License"); you may not use this file except in compliance with the License. You may obtain a
#copy of the License at
#
#http://www.apache.org/licenses/LICENSE-2.0
#
#Unless required by applicable law or agreed to in writing, software distributed under the License
#is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#or implied. See the License for the specific language governing permissions and limitations under
#the License.

REDIS_HOST=localhost
REDIS_PORT=6379

while getopts ":o:f:" opt; do
  case ${opt} in
  o)
    OPERATION=${OPTARG}
    ;;
  f)
    FILENAME=${OPTARG}
    ;;
  \?)
    echo "Usage: ${0}"
    ;;
  :)
    echo "Invalid option: $OPTARG requires an argument" 1>&2
    exit 1
    ;;
  esac
done

COMMAND=""
RPS=""

MINIMUM=1000000
NINETY_FIVE=0
ONE_HUNDRED=0

input="${FILENAME}"
while IFS= read -r line; do
  if [ "${COMMAND}" == "" ]; then
    echo $line | grep ${OPERATION} >/dev/null
    if [ $? -eq 0 ]; then
      continue
    fi
    COMMAND=$(echo "$line" | cut -d":" -f 1)
  fi

  echo "$line" | grep "\%" >/dev/null
  if [ $? -eq 0 ]; then
    CURRENT_VAL=$(echo "$line" | cut -d" " -f 3)
    if ((${CURRENT_VAL} < ${MINIMUM})); then
      MINIMUM=${CURRENT_VAL}
    fi
    CURRENT_PCT=$(echo "$line" | cut -d"." -f 1)
    if [ ${NINETY_FIVE} -eq 0 ]; then
      if ((${CURRENT_PCT} >= 95)) && ((${CURRENT_PCT} < 100)); then
          NINETY_FIVE=$(echo "$line" | cut -d" " -f 3)
      fi
    fi
    if ((${CURRENT_PCT} == 100)); then
      ONE_HUNDRED=$(echo "$line" | cut -d" " -f 3)
    fi
  fi

  echo "$line" | grep "requests per second" >/dev/null
  if [ $? -eq 0 ]; then
    RPS=$(echo "$line" | cut -d" " -f 1)
  fi
done <"$input"

if [ -z "${RPS}" ]; then
  echo "Unable to find line containing 'requests per second'"
  exit 1
fi

echo ${COMMAND}","${MINIMUM}","${NINETY_FIVE}","${ONE_HUNDRED}","${RPS}
