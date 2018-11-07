#!/usr/bin/env bash

cd test
echo "===> Changing directory to \"./test\""

docker-compose up -d --build --force-recreate kafka
rc=$?
if [[ $rc != 0 ]]
  then exit $rc
fi

echo "Waiting additional time for Kafka to be ready."
add_wait=20
cur_add_wait=0
while (( ++cur_add_wait != add_wait ))
do
  echo Additional Wait: $cur_add_wait of $add_wait seconds
  sleep 1
done

docker ps -a

docker-compose up --exit-code-from go-kafkacatch
exit $?
