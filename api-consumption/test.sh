#!/bin/bash

read -r -d '' KAFKAIPS <<- EOM
10.154.144.11
10.154.136.11
10.154.128.14
10.154.128.15
10.154.128.16
10.154.136.12
10.154.136.13
10.154.136.14
10.154.144.12
10.154.144.13
10.154.144.14
10.154.128.17
10.154.128.13
10.154.128.18
10.154.128.19
10.154.128.20
10.154.136.15
10.154.136.16
10.154.136.17
10.154.136.18
10.154.136.19
10.154.144.15
10.154.136.20
10.154.144.16
10.154.144.17
10.154.144.19
10.154.144.20
10.154.144.21
10.154.128.11
10.154.128.12
EOM

for ip in $KAFKAIPS; do
  piu request-access --odd-host  odd-eu-central-1-migration.aruha.zalan.do ${ip} "Check kafka open files" >/dev/null 2>/dev/null
  ssh -tA dsorokin@odd-eu-central-1-migration.aruha.zalan.do ssh -o StrictHostKeyChecking=no dsorokin@10.154.144.11 2>/dev/null
done