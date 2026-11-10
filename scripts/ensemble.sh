#!/bin/bash
# Этапы 1 и 2. Каждая команда печатается перед выполнением, вывод сохраняется в logs.
cd "$(dirname "$0")/.."
run() { echo "\$ $*"; eval "$@" 2>&1; }
port() { echo "218${1#zk}"; }
status_all() { for s in zk1 zk2 zk3; do run docker exec $s zkServer.sh status; done; }
role() { docker exec "$1" zkServer.sh status 2>/dev/null | sed -n 's/^Mode: //p'; }

stage_setup() {
  run docker pull zookeeper:3.9.6-jre-17
  run cat conf/zk1/zoo.cfg
  run cat conf/zk2/zoo.cfg
  run cat conf/zk3/zoo.cfg
  run cat data/zk1/myid data/zk2/myid data/zk3/myid
  run docker compose up -d
  run "docker inspect --format '{{.Name}} {{.Config.Cmd}}' zk1 zk2 zk3"
  run docker exec zk1 printenv JAVA_HOME
  run docker exec zk1 java -version
}

stage_roles() {
  run sleep 10
  run "docker compose ps --format '{{.Name}} {{.Image}} {{.Status}} {{.Ports}}'"
  status_all
}

stage_quorum() {
  leader=$(for s in zk1 zk2 zk3; do [ "$(role $s)" = leader ] && echo $s; done)
  echo "leader=$leader"
  run docker stop "$leader"
  run sleep 5
  alive=$(for s in zk1 zk2 zk3; do [ "$s" != "$leader" ] && echo $s; done)
  for s in $alive; do run docker exec $s zkServer.sh status; done
  first=$(echo $alive | cut -d' ' -f1); second=$(echo $alive | cut -d' ' -f2)
  run docker exec $first zkCli.sh -server $first:$(port $first) create /quorum-check one-server-down
  run docker exec $second zkCli.sh -server $second:$(port $second) get /quorum-check
  run docker stop "$first"
  run sleep 5
  run docker exec $second zkServer.sh status
  run "docker exec $second bash -c 'echo srvr | nc -w 2 localhost $(port $second)'"
  run docker exec $second timeout 30 zkCli.sh -server $second:$(port $second) ls /
  run docker start "$leader" "$first"
  run sleep 10
  until [ -n "$(role zk1)" ] && [ -n "$(role zk2)" ] && [ -n "$(role zk3)" ]; do run sleep 5; done
  status_all
  run docker exec zk1 zkCli.sh -server zk1:2181 get /quorum-check
}

stage_ephemeral() {
  # второе окно, клиент создает эфемерный узел и остается открытым
  (printf 'create -e /lab4-ephemeral window2\n'; sleep 60) | docker exec -i zk2 zkCli.sh -server zk2:2182 > logs/04_ephemeral_window2.txt 2>&1 &
  run sleep 5
  run docker exec zk1 zkCli.sh -server zk1:2181 get -s /lab4-ephemeral
  run docker exec zk2 pkill -9 -f ZooKeeperMain
  t0=$(date +%s)
  echo '$ while docker exec zk1 zkCli.sh -server zk1:2181 ls / | grep -q lab4-ephemeral; do date +%T; sleep 2; done'
  while docker exec zk1 zkCli.sh -server zk1:2181 ls / | grep -q lab4-ephemeral; do date +%T; sleep 2; done
  echo "node_gone_after_seconds=$(( $(date +%s) - t0 ))"
  run docker exec zk1 zkCli.sh -server zk1:2181 ls /
  run docker exec zk1 zkCli.sh -server zk1:2181 get /lab4-ephemeral
  wait
}

stage_setup | tee logs/01_setup.txt
stage_roles | tee logs/02_roles.txt
stage_quorum | tee logs/03_quorum.txt
stage_ephemeral | tee logs/04_ephemeral_window1.txt
