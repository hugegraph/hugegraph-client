#!/bin/bash

set -ev

if [ $# -ne 1 ]; then
    echo "Must pass base branch name of pull request"
    exit 1
fi

CLIENT_BRANCH=$1
HUGEGRAPH_BRANCH=$CLIENT_BRANCH

HUGEGRAPH_GIT_URL="https://github.com/hugegraph/hugegraph.git"

git clone $HUGEGRAPH_GIT_URL || exit 1

cd hugegraph

git checkout $HUGEGRAPH_BRANCH || exit 1

mvn package -DskipTests || exit 1

mv hugegraph-*.tar.gz ../

cd ../

mkdir hugegraph-https

mv hugegraph-*.tar.gz hugegraph-https

rm -rf hugegraph

cd  hugegraph-https

tar -zxvf hugegraph-*.tar.gz

cd hugegraph-*

cp ../../conf/https/rest-server.properties conf/

cp ../../conf/https/server.keystore conf/

bin/init-store.sh || exit 1

bin/start-hugegraph.sh || exit 1
