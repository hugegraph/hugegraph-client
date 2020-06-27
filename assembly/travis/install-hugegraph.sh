#!/bin/bash

set -ev

TRAVIS_DIR=`dirname $0`

if [ $# -ne 1 ]; then
    echo "Must pass base branch name of pull request"
    exit 1
fi

CLIENT_BRANCH=$1
HUGEGRAPH_BRANCH=$CLIENT_BRANCH

HUGEGRAPH_GIT_URL="https://github.com/hugegraph/hugegraph.git"

git clone $HUGEGRAPH_GIT_URL

cd hugegraph

git checkout $HUGEGRAPH_BRANCH

mvn package -DskipTests

mv hugegraph-*.tar.gz ../

cd ../

mkdir https

rm -rf hugegraph

tar -zxvf hugegraph-*.tar.gz

cp -r hugegraph-*/. https

cd hugegraph-*

cp ../$TRAVIS_DIR/conf/* conf

echo -e "pa" | bin/init-store.sh

bin/start-hugegraph.sh

cd ../

cd https

cp ../$TRAVIS_DIR/conf/server.keystore conf

rest_server_path="conf/rest-server.properties"

gremlin_server_path="conf/gremlin-server.yaml"

sed -i "s/http:\/\/127.0.0.1:8080/https:\/\/127.0.0.1:8443/g" "$rest_server_path"

sed -i "s/#port: 8182/port: 8282/g" "$gremlin_server_path"

echo "server.protocol=https" >> $rest_server_path

echo "ssl.server_keystore_password=123456" >> $rest_server_path

echo "ssl.server_keystore_file=conf/server.keystore" >> $rest_server_path

echo "gremlinserver.url=http://127.0.0.1:8282" >> $rest_server_path

bin/init-store.sh

bin/start-hugegraph.sh

cd ../
