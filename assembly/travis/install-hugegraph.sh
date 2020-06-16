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

rm -rf hugegraph

tar -zxvf hugegraph-*.tar.gz

cd hugegraph-*

cp ../$TRAVIS_DIR/conf/* conf

echo -e "pa" | bin/init-store.sh

bin/start-hugegraph.sh

cd ../
