#!/bin/bash
jar_file=`ls kylin-cli-*.jar | awk -F ' ' '{print $1}'`
if [ -z "$jar_file" ]; then
	echo 'Can not find kylin jar like kylin-cli-*.jar in current dir.'
	exit -1
fi
java -cp .:./${jar_file}:./lib/* org.apache.kylin.client.script.DailyBuildScript "$@" > /tmp/kylin-build.log 2>&1 &