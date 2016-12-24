all: compile

compile:
	./gradlew :flink-sample:shadowJar :beam:shadowJar

converge: vendor/bundle user_network galaxy
	bundle exec kitchen $@

lsflink:
	docker exec flink-master /opt/flink-1.1.2/bin/flink list

lstopic:
	docker exec flink-master /opt/kafka_2.11-0.10.0.1/bin/kafka-topics.sh \
		--zookeeper localhost:2181 --list

list: vendor/bundle
	bundle exec kitchen $@

destroy: vendor/bundle
	bundle exec kitchen $@
	docker network rm flink_nw

user_network:
	docker network inspect flink_nw || docker network create -d bridge flink_nw

galaxy:
	ansible-galaxy install --force --ignore-errors -r requirements.txt -p roles/

vendor/bundle: bundle-bin
	bundle install --path "$@"

bundle-bin: FORCE
	command -v bundle || gem install --user-install --no-ri --no-rdoc

FORCE:
