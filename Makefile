KITCHEN=bundle exec kitchen

all: compile

converge: vendor/bundle user_network galaxy
	$(KITCHEN) $@

test: vendor/bundle user_network galaxy
	./gradlew test
	$(KITCHEN) $@

compile:
	./gradlew :flink-sample:shadowJar :beam-dev:shadowJar

lsflink:
	docker exec flink-master /opt/flink-1.1.2/bin/flink list

lstopic:
	docker exec flink-master /opt/kafka_2.11-0.10.0.1/bin/kafka-topics.sh \
		--zookeeper localhost:2181 --list

list: vendor/bundle
	$(KITCHEN) $@

destroy: vendor/bundle
	$(KITCHEN) $@
	-docker rm flink-master
	-docker rm flink-slave1
	-docker rm flink-slave2
	-docker network rm flink_nw

user_network:
	docker network inspect flink_nw || docker network create -d bridge flink_nw

galaxy:
	ansible-galaxy install --force --ignore-errors -r galaxy-requirements.txt -p roles/

vendor/bundle: bundle-bin
	bundle install --path "$@"

bundle-bin: FORCE
	command -v bundle || gem install --user-install --no-ri --no-rdoc

FORCE:
