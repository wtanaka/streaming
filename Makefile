all: compile

compile:
	./gradlew :flink-sample:shadowJar

list:
	bundle exec kitchen $@

converge: user_network
	bundle exec kitchen $@

destroy:
	bundle exec kitchen $@
	docker network rm flink_nw

user_network:
	docker network inspect flink_nw || docker network create -d bridge flink_nw
