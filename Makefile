all: compile

compile:
	./gradlew :flink-sample:shadowJar

list: vendor/bundle
	bundle exec kitchen $@

converge: vendor/bundle user_network galaxy
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
