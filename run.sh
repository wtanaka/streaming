#!/bin/sh
# Copyright (C) 2016 Wesley Tanaka
ansible-galaxy install --force --ignore-errors -r requirements.txt -p roles/
rsync -av --delete ~/Dropbox/ansible-role-apache-flink/ roles/wtanaka.apache-flink/
rsync -av --delete ~/Dropbox/ansible-role-oracle-java/ roles/wtanaka.oracle-java/
rsync -av --delete ~/Dropbox/ansible-role-apache-kafka/ roles/wtanaka.apache-kafka/
rsync -av --delete ~/Dropbox/ansible-role-monit/ roles/wtanaka.monit/
rsync -av --delete ~/Dropbox/ansible-role-zookeeper/ roles/wtanaka.zookeeper/
