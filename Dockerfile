FROM ubuntu:14.10
ENV REFRESHED_AT 2015-09-29
RUN apt-get -y -q update
RUN apt-get -y -q upgrade
RUN apt-get install -y -q openjdk-8-jdk
RUN apt-get install -y -q git
# The Ubuntu's gradle is too old (version 1.5)
# Use the gradle wrapper instead
# RUN apt-get install -y -q gradle
