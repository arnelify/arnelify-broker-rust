FROM rust:1.91.1

WORKDIR /var/www/rust

RUN apt-get update -y && apt-get upgrade -y
RUN apt-get install sudo nano -y

EXPOSE 4433