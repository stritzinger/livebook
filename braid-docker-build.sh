#!/bin/sh
docker build --build-arg BASE_IMAGE=elixir:1.18-otp-27-alpine --build-arg VARIANT=default --platform linux/amd64  -t grisp/braid_livebook .
