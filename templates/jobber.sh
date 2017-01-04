#!/bin/bash

exec 3<>/dev/tcp/login01/$port
echo -e "getjob" >&3
FILE=$$(cat <&3)

$$1 $$FILE

