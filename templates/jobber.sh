#!/bin/bash

exec 3<>/dev/tcp/localhost/$port
echo -e "getjob" >&3
FILE=$$(cat <&3)

$$1 $$FILE

