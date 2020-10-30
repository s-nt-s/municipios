#!/bin/bash
grep "$1" -l -r $2 | xargs sed -i -e "s|${1}||g"