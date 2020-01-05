#! /bin/sh
cat haha2 | grep "Resize take" | awk '{print $4}' | grep -v "e" | awk '{sum+=$1;} END {print sum;}'
