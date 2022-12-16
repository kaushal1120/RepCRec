#!/bin/bash

for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30
do
    echo "RUNNING TEST $i"
    echo "v=============TEST $i START===========v"
    python3 main.py -f "tests/test$i.txt";
    echo "^=============TEST $i DONE=============^"
done
