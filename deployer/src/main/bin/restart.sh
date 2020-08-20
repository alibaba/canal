#!/bin/bash

args=$@

bash stop.sh $args
bash startup.sh $args
