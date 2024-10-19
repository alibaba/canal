#! /bin/bash

helm install canal-admin -f ./admin-values.yaml ../canal-admin --dry-run

helm install canal-server -f ./server-values.yaml ../canal-server --dry-run