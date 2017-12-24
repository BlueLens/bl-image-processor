#!/usr/bin/env bash

NAMESPACE=$1

kubectl delete pods --namespace=$NAMESPACE -l group=bl-image-processor