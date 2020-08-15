#!/bin/bash


echo "Applying Spark Master Deployments and Services"

kubectl create -f ./kubernetes/spark-master-deployment.yaml
kubectl create -f ./kubernetes/spark-master-service.yaml

echo "Verifying the Deployments and Master Service Creation"

echo "Displaying the Pods: kubectl get pods"

kubectl get pods

echo "Displaying the kubectl deployments: kubectl get deployments"


kubectl get deployments


sleep 10

echo "Applying Spark worker Deployments "

kubectl create -f ./kubernetes/spark-worker-deployment.yaml

echo "Enabling ingress in Minikube "

minikube addons enable ingress

sleep 8

echo "Applying Minikube ingress file  "

kubectl apply -f ./kubernetes/minikube-ingress.yaml



echo "Displaying the Master and Worked Pods: kubectl get pods"

kubectl get pods

echo "Displaying the kubectl deployments for Master and Worker : kubectl get deployments"


kubectl get deployments
