# Deploying Spark on Kubernetes

Check out the [post]() for the article .


## Minikube Setup

* Make sure the Spark version in you local and the container is the same. We will be using spark 2.4.5 here.


* Install and run [Minikube](https://kubernetes.io/docs/setup/minikube/):

*  Install a  [Hypervisor](https://kubernetes.io/docs/tasks/tools/install-minikube/#install-a-hypervisor) (like [VirtualBox]
(https://www.virtualbox.org/wiki/Downloads) or [HyperKit](https://github.com/moby/hyperkit)) to manage virtual machines

* Install and Set Up [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/) to deploy and manage apps on Kubernetes

*  Install [Minikube](https://github.com/kubernetes/minikube/releases)

### Start the Minikube cluster:

Below command will start the mini
```bash
$ minikube start --memory 10240 --cpus 4 --vm=true
```

#### Check the minikube dashboard for the cluster status

```
$ minikube dashboard
```

Build the Docker image from the root of the project :

```bash
$ eval $(minikube docker-env)
$ docker build -t spark-hadoop:2.2.1 -f ./docker/Dockerfile ./docker
```

Create the deployments and services:

```sh
$ kubectl create -f ./kubernetes/spark-master-deployment.yaml
$ kubectl create -f ./kubernetes/spark-master-service.yaml
$ kubectl create -f ./kubernetes/spark-worker-deployment.yaml
$ minikube addons enable ingress
$ kubectl apply -f ./kubernetes/minikube-ingress.yaml
```

Add an entry to /etc/hosts:

```sh
$ echo "$(minikube ip) spark-kubernetes" | sudo tee -a /etc/hosts

```

Test it out in the browser at [http://spark-kubernetes/](http://spark-kubernetes/).



## Check the Logs from the pOds

kubectl logs <Pod_Name>


## Delete the PoDS
 kubectl delete pods <pod_name>

 ```
 kubectl delete pods spark-worker-578ffdbb5f-jfnlv
 ```
 

 ## Describe the PODS

 kubectl describe pods
## Reference

