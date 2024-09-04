## bitnami/reids helm image를통한 구축  

#### local nfs directory  
##### /mnt/data/redis

1. pv-pvc 만들기
kubectl apply -f redis-pv-pvc.yaml

2. helm repo update
helm repo add bitnami https://charts.bitnami.com/bitnami

3. reids-values.yaml install
helm install redis bitnami/redis -f redis-values.yaml --namespace redis --set auth.enabled=false


