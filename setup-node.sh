a=`expr 100 + $1`
ip=10.0.0.$a

scp -i mexos-keypair-1.pem setup.sh ubuntu@$ip:
ssh -i mexos-keypair-1.pem ubuntu@$ip bash setup.sh
