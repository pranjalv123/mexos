a=`expr 100 + $1`
ip=10.0.0.$a

scp -i mexos-keypair.pem setup.sh ubuntu@ip:
ssh -i mexos-keypair.pem ubuntu@ip bash setup.sh
