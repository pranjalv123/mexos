a=`expr 100 + $1`
ip=10.0.0.$a

scp -i mexos-keypair.pem setup.sh ip:
ssh -i mexos-keypair.pem bash setup.sh
