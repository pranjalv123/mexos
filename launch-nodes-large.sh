for i in {101..112}
do
    ec2-run-instances ami-ee4f77ab --private-ip-address 10.0.0.$i -t m3.large -g sg-3a30dd5f --subnet subnet-4a21cc2f -k mexos-keypair-1 --associate-public-ip-address true --region us-west-1
done
