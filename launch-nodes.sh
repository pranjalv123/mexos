for i in {101..102}
do
    ec2-run-instances ami-b7667cde --private-ip-address 10.0.0.$i -t m1.small -g sg-e51d9980 --subnet subnet-563a3d10 -k mexos-keypair --associate-public-ip-address true
done
