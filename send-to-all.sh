for (( i=101; i<=110; i++ ))
do
    ip=10.0.0.$i
    scp $@ ubuntu@$ip: &
done
