for (( i=1; i<=10; i++ ))
do
    ip=10.0.0.$i
    scp $@ ubuntu@$ip: &
done
