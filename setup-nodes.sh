a=`expr 0 + $1`
b=`expr 0 + $2`
for i in {$a..$b}
do
    bash setup-node.sh $i
done
