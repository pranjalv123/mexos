a=$1
b=$2
echo $1 
echo $2
echo $a
echo $b
for (( i=$1; i<=$2; i++ ))
do
    echo $i
    bash setup-node.sh $i
done
