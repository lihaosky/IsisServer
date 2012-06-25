#!/bin/bash

if [ $# -ne 2 ]
then
echo 'Usage: ./Lauch.sh nodenumber shardsize'
exit
fi

nodenum=$1
shardsize=$2

echo 'Node number is '$nodenum
echo 'Shard size is '$shardsize

i=0
nodes=`cat NodeList`
for word in $nodes
do
    ssh -f $word "killall mono; killall memcached"
    words[$i]=$word
    ((i=$i+1))
done

i=0
while [ $i -lt $nodenum ]
do
echo "Lauch job on ${words[$i]}"
# Launch job
ssh -f ${words[$i]}  "cd  ~/Utils/C#_TCP; ~/bin/bin/mono IsisServer.exe -n $nodenum -s $shardsize -r $i -v 2>&1"
ssh -f ${words[$i]}  "cd ~/memcached; ./memcached -q 1234 -vv 2>&1"
((i=$i+1))
done


