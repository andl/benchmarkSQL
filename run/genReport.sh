echo -n "Total Tnx:"
ttnx=`cat $1/normal.txt | grep Term- | grep -v "Loading database driver" | less | wc -l`
echo $ttnx
echo "TnxName TotalRT AvgRT TotalNum TnxWeight%"
cat $1/normal.txt  | grep Term- | grep -v "Loading database driver" | sed s/Term-/\\nTerm-/g| awk '{print $2,$3}' | grep -v "Order ID" | awk -v ttnx=$ttnx '{S[$1]=S[$1]+$2} {++C[$1]} END {for(a in S) print a,S[a],S[a]/C[a],C[a], C[a]/ttnx*100}' | egrep 'Payment|Delivery|Order|Stock'  | sort
echo "----90% RT----"
for i in Delivery New-Order Order-Status Payment Stock-Level 
do
echo -n "$i:"
cat $1/normal.txt  | grep Term- | grep -v "Loading database driver" | sed s/Term-/\\nTerm-/g | grep $i |awk '{print $3}'  | sort  -n | awk -v r=0 '{r=r+1} {S[r]=$1} END {print S[int(r*0.9)]}'
done
echo "---- Measured tpmC ----"
echo $((`cat $1/normal.txt | grep "Measured tpmC" | awk -F= '{print $2}'`))
