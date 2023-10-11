#!/bin/bash  
  
for((i=1;i<=20;i++));  
do
	echo "loop "$i 
	make project3b > $i.log
	grep "FAIL" -rni $i.log
done
