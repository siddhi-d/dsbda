py hadoop 

 map.py

import sys
import io 

input_stream = io.TextIOWrapper(sys.stdin.buffer)

for line in input_stream:
     words = line.split()
     for word in words:
          print("%s\t%s" % (word,1))





reduce.py


import io 
import sys 

current_word=None 
word=None
current_count=0


for line in sys.stdin :
   word,count = line.split('\t',1)
   count = int(count)

   if current_word == word:
      current_count +=count 

   else:

      if current_word:
          print("%s\t%s" %(current_word,current_count))
         current_word = word 
         current_count = count
 
if current_word == word:
       print("%s\t%s" %(current_word,current_count))
