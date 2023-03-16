"""Project_job5_filter. filter the small v_out
"""

from mrjob.job import MRJob
from mrjob.step import MRStep


#get the total bitcoins
#from COINGEN get tx_hash then from VOUT get value
class project_job5_merchant(MRJob):

 def mapper(self, _, line):

     try:

         fields = line.split(",")
         hash=fields[0]
         value=float(fields[1])
         publicKey=fields[3]
         if (value< 1):
            yield(publicKey,(value,1))

     except:
         pass

 def reducer(self, feature, values):
     try:
         sumvalue = 0
         count =0
         for value in values:
            sumvalue = sumvalue + value[0]
            count = count + value[1]
         if ((count>20) and (sumvalue<10)):
             yield(feature,(sumvalue,count))
     except:
         pass



if __name__ == '__main__':
   project_job5_merchant.run()
