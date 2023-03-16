
from mrjob.job import MRJob
from mrjob.step import MRStep



class Job3_step1(MRJob):
    def  mapper(self,_,line):
        try:
          fields = line.split(",")
          if len(fields)==4:
              hash=fields[0]
              value = fields[1]
              publicKey = fields[3]
              yield(hash,(1,1,value,publicKey))

          if len(fields)==3:
              tx_hash=fields[1]
              yield(tx_hash,(0,1,0,0))
        except:
            pass

    def  reducer(self,key,values):
        repeat=0
        for value in values:
            repeat=repeat+value[1]
            money=value[2]
            publicKey=value[3]
            if ((repeat==1) and (value[0]==1)):
                print(f"{key},{money},{publicKey}")



if __name__ == '__main__':
    Job3_step1.run()
