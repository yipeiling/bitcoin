

from mrjob.job import MRJob
from mrjob.step import MRStep

from datetime import datetime

class project_job3_step3(MRJob):

    hash_table = {}

    def mapper_join_init(self,_,line):
        with open("job3.txt") as f:
            for line in f:
                fields = line.split(",")
                if (len(fields)==3):
                    hash = fields[0]
                    money=float(fields[1])
                    publicKey=fields[2]
                    if (money>1000):
                        self.hash_table[hash]=(money,publicKey)
                        yield(None,money)

    def mapper_year(self,_,lines):
        try:
            fields = lines.split(",")
            hash = fields[0]
            time = int(fields[2])
            year = datetime.utcfromtimestamp(time).year
            if ((hash in self.hash_table) and (year < 2014)):
                yield(None,self.hash_table[hash][0])
        except:
            pass

    def reducer_largeinvestment(self,_,values):
            yield(None,sum(values))

     def steps(self):
         return [MRStep(mapper_init=self.mapper_join_init,
                       mapper=self.mapper_year)]
                     reducer=self.reducer_largeinvestment)]


if __name__ == '__main__':
   project_job3_step3.run()
