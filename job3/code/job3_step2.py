

from mrjob.job import MRJob
from mrjob.step import MRStep



class project_job3_step2(MRJob):
    sumvalue=0
    def mapper_count(self,_,lines):
        try:
            fields = lines.split(",")
            money=float(fields[1])
            publicKey=fields[2]
            yield(publicKey,money)
        except:
            pass

    def reducer_count(self,key,value):
        yield(key,sum(value))

    def mapper_total(self,_,value):
        if (value>1000.0):
            yield(None,(value,value))
        else:
            yield(None,(value,0))

    def reducer_total(self,_,values):
        totalsum=0
        largesum=0
        for value in values:
           totalsum=totalsum+value[0]
           largesum=largesum+value[1]

        yield((totalsum,largesum,totalsum-largesum),(largesum/totalsum,(totalsum-largesum)/totalsum))

    def steps(self):
        return [MRStep(mapper=self.mapper_count,
                    reducer=self.reducer_count),
                MRStep(mapper=self.mapper_total,
                reducer=self.reducer_total)]


if __name__ == '__main__':
   project_job3_step2.run()
