
from mrjob.job import MRJob
from mrjob.step import MRStep

class project_job2_join2(MRJob):

    hash_table = {}
    def mapper_join_init(self):
        try:
            with open("out3.txt") as f:
               for line in f:
                   fields = line.split(",")
                   tx_hash = fields[1]
                   vout = fields[2]
                   self.hash_table[tx_hash]=vout
        except:
            pass


    def mapper_join2(self, _, line):
        try:
            fields = line.split(",")
            hash = fields[0]
            publickey = fields[3]
            value = float(fields[1])
            n= fields[2]
            if (hash in self.hash_table and n in self.hash_table[hash]) :
                yield(publickey,value)
        except:
            pass

    def reducer_count(self, feature, value):
        yield(None,(feature,sum(value)))

    def reducer_sort(self, _, values):
        sorted_values=sorted(values,reverse=True, key=lambda x: x[1])
        for i in range(10):
            yield(i+1,sorted_values[i])

    def steps(self):
        return [MRStep(mapper_init=self.mapper_join_init,
                        mapper=self.mapper_join2,
                        reducer=self.reducer_count),
                MRStep(reducer=self.reducer_sort)]


if __name__ == '__main__':
   project_job2_join2.JOBCONF= { 'mapreduce.reduce.memory.mb': 4096 }
   project_job2_join2.run()
