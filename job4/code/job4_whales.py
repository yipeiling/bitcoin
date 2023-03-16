"""project_job4_whales.
"""


from mrjob.job import MRJob
from mrjob.step import MRStep


class project_job4_whales(MRJob):
    hash_table = {}

    def mapper_join_init(self):

#        try:
            with open("job4_2.txt") as f:
                for line in f:
                    fields = line.split(",")
                    if len(fields)==3:
                        hash = fields[0]
                        value = fields[1]
                        publicKey = fields[2]
                        self.hash_table[hash]=(value,publicKey)
#        except:
#            pass

    def mapper_whales(self, _, line):
        #try:
            fields = line.split(",")
            if len(fields)==3:
                hash = fields[0]
                value = fields[1]
                publicKey = fields[2]
                if hash not in self.hash_table:
                    print(f"{hash},{value},{publicKey}")

        #except:
        #    pass


    # def mapper_count(self,_,lines):
    #     try:
    #         fields = lines.split(",")
    #         money=float(fields[1])
    #         publicKey=fields[2]
    #         yield(publicKey,(money,1))
    #     except:
    #         pass
    #
    # def reducer_count(self,key,values):
    #     valuesum = 0
    #     count = 0
    #     for value in values:
    #         valuesum=valuesum+float(value[0])
    #         count=count+value[1]
    #     yield(key,(valuesum,count))
    #
    # def mapper_total(self,key,values):
    #     if (values[1]>20 and values[0]<=1000):
    #        yield("Merchat",(key,values[0],values[1]))
    #
    #     if (values[1]==1 and values[0]>1000):
    #        yield("whales",(key,values[0],values[1]))
    #
    # def reducer_total(self,features,values):
    #     yield(features,values)
    #
    def steps(self):
        return [MRStep(mapper_init=self.mapper_join_init,
                        mapper=self.mapper_whales)]
    #             MRStep(mapper=self.mapper_total,
    #                 reducer = self.reducer_total)]


if __name__ == '__main__':
   project_job4_whales.run()
