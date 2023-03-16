
from mrjob.job import MRJob
from mrjob.step import MRStep

class project_job4_criminal2(MRJob):


    hash_table = {}

    def mapper_join_init(self):

        try:
            with open("job4.txt") as f:
                for line in f:
                    fields = line.split(",")
                    hash = fields[0]
                    value = fields[1]
                    publicKey = fields[2]
                    self.hash_table[hash]=(publicKey,value)
        except:
            pass

    def mapper_criminal(self, _, line):
        try:
            fields = line.split(",")
            tx_hash = fields[1]
            if tx_hash in self.hash_table:
                publicKey = self.hash_table[tx_hash][0]
                value=self.hash_table[tx_hash][1]
                print(f"{hash},{value},{publicKey}")
            #    yield(publicKey,(1,value))
        except:
             pass


    def steps(self):
        return [MRStep(mapper_init=self.mapper_join_init,
                        mapper=self.mapper_criminal)]
    #                    reducer=self.reducer_criminal)]


if __name__ == '__main__':

    project_job4_criminal2.run()
