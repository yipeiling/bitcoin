
from mrjob.job import MRJob
from mrjob.step import MRStep

class project_job2_join1(MRJob):


    hash_table = []

    def mapper_join_init(self):

        with open("out2.txt") as f:
              for line in f:
                fields = line.split(",")
                hash = fields[0]
                self.hash_table.append(hash)


    def mapper_join1(self, _, line):

        fields = line.split(",")
        key = fields[0]
        if key in self.hash_table:
            tx_hash = fields[1]
            vout = fields[2]
            print(f"{fields[0]},{tx_hash},{vout}")




    def steps(self):
        return [MRStep(mapper_init=self.mapper_join_init,
                        mapper=self.mapper_join1)]

if __name__ == '__main__':

     project_job2_join1.run()
