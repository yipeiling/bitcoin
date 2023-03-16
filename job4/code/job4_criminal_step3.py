

from mrjob.job import MRJob
from mrjob.step import MRStep


class project_job4_criminal3(MRJob):
 i = 0
 def mapper(self, _, line):
    fields = line.split(",")
    if len(fields)==3:
        hash=fields[0]
        value= float(fields[1])
        publicKey=fields[2]
        yield(publicKey,(value,1))

 def reducer(self, feature, values):
    sumvalue = 0
    count=0
    for value in values:
        sumvalue = sumvalue+value[0]
        count = count+value[1]
    if (count==1):
        self.i = self.i+1
        yield(feature,(self.i,sumvalue))

if __name__ == '__main__':
    project_job4_criminal3.run()
