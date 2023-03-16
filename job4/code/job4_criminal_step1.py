
from mrjob.job import MRJob
from mrjob.step import MRStep



class project_job4_criminal(MRJob):

 def mapper(self, _, line):

    try:

        fields = line.split(",")
        hash=fields[0]
        value=float(fields[1])
        publicKey=fields[3]
        if (value > 4000):
            print(f"{hash},{value},{publicKey}")

    except:
        pass

if __name__ == '__main__':
   project_job4_criminal.run()
