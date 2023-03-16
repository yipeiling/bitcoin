
from mrjob.job import MRJob
from mrjob.step import MRStep



class project_job2_filter(MRJob):

 def mapper(self, _, line):
    try:
        key = "{1HB5XMLmzFVj8ALj6mfBsbifRoD4miY36v}"
        fields = line.split(",")
        if (fields[3] == key):

            print(f"{fields[0]},{fields[1]},{fields[2]},{fields[3]}")

    except:
          pass

if __name__ == '__main__':
   project_job2_filter.run()
