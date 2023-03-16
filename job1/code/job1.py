"""job 1. Time Analysis
"""
from mrjob.job import MRJob
from datetime import datetime
import time



class Job1(MRJob):

   def mapper(self, _, line):
    try:
      fields = line.split(",")
#     if (len(fields)==5):
      time = int(fields[2])
      year = datetime.utcfromtimestamp(time).year
      month =datetime.utcfromtimestamp(time).month
      yield ((year,month),1)
    except:
      pass

   def reducer(self, key, value):
      yield(key,sum(value))


if __name__ == '__main__':
#    Job1.JOBCONF= { 'mapreduce.job.reduces': '3' }
     Job1.JOBCONF= { 'mapreduce.reduce.memory.mb': 4096 }
     Job1.run()
