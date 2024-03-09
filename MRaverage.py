#Part1_it7005

from mrjob.job import MRJob
import re

class WindDirectionAvg(MRJob):

    # Mapper function
    def mapper(self, _, line):
        val = line.strip()
        (year, month, wind_direction, q) = (val[15:19], val[19:21], val[60:63], val[92:])
        if (wind_direction != "999" and re.match("[01459]", q)):
            yield ("".join([year, month]), int(wind_direction))

    # Reducer function
    def reducer(self, year_month, wind_direction):
        sum,count = 0,0
        for direction in wind_direction:
            sum += direction
            count+=1

        # Calculating the average wind direction,    
        average_wind_direction = sum/count if count != 0 else 0 
        yield (year_month, average_wind_direction)

# Running the MRjob
if __name__ == '__main__':
    WindDirectionAvg.run()

