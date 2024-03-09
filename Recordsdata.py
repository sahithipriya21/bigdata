#Part3_it7005

from mrjob.job import MRJob
import re

class StationDistance(MRJob):

    # Mapper function
    def mapper(self, _, line):
        val = line.strip()
        station_id = val[4:10]
        visibility_distance = val[78:84]
        q = val[84:85]

        if visibility_distance != "999999" and re.match("[01459]", q):
            yield (station_id, int(visibility_distance))

    # Reducer function
    def reducer(self, station_id, visibility_distances):
        for distance in visibility_distances:
            yield (station_id, distance)

# Running the MRjob
if __name__ == '__main__':
    StationDistance.run()
