#Part2_it7005

from pyspark import SparkContext
import re

def parse_line(line):
    station_id = line[4:10]
    ceiling_height = line[70:75]
    q = line[75:76]

    if ceiling_height != "99999" and re.match("[01459]", q):
        return str(station_id), int(ceiling_height)
    else:
        return str(station_id), None

def main():

    sc = SparkContext(appName='CeilingHeightRange')

    # Loading input data
    input_rdd = sc.textFile('/home/19student19/inputheight/*.gz')

    # Parsing each line
    station_ceiling = input_rdd.map(parse_line)

    # Filtering out invalid data
    filtered_station_ceiling = station_ceiling.filter(lambda x: x[1] is not None)

    # Grouping the data by station_id
    grouped_station_ceiling = filtered_station_ceiling.map(lambda x: (x[0], x[1])).groupByKey()

    # Calculating the ceiling height range
    ceiling_range = grouped_station_ceiling.mapValues(lambda values: max(values) - min(values))

    # Saving the result
    ceiling_range.saveAsTextFile('/home/19student19/outputheightrange')

    sc.stop()

if __name__ == "__main__":
    main()