import sys

sys.path.insert(0, "./pylib/")
print sys.path

import numpy as np
import pandas as pd
import csv
import os
import time
from datetime import date, timedelta



def main():
    "main function"

    local_run = False

    config = {
        "from-year": "1981",
        "to-year": "2012",
        "start-row": "0",
        "end-row": "2544",
        "input-path": "P:/carbiocial/climate-data-years-1981-2012-rows-0-2544/" if local_run else "/archiv-daten/md/projects/carbiocial/climate-data-years-1981-2012-rows-0-2544/",
        "output-path": "./" if local_run else "/archiv-daten/md/projects/carbiocial/onsets-1981-2012/"
    }
    if len(sys.argv) > 1:
        for arg in sys.argv[1:]:
            k,v = arg.split("=")
            if k in config:
                config[k] = v 

    for row_no in xrange(int(config["start-row"]), int(config["end-row"]) + 1):
        path = config["input-path"] + "row-" + str(row_no)

        results = []
        results.append(["year", "onset", "column"])

        extract_col_no = lambda s: int(s.split("-")[1].split(".")[0])

        start_time = time.clock()
        #iii = 0

        #files <- list.files(path=path, full.names = T, recursive = T, pattern=glob2rx("*col*.csv*")) 
        for file_ in sorted(os.listdir(path), key=extract_col_no):
            
            if ".csv" not in file_:
                continue

            #iii += 1
            #if iii > 10:
            #    break

            col_no = extract_col_no(file_)

            filepath = path + "/" + file_
            df = pd.read_csv(filepath, sep=",")

            precip_mean = df["precip"].mean()
            daily_means_ = []
            for i in range(1, 12+1):
                for j in range(1, 31+1):
                    d = df[(df["month"] == i) & (df["day"] == j)]
                    daily_means_.append(d["precip"].mean())
             
            # drop na's created by months with no 30th and 31st day
            daily_means = pd.Series(daily_means_).dropna()

            # substract annual precipitation from daily precipitation 
            variance_from_daily_means = daily_means - precip_mean
            # build the cumulative sum of anomaly
            cumsum = variance_from_daily_means.cumsum()

            min_cumsum_index = cumsum.idxmin()
            #print "min_cumsum_index:", min_cumsum_index

            #calc onset in a three year range
            for year in xrange(int(config["from-year"])+1, int(config["to-year"])-1+1):
                # get a slice of the two adjacent years around year
                df1 = df[(df["year"] == year-1) | (df["year"] == year) | (df["year"] == year+1)]
                                
                dsiy1 = date(year-1, 12, 31).timetuple().tm_yday
                dsiy2 = date(year, 12, 31).timetuple().tm_yday
                dsiy3 = date(year+1, 12, 31).timetuple().tm_yday

                # create dateframe with added DOY and continuous index for the three years from df1
                df11 = df1.assign(
                    DOY = range(1, dsiy1+1) + range(1, dsiy2+1) + range(1, dsiy3+1),
                    idx = range(1, dsiy1+dsiy2+dsiy3+1))

                # get index where DOY was the doy of the min_cumsum_index in the the average year we calculated above for this column
                idx = df11[(df11["DOY"] == min_cumsum_index) & (df11["year"] == year)]["idx"].values[0]

                # get a sub-dataframe 100 days around min_cumsum_index (which was based on global average)
                df2 = df11[(df11["idx"] >= idx-50) & (df11["idx"] < idx+50)]

                # add anomaly and accum columns
                anomaly = df2["precip"] - precip_mean
                accum = anomaly.cumsum()
                min_accum_index = accum.idxmin()

                # get row a new min index
                min_row = df2.loc[min_accum_index]
                os_year = min_row["year"]
                onset = min_row["DOY"]
                
                #print "year:", os_year, "doy:", onset, "col:", col_no
                results.append([os_year, onset, col_no])

            #print col_no,

        with open(config["output-path"] + "onsets_row-" + str(row_no) + ".csv", "w") as _:
            w = csv.writer(_, delimiter=",")
            for line in results:
                w.writerow(line)

        end_time = time.clock()
        #print "runtime:", (end_time - start_time), "s for row:", row_no
    

main()
