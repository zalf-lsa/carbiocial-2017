import numpy as np
import os
from datetime import date, timedelta

input_dir = "Z:/projects/carbiocial/out_grids/future_starr_2017-09-06/"
out_dir = "Z:/projects/carbiocial/out_grids/future_starr_2017-09-06/sowing_dates_test/"

#a = np.array([[1,2,3], [4,5,6]])
#t = np.zeros((a.shape[0], a.shape[1]))
#t = np.empty_like(a, dtype=str)
#t = np.empty((a.shape[0], a.shape[1]), dtype=object)
#t.fill("-9999")

#print np.__version__

for filename in os.listdir(input_dir):
    if "sowing" in filename:
        s_doy_file = input_dir + filename
        s_year_file = s_doy_file.replace("sowing", "s-year")

        s_doys  = np.loadtxt(s_doy_file, skiprows=6)
        print(s_doy_file + " loaded!")
        s_years  = np.loadtxt(s_year_file, skiprows=6)
        print(s_year_file + " loaded!")

        s_dates = np.empty((s_doys.shape[0], s_doys.shape[1]), dtype=object)
        s_dates.fill("-9999")

        for i in range(len(s_doys)):
            for j in range(len(s_doys[i])):
                sowing_doy = int(s_doys[i][j])
                sowing_year = int(s_years[i][j])

                if sowing_doy == -9999:
                    continue
                
                sowing_date = date(sowing_year, 1, 1) + timedelta(days=sowing_doy - 1)
                s_dates[i][j] = sowing_date.isoformat()

        out_file = out_dir + filename.replace("sowing", "s-date")

        with open(out_file, "w") as f:
            header = "ncols     1928\n"
            header += "nrows    2544\n"
            header += "xllcorner -9345.000000\n"
            header += "yllcorner 8000665.000000\n"
            header += "cellsize 900\n"
            header += "NODATA_value -9999\n"
            f.write(header)

            for i in range(len(s_dates)):
                print s_dates[i]
                #f.write(s_dates[i] + "\n")

        print(out_file + "written!")

print("I'm done")


        
