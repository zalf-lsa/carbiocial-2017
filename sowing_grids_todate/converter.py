import numpy as np
import os
from datetime import date, timedelta


def walklevel(some_dir, level=1):
    some_dir = some_dir.rstrip(os.path.sep)
    assert os.path.isdir(some_dir)
    num_sep = some_dir.count(os.path.sep)
    for root, dirs, files in os.walk(some_dir):
        yield root, dirs, files
        num_sep_this = root.count(os.path.sep)
        if num_sep + level <= num_sep_this:
            del dirs[:]


input_dir = "Z:/projects/carbiocial/out_grids/future_wrf_2017-09-25/"
out_dir = "Z:/projects/carbiocial/out_grids/future_wrf_2017-09-25/sowing_dates/"

#a = np.array([[1,2,3], [4,5,6]])
#t = np.zeros((a.shape[0], a.shape[1]))
#t = np.empty_like(a, dtype=str)
#t = np.empty((a.shape[0], a.shape[1]), dtype=object)
#t.fill("-9999")

#avoid repeating what has already been done
dont_write = []
for filename in os.listdir(out_dir):
    dont_write.append(out_dir + filename)

for root, dirs, filenames in walklevel(input_dir, level=0):
    for filename in filenames:
        if "sowing" in filename:
            s_doy_file = input_dir + filename
            s_year_file = s_doy_file.replace("sowing", "s-year")
            out_file = out_dir + filename.replace("sowing", "s-date")

            if out_file in dont_write:
                continue

            s_doys  = np.loadtxt(s_doy_file, skiprows=6)
            print(s_doy_file + " loaded!")
            s_years  = np.loadtxt(s_year_file, skiprows=6)
            print(s_year_file + " loaded!")

            #s_dates = np.empty((s_doys.shape[0], s_doys.shape[1]), dtype=object)
            #s_dates = np.empty((s_doys.shape[0]), dtype=object)
            s_dates = []
            #s_dates.fill("-9999")

            for i in range(len(s_doys)):
                row = []
                for j in range(len(s_doys[i])):
                    sowing_doy = int(s_doys[i][j])
                    sowing_year = int(s_years[i][j])

                    if sowing_doy == -9999:
                        row.append("-9999")
                        continue
                    
                    sowing_date = date(sowing_year, 1, 1) + timedelta(days=sowing_doy - 1)
                    #s_dates[i][j] = sowing_date.isoformat()
                    row.append(sowing_date.isoformat())

                s_dates.append(" ".join(row))
            
            

            with open(out_file, "w") as f:
                header = "ncols     1928\n"
                header += "nrows    2544\n"
                header += "xllcorner -9345.000000\n"
                header += "yllcorner 8000665.000000\n"
                header += "cellsize 900\n"
                header += "NODATA_value -9999\n"
                f.write(header)

                for row in s_dates:
                    f.write(row + "\n")

            print(out_file + "written!")

print("I'm done")


        
