import os


def rename_files(row):

    folder = "M:/projects/carbiocial/climate-data-years-2001-2040-rows-0-2544/row-"+str(row)+"/"
    for filename in sorted(os.listdir(folder)):
        if filename[-4:] == ".csv":
            os.rename(folder + filename, folder + filename + "_old")
            print "renamed", (folder+filename), "to", (folder+filename + "_old")
        if filename[-8:] == ".csv_fix":
            os.rename(folder + filename, folder + filename[:-4])
            print "renamed", (folder+filename), "to", (folder+filename[:-4])



def write_fixed_files(row):

    folder = "M:/projects/carbiocial/climate-data-years-2001-2040-rows-0-2544/row-"+str(row)+"/"
    for filename in sorted(os.listdir(folder)):

        if filename[-4:] != ".csv":
            continue

        new_lines = []
        write_new_file = False
        with open(folder + filename) as _:

            prev_sline = []
            for line in _:

                sline = line.split(",")
                if sline[0] == "day":
                    new_lines.append(line)
                    continue

                fix = False
                for i, item in enumerate(sline):
                    if item == "":
                        fix = True
                        write_new_file = True
                        if prev_sline:
                            sline[i] = prev_sline[i]
                        else:
                            sline[i] = str(0)
                    
                        print filename, "--> +", prev_sline[i], "-->", sline

                new_lines.append(",".join(sline) if fix else line)
                prev_sline = sline

        if write_new_file:
            os.rename(folder + filename, folder + filename + "_old")

            with open(folder + filename, "w") as _:
                for nline in new_lines:
                    _.write(nline)

for r in [154, 155, 156, 157]:
    write_fixed_files(r)
#write_fixed_files()

def count_data_nodata():

    new_f = open("G:/carbiocial-2017-out/future_wrf_/cottonbrmid_in_soybean_7_cotton_anthesis_2.asc")
    old_f = open("P:/carbiocial/out_grids/historical/cottonbrmid_in_soybean_7_cotton_anthesis_2.asc")
    soil_f = open("P:/carbiocial/Soil/Carbiocial_Soil_Raster_final.asc")

    def count_nodata(items):
        count = 0
        for item in items:
            if item != "-9999":
                break
            count += 1
        return count

    def count_all_data(items):
        count = 0
        for item in items:
            if item != "-9999":
                count += 1

        return count


    line_no = 0
    for nline in new_f:

        line_no += 1

        snl = nline.strip().split(" ")  
        snlc = count_nodata(snl)
        snldc = count_all_data(snl)

        oline = old_f.readline()
        sol = oline.strip().split(" ")
        solc = count_nodata(sol)
        soldc = count_all_data(sol)

        sline = soil_f.readline()
        ssl = sline.strip().split(" ")
        sslc = count_nodata(ssl)
        ssldc = count_all_data(ssl)

        if snlc != solc:
            print "snlc:", snlc, "solc:", solc, "sslc:", sslc, "snldc:", snldc, "soldc:", soldc, "ssldc:", ssldc, "row:", (line_no-6), "line:", line_no

    new_f.close()
    old_f.close()

#count_data_nodata()

def check_debug_out():
    with open("debug.out") as _:

        all_rows = set()
        for line in _:

            sline = line.split(" ")
            rot = sline[5].split("|")[3]
            col, row = sline[12].split("@")

            if col == "1" and rot == "soybean_8_maize":
                all_rows.add(int(row))
                print row,

        prev_row = -1
        for row in sorted(all_rows):
            if prev_row + 1 != row:
                print "problem: prev_row:", prev_row, "row:", row
            prev_row = row


