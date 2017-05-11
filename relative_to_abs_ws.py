import os
import csv
from datetime import date, timedelta
import json
import copy

def generate_abs_rotation(rel_rotation, start_year, end_year, ref_dates, crops_data):
    "generate absolute dates rotation based on rain season onset"    

    def next_crop(rotation, index):
        if index == len(rotation) - 1:
            next_crp = rotation[0]
            next_index = 0
        else:
            next_crp = rotation[index + 1]
            next_index = index + 1
        return next_crp, next_index

    def create_cultivation_method(year, crop_info, crop_id, sowing_soy, early_harvest_soy):
        cultivation_method = {"worksteps": []}
        added_year = False
        for step in crop_info["worksteps"]:
            mystep = copy.deepcopy(step)
            for k,v in mystep.iteritems():
                if "0000-" in str(v):
                    mystep[k] = v.replace("0000", str(year))
                    if crop_id == "soybean" and mystep["type"] == "AutomaticSowing":
                        earliest_sowing = date(year, 1, 1) + timedelta(days=sowing_soy - 1)
                        mystep[k] = mystep[k].replace("09", str.zfill(str(earliest_sowing.month), 2))
                        mystep[k] = mystep[k].replace("15", str.zfill(str(earliest_sowing.day), 2))
                        mystep["latest-date"] = unicode((earliest_sowing + timedelta(days=30)).isoformat())
                if "0001-" in str(v):
                    if not added_year: year +=1
                    added_year = True
                    if crop_id == "soybean" and mystep["type"] == "AutomaticHarvest" and early_harvest_soy:
                        v = v.replace("03-01", "02-10")
                    mystep[k] = v.replace("0001", str(year))
                    
            cultivation_method["worksteps"].append(mystep)
        return year, cultivation_method

    current_year = start_year
    current_index = -1 #identifies current crop
    
    rotation_abs_dates = []

    early_harvest_soy = False
    if "cotton" in rel_rotation:
        early_harvest_soy = True
    
    while current_year <= end_year:

        crop_in_rotation, current_index = next_crop(rel_rotation, current_index)
        
        current_year, cultivation_method = create_cultivation_method(current_year, crops_data[crop_in_rotation], crop_in_rotation, ref_dates[current_year], early_harvest_soy)

        rotation_abs_dates.append(cultivation_method)
    
    #with open("test_abs_rot.json", "w") as _:
    #    _.write(json.dumps(rotation_abs_dates))

    return rotation_abs_dates

#rotation = ["soybean", "cotton"]
#ref_dates = {}
#doy = 244
#for i in range(1980, 2011):
#    ref_dates[i] = doy
#    doy += 1 

#generate_abs_rotation(rotation, 1980, 2010, ref_dates)
