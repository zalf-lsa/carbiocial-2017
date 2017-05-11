import os
import csv
from datetime import date, timedelta
import json
import copy

def next_crop(rotation, index):
        if index == len(rotation) - 1:
            next_crp = rotation[0]
            next_index = 0
        else:
            next_crp = rotation[index + 1]
            next_index = index + 1
        return next_crp, next_index

def generate_template_abs(rel_rotation, start_year, end_year, crops_data):
    ""

    def create_cultivation_method(year, crop_info, crop_id):
        cultivation_method = {"worksteps": []}
        added_year = False
        for step in crop_info["worksteps"]:
            mystep = copy.deepcopy(step)
            for k,v in mystep.iteritems():
                if "0001-" in str(v):
                    if not added_year: year +=1
                    added_year = True
                    break
            cultivation_method["worksteps"].append(mystep)
        return year, cultivation_method

    current_year = start_year
    current_index = -1 #identifies current crop

    template_abs_dates = []   

    while current_year <= end_year:

        crop_in_rotation, current_index = next_crop(rel_rotation, current_index)

        current_year, cultivation_method = create_cultivation_method(current_year, crops_data[crop_in_rotation], crop_in_rotation)

        template_abs_dates.append(cultivation_method)

    return template_abs_dates

def rel_to_abs_dates(rot, template_abs_rot, start_year, end_year, ref_dates_sowing):
    
    early_harvest_soy = False
    if "cotton" in rot:
        early_harvest_soy = True

    year = start_year

    current_index = -1 #identifies current crop

    for cm in range(len(template_abs_rot)):
        added_year = False
        crop_in_rotation, current_index = next_crop(rot, current_index)
        for step in template_abs_rot[cm]["worksteps"]:
            for k,v in step.iteritems():
                if "0000-" in str(v) and "_relt" not in str(k):
                    rel_template = k + "_relt"
                    template = step[rel_template]
                    template = template.replace("0000", str(year))
                    if crop_in_rotation == "soybean" and step["type"] == "AutomaticSowing":
                        earliest_sowing = date(year, 1, 1) + timedelta(days=ref_dates_sowing[year] - 1)
                        template = template.replace("09", str.zfill(str(earliest_sowing.month), 2))
                        template = template.replace("15", str.zfill(str(earliest_sowing.day), 2))
                        step["latest-date"] = unicode((earliest_sowing + timedelta(days=30)).isoformat())
                    step[k] = template
                if "0001-" in str(v) and "_relt" not in str(k):
                    if not added_year:
                        year += 1
                        added_year = True
                    rel_template = k + "_relt"
                    template = step[rel_template]
                    if crop_in_rotation == "soybean" and step["type"] == "AutomaticHarvest" and early_harvest_soy:
                        template = template.replace("03-01", "02-10")
                    template = template.replace("0001", str(year))
                    step[k] = template
    
    return template_abs_rot

