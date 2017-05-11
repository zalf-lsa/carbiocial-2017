#!/usr/bin/python
# -*- coding: UTF-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */

# Authors:
# Michael Berg-Mohnicke <michael.berg@zalf.de>
#
# Maintainers:
# Currently maintained by the authors.
#
# This file has been created at the Institute of
# Landscape Systems Analysis at the ZALF.
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF)

import csv
import json
import os
import sqlite3
import sys
import time
import zmq
import monica_io
import soil_io
import ascii_io
from datetime import date, timedelta
import copy
from relative_to_abs_ws import generate_abs_rotation
import numpy as np


USER = "stella"

PATHS = {
    "stella": {
        "INCLUDE_FILE_BASE_PATH": "C:/Users/stella/Documents/GitHub",
        "LOCAL_PATH_TO_ARCHIV": "Z:/projects/carbiocial/",
        "LOCAL_PATH_TO_REPO": "C:/Users/stella/Documents/GitHub/carbiocial-2017/"
    }
}

PATH_TO_ARCHIV_DIR = "/archiv-daten/md/projects/carbiocial/"
#PATH_TO_ARCHIV_DIR = "Z:/projects/carbiocial/"
start_send = time.clock()

def main():
    "main function"

    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    port = 6666 if len(sys.argv) == 1 else sys.argv[1]
    socket.connect("tcp://cluster2:" + str(port))
    #socket.connect("tcp://localhost6666:" + str(port))

    soil_db_con = sqlite3.connect(PATHS[USER]["LOCAL_PATH_TO_REPO"] + "soil-carbiocial.sqlite")

    with open("sim.json") as _:
        sim = json.load(_)

    with open("site.json") as _:
        site = json.load(_)

    with open("crop.json") as _:
        crop = json.load(_)

    with open("all_crops.json") as _:
        all_crops = json.load(_)

    sim["include-file-base-path"] = PATHS[USER]["INCLUDE_FILE_BASE_PATH"]

    periods = [
        {
            "name": "historical",
            "start_year": 1981,
            "end_year": 1984, #TODO 2012
            "climate_folder": "climate-data-years-1981-2012-rows-0-2544"
        },
        {
            "name": "future_wrf",
            "start_year": 2013,
            "end_year": 2040,
            "climate_folder": "climate-data-years-2001-2040-rows-0-2544"
        },
        {
            "name": "future_starr",
            "start_year": 2013,
            "end_year": 2040,
            "climate_folder": "climate-data-years-2013-2040-rows-0-2544"
        }
    ]

    rotations = [
        ("soybean", "cotton"),
        ("soybean", "maize")
    ]

    n_rows = 2545
    n_cols = 1928

    def ascii_grid_to_np2darray(path_to_file):
        "0=row, 1=col"
        with open(path_to_file) as file_:
            for header in range(0, 6):
                file_.next()
            out = np.empty((n_rows, n_cols), np.dtype(int))
            r = 0
            for line in file_:
                c = 0
                for val in line.split(' '):
                    out[r, c] = int(val)
                    c = c + 1
                r = r + 1
            return out
    
    def grids_to_3darrays(path_to_grids):
        "key=year, 0=row, 1=col"
        out = {}
        for filename in os.listdir(path_to_grids):
            year = int(filename.split(".")[0])
            out[year] = ascii_grid_to_np2darray(path_to_grids + "/" + filename)
            print "created", year
        return out
    

    profile_cache = {}
    latitude_cache = {}
    def update_soil(row, col):
        "in place update the env"

        def lat(con, profile_id):
            query = """
                select                
                lat_times_1000
                from soil_profile_data 
                where id = ? 
                order by id
            """
            con.row_factory = sqlite3.Row
            for row in con.cursor().execute(query, (profile_id,)):
                if row["lat_times_1000"]:
                    return float(row["lat_times_1000"])

        profile_id = soil_ids[row, col]
        if profile_id in profile_cache:
            profile = profile_cache[profile_id]
            latitude = latitude_cache[profile_id]
        else:
            profile = soil_io.soil_parameters(soil_db_con, profile_id)
            profile_cache[profile_id] = profile
            latitude = lat(soil_db_con, profile_id)
            latitude_cache[profile_id] = latitude
        
        for env in envs.itervalues():
            env["params"]["siteParameters"]["Latitude"] = latitude
            #site["HeightNN"] = #TODO
            env["params"]["siteParameters"]["SoilProfileParameters"] = profile

    soil_ids = ascii_grid_to_np2darray(PATHS[USER]["LOCAL_PATH_TO_ARCHIV"] + "Soil/Carbiocial_Soil_Raster_final.asc")

    i= 0
    
    #create an env for each rotation (templates, customized within the loop)
    crops_data = {}
    envs = {}
    for rot in rotations:
        my_rot = []
        for cp in rot:
            my_rot.append(all_crops[cp])
        crop["cropRotation"] = my_rot
        envs[rot] = monica_io.create_env_json_from_json_config({
                            "crop": crop,
                            "site": site,
                            "sim": sim,
                            "climate": ""
                        })

        for i in range(len(rot)):
            cp = rot[i]
            if cp not in crops_data:
                crops_data[cp] = envs[rot]["cropRotation"][i]
        
    #written = False
    for p in periods:
        rain_onset = grids_to_3darrays(PATHS[USER]["LOCAL_PATH_TO_ARCHIV"] + "rain_onset_grids/" + p["name"])
        for row in range(n_rows):            
            for col in range(n_cols):
                if soil_ids[row, col] == -9999:
                    continue
                #update soil data
                update_soil(row, col)
                
                #produce rotation
                ref_dates = {}
                for year in range(p["start_year"], p["end_year"] + 1):
                    ref_dates[year] = rain_onset[year][row, col]
                for rot in rotations:
                    env = envs[rot]                    
                    env["cropRotation"] = generate_abs_rotation(rot, p["start_year"], p["end_year"], ref_dates, crops_data)                    

                    #set climate file - read by the server
                    env["csvViaHeaderOptions"] = sim["climate.csv-options"]
                    env["csvViaHeaderOptions"]["start-date"] = sim["start-date"]
                    env["csvViaHeaderOptions"]["end-date"] = sim["end-date"]
                    env["pathToClimateCSV"] = PATH_TO_ARCHIV_DIR + "row-" + str(row) + "/col-" + str(col) + ".csv"

                    #if not written:
                    #    with open("test_env_new.json", "w") as _:
                    #        _.write(json.dumps(env))
                    #    written = True
                    
                    rot_id = rot[0] + "_" + rot[1]
                    env["customId"] = p["name"] \
                                        + "|" + str(row) \
                                        + "|" + str(col) \
                                        + "|" + rot_id

                    #socket.send_json(env) 
                    print "sent env ", i, " customId: ", env["customId"]
                    i += 1

    stop_send = time.clock()

    print "sending ", i, " envs took ", (stop_send - start_send), " seconds"

main()
