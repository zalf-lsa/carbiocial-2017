#!/usr/bin/python
# -*- coding: UTF-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */

# Authors:
# Michael Berg-Mohnicke <michael.berg@zalf.de>
# Tommaso Stella <tommaso.stella@zalf.de>
#
# Maintainers:
# Currently maintained by the authors.
#
# This file has been created at the Institute of
# Landscape Systems Analysis at the ZALF.
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF)

import sys

#import ascii_io
#import json
import csv
import types
import os
from datetime import datetime
from collections import defaultdict

import zmq
#print zmq.pyzmq_version()
import monica_io
import re
import numpy as np
#from dateutil.parser import parse

import total_size as ts

USER = "berg-xps15"

PATHS = {
    "stella": {
        "INCLUDE_FILE_BASE_PATH": "C:/Users/stella/Documents/GitHub",
        "LOCAL_PATH_TO_ARCHIV": "Z:/projects/carbiocial/",
        "LOCAL_PATH_TO_REPO": "C:/Users/stella/Documents/GitHub/carbiocial-2017/"
    }
    ,
    "berg-xps15": {
        "INCLUDE_FILE_BASE_PATH": "C:/Users/berg.ZALF-AD/GitHub",
        "LOCAL_PATH_TO_ARCHIV": "P:/carbiocial/",
        "LOCAL_PATH_TO_REPO": "C:/Users/berg.ZALF-AD/GitHub/carbiocial-2017/"
    }
}

def create_output(result):
    "create output structure for single run"

    year_to_crop_to_vals = defaultdict(lambda: defaultdict(dict))
    if len(result.get("data", [])) > 0 and len(result["data"][0].get("results", [])) > 0:

        for data in result.get("data", []):
            results = data.get("results", [])
            oids = data.get("outputIds", [])

            #skip empty results, e.g. when event condition haven't been met
            if len(results) == 0:
                continue

            assert len(oids) == len(results)
            for kkk in range(0, len(results[0])):
                vals = {}

                for iii in range(0, len(oids)):
                    oid = oids[iii]
                    val = results[iii][kkk]

                    name = oid["name"] if len(oid["displayName"]) == 0 else oid["displayName"]

                    if isinstance(val, types.ListType):
                        for val_ in val:
                            vals[name] = val_
                    else:
                        vals[name] = val

                if "Year" not in vals or "Crop" not in vals:
                    print "Missing Year or Crop in result section. Skipping results section."
                    continue

                year_to_crop_to_vals[vals["Year"]][vals["Crop"]].update(vals)

    return year_to_crop_to_vals

def create_template_grid(path_to_file, n_rows, n_cols):
    "0=no data, 1=data"

    with open(path_to_file) as file_:
        for header in range(0, 6):
            file_.next()

        out = np.full((n_rows, n_cols), 0, dtype=np.int8)

        row = 0
        for line in file_:
            col = 0
            for val in line.split(" "):
                out[row, col] = 0 if int(val) == -9999 else 1
                col += 1
            row += 1

        return out


HEADER = """ncols         1928
nrows         2545
xllcorner     -9345.000000
yllcorner     8000665.000000
cellsize      900
NODATA_value  -9999
"""

#@profile
def write_row_to_grids(all_data, template_grid, row, rotation, period, insert_nodata_rows_count):
    "write grids row by row"

    row_template = template_grid[row]
    rows, cols = template_grid.shape

    make_dict_dict_nparr = lambda: defaultdict(lambda: defaultdict(lambda: np.full((cols,), -9999, dtype=np.float)))

    output_grids = {
        "sowing": make_dict_dict_nparr(),
        "harvest": make_dict_dict_nparr(),
        "Yield": make_dict_dict_nparr(),
        "Nstressavg": make_dict_dict_nparr(),
        "TraDefavg": make_dict_dict_nparr(),
        "anthesis": make_dict_dict_nparr(),
        "matur": make_dict_dict_nparr(),
        "Nstress1": make_dict_dict_nparr(),
        "TraDef1": make_dict_dict_nparr(),
        "Nstress2": make_dict_dict_nparr(),
        "TraDef2": make_dict_dict_nparr(),
        "Nstress3": make_dict_dict_nparr(),
        "TraDef3": make_dict_dict_nparr(),
        "Nstress4": make_dict_dict_nparr(),
        "TraDef4": make_dict_dict_nparr(),
        "Nstress5": make_dict_dict_nparr(),
        "TraDef5": make_dict_dict_nparr(),
        "Nstress6": make_dict_dict_nparr(),
        "TraDef6": make_dict_dict_nparr()
    }

    for col in xrange(0, cols):
        if row_template[col] == 1:
            for year, crop_to_data in all_data[row][col].iteritems():
                for crop, data in crop_to_data.iteritems():
                    for key, val in output_grids.iteritems():
                        val[year][crop][col] = data.get(key, -9999)

    for key, y2c2d in output_grids.iteritems():
        
        for year, c2d in y2c2d.iteritems():

            for crop, row_arr in c2d.iteritems():
            
                crop = crop.replace("/", "").replace(" ", "")
                path_to_file = "out/" + period + "/" + crop + "_in_" + rotation + "_" + key + "_" + str(year) + ".asc"

                if not os.path.isfile(path_to_file):
                    with open(path_to_file, "w") as _:
                        _.write(HEADER)

                with open(path_to_file, "a") as _:
                    if insert_nodata_rows_count > 0:
                        for i in xrange(0, insert_nodata_rows_count):
                            rowstr = " ".join(map(lambda x: "-9999", row_template))
                            _.write(rowstr +  "\n")

                    rowstr = " ".join(map(lambda x: "-9999" if int(x) == -9999 else str(x), row_arr))
                    _.write(rowstr +  "\n")
    
    #all_data[row].clear()
    #del all_data[row]


def main():
    "collect data from workers"

    config = {
        "port": 7777
    }
    if len(sys.argv) > 1:
        for arg in sys.argv[1:]:
            k,v = arg.split("=")
            if k in config:
                config[k] = int(v) 

    local_run = False
    i = 0
    context = zmq.Context()
    socket = context.socket(zmq.PULL)
    if local_run:
        socket.connect("tcp://localhost:" + str(config["port"]))
    else:
        socket.connect("tcp://cluster2:" + str(config["port"]))    
    socket.RCVTIMEO = 1000
    leave = False
    
    n_rows = 2545
    n_cols = 1928
        
    print("loading template for output...")
    template_grid = create_template_grid(PATHS[USER]["LOCAL_PATH_TO_ARCHIV"] + "Soil/Carbiocial_Soil_Raster_final.asc", n_rows, n_cols)
    datacells_per_row = np.sum(template_grid, axis=1) #.tolist()
    print("load complete")

    period_to_rotation_to_row_to_col_to_data = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
    period_to_rotation_to_row_to_datacell_count = defaultdict(lambda: defaultdict(lambda: datacells_per_row.copy()))
    period_to_rotation_to_next_row = defaultdict(lambda: defaultdict(lambda: 0))
    period_to_rotation_to_insert_nodata_rows_count = defaultdict(lambda: defaultdict(lambda: 0))

    while not leave:
        try:
            result = socket.recv_json(encoding="latin-1")
        except:
            continue

        if result["type"] == "finish":
            print "received finish message"
            leave = True

        else:
            custom_id = result["customId"]
            ci_parts = custom_id.split("|")
            period = ci_parts[0]
            row = int(ci_parts[1])
            col = int(ci_parts[2])
            rotation = ci_parts[3]

            print "received work result", i, "customId:", result.get("customId", ""), "size:",len(period_to_rotation_to_row_to_col_to_data[period][rotation])

            period_to_rotation_to_row_to_col_to_data[period][rotation][row][col] = create_output(result)
            period_to_rotation_to_row_to_datacell_count[period][rotation][row] -= 1

            for per, rotation_to_row_to_datacell_count in period_to_rotation_to_row_to_datacell_count.iteritems():
                for rot, row_to_datacell_count in rotation_to_row_to_datacell_count.iteritems():
                    next_row = period_to_rotation_to_next_row[per][rot]
                    while row_to_datacell_count[next_row] == 0:
                        # if rows have been initially completely nodata, remember to write these rows before the next row with some data
                        if datacells_per_row[next_row] == 0:
                            period_to_rotation_to_insert_nodata_rows_count[per][rot] += 1
                        else:
                            print "writing ",per,rot,"row:", next_row, "-----------------------------------"
                            print "before size:",len(period_to_rotation_to_row_to_col_to_data[per][rot])
                            write_row_to_grids(period_to_rotation_to_row_to_col_to_data[per][rot], template_grid, next_row, rotation, period, period_to_rotation_to_insert_nodata_rows_count[per][rot])
                            x = period_to_rotation_to_row_to_col_to_data[per][rot]
                            del x[row]
                            print "after size:",len(period_to_rotation_to_row_to_col_to_data[per][rot])
                            period_to_rotation_to_insert_nodata_rows_count[per][rot] = 0 # should have written the nodata rows for this period and rotation
                        
                        period_to_rotation_to_next_row[per][rot] += 1 # move to next row (to be written)

            i = i + 1

            #if i > 5000:
            #    return


main()


