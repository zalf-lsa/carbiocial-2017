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

USER = "stella"

PATHS = {
    "hampf": {
        "INCLUDE_FILE_BASE_PATH": "C:/GitHub",
        "LOCAL_PATH_TO_ARCHIV": "Z:/md/projects/carbiocial/",
        "LOCAL_PATH_TO_REPO": "C:/GitHub/carbiocial-2017/",
        "LOCAL_PATH_TO_OUTPUT_DIR": "out/"
    },

    "stella": {
        "INCLUDE_FILE_BASE_PATH": "C:/Users/stella/Documents/GitHub",
        "LOCAL_PATH_TO_ARCHIV": "Z:/projects/carbiocial/",
        "LOCAL_PATH_TO_REPO": "C:/Users/stella/Documents/GitHub/carbiocial-2017/",
        "LOCAL_PATH_TO_OUTPUT_DIR": "out/"
    },

    "berg-xps15": {
        "INCLUDE_FILE_BASE_PATH": "C:/Users/berg.ZALF-AD/GitHub",
        "LOCAL_PATH_TO_ARCHIV": "P:/carbiocial/",
        "LOCAL_PATH_TO_REPO": "C:/Users/berg.ZALF-AD/GitHub/carbiocial-2017/",
        "LOCAL_PATH_TO_OUTPUT_DIR": "out/"
    },
    "berg-lc": {
        "INCLUDE_FILE_BASE_PATH": "C:/Users/berg.ZALF-AD.000/Documents/GitHub",
        "LOCAL_PATH_TO_ARCHIV": "P:/carbiocial/",
        "LOCAL_PATH_TO_REPO": "C:/Users/berg.ZALF-AD.000/Documents/GitHub/carbiocial-2017/",
        "LOCAL_PATH_TO_OUTPUT_DIR": "D:/carbiocial-2017-out/"
    }
}

def create_output(result):
    "create output structure for single run"

    cm_count_to_crop_to_vals = defaultdict(lambda: defaultdict(dict))
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

                if "CM-count" not in vals or "Crop" not in vals:
                    print "Missing CM-count or Crop in result section. Skipping results section."
                    continue

                cm_count_to_crop_to_vals[vals["CM-count"]][vals["Crop"]].update(vals)

    return cm_count_to_crop_to_vals

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
nrows         2544
xllcorner     -9345.000000
yllcorner     8000665.000000
cellsize      900
NODATA_value  -9999
"""

def write_row_to_grids(row_col_data, row, insert_nodata_rows_count, template_grid, rotation, period):
    "write grids row by row"

    row_template = template_grid[row]
    rows, cols = template_grid.shape

    make_dict_dict_nparr = lambda: defaultdict(lambda: defaultdict(lambda: np.full((cols,), -9999, dtype=np.float)))

    output_grids = {
        "sowing": {"data" : make_dict_dict_nparr(), "cast-to": "int", "digits": 0},
        "harvest": {"data" : make_dict_dict_nparr(), "cast-to": "int", "digits": 0},
        "Year": {"data" : make_dict_dict_nparr(), "cast-to": "int", "digits": 0},
        "Yield": {"data" : make_dict_dict_nparr(), "cast-to": "float", "digits": 2},
        "Nstressavg": {"data" : make_dict_dict_nparr(), "cast-to": "float", "digits": 4},
        "TraDefavg": {"data" : make_dict_dict_nparr(), "cast-to": "float", "digits": 4},
        "anthesis": {"data" : make_dict_dict_nparr(), "cast-to": "int", "digits": 0},
        "matur": {"data" : make_dict_dict_nparr(), "cast-to": "int", "digits": 0},
        "Nstress1": {"data" : make_dict_dict_nparr(), "cast-to": "float", "digits": 4},
        "TraDef1": {"data" : make_dict_dict_nparr(), "cast-to": "float", "digits": 4},
        "Nstress2": {"data" : make_dict_dict_nparr(), "cast-to": "float", "digits": 4},
        "TraDef2": {"data" : make_dict_dict_nparr(), "cast-to": "float", "digits": 4},
        "Nstress3": {"data" : make_dict_dict_nparr(), "cast-to": "float", "digits": 4},
        "TraDef3": {"data" : make_dict_dict_nparr(), "cast-to": "float", "digits": 4},
        "Nstress4": {"data" : make_dict_dict_nparr(), "cast-to": "float", "digits": 4},
        "TraDef4": {"data" : make_dict_dict_nparr(), "cast-to": "float", "digits": 4},
        "Nstress5": {"data" : make_dict_dict_nparr(), "cast-to": "float", "digits": 4},
        "TraDef5": {"data" : make_dict_dict_nparr(), "cast-to": "float", "digits": 4},
        "Nstress6": {"data" : make_dict_dict_nparr(), "cast-to": "float", "digits": 4},
        "TraDef6": {"data" : make_dict_dict_nparr(), "cast-to": "float", "digits": 4},
        "NFert": {"data" : make_dict_dict_nparr(), "cast-to": "float", "digits": 4},
        "NLeach": {"data" : make_dict_dict_nparr(), "cast-to": "float", "digits": 4},
        "PercolationRate": {"data" : make_dict_dict_nparr(), "cast-to": "float", "digits": 4},
        "Nmin": {"data" : make_dict_dict_nparr(), "cast-to": "float", "digits": 4}
    }

    for col in xrange(0, cols):
        if row_template[col] == 1:
            for cm_count, crop_to_data in row_col_data[row][col].iteritems():
                for crop, data in crop_to_data.iteritems():
                    for key, val in output_grids.iteritems():
                        val["data"][cm_count][crop][col] = data.get(key, -9999)

    for key, y2c2d_ in output_grids.iteritems():
        
        y2c2d = y2c2d_["data"]
        cast_to = y2c2d_["cast-to"]
        digits = y2c2d_["digits"]
        if cast_to == "int":
            mold = lambda x: str(int(x))
        else:
            mold = lambda x: str(round(x, digits))

        for cm_count, c2d in y2c2d.iteritems():

            for crop, row_arr in c2d.iteritems():
            
                crop = crop.replace("/", "").replace(" ", "")
                path_to_file = PATHS[USER]["LOCAL_PATH_TO_OUTPUT_DIR"] + period + "/" + crop + "_in_" + rotation + "_" + key + "_" + str(cm_count) + ".asc"

                if not os.path.isfile(path_to_file):
                    with open(path_to_file, "w") as _:
                        _.write(HEADER)

                with open(path_to_file, "a") as _:
                    if insert_nodata_rows_count > 0:
                        for i in xrange(0, insert_nodata_rows_count):
                            rowstr = " ".join(map(lambda x: "-9999", row_template))
                            _.write(rowstr +  "\n")

                    rowstr = " ".join(map(lambda x: "-9999" if int(x) == -9999 else mold(x), row_arr))
                    _.write(rowstr +  "\n")
    
    del row_col_data[row]


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
    write_normal_output_files = False
    
    i = 0
    context = zmq.Context()
    socket = context.socket(zmq.PULL)
    if local_run:
        socket.connect("tcp://localhost:" + str(config["port"]))
    else:
        socket.connect("tcp://cluster2:" + str(config["port"]))    
    socket.RCVTIMEO = 1000
    leave = False
    
    n_rows = 2544
    n_cols = 1928
        
    print("loading template for output...")
    template_grid = create_template_grid(PATHS[USER]["LOCAL_PATH_TO_ARCHIV"] + "Soil/Carbiocial_Soil_Raster_final.asc", n_rows, n_cols)
    datacells_per_row = np.sum(template_grid, axis=1) #.tolist()
    print("load complete")

    period_to_rotation_to_data = defaultdict(lambda: defaultdict(lambda: {
        "row-col-data": defaultdict(dict),
        "datacell-count": datacells_per_row.copy(), 
        "next-row": 0,
        "insert-nodata-rows-count": 0,
        "cached-rows-count": 0
    }))

    cached_rows_threshold = 1 #10
    while not leave:
        try:
            result = socket.recv_json(encoding="latin-1")
        except:
            continue

        if result["type"] == "finish":
            print "received finish message"
            leave = True

        elif not write_normal_output_files:
            custom_id = result["customId"]
            ci_parts = custom_id.split("|")
            period = ci_parts[0]
            row = int(ci_parts[1])
            col = int(ci_parts[2])
            rotation = ci_parts[3]

            data = period_to_rotation_to_data[period][rotation]
            print "received work result", i, "customId:", result.get("customId", ""), "size:",len(data["row-col-data"])

            data["row-col-data"][row][col] = create_output(result)
            data["datacell-count"][row] -= 1

            while data["datacell-count"][data["next-row"]] == 0:
                # if rows have been initially completely nodata, remember to write these rows before the next row with some data
                if datacells_per_row[data["next-row"]] == 0:
                    data["insert-nodata-rows-count"] += 1
                else:
                    data["cached-rows-count"] += 1
                    if data["cached-rows-count"] >= cached_rows_threshold or data["next-row"] == (n_rows-1):
                        for row_no in range(data["next-row"]-data["cached-rows-count"]+1, data["next-row"]+1):
                            write_row_to_grids(data["row-col-data"], row_no, data["insert-nodata-rows-count"], template_grid, rotation, period)
                            data["insert-nodata-rows-count"] = 0 # should have written the nodata rows for this period and 
                        data["cached-rows-count"] = 0
                
                data["next-row"] += 1 # move to next row (to be written)

            i = i + 1

            #if i > 5000:
            #    return
        
        elif write_normal_output_files:
            print "received work result ", i, " customId: ", result.get("customId", "")

            custom_id = result["customId"]
            ci_parts = custom_id.split("|")
            period = ci_parts[0]
            row = int(ci_parts[1])
            col = int(ci_parts[2])
            rotation = ci_parts[3]
            file_name = str(row) + "_" + str(col) + "_" + rotation + "_" + period
            

            #with open("out/out-" + str(i) + ".csv", 'wb') as _:
            with open("out/out-" + file_name + ".csv", 'wb') as _:
                writer = csv.writer(_, delimiter=",")

                for data_ in result.get("data", []):
                    results = data_.get("results", [])
                    orig_spec = data_.get("origSpec", "")
                    output_ids = data_.get("outputIds", [])

                    if len(results) > 0:
                        writer.writerow([orig_spec.replace("\"", "")])
                        for row in monica_io.write_output_header_rows(output_ids,
                                                                      include_header_row=True,
                                                                      include_units_row=True,
                                                                      include_time_agg=False):
                            writer.writerow(row)

                        for row in monica_io.write_output(output_ids, results):
                            writer.writerow(row)

                    writer.writerow([])

            i = i + 1


main()


