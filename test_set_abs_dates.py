import json
import monica_io
from absolute_rot_generator import generate_template_abs, set_abs_dates

PATHS = {
    "hampf": {
        "INCLUDE_FILE_BASE_PATH": "C:/GitHub",
        "LOCAL_PATH_TO_ARCHIV": "Z:/md/projects/carbiocial/",
        "LOCAL_PATH_TO_REPO": "C:/GitHub/carbiocial-2017/"
    },
    "stella": {
        "INCLUDE_FILE_BASE_PATH": "C:/Users/stella/Documents/GitHub",
        "LOCAL_PATH_TO_ARCHIV": "Z:/projects/carbiocial/",
        "LOCAL_PATH_TO_REPO": "C:/Users/stella/Documents/GitHub/carbiocial-2017/"
    },
    "berg-xps15": {
        "INCLUDE_FILE_BASE_PATH": "C:/Users/berg.ZALF-AD/GitHub",
        "LOCAL_PATH_TO_ARCHIV": "P:/carbiocial/",
        "LOCAL_PATH_TO_REPO": "C:/Users/berg.ZALF-AD/GitHub/carbiocial-2017/"
    },
    "berg-lc": {
        "INCLUDE_FILE_BASE_PATH": "C:/Users/berg.ZALF-AD.000/Documents/GitHub",
        "LOCAL_PATH_TO_ARCHIV": "P:/carbiocial/",
        "LOCAL_PATH_TO_REPO": "C:/Users/berg.ZALF-AD.000/Documents/GitHub/carbiocial-2017/"
    }
}

USER = "stella"

#load crop, sim, site info
with open("sim.json") as _:
    sim = json.load(_)

with open("site.json") as _:
    site = json.load(_)

with open("crop.json") as _:
    crop = json.load(_)

with open("all_crops.json") as _:
    all_crops = json.load(_)

sim["include-file-base-path"] = PATHS[USER]["INCLUDE_FILE_BASE_PATH"]

# define rotations (keep soybean as the first element please)
rotations = [
    ("soybean_7", "cotton"),
    ("soybean_8", "maize")
]

start_year = 1981
end_year = 1990

#create proper envs for each rotation
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

#generate templates
templates_abs_rot = {}
for rot in rotations:
    templates_abs_rot[rot] = generate_template_abs(rot, start_year, end_year, crops_data)

#define reference dates
ref_dates = []
ref_dates.append((1981, 330))
ref_dates.append((1982, 360))
ref_dates.append((1984, 1))
ref_dates.append((1984, 330))
ref_dates.append((1985, 10))

#modify templates
for rot in rotations:
    env = envs[rot]                    
    env["cropRotation"] = set_abs_dates(rot, templates_abs_rot[rot], ref_dates)


