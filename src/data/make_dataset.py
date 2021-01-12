# -*- coding: utf-8 -*-
import click

# import logging
from pathlib import Path
from dotenv import find_dotenv, load_dotenv, set_key
import urllib3
import shutil
import os
import csv
from datetime import date
import pandas as pd
import numpy as np
import random
from pyproj import Transformer
from typing import Union


# Load project directory
PROJECT_DIR = Path(__file__).resolve().parents[2]


@click.command()
@click.argument("input_filedir", type=click.Path(exists=True))
@click.argument("output_filedir", type=click.Path())
def main(input_filedir: str, output_filedir: str):
    """Downloads full dataset from lacity.org, and runs data processing
    scripts to turn raw data into cleaned data ready
    to be analyzed. Also updates environmental
    variable RAW_DATA_FILEPATH.
    """
    # If run as main, data is downloaded,  10% sampled, and cleaned
    # automatically
    clean(
        create_sample(download_raw(input_filedir), "data/interim", 0.1), output_filedir
    )


def download_raw(input_filedir: str) -> Path:
    """Downloads raw dataset from lacity.org to input_filedir as {date}
    raw.csv. Also updates environmental variable RAW_DATA_FILEPATH.
    """
    # Create name string using download date
    date_string = date.today().strftime("%Y-%m-%d")

    print("This will take a few minutes")

    # Setup connection and download into raw data folder
    http = urllib3.PoolManager()
    url = (
        "https://data.lacity.org/api/views/" + "wjz9-h9np/rows.csv?accessType=DOWNLOAD"
    )
    RAW_DATA_FILEPATH = PROJECT_DIR / input_filedir / (date_string + "_raw.csv")
    with http.request("GET", url, preload_content=False) as res, open(
        RAW_DATA_FILEPATH, "wb"
    ) as out_file:
        shutil.copyfileobj(res, out_file)

    print("Finished downloading raw dataset")

    # Save raw file path as RAW_DATA_FILEPATH into .env
    set_key(find_dotenv(), "RAW_DATA_FILEPATH", str(RAW_DATA_FILEPATH))
    return RAW_DATA_FILEPATH


def create_sample(
    target_file: Union[Path, str], output_filedir: str, sample_frac: float
) -> Path:
    """Samples the raw dataset to create a smaller dataset via random
    sampling according to sample_frac.
    """
    # Change str filepath into Path
    if isinstance(target_file, str):
        target_file = Path(target_file)

    # Check if sample_frac is between 0 and 1
    assert (sample_frac <= 1) and (sample_frac > 0)

    # Create filename with sample fraction appended to the name
    # 0.1 turns into 01, 0.25 turns into 025, etc
    SAMPLE_FILEPATH = (
        PROJECT_DIR
        / output_filedir
        / (target_file.stem + "_" + str(sample_frac).replace(".", "") + "samp.csv")
    )

    print("Creating sample")

    # Read raw data and skiprows using random.random()
    df = pd.read_csv(
        target_file,
        header=0,
        usecols=["Make", "Violation code", "Violation Description"],
        skiprows=lambda i: i > 0 and random.random() > sample_frac,
        low_memory=False,
    ).reset_index(drop=True)

    df.columns = [
        # "state_plate",
        "make",
        # "body_style",
        # "color",
        # "location",
        "violation_code",
        "violation_description",
        # "fine_amount",
        # "Latitude",
        # "Longitude",
        # "datetime",
    ]

    df.to_csv(SAMPLE_FILEPATH, index=False)

    print("Sample complete")

    return SAMPLE_FILEPATH


def clean(target_file: Union[Path, str], output_filedir: str):
    """Removes unnecessary columns, erroneous data points and aliases,
    changes geometry projection from epsg:2229 to epsg:4326, and converts
    time to datetime type.
    """
    # Change str filepath into Path
    if isinstance(target_file, str):
        target_file = Path(target_file)

    print("Cleaning dataset")

    # Read file into dataframe
    df = pd.read_csv(target_file, low_memory=False)

    # Select columns of interest
    # df = df[
    #     [
    #         # "Issue Date",
    #         # "Issue time",
    #         # "RP State Plate",
    #         "Make",
    #         # "Body Style",
    #         # "Color",
    #         # "Location",
    #         # "Violation code",
    #         "Violation Description",
    #         # "Fine amount",
    #         # "Latitude",
    #         # "Longitude",
    #     ]
    # ]

    # Filter out data points with bad coordinates
    # df = df[(df.Latitude != 99999) & (df.Longitude != 99999)]

    # Filter out data points with no time/date stamps
    # df = df[
    #     (df["Issue Date"].notna())
    #     & (df["Issue time"].notna())
    #     & (df["Fine amount"].notna())
    # ]

    # Convert Issue time and Issue Date strings into a combined datetime type
    # df["Issue time"] = df["Issue time"].apply(
    #     lambda x: "0" * (4 - len(str(int(x)))) + str(int(x))
    # )
    # df["Datetime"] = pd.to_datetime(
    #     df["Issue Date"] + " " + df["Issue time"], format="%m/%d/%Y %H%M"
    # )

    # Drop original date/time columns
    # df = df.drop(["Issue Date", "Issue time"], axis=1)

    # Make column names more coding friendly except for Lat/Lon
    # df.columns = [
    #     # "state_plate",
    #     "make",
    #     # "body_style",
    #     # "color",
    #     # "location",
    #     # "violation_code",
    #     "violation_description",
    #     # "fine_amount",
    #     # "Latitude",
    #     # "Longitude",
    #     # "datetime",
    # ]

    # Read in make aliases
    make_df = pd.read_csv(PROJECT_DIR / "references/make.csv", delimiter=",")
    make_df["alias"] = make_df.alias.apply(lambda x: x.split(","))

    # Iterate over makes and replace aliases
    for _, data in make_df.iterrows():
        df = df.replace(data["alias"], data["make"])

    # Instantiate projection converter and change projection
    # transformer = Transformer.from_crs("epsg:2229", "epsg:4326")
    # df["latitude"], df["longitude"] = transformer.transform(
    #     df["Latitude"].values, df["Longitude"].values
    # )

    # Drop original coordinate columns
    # df = df.drop(["Latitude", "Longitude"], axis=1)
    # df.reset_index(drop=True,inplace=True)
    # df.drop('Ticket number', axis=1)

    # Extract weekday and add as column
    # df["weekday"] = df.datetime.dt.weekday.astype(str).replace(
    #     {
    #         "0": "Monday",
    #         "1": "Tuesday",
    #         "2": "Wednesday",
    #         "3": "Thursday",
    #         "4": "Friday",
    #         "5": "Saturday",
    #         "6": "Sunday",
    #     }
    # )

    # Set fine amount as int
    # df["fine_amount"] = df.fine_amount.astype(int)

    same_codes = set(df["violation_code"]).intersection(
        set(df["violation_description"])
    )

    # Create function to swap codes and descriptions
    def code_swap(df):
        df["violation_code"] = df["violation_description"]
        df["violation_description"] = np.nan
        return df

    code_swap_filter = df["violation_description"].isin(same_codes) | (
        df["violation_code"] == "000"
    )

    df.loc[code_swap_filter, ["violation_code", "violation_description"]] = df[
        ["violation_code", "violation_description"]
    ][code_swap_filter].apply(code_swap, axis=1)

    # Remove new entries with both violation code and violation description missing
    df = df[~(df["violation_code"].isna() & df["violation_description"].isna())]

    # Cleaned misspelled violation description
    viol_aliases = {
        "22500B": "PARKED IN CROSSWALK",
        "22507.8B": "DISABLED PARKING/OBS",
        "80.69A+": "STOP/STAND PROHIBIT",
        "8056E2": "YELLOW ZONE",
        "22507.8B-": "DISABLED PARKING/OBSTRUCT ACCESS",
        "80.61": "STANDNG IN ALLEY",
        "80.66.1D": "RESTRICTED TAXI ZONE",
        "80.54": "OVERNIGHT PARKING",
        "8061#": "STANDING IN ALLEY",
        "80.69.1C": "PK TRAILER",
        "225001": "PARK FIRE LANE",
        "80.69D": "VEH/LOAD OVR 6' HIGH",
        "22500.1+": "PARKED IN FIRE LANE",
        "22507.8A-": "DISABLED PARKING/NO DP ID",
        "557": "8755*",
        "80.69BS": "NO PARK/STREET CLEAN",
        "22502E": "18 IN. CURB/1 WAY",
        "5200A": "DSPLYPLATE A",
        "22507.8A": "DISABLED PARKING/NO",
        "80692*": "COMVEH RES/OV TM B-2",
        "553": "80581",
        "22511.57B": "DP- RO NOT PRESENT",
        "80.69.4": "PK OVERSIZ",
        "80.75.1": "AUDIBLE ALARM",
        "5204A-": "DISPLAY OF TABS",
        "80.69C": "PARKED OVER TIME LIMIT",
        "80713": "PARKING/FRONT YARD 1",
        "22500K": "PARKED ON BRIDGE",
        "569": "2251157A",
        "88.03A": "OUTSIDE LINES/METER",
        "22522-": "3 FT. SIDEWALK RAMP",
        "22507.8C1": "DISABLED PARKING/BOUNDARIES",
        "8709D": "LOADING ZONES",
        "88.66": "ELECTRIC CHARGING STATION SPACES",
        "8070": "PARK IN GRID LOCK ZN",
        "88.63B+": "OFF STR/OVERTIME/MTR",
        "88.53": "OFF STR MTR/OUT LINE",
        "80692": "COMVEH RES/OV TM LMT",
        "17104H": "LOAD/UNLOAD ONLY",
        "80.58.1": "CARSHARE PARKING",
        "80.69AP+": "NO STOP/STANDING",
        "8056E1": "WHITE ZONE",
        "6344K2": "NO PARKING BETWEEN POSTED HOURS",
        "80.58L": "PREFERENTIAL PARKING",
        "80.53": "PARKED IN PARKWAY",
        "8069BS": "NO PARK/STREET CLEAN",
        "556": "8755",
        "5201": "POSITION OF PLATES",
        "8936": "RED CURB",
        "80.56E4+": "RED ZONE",
        "8061": "STNDNG IN ALLEY",
        "8056": "YELLOW ZONE",
        "80.72": "RED FLAG DAY",
        "22507A": "OVERSIZED VEHICLE PARKING TOPHAM ST",
        "22500L-": "DP-BLKNG ACCESS RAMP",
        "80661D": "RESTRICTED ZONE",
        "80.69AA+": "NO STOP/STAND",
        "22500H": "DOUBLE PARKING",
        "572521D": "MT FIRE RD NO PERMIT",
        "87.55": "FOR SALE SIGN",
        "8813B": "METER EXPIRED",
        "031": "22523A",
        "5202": "PERIOD OF DISPLAY",
        "8069B": "NO PARKING",
        "22511.1B": "PRK IN ELEC VEH SPACE",
        "805.6": "WHITE ZONE",
        "8056E4": "RED ZONE",
        "22502A": "18 IN. CURB/2 WAY",
        "89391C": "EXCEED TIME LMT",
        "85.01": "REPAIRING VEH/STREET",
        "8069C": "PKD OVER TIME LIMIT",
        "80732": "EXCEED 72 HOURS",
        "8606": "PK OTSD PSTD AR",
        "8603": "PK IN PROH AREA",
        "89391A": "STOP/STAND PROHB",
        "8053": "PKD IN/ON PARKWAY",
        "8939": "WHITE CURB",
        "86.03": "CITY PARK/PROHIB",
        "80714#": "PRIVATE PROPERTY",
        "80.69.2": "COMM VEH OVER TIME LIMIT",
        "572521E": "OBST FIRE RD",
        "045": "4000",
        "80.70": "NO STOPPING/ANTI-GRIDLOCK ZONE",
        "8940": "PARKING AREA",
        "21113": "PRKG PUBL GRNDS",
        "80714": "PRIVATE PROPERTY",
        "80.49+": "18 IN/CURB/COMM VEH",
        "5204A": "EXPIRED TAGS",
        "8051A": "LEFT SIDE OF ROADWAY",
        "80692**": "COMVEH RES/OV TM C-3",
        "22507.8C2": "DISABLED PARKING/CROSS HATCH",
        "80.7": "NO STOPPING/ANTI-GRIDLOCK ZONE",
        "225078A": "HANDICAP/NO DP ID",
        "88.64A": "TIME LIMIT/CITY LOT",
        "8501": "REPAIRING VEH/STREET",
        "80.73.2": "EXCEED 72HRS-ST",
        "22511.56B": "DP-REFUSE ID",
        "8863B": "OFF STR/OVERTIME/MTR",
        "80.56E2": "YELLOW ZONE",
        "8056E3": "GREEN ZONE",
        "80.71.3": "PARKING/FRONT YARD",
        "030": "22522",
        "8069A": "NO STOPPING/STANDING",
        "8058L": "PREF PARKING",
        "88.13B+": "METER EXP.",
        "22500A": "WITHIN INTERSECTION",
        "22507.8C": "DISABLED PARKING/CRO",
        "80.54H1": "OVNIGHT PRK W/OUT PE",
        "89355C": "ILGL EXT OF TM",
        "2251156B": "MISUSE/DP PRIVILEGE",
        "21211B": "BLK BIKE PATH OR LANE",
        "4000A1": "NO EVIDENCE OF REG",
        "22511.57": "DP- RO NOT PRESENT",
        "22500C": "SAFETY ZONE/CURB",
        "225078C2": "HANDICAP/CROSS HATCH",
        "6344K7": "PARKING OUTSIDE PARKING STALLS",
        "86.06": "CITY PARK/PROHIB",
        "22500E": "BLOCKING DRIVEWAY",
        "22500I-": "PARKED IN BUS ZONE",
        "80.69.1A": "COMM TRAILER/22 FT.",
        "225078C1": "HANDICAP/ON LINE",
        "80691C": "PARKING UNHITCHED TR",
        "22515": "UNATT/MOTOR ON",
        "5200": "DISPLAY OF PLATES",
        "17104C": "R/PRIV PARKING AREA",
        "22526": "BLOCKING INTERSECTION",
        "80.56E1": "WHITE ZONE",
        "80691A": "COMM TRAILER/22 FT.",
        "571": "2251157C",
        "8709B": "PARK-PSTD AREAS",
        "22514": "FIRE HYDRANT",
        "029": "22521",
        "80.69B": "NO PARKING",
        "8803A": "PK OUTSD SPACE",
        "8049": "WRG SD/NOT PRL",
        "22500F": "PARKED ON SIDEWALK",
        "8072": "PARK RED FLAG DAY",
        "80.74": "CLEANING VEH/STREET",
        "8074": "CLEANING VEH/STREET",
        "8069AP": "NO STOP/STAND PM",
        "8073C": "CATERING/CENTER CITY",
        "8709K": "PK OVR PNTD LNS",
        "80.56E3": "GREEN ZONE",
        "8943": "PARK IN XWALK",
        "22511.57C": "DP-ALTERED",
        "017": "22502",
        "21113A+": "PUBLIC GROUNDS",
        "80731": "STORING VEH/ON STR",
        "6344C": "COMMERCIAL - UNDESIG",
        "225078B": "HANDICPD/BLOCKING",
        "80.71.4": "PRIVATE PROPERTY",
        "8069AA": "NO STOP/STAND AM",
        "6344K8": "SIGN POSTED - NO PARKING",
    }

    for key in viol_aliases.keys():
        df.loc[df["violation_code"] == key, "violation_description"] = viol_aliases[key]

    # Drop filtered index and add new one
    df.drop("violation_code", axis=1, inplace=True)
    df.reset_index(drop=True, inplace=True)
    df.reset_index(inplace=True)

    # Save as csv to output_filedir annotated as processed
    df.to_csv(
        PROJECT_DIR
        / output_filedir
        / (target_file.stem.replace("_raw", "_processed") + ".csv"),
        index=False,
        quoting=csv.QUOTE_ALL,
    )
    print("Finished!")


if __name__ == "__main__":
    # log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    # logging.basicConfig(level=logging.INFO, format=log_fmt)

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    if find_dotenv():
        load_dotenv(find_dotenv())
    else:
        with open(PROJECT_DIR / ".env", "w"):
            pass
    # Create data folders
    data_folders = ["raw", "interim", "external", "processed"]
    if not os.path.exists(PROJECT_DIR / "data"):
        os.makedirs(PROJECT_DIR / "data")
    for _ in data_folders:
        if not os.path.exists(PROJECT_DIR / "data" / _):
            os.makedirs(PROJECT_DIR / "data" / _)
            with open(PROJECT_DIR / "data" / _ / ".gitkeep", "w"):
                pass

    # Run main function
    # logger = logging.getLogger(__name__)
    # logger.info(
    #     'Starting download of raw dataset: this will take a few minutes'
    # )
    main()
    # logger.info('Finished downloading!')
