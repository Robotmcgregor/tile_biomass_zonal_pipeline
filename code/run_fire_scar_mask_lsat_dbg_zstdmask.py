from __future__ import print_function, division
import sys
from rios import applier, fileinfo
import numpy as np
import csv
import pdb
import argparse
import pandas as pd
import scipy.stats.mstats as mstats
import numpy.ma as ma
import os
import geopandas as gpd
import shutil
from datetime import datetime


# def getCmdargs():
#     """
#     Get command line arguments
#     """
#     p = argparse.ArgumentParser()
#
#     # p.add_argument("--imglist", help="Input csv file with list of dbg and dk7 imagery to process")
#
#     p.add_argument("-l", "--lsat_dir", help="Path to the Landsat WRS directory",
#                    default=r"N:\landsat\wrs2")
#
#     p.add_argument("-b", "--burn_scar_dir", help="Path to the Landsat Burn scar (dkk) directory",
#                    default=r"U:\biomass\fire_scar")
#
#     p.add_argument("-n", "--lsat_nafi_dir", help="Path to the NAFI Landsat Burn scar (dkn) directory",
#                    default=r"U:\biomass\fire_scar")
#
#     p.add_argument("-p", "--part_dkn", help="Uncompleted NAFI Landsat Burn Scar (i.e. 'dkh') up to August")
#
#     p.add_argument("-g", "--lsat_tile_grid", help="Path to lsat tile grid",
#                    default=r"N:\Landsat\tilegrid\Landsat_wrs2_TileGrid.shp")
#
#     cmdargs = p.parse_args()
#
#     if cmdargs.lsat_dir is None:
#         p.print_help()
#         sys.exit()
#
#     return cmdargs


def temporary_dir_fn():
    """ Create a temporary directory 'user_YYYMMDD_HHMM'.

    @return temp_dir_path: string object containing the newly created directory path.
    @return final_user: string object containing the user id or the operator.
    """

    # extract user name
    home_dir = os.path.expanduser("~")
    _, user = home_dir.rsplit('\\', 1)
    final_user = user[3:]

    # create file name based on date and time.
    date_time_replace = str(datetime.now()).replace('-', '')
    date_time_list = date_time_replace.split(' ')
    date_time_list_split = date_time_list[1].split(':')
    temp_dir_path = '\\' + str(final_user) + '_' + str(date_time_list[0]) + '_' + str(
        date_time_list_split[0]) + str(date_time_list_split[1])

    # check if the folder already exists - if False = create directory, if True = return error message zzzz.
    try:
        shutil.rmtree(temp_dir_path)

    except:
        print('The following temporary directory will be created: ', temp_dir_path)
        pass

    # create folder a temporary folder titled (titled 'tempFolder'
    os.makedirs(temp_dir_path)

    return temp_dir_path, final_user


def main_routine(im_list, zone, temp_dir_path, tile, burn_dir):
    """Run mainRoutine"""

    # lsat_tile = cmdargs.lsat_tile
    # lsat_dir = r"N:\landsat\wrs2"
    # burn_dir = r"U:\biomass\fire_scar"
    # nafi_dir = r"U:\biomass\fire_scar"
    #part_dkn = cmdargs.part_dkn
    #tile_grid = cmdargs.lsat_tile_grid

    fire_mask_dir = os.path.join(temp_dir_path, "fire_mask")



    # -------------search for burn scar mapping dkk composites ------------
    image_list = []
    file_path_list = []
    year_list = []

    for root, dirs, files in os.walk(burn_dir, topdown=False):
        for name in files:
            # print("fire name: ", name)
            if name.endswith(f"dkaa2.tif"):
                name_list = name.split("_")

                image_list.append(name)
                file_path_list.append(os.path.join(root, name))
                # print(name_list[2])
                year = name_list[2]
                # print(year)
                year_list.append(str(year))

    dka_df = pd.DataFrame(
        {'dka': image_list,
         'dka_path': file_path_list,
         'year': year_list,

         })
    print(dka_df)

    # -------------search for burn scar mapping dkn composites ------------
    image_list = []
    file_path_list = []
    year_list = []

    for root, dirs, files in os.walk(burn_dir, topdown=False):
        for name in files:
            # print("fire name: ", name)
            if name.endswith(f"dkna2.tif"):
                name_list = name.split("_")

                image_list.append(name)
                file_path_list.append(os.path.join(root, name))
                # print(name_list[2])
                year = name_list[2]
                # print(year)
                year_list.append(str(year))

    dkn_df = pd.DataFrame(
        {'dkn': image_list,
         'dkn_path': file_path_list,
         'year': year_list,

         })
    print(dkn_df)

    foot_img_list = []
    foot_img_path_list = []
    foot_year = []
    orig_fire_list = []
    orig_dbi_list = []
    file_path_list = []
    year_list = []
    month_list = []
    day_list = []
    image_list = []


    with open(im_list, 'r') as imagery_list:


        # Extract each image path from the image list
        for im in imagery_list:
            print("im: ", im)

            if im.endswith(".img\n"):
                print("//")
                image = im.replace(".img\n", ".img")
            else:
                print("No //")
                image = im

            print("image: ", image)
            print("+" * 100)
            p, f = os.path.split(image)
            image_list.append(f)
            name_list = f.split("_")
            print("name_list: ", name_list)

            file_path_list.append(image)
            # print(name_list[2])
            # print(name_list[2])
            year = name_list[2][:4]
            # print(year)
            year_list.append(str(year))
            month = name_list[2][4:6]
            # print(month)
            month_list.append(str(month))
            day = name_list[2][-2:]
            # print(day)
            day_list.append(str(day))

        print("image_list: ", len(image_list))
        print("file_path_list: ", len(file_path_list))
        print("year_list: ", len(year_list))
        print("month_list: ", len(month_list))
        print("day_list: ", len(day_list))


        # print(single_dp0_df)
    single_dbg_df = pd.DataFrame(
        {'dbg': image_list,
         'dbg_path': file_path_list,
         'year': year_list,
         'month': month_list,
         'day': day_list
         })
    print(single_dbg_df)

    foot_img_list = []
    foot_img_path_list = []
    foot_year = []
    orig_fire_list = []
    orig_dbg_list = []

    for index, row in single_dbg_df.iterrows():
        dbg_image = row["dbg"]
        dbg_path = row["dbg_path"]
        year = row["year"]
        month = row["month"]
        #print("month: ", month)
        #print("-" * 50)
        int_year = int(year)
        #if int_year <= 2017:
        if int_year <= 1999:


            reffile = dbg_path
            mask_out_name = reffile.replace("_zstdmask.img", "_dksdmask.img")

            if os.path.isfile(mask_out_name):
                print("Already created: ", mask_out_name)
            else:
                print("-"*100)
                print("Creating: ", mask_out_name)
                required_year = year
                print("Required mask year: ", required_year)

                # hunt for fire year

                for index, row in dka_df.iterrows():
                    f_year = row["year"]
                    if f_year == required_year:
                        print("located annual dbg and matching fire year")

                        f_name = row["dka"]
                        f_path = row["dka_path"]

                        infile = f_path
                        f_name_split = f_name.split("_")
                        str_tile = str(tile)
                        # print("f_name_split: ", f_name_split)

                        out_name = f"{str(f_name_split[0])}_{str(required_year)}{str(month)}{str(day)}_p{str(str_tile[:3])}_r{str(str_tile[3:])}_dksd{str(zone)}.img"
                        print(out_name)

                        # import sys
                        # sys.exit()
                        outfile = os.path.join(temp_dir_path, out_name)
                        print("outfile: ", outfile)

                        foot_img_list.append(out_name)
                        foot_img_path_list.append(outfile)
                        foot_year.append(required_year)
                        orig_fire_list.append(f_path)
                        orig_dbg_list.append(reffile)

                        import imgFootPrintConverter_rios2
                        imgFootPrintConverter_rios2.main(reffile, infile, outfile)

                        mask_out_name = reffile.replace("_zstdmask.img", "_dksdmask.img")
                        print(mask_out_name)

                        import apply_lsat_dksdmask
                        apply_lsat_dksdmask.main_routine(reffile, outfile, mask_out_name, month)

                        print("file exported to: ", mask_out_name)

        # ----------------------------------------------- DKN ________________________________________________________

        #elif int_year > 2017:
        elif int_year > 1999:
            reffile = dbg_path
            mask_out_name = reffile.replace("_zstdmask.img", "_dkndmask.img")

            if os.path.isfile(mask_out_name):
                print("Already created: ", mask_out_name)
            else:
                print("Creating: ", mask_out_name)
                required_year = year
                print("Required mask year: ", required_year)

                # hunt for fire year

                for index, row in dkn_df.iterrows():
                    f_year = row["year"]
                    if f_year == required_year:
                        print("located annual dbg and matching fire year")

                        f_name = row["dkn"]
                        f_path = row["dkn_path"]

                        infile = f_path
                        f_name_split = f_name.split("_")
                        str_tile = str(tile)
                        # print("f_name_split: ", f_name_split)

                        out_name = f"{str(f_name_split[0])}_{str(required_year)}{str(month)}{str(day)}_p{str(str_tile[:3])}_r{str(str_tile[3:])}_dknd{str(zone)}.img"
                        print(out_name)

                        # import sys
                        # sys.exit()
                        outfile = os.path.join(temp_dir_path, out_name)
                        print("outfile: ", outfile)

                        foot_img_list.append(out_name)
                        foot_img_path_list.append(outfile)
                        foot_year.append(required_year)
                        orig_fire_list.append(f_path)
                        orig_dbg_list.append(reffile)

                        import imgFootPrintConverter_rios2
                        imgFootPrintConverter_rios2.main(reffile, infile, outfile)

                        mask_out_name = reffile.replace("_zstdmask.img", "_dkndmask.img")
                        print(mask_out_name)

                        import apply_lsat_dksdmask
                        apply_lsat_dksdmask.main_routine(reffile, outfile, mask_out_name, month)

                        print("file exported to: ", mask_out_name)

        else:
            pass


    print("script complete, goodbye.......")


if __name__ == "__main__":
    main_routine()
