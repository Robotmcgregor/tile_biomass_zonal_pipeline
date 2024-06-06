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


def getCmdargs():
    """
    Get command line arguments
    """
    p = argparse.ArgumentParser()

    p.add_argument("-l", "--lsat_dir", help="Path to the Landsat WRS directory",
                   default=r"N:\landsat\mosaics\SeasonalComposites")

    p.add_argument("-b", "--burn_scar_dir", help="Path to the Landsat Burn scar (dkk) directory",
                   default=r"U:\biomass\fire_scar")

    p.add_argument("-n", "--lsat_nafi_dir", help="Path to the NAFI Landsat Burn scar (dkn) directory",
                   default=r"U:\biomass\fire_scar")

    p.add_argument("-p", "--part_dkn", help="Uncompleted NAFI Landsat Burn Scar (i.e. 'dkh') up to August")

    cmdargs = p.parse_args()

    if cmdargs.lsat_dir is None:
        p.print_help()
        sys.exit()

    return cmdargs


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


def main_routine():
    """Run mainRoutine"""

    cmdargs = getCmdargs()

    # lsat_tile = cmdargs.lsat_tile
    lsat_dir = cmdargs.lsat_dir
    burn_dir = cmdargs.burn_scar_dir
    nafi_dir = cmdargs.lsat_nafi_dir
    part_dkn = cmdargs.part_dkn
    # tile_grid = cmdargs.lsat_tile_grid

    temp_dir_path, user = temporary_dir_fn()
    zone = "Albers"

    # ------------------------------------------------ DKK Fire Scars -----------------------------------------

    # ------------- search for burn scar mapping dkk composites ------------
    fimage_list = []
    ffile_path_list = []
    fyear_list = []

    for froot, fdirs, ffiles in os.walk(burn_dir, topdown=False):
        for fname in ffiles:
            print("fire name: ", fname)
            if fname.endswith(f"dkaa2.tif"):
                fname_list = fname.split("_")

                fimage_list.append(fname)
                ffile_path_list.append(os.path.join(froot, fname))
                # print(fname_list[2])
                fyear = fname_list[2]
                # print(fyear)
                fyear_list.append(str(fyear))

    dka_df = pd.DataFrame(
        {'dka': fimage_list,
         'dka_path': ffile_path_list,
         'year': fyear_list,

         })
    #print("dka: ", dka_df)

    # ------------- search for burn scar mapping dkn composites ------------
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



    print("lsat_dir: ", lsat_dir)

    file_ext_list = ["dbi", "dim", "dis", "dja", "dp1", "dpa", "fpc"]

    for exten in file_ext_list:
        print("extension: ", exten)

        for root, dirs, files in os.walk(lsat_dir, topdown=False):
            # print(root)
            # print(dirs)
            # print(files)
            for dir_ in dirs:
                print("dir: ", dir_)

                # todo add not
                if dir_ == exten:
                    print("+" * 50)
                    print("Working on dir_: ", dir_)
                    print("root: ", root)
                    out_dir = exten  # dir_.replace("_test", "")
                    dir_root = os.path.join(root, exten)
                    # print("dir_root: ", dir_root)

                    image_list = []
                    file_path_list = []
                    year_list = []
                    st_month_list = []
                    end_month_list = []

                    # import sys
                    # sys.exit()

                    for root, dirs, files in os.walk(dir_root, topdown=False):
                        for name in files:
                            # print("-"*50)
                            # print(os.path.join(root, name))

                            if name.endswith(f"{exten}a2.tif"):
                                name_list = name.split("_")
                                # select seasonal composites
                                # print(name_list)
                                image_list.append(name)
                                file_path_list.append(os.path.join(root, name))
                                # print(name_list[2])
                                year = name_list[2][1:5]
                                # print(year)
                                year_list.append(str(year))
                                st_month = name_list[2][5:7]
                                # print(st_month)
                                st_month_list.append(str(st_month))
                                end_month = name_list[2][-2:]
                                # print(end_month)
                                end_month_list.append(str(end_month))

                            else:
                                print("does not meet criteria: ", name)

                        seasonal_dbi_df = pd.DataFrame(
                            {'mosaic': image_list,
                             'mosaic_path': file_path_list,
                             'year': year_list,
                             'st_month': st_month_list,
                             'end_month': end_month_list
                             })
                        # print("seasonal dbi df: ", seasonal_dbi_df)

                        # ------------------------------------------------------------------------------

                        foot_img_list = []
                        foot_img_path_list = []
                        foot_year = []
                        orig_fire_list = []
                        orig_dbi_list = []
                        print("Go through mosaic df")
                        for index, row in seasonal_dbi_df.iterrows():
                            # print("row: ", row)
                            dbi_image = row["mosaic"]
                            dbi_path = row["mosaic_path"]
                            year = row["year"]
                            st_month = row["st_month"]
                            # print("st_month: ", st_month)
                            # print("-"*50)

                            if st_month == "06":
                                season = "0608"
                                print("June to August composite....")

                            elif st_month == "09":
                                season = "0911"
                                print("September to November composite....")
                            elif st_month == "12":
                                season = "1202"
                                print("December to February composite....")

                            else:
                                season == "unknown"

                            reffile = dbi_path
                            print("reffile: ", reffile)
                            mask_out_name1 = reffile.replace(".tif", "_dkbsmask.tif")
                            mask_out_name2 = reffile.replace(".tif", "_dknsmask.tif")

                            if os.path.isfile(mask_out_name1) or os.path.isfile(mask_out_name2):
                                print("Already created: ", mask_out_name1, " or ", mask_out_name2)
                            else:
                                required_year = year

                                # hunt for fire year
                                print("-"*50)
                                int_year = int(year)
                                print(int_year)
                                if int_year <= 2017:

                                    for index, row in dka_df.iterrows():
                                        f_year = row["year"]
                                        if f_year == required_year:
                                            print("located annual dbi and matching fire year")

                                            f_name = row["dka"]
                                            f_path = row["dka_path"]

                                            infile = f_path
                                            f_name_split = f_name.split("_")
                                            # str_tile = str(tile)
                                            # print("f_name_split: ", f_name_split)
                                            out_name = f"{str(f_name_split[0])}_nt_{required_year}_{out_dir}a2.img"

                                            outfile = os.path.join(temp_dir_path, out_name)
                                            print("outfile: ", outfile)

                                            foot_img_list.append(out_name)
                                            foot_img_path_list.append(outfile)
                                            foot_year.append(required_year)
                                            orig_fire_list.append(f_path)
                                            orig_dbi_list.append(reffile)

                                            if os.path.isfile(mask_out_name1):
                                                print("Exists: ", mask_out_name1)
                                            else:

                                                import imgFootPrintConverter_rios2
                                                imgFootPrintConverter_rios2.main(reffile, infile, outfile)

                                                import apply_lsat_dkbsmask
                                                apply_lsat_dkbsmask.main_routine(reffile, outfile, mask_out_name1, season)

                                                print("Fire masked footprint exported to: ", outfile)
                                                print("Masked data: ", mask_out_name1)
                                                print("+"*50)

                                elif int_year > 2017:
                                    print("-"*50)
                                    print(int_year)

                                    for index, row in dkn_df.iterrows():
                                        f_year = row["year"]
                                        if f_year == required_year:
                                            print("located annual dbi and matching fire year")

                                            f_name = row["dkn"]
                                            f_path = row["dkn_path"]

                                            infile = f_path
                                            f_name_split = f_name.split("_")
                                            # str_tile = str(tile)
                                            # print("f_name_split: ", f_name_split)
                                            out_name = f"{str(f_name_split[0])}_nt_{required_year}_{out_dir}a2.img"

                                            outfile = os.path.join(temp_dir_path, out_name)
                                            print("outfile: ", outfile)

                                            foot_img_list.append(out_name)
                                            foot_img_path_list.append(outfile)
                                            foot_year.append(required_year)
                                            orig_fire_list.append(f_path)
                                            orig_dbi_list.append(reffile)

                                            if os.path.isfile(mask_out_name2):
                                                print("Exists: ", mask_out_name2)
                                            else:

                                                import imgFootPrintConverter_rios2
                                                imgFootPrintConverter_rios2.main(reffile, infile, outfile)

                                                import apply_lsat_dkbsmask
                                                apply_lsat_dkbsmask.main_routine(reffile, outfile, mask_out_name2,
                                                                                 season)

                                                print("Fire masked footprint exported to: ", outfile)
                                                print("Masked data: ", mask_out_name2)

                                                print("+"*50)


                        footprint_df = pd.DataFrame(
                            {"footprint": foot_img_list,
                             "foot_path": foot_img_path_list,
                             "foot_year": foot_year,
                             "orig_fire": orig_fire_list,
                             "orig_dbi": orig_dbi_list,
                             })
                        print(footprint_df)
                        footprint_df.to_csv(os.path.join(temp_dir_path, f"{out_dir}_dka_df.csv"), index=False)
                else:
                    print("This is not the correct dir..")

    # # ------------------------------------------------ DKN Fire Scars -----------------------------------------
    #
    # # -------------search for burn scar mapping dkk composites ------------
    # fimage_list = []
    # ffile_path_list = []
    # fyear_list = []
    #
    # for froot, fdirs, ffiles in os.walk(nafi_dir, topdown=False):
    #     for fname in ffiles:
    #         print("fire name: ", fname)
    #         if fname.endswith("dkna2.tif"):
    #             fname_list = fname.split("_")
    #
    #             fimage_list.append(fname)
    #             ffile_path_list.append(os.path.join(froot, fname))
    #             # print(fname_list[2])
    #             fyear = fname_list[2]
    #             # print(fyear)
    #             fyear_list.append(str(fyear))
    #
    # dka_df = pd.DataFrame(
    #     {'dka': fimage_list,
    #      'dka_path': ffile_path_list,
    #      'year': fyear_list,
    #
    #      })
    # print("dka: ", dka_df)
    #
    # print("lsat_dir: ", lsat_dir)
    #
    # for root, dirs, files in os.walk(lsat_dir, topdown=False):
    #     # print(root)
    #     # print(dirs)
    #     # print(files)
    #     for dir_ in dirs:
    #
    #         # todo add not
    #         if dir_.endswith("test"):
    #             print("Working on dir_: ", dir_)
    #             out_dir = dir_.replace("_test", "")
    #             dir_root = root  # os.path.join(root, dir_)
    #             # print("dir_root: ", dir_root)
    #
    #             image_list = []
    #             file_path_list = []
    #             year_list = []
    #             st_month_list = []
    #             end_month_list = []
    #
    #             file_ext_list = ["dbi_test", "dim_test"]
    #
    #             for exten in file_ext_list:
    #                 print("extension: ", exten)
    #
    #             for root, dirs, files in os.walk(dir_root, topdown=False):
    #                 print("dir: ", dirs)
    #                 for name in files:
    #                     # print("-"*50)
    #                     # print(os.path.join(root, name))
    #                     import sys
    #                     sys.exit()
    #                     if name.endswith(".tif"):
    #                         name_list = name.split("_")
    #                         # select seasonal composites
    #                         # print(name_list)
    #                         image_list.append(name)
    #                         file_path_list.append(os.path.join(root, name))
    #                         # print(name_list[2])
    #                         year = name_list[2][1:5]
    #                         # print(year)
    #                         year_list.append(str(year))
    #                         st_month = name_list[2][5:7]
    #                         # print(st_month)
    #                         st_month_list.append(str(st_month))
    #                         end_month = name_list[2][-2:]
    #                         # print(end_month)
    #                         end_month_list.append(str(end_month))
    #
    #                 seasonal_dbi_df = pd.DataFrame(
    #                     {'mosaic': image_list,
    #                      'mosaic_path': file_path_list,
    #                      'year': year_list,
    #                      'st_month': st_month_list,
    #                      'end_month': end_month_list
    #                      })
    #                 # print("seasonal dbi df: ", seasonal_dbi_df)
    #
    #                 foot_img_list = []
    #                 foot_img_path_list = []
    #                 foot_year = []
    #                 orig_fire_list = []
    #                 orig_dbi_list = []
    #                 print("Go through mosaic df")
    #                 for index, row in seasonal_dbi_df.iterrows():
    #                     # print("row: ", row)
    #                     dbi_image = row["mosaic"]
    #                     dbi_path = row["mosaic_path"]
    #                     year = row["year"]
    #                     st_month = row["st_month"]
    #                     # print("st_month: ", st_month)
    #                     # print("-"*50)
    #
    #                     if st_month == "06":
    #                         season = "0608"
    #                         print("June to August composite....")
    #
    #                     elif st_month == "09":
    #                         season = "0911"
    #                         print("September to November composite....")
    #                     elif st_month == "12":
    #                         season = "1202"
    #                         print("December to February composite....")
    #
    #                     else:
    #                         season == "unknown"
    #
    #                     reffile = dbi_path
    #                     print("reffile: ", reffile)
    #                     mask_out_name = reffile.replace(".tif", "_dknsmask.tif")
    #
    #                     if os.path.isfile(mask_out_name):
    #                         print("Already created: ", mask_out_name)
    #                     else:
    #                         required_year = year
    #
    #                         # hunt for fire year
    #
    #                         for index, row in dka_df.iterrows():
    #                             f_year = row["year"]
    #                             if f_year == required_year:
    #                                 print("located annual dbi and matching fire year")
    #
    #                                 f_name = row["dka"]
    #                                 f_path = row["dka_path"]
    #
    #                                 infile = f_path
    #                                 f_name_split = f_name.split("_")
    #                                 # str_tile = str(tile)
    #                                 # print("f_name_split: ", f_name_split)
    #                                 out_name = f"{str(f_name_split[0])}_nt_r_{out_dir}a2.img"
    #
    #                                 outfile = os.path.join(temp_dir_path, out_name)
    #                                 print("outfile: ", outfile)
    #
    #                                 foot_img_list.append(out_name)
    #                                 foot_img_path_list.append(outfile)
    #                                 foot_year.append(required_year)
    #                                 orig_fire_list.append(f_path)
    #                                 orig_dbi_list.append(reffile)
    #
    #                                 if os.path.isfile(mask_out_name):
    #                                     print("Exists: ", mask_out_name)
    #                                 else:
    #
    #                                     import imgFootPrintConverter_rios2
    #                                     imgFootPrintConverter_rios2.main(reffile, infile, outfile)
    #
    #                                     import apply_lsat_dkbsmask
    #                                     apply_lsat_dkbsmask.main_routine(reffile, outfile, mask_out_name, season)
    #
    #                                     print("Fire footprint exported to: ", outfile)
    #                                     print("Masked output", mask_out_name)
    #
    #
    #                             else:
    #                                 pass
    #
    #                 footprint_df = pd.DataFrame(
    #                     {"footprint": foot_img_list,
    #                      "foot_path": foot_img_path_list,
    #                      "foot_year": foot_year,
    #                      "orig_fire": orig_fire_list,
    #                      "orig_dbi": orig_dbi_list,
    #                      })
    #                 print(footprint_df)
    #                 footprint_df.to_csv(os.path.join(temp_dir_path, f"{out_dir}_dkn_df.csv"), index=False)
    #

    print("script complete, goodbye.......")


if __name__ == "__main__":
    main_routine()
