#!/usr/bin/env python

"""
Fractional cover zonal statistics pipeline
==========================================

Description: This pipeline comprises of 12 scripts which read in the Rangeland Monitoring Branch odk instances
{instance names, odk_output.csv and ras_odk_output.csv: format, .csv: location, directory}
Outputs are files to a temporary directory located in your working directory (deleted at script completion),
or to an export directory located a the location specified by command argument (--export_dir).
Final outputs are files to their respective property sub-directories within the Pastoral_Districts directory located in
the Rangeland Working Directory.


step1_1_initiate_fractional_cover_zonal_stats_pipeline.py
===============================
Description: This script initiates the Fractional cover zonal statistics pipeline.
This script:

1. Imports and passes the command line arguments.

2. Creates two directories named: user_YYYYMMDD_HHMM. If either of the directories exist, they WILL BE DELETED.

3. Controls the workflow of the pipeline.

4. deletes the temporary directory and its contents once the pipeline has completed.


Author: Rob McGregor
email: Robert.Mcgregor@nt.gov.au
Date: 27/10/2020
Version: 1.0

###############################################################################################

MIT License

Copyright (c) 2020 Rob McGregor

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the 'Software'), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.


THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

##################################################################################################

===================================================================================================

Command arguments:
------------------

--tile_grid: str
string object containing the path to the Landsat tile grid shapefile.

--directory_odk: str
string object containing the path to the directory housing the odk files to be processed - the directory can contain 1
to infinity odk outputs.
Note: output runtime is approximately 1 hour using the remote desktop or approximately  3 hours using your laptop
(800 FractionalCover images).

--export_dir: str
string object containing the location of the destination output folder and contents(i.e. 'C:Desktop/odk/YEAR')
NOTE1: the script will search for a duplicate folder and delete it if found.
NOTE2: the folder created is titled (YYYYMMDD_TIME) to avoid the accidental deletion of data due to poor naming
conventions.

--image_count
integer object that contains the minimum number of Landsat images (per tile) required for the fractional cover
zonal stats -- default value set to 800.

--landsat_dir: str
string object containing the path to the Landsat Directory -- default value set to r'Z:\Landsat\wrs2'.

--no_data: int
ineger object containing the Landsat Fractional Cover no data value -- default set to 0.

--rainfall_dir: str
string object containing the pathway to the rainfall image directory -- default set to r'Z:\Scratch\mcintyred\Rainfall'.

--search_criteria1: str
string object containing the end part of the filename search criteria for the Fractional Cover Landsat images.
-- default set to 'dilm2_zstdmask.img'

--search_criteria2: str
string object from the concatenation of the end part of the filename search criteria for the Fractional Cover
Landsat images. -- default set to 'dilm3_zstdmask.img'

--search_criteria3: str
string object from the concatenation of the end part of the filename search criteria for the QLD Rainfall images.
-- default set to '.img'

======================================================================================================

"""

# Import modules
from __future__ import print_function, division
import os
from datetime import datetime
import argparse
import shutil
import sys
import warnings
import glob
import pandas as pd
import geopandas

warnings.filterwarnings("ignore")


def get_cmd_args_fn():
    p = argparse.ArgumentParser(
        description='''Input a single or multi-band raster to extracts the values from the input shapefile. ''')

    p.add_argument('-d', '--data', help='The directory the site points csv file.')

    p.add_argument('-t', '--tile_grid',
                   help="Enter filepath for the Landsat Tile Grid.shp.",
                   default=r"N:\Landsat\tilegrid\Landsat_wrs2_TileGrid.shp")

    p.add_argument('-x', '--export_dir',
                   help='Enter the export directory for all of the final outputs.',
                   default=r'U:\scratch\rob\pipelines\outputs')

    p.add_argument('-i', '--image_count', type=int,
                   help='Enter the minimum amount of Landsat images required per tile as an integer (i.e. 950).',
                   default=100)

    p.add_argument('-l', '--lsat_dir', help="The wrs2 directory containing landsat data",
                   default=r"N:\Landsat\wrs2")

    p.add_argument('-n', '--no_data', help="Enter the Landsat Fractional Cover no data value (i.e. 0)",
                   default=0)

    p.add_argument('-p', '--path', help="Enter the Landsat path (i.e. 106)",
                   default=0)

    p.add_argument('-r', '--row', help="Enter the Landsat row (i.e. 069)",
                   default=0)

    p.add_argument('-z', '--zone', help="Enter the Landsat tile zone (i.e. 2 or 3)",
                   default=0)

    p.add_argument("-b", "--burn_dir", help="Path to the Landsat Burn scar (dkk) directory",
                    default=r"U:\biomass\fire_scar")


    cmd_args = p.parse_args()

    if cmd_args.data is None:
        p.print_help()

        sys.exit()

    return cmd_args


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


def temp_dir_folders_fn(temp_dir_path):
    """ Create folders within the temp_dir directory.

    @param temp_dir_path: string object containing the newly created directory path.
    @return prime_temp_grid_dir: string object containing the newly created folder (temp_tile_grid) within the
    temporary directory.
    @return prime_temp_buffer_dir: string object containing the newly created folder (temp_1ha_buffer)within the
    temporary directory.

    """

    prime_temp_grid_dir = temp_dir_path + '\\temp_tile_grid'
    os.mkdir(prime_temp_grid_dir)

    zonal_stats_ready_dir = prime_temp_grid_dir + '\\zonal_stats_ready'
    os.makedirs(zonal_stats_ready_dir)

    proj_tile_grid_sep_dir = prime_temp_grid_dir + '\\separation'
    os.makedirs(proj_tile_grid_sep_dir)

    prime_temp_buffer_dir = temp_dir_path + '\\temp_1ha_buffer'
    os.mkdir(prime_temp_buffer_dir)

    gcs_wgs84_dir = (temp_dir_path + '\\gcs_wgs84')
    os.mkdir(gcs_wgs84_dir)

    albers_dir = (temp_dir_path + '\\albers')
    os.mkdir(albers_dir)

    return prime_temp_grid_dir, prime_temp_buffer_dir, zonal_stats_ready_dir


def export_file_path_fn(export_dir, final_user, path, row):
    """ Create an export directory 'user_YYYMMDD_HHMM' at the location specified in command argument export_dir.

    @param final_user: string object containing the user id or the operator.
    @param export_dir: string object containing the path to the export directory (command argument).
    @return export_dir_path: string object containing the newly created directory path for all retained exports.
    """

    # create string object from final_user and datetime.
    date_time_replace = str(datetime.now()).replace('-', '')
    date_time_list = date_time_replace.split(' ')
    date_time_list_split = date_time_list[1].split(':')
    export_dir_path = export_dir + '\\' + final_user + '_' + str(path) + '_' + str(row) + '_zs_' + str(
        date_time_list[0]) + '_' + str(
        date_time_list_split[0]) + str(
        date_time_list_split[1])

    # check if the folder already exists - if False = create directory, if True = return error message.
    try:
        shutil.rmtree(export_dir_path)

    except:
        print('The following export directory will be created: ', export_dir_path)
        pass

    # create folder.
    os.makedirs(export_dir_path)

    return export_dir_path


def export_dir_folders_fn(export_dir_path, lsat_tile):
    """ Create sub-folders within the export directory.

    @param export_dir_path: string object containing the newly created export directory path.
    @return tile_status_dir: string object containing the newly created folder (tile_status) with three sub-folders:
    for_processing, insufficient_files and tile_status_lists.
    @return tile_status_dir:
    @return plot_dir:
    @return zonal_stats_output_dir:
    @return rainfall_output_dir:
    """

    dbg_tile_status_dir = (export_dir_path + '\\dbg_tile_status')
    os.mkdir(dbg_tile_status_dir)

    dbg_mask_tile_status_dir = (export_dir_path + '\\dbg_mask_tile_status')
    os.mkdir(dbg_mask_tile_status_dir)

    dbi_tile_status_dir = (export_dir_path + '\\dbi_tile_status')
    os.mkdir(dbi_tile_status_dir)

    dbi_mask_tile_status_dir = (export_dir_path + '\\dbi_mask_tile_status')
    os.mkdir(dbi_mask_tile_status_dir)

    dp0_tile_status_dir = (export_dir_path + '\\dp0_tile_status')
    os.mkdir(dp0_tile_status_dir)

    dp0_mask_tile_status_dir = (export_dir_path + '\\dp0_mask_tile_status')
    os.mkdir(dp0_mask_tile_status_dir)

    dp1_tile_status_dir = (export_dir_path + '\\dp1_tile_status')
    os.mkdir(dp1_tile_status_dir)

    dp1_mask_tile_status_dir = (export_dir_path + '\\dp1_mask_tile_status')
    os.mkdir(dp1_mask_tile_status_dir)

    # ----------------------------------------------------------------------

    dbg_tile_for_processing_dir = (dbg_tile_status_dir + '\\dbg_for_processing')
    os.mkdir(dbg_tile_for_processing_dir)

    dbg_mask_tile_for_processing_dir = (dbg_mask_tile_status_dir + '\\dbg_mask_for_processing')
    os.mkdir(dbg_mask_tile_for_processing_dir)

    dbi_tile_for_processing_dir = (dbi_tile_status_dir + '\\dbi_for_processing')
    os.mkdir(dbi_tile_for_processing_dir)

    dbi_mask_tile_for_processing_dir = (dbi_mask_tile_status_dir + '\\dbi_mask_for_processing')
    os.mkdir(dbi_mask_tile_for_processing_dir)

    dp0_tile_for_processing_dir = (dp0_tile_status_dir + '\\dp0_for_processing')
    os.mkdir(dp0_tile_for_processing_dir)

    dp0_mask_tile_for_processing_dir = (dp0_mask_tile_status_dir + '\\dp0_mask_for_processing')
    os.mkdir(dp0_mask_tile_for_processing_dir)

    dp1_tile_for_processing_dir = (dp1_tile_status_dir + '\\dp1_for_processing')
    os.mkdir(dp1_tile_for_processing_dir)

    dp1_mask_tile_for_processing_dir = (dp1_mask_tile_status_dir + '\\dp1_mask_for_processing')
    os.mkdir(dp1_mask_tile_for_processing_dir)


    # # -----------------------------------------------------------------------
    #

    dbg_insuf_files_dir = (dbg_tile_status_dir + '\\dbg_insufficient_files')
    os.mkdir(dbg_insuf_files_dir)

    dbg_mask_insuf_files_dir = (dbg_mask_tile_status_dir + '\\dbg_mask_insufficient_files')
    os.mkdir(dbg_mask_insuf_files_dir)

    dbi_insuf_files_dir = (dbi_tile_status_dir + '\\dbi_insufficient_files')
    os.mkdir(dbi_insuf_files_dir)

    dbi_mask_insuf_files_dir = (dbi_mask_tile_status_dir + '\\dbi_mask_insufficient_files')
    os.mkdir(dbi_mask_insuf_files_dir)

    dp0_insuf_files_dir = (dp0_tile_status_dir + '\\dp0_insufficient_files')
    os.mkdir(dp0_insuf_files_dir)

    dp0_mask_insuf_files_dir = (dp0_tile_status_dir + '\\dp0_mask_insufficient_files')
    os.mkdir(dp0_mask_insuf_files_dir)

    dp1_insuf_files_dir = (dp1_tile_status_dir + '\\dp1_insufficient_files')
    os.mkdir(dp1_insuf_files_dir)

    dp1_mask_insuf_files_dir = (dp1_tile_status_dir + '\\dp1_mask_insufficient_files')
    os.mkdir(dp1_mask_insuf_files_dir)

    # # ------------------------------------------------------------------------


    dbg_stat_list_dir = dbg_tile_status_dir + '\\dbg_tile_status_lists'
    os.mkdir(dbg_stat_list_dir)

    dbg_mask_stat_list_dir = dbg_mask_tile_status_dir + '\\dbg_mask_tile_status_lists'
    os.mkdir(dbg_mask_stat_list_dir)

    dbi_stat_list_dir = dbi_tile_status_dir + '\\dbi_tile_status_lists'
    os.mkdir(dbi_stat_list_dir)

    dbi_mask_stat_list_dir = dbi_mask_tile_status_dir + '\\dbi_mask_tile_status_lists'
    os.mkdir(dbi_mask_stat_list_dir)

    dp0_stat_list_dir = dp0_tile_status_dir + '\\dp0_tile_status_lists'
    os.mkdir(dp0_stat_list_dir)

    dp0_mask_stat_list_dir = dp0_mask_tile_status_dir + '\\dp0_mask_tile_status_lists'
    os.mkdir(dp0_mask_stat_list_dir)

    dp1_stat_list_dir = dp1_tile_status_dir + '\\dp1_tile_status_lists'
    os.mkdir(dp1_stat_list_dir)

    dp1_mask_stat_list_dir = dp1_mask_tile_status_dir + '\\dp1_mask_tile_status_lists'
    os.mkdir(dp1_mask_stat_list_dir)

    # -------------------------------------------------------------------------

    dbg_zonal_stats_output_dir = (export_dir_path + '\\dbg_zonal_stats')
    os.mkdir(dbg_zonal_stats_output_dir)

    dbg_mask_zonal_stats_output_dir = (export_dir_path + '\\dbg_mask_zonal_stats')
    os.mkdir(dbg_mask_zonal_stats_output_dir)

    dbi_zonal_stats_output_dir = (export_dir_path + '\\dbi_zonal_stats')
    os.mkdir(dbi_zonal_stats_output_dir)

    dbi_mask_zonal_stats_output_dir = (export_dir_path + '\\dbi_mask_zonal_stats')
    os.mkdir(dbi_mask_zonal_stats_output_dir)

    dp0_zonal_stats_output_dir = (export_dir_path + '\\dp0_zonal_stats')
    os.mkdir(dp0_zonal_stats_output_dir)

    dp0_mask_zonal_stats_output_dir = (export_dir_path + '\\dp0_mask_zonal_stats')
    os.mkdir(dp0_mask_zonal_stats_output_dir)

    dp1_zonal_stats_output_dir = (export_dir_path + '\\dp1_zonal_stats')
    os.mkdir(dp1_zonal_stats_output_dir)

    dp1_mask_zonal_stats_output_dir = (export_dir_path + '\\dp1_mask_zonal_stats')
    os.mkdir(dp1_mask_zonal_stats_output_dir)


    return dbg_tile_status_dir, dbg_zonal_stats_output_dir, dbg_mask_tile_status_dir, dbg_mask_zonal_stats_output_dir, \
        dbi_tile_status_dir, dbi_zonal_stats_output_dir, dbi_mask_tile_status_dir, dbi_mask_zonal_stats_output_dir, \
        dp0_tile_status_dir, dp0_zonal_stats_output_dir, dp0_mask_tile_status_dir, dp0_mask_zonal_stats_output_dir, \
        dp1_tile_status_dir, dp1_zonal_stats_output_dir, dp1_mask_tile_status_dir, dp1_mask_zonal_stats_output_dir

def main_routine():
    """" Description: This script determines which Landsat tile had the most non-null zonal statistics records per site
    and files those plots (bare ground, all bands and interactive) into final output folders. """

    # print('fcZonalStatsPipeline.py INITIATED.')
    # read in the command arguments
    cmd_args = get_cmd_args_fn()
    data = cmd_args.data
    tile_grid = cmd_args.tile_grid
    export_dir = cmd_args.export_dir
    lsat_dir = cmd_args.lsat_dir
    no_data = int(cmd_args.no_data)
    path = cmd_args.path
    row = cmd_args.row
    zone = cmd_args.zone
    burn_dir = cmd_args.burn_dir
    image_count = int(cmd_args.image_count)

    # call the temporaryDir function.
    temp_dir_path, final_user = temporary_dir_fn()
    # call the tempDirFolders function.
    prime_temp_grid_dir, prime_temp_buffer_dir, zonal_stats_ready_dir = temp_dir_folders_fn(temp_dir_path)
    # call the exportFilepath function.
    export_dir_path = export_file_path_fn(export_dir, final_user, path, row)
    print("zonal_stats_ready_dir: ", zonal_stats_ready_dir)
    # # create a list of variable subdirectories
    # sub_dir_list = next(os.walk(lsat_dir))[1]

    lsat_tile = str(path) + "_" + str(row)
    #
    # call the exportDirFolders function.
    dbg_tile_status_dir, dbg_zonal_stats_output_dir, dbg_mask_tile_status_dir, dbg_mask_zonal_stats_output_dir, \
        dbi_tile_status_dir, dbi_zonal_stats_output_dir, dbi_mask_tile_status_dir, dbi_mask_zonal_stats_output_dir, \
        dp0_tile_status_dir, dp0_zonal_stats_output_dir, dp0_mask_tile_status_dir, dp0_mask_zonal_stats_output_dir, \
        dp1_tile_status_dir, dp1_zonal_stats_output_dir, dp1_mask_tile_status_dir, \
        dp1_mask_zonal_stats_output_dir = export_dir_folders_fn(export_dir_path, lsat_tile)


    # export_dir_folders_fn(export_dir_path, lsat_tile)

    #print("data: ", data)
    import step1_3_project_buffer
    geo_df2, crs_name = step1_3_project_buffer.main_routine(data, zone, export_dir_path, prime_temp_buffer_dir)

    import step1_4_landsat_tile_grid_identify2
    comp_geo_df, zonal_stats_ready_dir = step1_4_landsat_tile_grid_identify2.main_routine(
        tile_grid, geo_df2, data, zone, export_dir_path, prime_temp_grid_dir)


    print("zonal_stats_ready_dir: ", zonal_stats_ready_dir)
    comp_geo_df.to_file(os.path.join(export_dir_path, "biomass_1ha.shp"))
    print("comp_geo_df: ", comp_geo_df)


    tile = str(path) + str(row)
    print("tile: ", tile)
    geo_df3 = comp_geo_df[comp_geo_df["tile"] == tile]
    print("geo_df3: ", geo_df3)
    geo_df4 = geo_df3[["site_name", "tile", "geometry"]]
    print("geodf4: ", geo_df4)


    geo_df4.reset_index(drop=True, inplace=True)
    geo_df4['uid'] = geo_df4.index + 1

    shapefile_path = os.path.join(export_dir_path, "biomass_1ha_{0}.shp".format(str(path) + "_" + str(row)))
    geo_df4.to_file(os.path.join(export_dir_path, "biomass_1ha_{0}.shp".format(str(path) + "_" + str(row))),
                    driver="ESRI Shapefile")

    print("Exported shapefile: ", shapefile_path)


    # --------------------------------------------------- DP1 WORKING---------------------------------------------------

    extension = "dp1"
    no_data = 255.0

    print("DP1_" * 50)

    # call the step1_5_dp1_landsat_list.py script.
    import step1_5_dp1_landsat_list2
    step1_5_dp1_landsat_list2.main_routine(
        export_dir_path, geo_df3, image_count, lsat_dir, path, row, zone, extension)

    # define the tile for processing directory.
    dp1_tile_for_processing_dir = (dp1_tile_status_dir + '\\dp1_for_processing')
    print('-' * 50)

    dp1_zonal_stats_output = (export_dir_path + '\\dp1_zonal_stats')
    # print('dp1 zonal_stats_output: ', dp1_zonal_stats_output)
    dp1_list_zonal_tile = []

    for file in glob.glob(dp1_tile_for_processing_dir + '\\*.csv'):
        print(file)
        # append tile paths to list.
        dp1_list_zonal_tile.append(file)

    print("-" * 50)
    print("dp1: ", dp1_list_zonal_tile)

    if len(dp1_list_zonal_tile) >= 1:
        #
        for csv_file in dp1_list_zonal_tile:
            print("csv_file: ", csv_file)

            # call the step1_6_dp1_zonal_stats.py script.
            # import step1_6_dp1_zonal_stats
            # dp1_output_zonal_stats, dp1_complete_tile, dp1_tile, dp1_temp_dir_bands = step1_6_dp1_zonal_stats.main_routine(
            #     temp_dir_path, zonal_stats_ready_dir, no_data, csv_file, dp1_zonal_stats_output, shapefile_path, "dp1")


        # -------------------------------------------------- DP1 fire ----------------------------------------------------

        print("dp1_list_zonal_tile: ", dp1_list_zonal_tile)
        for i in dp1_list_zonal_tile:
            print("Checking if there is a fire mask for: ", i)
            import run_fire_scar_mask_lsat_dp1
            run_fire_scar_mask_lsat_dp1.main_routine(i, zone, temp_dir_path, tile, burn_dir)

            # call the step1_5_dp1_landsat_list.py script.


        import step1_5_dp1_landsat_list_fire_mask
        step1_5_dp1_landsat_list_fire_mask.main_routine(
            export_dir_path, geo_df3, image_count, lsat_dir, path, row, zone, extension)

        # define the tile for processing directory.
        dp1_mask_tile_for_processing_dir = (dp1_mask_tile_status_dir + '\\dp1_mask_for_processing')
        print('-' * 50)

        dp1_mask_zonal_stats_output = (export_dir_path + '\\dp1_mask_zonal_stats')
        dp1_mask_list_zonal_tile = []


        print("looking for dp1 masked data: ", dp1_mask_tile_for_processing_dir)
        for file in glob.glob(dp1_mask_tile_for_processing_dir + '\\*.csv'):
            print("dp1 masked data ready for zonal stats: ", file)
            # append tile paths to list.
            dp1_mask_list_zonal_tile.append(file)

        print("-" * 50)
        print("dp1 MASK: ", dp1_mask_list_zonal_tile)
        print("length: ", len(dp1_mask_list_zonal_tile))

        if len(dp1_mask_list_zonal_tile) >= 1:
            #
            for csv_file in dp1_mask_list_zonal_tile:
                print("csv_file: ", csv_file)
                #call the step1_6_dp1_mask_zonal_stats.py script.
                # import step1_6_dp1_mask_zonal_stats
                # dp1_output_zonal_stats, dp1_complete_tile, dp1_tile, dp1_temp_dir_bands = step1_6_dp1_mask_zonal_stats.main_routine(
                #     temp_dir_path, zonal_stats_ready_dir, no_data, csv_file, dp1_mask_zonal_stats_output,
                #     shapefile_path, "dp1")

        else:
            print("No dp1 mask images were located")

    else:
        print("No dp1 images were located")


    # --------------------------------------------------- DP0 --------------------------------------------------

    extension = "dp0"
    no_data = 255.0

    print('-' * 50)
    print('DP0-' * 50)
    print('-' * 50)

    import step1_5_dp0_landsat_list3
    step1_5_dp0_landsat_list3.main_routine(
        export_dir_path, geo_df3, image_count, lsat_dir, path, row, zone, extension)

    # define the tile for processing directory.
    dp0_tile_for_processing_dir = (dp0_tile_status_dir + '\\dp0_for_processing')

    print('-' * 50)
    print('DP0-' * 50)
    print('-' * 50)

    dp0_zonal_stats_output = (export_dir_path + '\\dp0_zonal_stats')
    # print('dp0 zonal_stats_output: ', dp0_zonal_stats_output)
    dp0_list_zonal_tile = []

    for file in glob.glob(dp0_tile_for_processing_dir + '\\*.csv'):
        print(file)
        # append tile paths to list.
        dp0_list_zonal_tile.append(file)

    print("-" * 50)
    print("dp0: ", dp0_list_zonal_tile)

    if len(dp0_list_zonal_tile) >= 1:
        #
        for csv_file in dp0_list_zonal_tile:
            print("csv_file: ", csv_file)
            # call the step1_6_dp0_zonal_stats.py script.

            print("dp0_zonal_stats_output: ", dp0_zonal_stats_output)
            # import step1_6_dp0_zonal_stats3
            # dp0_output_zonal_stats, dp0_complete_tile, dp0_tile, dp0_temp_dir_bands = step1_6_dp0_zonal_stats3.main_routine(
            #     temp_dir_path, zonal_stats_ready_dir, no_data, csv_file, dp0_zonal_stats_output, shapefile_path, "dp0")

            # ------------------------------------------- DP0 fire -----------------------------------------------------

            # print("dp0_list_zonal_tile: ", dp0_list_zonal_tile)
            for i in dp0_list_zonal_tile:
                print("i of list: ", i)

                import run_fire_scar_mask_lsat_dp0_zstdmask
                run_fire_scar_mask_lsat_dp0_zstdmask.main_routine(i, zone, temp_dir_path, tile, burn_dir)

            print("Run list of masks")

            import step1_5_dp0_landsat_list_fire_mask
            step1_5_dp0_landsat_list_fire_mask.main_routine(
                export_dir_path, geo_df3, image_count, lsat_dir, path, row, zone, extension)

            # define the tile for processing directory.
            dp0_mask_tile_for_processing_dir = (dp0_mask_tile_status_dir + '\\dp0_mask_for_processing')

            print('-' * 50)
            print(dp0_mask_tile_for_processing_dir)

            dp0_mask_zonal_stats_output = (export_dir_path + '\\dp0_mask_zonal_stats')
            # print('dp0 zonal_stats_output: ', dp0_zonal_stats_output)
            dp0_mask_list_zonal_tile = []

            for file in glob.glob(dp0_mask_tile_for_processing_dir + '\\*.csv'):
                print(file)
                # append tile paths to list.
                dp0_mask_list_zonal_tile.append(file)

            print("dp0 MASK: ", dp0_mask_list_zonal_tile)

            if len(dp0_mask_list_zonal_tile) >= 1:
                #
                for csv_file in dp0_mask_list_zonal_tile:
                    print("csv_file: ", csv_file)
                    # call the step1_6_dp0_zonal_stats.py script.

                    print("dp0_mask_zonal_stats_output: ", dp0_mask_zonal_stats_output)
                    import step1_6_dp0_mask_zonal_stats
                    dp0_output_zonal_stats, dp0_complete_tile, dp0_tile, dp0_temp_dir_bands = step1_6_dp0_mask_zonal_stats.main_routine(
                        temp_dir_path, zonal_stats_ready_dir, no_data, csv_file, dp0_mask_zonal_stats_output, shapefile_path, "dp0")

            else:
                print("No dp0 images were located")

    else:
        print("No dp0 images were located")

    # ------------------------------------------------------ DBG -------------------------------------------------------

    extension = "dbg"
    no_data = 32767.0

    print('-' * 50)
    print('DBG-' * 50)
    print('-' * 50)

    import step1_5_dbg_landsat_list3
    step1_5_dbg_landsat_list3.main_routine(
        export_dir_path, geo_df3, image_count, lsat_dir, path, row, zone, extension)

    # define the tile for processing directory.
    dbg_tile_for_processing_dir = (dbg_tile_status_dir + '\\dbg_for_processing')
    print('-' * 50)

    dbg_zonal_stats_output = (export_dir_path + '\\dbg_zonal_stats')
    print('dbg_zonal_stats_output: ', dbg_zonal_stats_output)
    dbg_list_zonal_tile = []

    for file in glob.glob(dbg_tile_for_processing_dir + '\\*.csv'):
        print(file)
        # append tile paths to list.
        dbg_list_zonal_tile.append(file)

    print("-" * 50)
    print("dbg: ", dbg_list_zonal_tile)

    if len(dbg_list_zonal_tile) >= 1:

        for csv_file in dbg_list_zonal_tile:

            # call the step1_6_dbg_zonal_stats.py script.
            print("csv_file: ", csv_file)
            # import step1_6_dbg_zonal_stats3
            # dbg_output_zonal_stats, dbg_complete_tile, dbg_tile, dbg_temp_dir_bands = step1_6_dbg_zonal_stats3.main_routine(
            #     temp_dir_path, zonal_stats_ready_dir, no_data, csv_file, dbg_zonal_stats_output, shapefile_path, "dbg")

        #  --------------------------------------------- dbg fire  -----------------------------------------------------

        for i in dbg_list_zonal_tile:
            print("Creating fire mask for dbg: ", i)

            import run_fire_scar_mask_lsat_dbg_zstdmask
            run_fire_scar_mask_lsat_dbg_zstdmask.main_routine(i, zone, temp_dir_path, tile, burn_dir)
            print("created...")
            print("-" * 50)

        import step1_5_dbg_landsat_list_fire_mask
        step1_5_dbg_landsat_list_fire_mask.main_routine(
            export_dir_path, geo_df3, image_count, lsat_dir, path, row, zone, extension)

        # define the tile for processing directory.
        dbg_mask_tile_for_processing_dir = (dbg_mask_tile_status_dir + '\\dbg_mask_for_processing')
        print('-' * 50)

        dbg_mask_zonal_stats_output = (export_dir_path + '\\dbg_mask_zonal_stats')
        print('dbg_mask_zonal_stats_output: ', dbg_mask_zonal_stats_output)
        dbg_mask_list_zonal_tile = []

        for file in glob.glob(dbg_mask_tile_for_processing_dir + '\\*.csv'):
            print(file)
            # append tile paths to list.
            dbg_mask_list_zonal_tile.append(file)

        print("-" * 50)
        print("dbg: ", dbg_mask_list_zonal_tile)

        if len(dbg_mask_list_zonal_tile) >= 1:

            for csv_file in dbg_mask_list_zonal_tile:
                # call the step1_6_dbg_zonal_stats.py script.
                print("csv_file: ", csv_file)
                import step1_6_dbg_mask_zonal_stats
                dbg_output_zonal_stats, dbg_complete_tile, dbg_tile, dbg_temp_dir_bands = step1_6_dbg_mask_zonal_stats.main_routine(
                    temp_dir_path, zonal_stats_ready_dir, no_data, csv_file, dbg_mask_zonal_stats_output, shapefile_path,
                    "dbg")

        else:
            print("No dbg fire mask images were located")


    else:
        print("No dbg images were located")

    # # ---------------------------------------------------- DBI --------------------------------------------------------

    extension = "dbi"
    no_data = 32767.0

    print('-' * 50)
    print('DBI-' * 50)
    print('-' * 50)

    import step1_5_dbi_landsat_list
    step1_5_dbi_landsat_list.main_routine(
        export_dir_path, geo_df3, image_count, lsat_dir, path, row, zone, extension)

    # define the tile for processing directory.
    dbi_tile_for_processing_dir = (dbi_tile_status_dir + '\\dbi_for_processing')
    print('-' * 50)

    dbi_zonal_stats_output = (export_dir_path + '\\dbi_zonal_stats')
    print('dbi_zonal_stats_output: ', dbi_zonal_stats_output)
    dbi_list_zonal_tile = []

    print("Looking here: ", dbi_tile_for_processing_dir)

    for file in glob.glob(dbi_tile_for_processing_dir + '\\*.csv'):
        print(file)
        # append tile paths to list.
        dbi_list_zonal_tile.append(file)


    print("-" * 50)
    print("dbi: ", dbi_list_zonal_tile)
    print("=" * 50)


    if len(dbi_list_zonal_tile) >= 1:

        for csv_file in dbi_list_zonal_tile:
            # call the step1_6_dbi_zonal_stats.py script.
            print("csv_file: ", csv_file)
            # import step1_6_dbi_zonal_stats
            # dbi_output_zonal_stats, dbi_complete_tile, dbi_tile, dbi_temp_dir_bands = step1_6_dbi_zonal_stats.main_routine(
            #     temp_dir_path, zonal_stats_ready_dir, no_data, csv_file, dbi_zonal_stats_output, shapefile_path, "dbi")

        #  ---------------------------------------------------- dbi fire  -------------------------------------------

        for i in dbi_list_zonal_tile:
            import run_fire_scar_mask_lsat_dbi_v2
            run_fire_scar_mask_lsat_dbi_v2.main_routine(i, zone, temp_dir_path, tile, burn_dir)

        import step1_5_dbi_landsat_list_fire_mask
        step1_5_dbi_landsat_list_fire_mask.main_routine(
            export_dir_path, geo_df3, image_count, lsat_dir, path, row, zone, extension)

        # define the tile for processing directory.
        dbi_mask_tile_for_processing_dir = (dbi_mask_tile_status_dir + '\\dbi_mask_for_processing')
        print('-' * 50)

        dbi_mask_zonal_stats_output = (export_dir_path + '\\dbi_mask_zonal_stats')
        print('dbi_mask_zonal_stats_output: ', dbi_mask_zonal_stats_output)
        dbi_mask_list_zonal_tile = []

        for file in glob.glob(dbi_mask_tile_for_processing_dir + '\\*.csv'):
            print(file)
            # append tile paths to list.
            dbi_mask_list_zonal_tile.append(file)

        print("-" * 50)
        print("dbi: ", dbi_mask_list_zonal_tile)
        print("=" * 50)

        if len(dbi_mask_list_zonal_tile) >= 1:

            for csv_file in dbi_mask_list_zonal_tile:
                # call the step1_6_dil_zonal_stats.py script.
                print("csv_file: ", csv_file)
                import step1_6_dbi_mask_zonal_stats
                dbi_output_zonal_stats, dbi_complete_tile, dbi_tile, dbi_temp_dir_bands = step1_6_dbi_mask_zonal_stats.main_routine(
                    temp_dir_path, zonal_stats_ready_dir, no_data, csv_file, dbi_mask_zonal_stats_output, shapefile_path,
                    "dbi")

        else:
            print("No dbi images were located")

    else:
        print("No dbi images were located")

    # ---------------------------------------------------- Clean up ----------------------------------------------------

    shutil.rmtree(temp_dir_path)
    print('Temporary directory and its contents has been deleted from your working drive.')
    print(' - ', temp_dir_path)
    print('fractional cover zonal stats pipeline is complete.')
    print('goodbye.')


if __name__ == '__main__':
    main_routine()
