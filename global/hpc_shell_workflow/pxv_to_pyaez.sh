#! /bin/bash
set -e # exit on error

# Description
# this script strings together all codes that process gaez pxv files to pyaez inputs

# RUN INSTRUCTIONS
# Run this from a compute node (not a login node). 
# You can access a compute node by launching a JupyterLab terminal or by issuing the srun command.
# YOU MUST ACTIVATE A CONDA ENVIRONMENT WITH THE APPROPRIATE PACKAGES INSTALLED e.g. /work/hpc/users/kerrie/envs/dataprep 

########################################
# UPDATE THESE PARAMETERS BEFORE RUNNING
########################################
experiment="Hist" 
years=("2021") # "2022")
# variables=("Precip" "Srad" "Tmax-2m" "Tmin-2m" "Vapr" "Wind-10m")
# variables=("Tmin-2m" "Vapr" "Wind-10m")
variables=("Vapr"  "Wind-10m")
# varelev="Elev"
# varmask="Mask"
fillval=-9999.

# directory with the pxv daily deviation files
# pxv_dir="/work/hpc/datasets/un_fao/gaez_v5_2023OCT/"$experiment"/daily5m/"
pxv_dir="/work/hpc/datasets/un_fao/gaez_v5/clim/AgERA5/"$experiment"/"

# directory with the rst monthly mean files
# rst_dir="/work/hpc/datasets/un_fao/gaez_v5_2023OCT/"$experiment"/monthly5m/"

# directory with the gaez static mask and elev files
# static_dir="/work/hpc/datasets/un_fao/gaez_v5_2023OCT/static/"
static_dir="/work/hpc/datasets/un_fao/gaez_v5/land/"

# gaez mask file name
maskfile="ALOSmask5m_fill.rst"

# gaez elev file name
# elevfile="ALOSdem5m_fill.rst"

# directory for writing intermediate files (dat, netcdf, etc)
# temp_dir="/work/hpc/datasets/un_fao/gaez_v5_intermediate/"
temp_dir="/work/hpc/datasets/un_fao/gaez_v5_intermediate/"
dat_dir="dat/"

# directory for writing the final npy and tif inputs for pyaez
output_dir="/work/hpc/datasets/un_fao/pyaez/global_daily/"

# echo $experiment
# echo $years
# echo $variables
# echo ${variables[@]}
# echo ${variables[1]}
# echo $pxv_dir
# echo $temp_dir
# echo $static_dir$maskfile
########################################
########################################
########################################


connector="_"

# pre-loop things for 00_pxv_to_dat_daily.f95
echo "compiling 01_pxv_to_dat_monthly.f95"
gfortran 01_pxv_to_dat_monthly.f95 -o 01_RunMe

# pre-loop things for 00_pxv_to_dat_daily.f95
echo "compiling 02_pxv_to_dat_daily.f95"
gfortran 02_pxv_to_dat_daily.f95 -o 02_RunMe





for y in "${years[@]}"
do

    #############################################################################################
    echo "########################################################################"
    echo "running script 01_pxv_to_dat_monthly.f95"
    echo "########################################################################"
    # this script translates the gaez monthly mean data from binary to human readable format
    # OUTPUT GOES TO $temp_dir/dat/
    for v in "${variables[@]}"
    do
        echo "processing $v"
        if [ $v == "Precip" ]
        then
            orion_dirname="prec/"
        elif [ $v == "Srad" ]
        then
            orion_dirname="srad/"
        elif [ $v == "Tmax-2m" ]
        then
            orion_dirname="tmax/"
        elif [ $v == "Tmin-2m" ]
        then
            orion_dirname="tmin/"
        elif [ $v == "Vapr" ]
        then
            orion_dirname="vapr/"
        elif [ $v == "Wind-10m" ]
        then
            orion_dirname="wind/"
        else
            echo "variable $v must be Precip, Srad, Tmax-2m, Tmin-2m, Vapr, or Wind-10m"
        fi             

         ./01_RunMe $y $v $experiment$connector $pxv_dir$orion_dirname $static_dir$maskfile $temp_dir$dat_dir
    done
    #############################################################################################


    #############################################################################################
    echo "########################################################################"
    echo "running script 02_pxv_to_dat_daily.f95"
    echo "########################################################################"
    # this script translates the gaez daily deviation data from binary to human readable format
    # OUTPUT GOES TO $temp_dir/dat/
    for v in "${variables[@]}"
    do
        echo "processing $v"
        if [ $v == "Precip" ]
        then
            orion_dirname="prec/"
        elif [ $v == "Srad" ]
        then
            orion_dirname="srad/"
        elif [ $v == "Tmax-2m" ]
        then
            orion_dirname="tmax/"
        elif [ $v == "Tmin-2m" ]
        then
            orion_dirname="tmin/"
        elif [ $v == "Vapr" ]
        then
            orion_dirname="vapr/"
        elif [ $v == "Wind-10m" ]
        then
            orion_dirname="wind/"
        else
            echo "variable $v must be Precip, Srad, Tmax-2m, Tmin-2m, Vapr, or Wind-10m"
        fi             
         ./02_RunMe $y $v $experiment$connector $pxv_dir$orion_dirname $static_dir$maskfile $temp_dir$dat_dir
    done
    #############################################################################################


    if [ $y == '2020' ]
    then
        echo "########################################################################"
        echo "running script 03_create_all_masks.py"
        echo "########################################################################"
        # we're processing the year 2020 first so this script is set only to run for 2020
        # data contains 3 masks. One from ALOSmask5m_fill.rst, 
        # one created from the monthly pxv file, one created from the daily pxv files 
        # see the python script or jupyter notebook for more information
        # NOTE: THIS SCRIPT IS HARD CODED TO USE THE FOLLOWING FILES AND WILL NEED UPDATING
        # IF THESE FILES CHANGE IN CONTENT, LOCATION, OR NAME
        # /work/hpc/datasets/un_fao/gaez_v5/land/ALOSmask5m_fill.rst
        # temp_dir dat/Precip_AgERA5_Hist_2020_5m.dat
        # temp_dir dat/Precip365_AgERA5_Hist_2020_5m.dat'
        # OUTPUT GOES TO temp_dir static/
        python 03_create_all_masks.py $static_dir$maskfile $temp_dir
     fi


    echo "########################################################################"
    echo "running script 04_dat_to_nc_monthly.py"
    echo "########################################################################"
    # this script puts the gaez monthly mean data on a global grid so we can inspect it easily with plots
    # plots are output to $temp_dir/plots/
    # netcdf files are output to $temp_dir/netcdf/
    for v in "${variables[@]}"
    do
        echo "##### processing $v #####"
        python 04_dat_to_nc_monthly.py $y $fillval $v $experiment $temp_dir 
    done


    echo "########################################################################"
    echo "running script 05_dat_to_nc_daily.py"
    echo "########################################################################"
    # this script puts the gaez daily deviation data on a global grid so we can inspect it easily with plots
    # plots are output to $temp_dir/plots/
    # netcdf files are output to $temp_dir/netcdf/
    for v in "${variables[@]}"
    do
        echo "##### processing $v #####"
        python 05_dat_to_nc_daily.py $y $fillval $v $experiment $temp_dir 
    done
    
    
    echo "########################################################################"
    echo "running script 06_create_daily.py"
    echo "########################################################################"
    # this script combines gaez daily dev and monthly mean files to get daily data values, converts to 
    # pyaez units, and creates relative humidity
    # plots are output to $temp_dir/plots/
    # netcdf files are output to $temp_dir/netcdf/    
    for v in "${variables[@]}"
    do
        echo "##### processing $v #####"
        python 06_create_daily.py $y $v $experiment $temp_dir 
    done    
    
    
    echo "########################################################################"
    echo "running script 07_nc_to_npy.py"
    echo "########################################################################"
    # this script 
    # npy and tiff files are output to $output_dir    
    for v in "${variables[@]}"
    do
        if [ $v == 'Vapr' ] 
        then
            v="Rhum"
        fi
        if [ $v == 'Wind-10m' ] 
        then
            v="Wind-2m"            
        fi
        echo "##### processing $v #####"
        python 07_nc_to_npy.py $y $v $experiment $temp_dir $output_dir
    done      


#     echo "running script 02_rst_to_nc.py"
#     # this script plots gaez monthly mean data and also
#     # converts rst to netcdf, because the math in future
#     # scripts is much easier with xarray
#     # plot output to temp_dir/plots
#     python 02_rst_to_nc.py $y $fillval $varelev $static_dir$maskfile $temp_dir
#     python 02_rst_to_nc.py $y $fillval $varelev $static_dir$maskfile $temp_dir
#     for v in "${variables[@]}"
#     do
#         echo "processing $v"
#         python 01_rst_to_nc.py $y $fillval $v $static_dir$maskfile $temp_dir 
#     done    
done