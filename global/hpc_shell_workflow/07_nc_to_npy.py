import glob
import xarray as xr
import dask.array as da
import dask
# import rioxarray as rio
# import numpy as np
import os
import sys


# get command line arguments
year=str(sys.argv[1])
var_in=str(sys.argv[2])
experiment=str(sys.argv[3])
temp_dir=str(sys.argv[4])
npy_dir=str(sys.argv[5])

nc_dir = temp_dir+'netcdf/'
# static_dir = temp_dir+'static/'

# if var_in=='Wind-10m': var_in='Wind-2m'
# if var_in=='Vapr': var_in='Rhum'
var_out=var_in+'365'

chunks={'time':-1,'lat':450,'lon':2160}

with dask.config.set(**{'array.slicing.split_large_chunks': False}):
    try:
        f = glob.glob(nc_dir+var_in+'_'+experiment+'_daily_'+year+'_5m.nc')
    except:
        pass
    
    if f:
        print('reading',f[0])
        try:
            if (int(year)>=1981) & (int(year)<=2024):
                dropdate=year+'02-29'
            else: 
                dropdate='1900-02-29'
            data = xr.open_dataset(f[0],chunks=chunks)[var_in].sel(lat=slice(90,-60.)).drop_sel(time=dropdate).transpose('lat','lon','time').data
        except:
            data = xr.open_dataset(f[0],chunks=chunks)[var_in].sel(lat=slice(90,-60.)).transpose('lat','lon','time').data

        # set up dir for writing npy
        out_dir=npy_dir+year+'/'+var_out+'/'
        isExist = os.path.exists(out_dir)
        if not isExist:
            os.makedirs(out_dir)
            
        # write npy data
        print('writing to',out_dir+'0.npy')     
        da.to_npy_stack(out_dir,data,axis=2)          
    else:
        print('no file',f)