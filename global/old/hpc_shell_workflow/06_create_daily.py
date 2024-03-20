import numpy as np
import xarray as xr 
import dask
import pandas as pd 
import glob
from dask_jobqueue import SLURMCluster
from dask.distributed import Client
import sys
import os
import matplotlib.pyplot as plt

# get command line arguments
year=str(sys.argv[1])
varname=str(sys.argv[2])
experiment=str(sys.argv[3])
temp_dir=str(sys.argv[4])

# metadata and encoding for writing netcdf files
timeattrs={'standard_name':'time','long_name':'time','axis':'T'}
latattrs={'standard_name':'latitude','long_name':'latitude','units':'degrees_north','axis':'Y'}
lonattrs={'standard_name':'longitude','long_name':'longitude','units':'degrees_east','axis':'X'}
time_encoding={'calendar':'standard','units':'days since 1900-01-01 00:00:00','_FillValue':None}
lat_encoding={'_FillValue':None}
lon_encoding={'_FillValue':None}
var_encoding = {'zlib':True,'dtype':'float32'}

source_data=[temp_dir+' DailyDev and Monthly nc files']
source_code='https://github.com/kerriegeil/pyAEZ_data_prep/blob/main/global/06_create_daily.ipynb'

# for dask array automatic parallel computing
chunks={'time':-1,'lat':200,'lon':-1}

# get files names
ddfile=glob.glob(temp_dir+'netcdf/'+varname+'_'+experiment+'_DailyDev_'+year+'_5m.nc')[0] # daily deviations
mfile=glob.glob(temp_dir+'netcdf/'+varname+'_'+experiment+'_Monthly_'+year+'_5m.nc')[0] # monthly data

# create a directory for the dask worker logs
homedir = os.environ['HOME']
daskpath=os.path.join(homedir, "dask-worker-space-can-be-deleted")
try: 
    os.mkdir(daskpath) 
except OSError as error: 
    pass

# connect to a single compute node (20 cores, 40 threads)
print('starting a compute cluster...')
try:
    cluster = SLURMCluster(
        queue='bigmem',#'orion',
        account="191000-nf0001",
        processes=1,
        cores=20,
        memory='240GB',#'160GB',
        walltime="00:6:00",
        log_directory=daskpath)
except:
    sys.exit('Error starting a cluster with dask SLURMCluster')

client=Client(cluster)
nworkers=1
cluster.scale(nworkers)
client.wait_for_workers(nworkers,timeout=300)

print('Computing...')

# function to lazy calculate daily values as daily_value = daily_deviation + monthly_mean
def calc_daily_ds(v):
    # get data 
    var_prime=xr.open_dataset(ddfile,chunks=chunks)[v] # daily devs
    var_mean=xr.open_dataset(mfile,chunks=chunks)[v] # monthly means
    
    # save variable metadata and rework for the groupby below
    varattrs=var_mean.attrs
    del varattrs['description']
    var_mean=var_mean.rename({'time':'month'})
    months=np.arange(12)+1
    var_mean['month']=months

    # lazy compute
    with dask.config.set(**{'array.slicing.split_large_chunks': False}):    
        var_daily=var_prime.groupby('time.month') + var_mean
    
    # clean up variable metadata
    var_daily=var_daily.drop('month')
    var_daily.attrs=varattrs

    # convert array to dataset and set global attributes
    ds=var_daily.to_dataset()
    ds=ds.assign_attrs({'source_data':source_data,'source_code':source_code})    
    return ds  

# function to lazy calculate daily_value = fraction_of_monthly_total * monthly_total
def calc_daily_precip_ds(v):
    # get data 
    var_frac=xr.open_dataset(ddfile,chunks=chunks)[v] # daily frac
    var_acc=xr.open_dataset(mfile,chunks=chunks)[v] # monthly acc   
    
    # save variable metadata and rework for the groupby below
    varattrs=var_acc.attrs
    del varattrs['description']
    var_acc=var_acc.rename({'time':'month'})
    months=np.arange(12)+1
    var_acc['month']=months

    # lazy compute
    with dask.config.set(**{'array.slicing.split_large_chunks': False}):        
        var_daily=var_frac.groupby('time.month')*var_acc  # times here instead of add
    
    # clean up variable metadata
    var_daily=var_daily.drop('month')
    var_daily.attrs=varattrs

    # convert array to dataset and set global attributes
    ds=var_daily.to_dataset()
    ds=ds.assign_attrs({'source_data':source_data,'source_code':source_code})
    return ds  


# call lazy compute 
if varname in ['Srad','Tmax-2m','Tmin-2m','Vapr','Wind-10m']:
    ds=calc_daily_ds(varname)
elif varname == 'Precip':
    ds=calc_daily_precip_ds(varname)
    ds[varname].attrs['units']='mm/day'
else:
    print('Variable name',varname,'not recognized')
    sys.exit()


# other edits needed to make variables appropriate for pyaez inputs
if varname == 'Srad':
    print('changing units to W/m2')
    attrs=ds[varname].attrs
    attrs['units']='W/m2'
    
    # Convert J/m2/day to W/m2
    s_per_day=86400
    ds[varname]=ds[varname]/s_per_day
    ds[varname].attrs=attrs    
    
if varname == 'Wind-10m':
    # interp from 10m to 2m height
    print('interpolating wind to 2m')
    z=10
    z_adjust=4.87/(np.log(67.8*z-5.42))
    ds[varname]=ds[varname]*z_adjust
    
    # fix metadata
    newvarname='Wind-2m'
    attrs={'standard_name':newvarname,'long_name':'2m Wind Speed','units':'m/s'}
    ds=ds.rename({varname:newvarname})
    ds[newvarname].attrs=attrs
    varname=newvarname
    

# compute and write file, expect about 2-3 min per variable
outfile=temp_dir+'netcdf/'+varname+'_'+experiment+'_daily_'+year+'_5m.nc'
print('writing',outfile)
# at some point the distributed file writing below became unreliable and the file would open and hang without writing
# work around is to load the computation result fully into memory then write the file
# write_job=ds.to_netcdf(outfile,
#             encoding={'lat':lat_encoding,'lon':lon_encoding,'time':time_encoding,varname:var_encoding},
#             compute=False)
# write_job.compute()
ds.to_netcdf(outfile,encoding={'lat':lat_encoding,'lon':lon_encoding,'time':time_encoding,varname:var_encoding})

    
# after writing Vapr
# create relative humidity
if varname == 'Vapr':
    print('computing relative humidity...')
    try:
        vapr=xr.open_dataset(temp_dir+'netcdf/Vapr_'+experiment+'_daily_'+year+'_5m.nc',chunks=chunks)['Vapr']*0.1
    except:
        sys.exit('Cant load daily Vapr data. To output relative humidity, you must process Vapr, Tmax-2m, Tmin-2m first')
                 
    try:
        tmax=xr.open_dataset(temp_dir+'netcdf/Tmax-2m_'+experiment+'_daily_'+year+'_5m.nc',chunks=chunks)['Tmax-2m']
    except:
        sys.exit('Cant load daily Tmax data. To output relative humidity, you must process Vapr, Tmax-2m, Tmin-2m first')

    try:
        tmin=xr.open_dataset(temp_dir+'netcdf/Tmin-2m_'+experiment+'_daily_'+year+'_5m.nc',chunks=chunks)['Tmin-2m']
    except:
        sys.exit('Cant load daily Tmin data. To output relative humidity, you must process Vapr, Tmax-2m, Tmin-2m first')

    # saturation vapor pressure (lazy)
    vapr_sat=0.5*( np.exp((17.27*tmax)/(tmax+237.3)) + np.exp((17.27*tmin)/(tmin+237.3)) )
              
    # relative humidity (lazy)
    Rhum=(vapr/vapr_sat) # fraction not percent                
             
    # fix up metadata
    Rhum.name='Rhum'
    attrs={'standard_name':Rhum.name,'long_name':'relative humidity','units':'-'}
    Rhum.attrs=attrs

    # convert array to dataset and set global attributes
    ds=Rhum.to_dataset()
    ds=ds.assign_attrs({'source_data':source_data,'source_code':source_code})       
    
    # compute and write file, expect about 7 min
    varname=Rhum.name
    outfile=temp_dir+'netcdf/'+varname+'_'+experiment+'_daily_'+year+'_5m.nc'
    print('writing',outfile)
    # at some point the distributed file writing below became unreliable and the file would open and hang without writing
    # work around is to load the computation result fully into memory then write the file
    # write_job=ds.to_netcdf(outfile,
    #             encoding={'lat':lat_encoding,'lon':lon_encoding,'time':time_encoding,varname:var_encoding},
    #             compute=False)
    # write_job.compute()
    ds.to_netcdf(outfile,encoding={'lat':lat_encoding,'lon':lon_encoding,'time':time_encoding,varname:var_encoding})    
try:
    client.shutdown()
except:
    pass