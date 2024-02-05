import numpy as np
import glob
import rioxarray as rio
import xarray as xr
# from natsort import natsorted 
import matplotlib.pyplot as plt
import pandas as pd

# get command line arguments
year=int(sys.argv[1])
fillval=np.float32(sys.argv[2])
varname=str(sys.argv[3])
maskfile=str(sys.argv[4])
elevfile=str(sys.argv[5])
data_dir=str(sys.argv[6])
temp_dir=str(sys.argv[7])

# # input file name components
# dataset='AgERA5'
# # experiment='Hist'
# # year='2020'
# months=[str(x).rjust(2,'0') for x in np.arange(1,13)]
# gridsize='5m'
# file_ext_d='.pxv'
# file_exts_m=['.rst','.rdc']
# variables=['Precip','Srad','Tmax-2m','Tmin-2m','Vapr','Wind-10m','Elevation']



#  metadata & encoding to include in netcdf files
source_dirs=['on Orion '+temp_dir] # data location
file_link='https://github.com/kerriegeil/pyAEZ_data_prep/blob/main/global/hpc_shell_workflow/02_rst_to_nc.py'
timeattrs={'standard_name':'time','long_name':'time','axis':'T'}
latattrs={'standard_name':'latitude','long_name':'latitude','units':'degrees_north','axis':'Y'}
lonattrs={'standard_name':'longitude','long_name':'longitude','units':'degrees_east','axis':'X'}
time_encoding={'calendar':'standard','units':'days since 1900-01-01 00:00:00','_FillValue':None}
lat_encoding={'_FillValue':None}
lon_encoding={'_FillValue':None}
var_encoding = {'zlib':True,'dtype':'float32'}    
  
    
if varname=='Elev':
    ds=xr.open_dataset(elevfile,engine='rasterio').squeeze() 
    del ds.coords['band']    
else if varname=='Mask':
    ds=xr.open_dataset(maskfile,engine='rasterio').squeeze() 
    del ds.coords['band']  
else:  
    filename=varname+'365_AgERA5_*_'+str(year)+'_*_5m.rst'
    filelist=glob.glob(data_dir+'AgERA5/'+filename)    
    
    # create time variable
    time_ms=pd.date_range(year+'-01-01',year+'-12-31',freq='MS')
    time_me=pd.date_range(year+'-01-01',year+'-12-31',freq='M')+pd.Timedelta('1d')
    offset=(time_me-time_ms)/2
    time=time_me-offset

    data=[] # empty list to store all months of data
    for t,f in zip(time,filelist):
        d=xr.open_dataset(f,engine='rasterio') # open data file
        d=d.rename({'band':'time'}) # change metadata
        d.coords['time']=[t]        # assing time info
        data.append(d)              # append month to list
    ds=xr.combine_by_coords(data)   # combine into one big array with 12 times

if 'y' in list(ds.coords): 
    ds['y']=ds['y'].astype('float32')
if 'x' in list(ds.coords):
    ds['x']=ds['x'].astype('float32')        

    
vname='Elevation'


# rename variable and dimensions
ds=ds.rename({'x':'lon','y':'lat','band_data':vname})

# apply mask 
ds=ds.where(admin_mask>0)

# variable/coordinate metadata
varattrs={'standard_name':'Elevation',
          'long_name':'Elevation',
          'units':'m'}
ds[vname].attrs=varattrs
ds['lat'].attrs=latattrs
ds['lon'].attrs=lonattrs

# global attributes
ds=ds.assign_attrs({'source_data':elevfile,
                    'source_code':nb_link})

ds












