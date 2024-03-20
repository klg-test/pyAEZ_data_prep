import numpy as np
import xarray as xr 
import pandas as pd 
import os
import sys
import glob
import matplotlib.pyplot as plt
    
# get command line arguments
year=str(sys.argv[1])
fillval=np.float32(sys.argv[2])
varname=str(sys.argv[3])
experiment=str(sys.argv[4])
temp_dir=str(sys.argv[5])

maskfile='mask_2287408_5m.nc'

# create a time dimension
if (int(year)>=1980) & (int(year)<=2024):
    time=pd.date_range(year+'-01-01',year+'-12-31',freq='D')
else:
    time=pd.date_range('1900-01-01','1900-12-31',freq='D')    

# scale and units info from the file "UnitScaleFactors.txt" from Gunther
varinfo={'Precip':{'scale_factor':1E-4, # updated from 1E-5 per Gunther on 5 Feb 2024
                   'units':'unitless (mm/mm)',
                   'long_name':'precipitation fraction',
                   'description':'daily fraction of monthly accumulated precitation'},
         'Srad':{'scale_factor':1000.,
                 'units':'J/m2/day',
                 'long_name':'surface downwelling short wave radiation',
                 'description':'daily deviation from monthly mean downwelling SW radation at the surface'},
         'Tmax-2m':{'scale_factor':0.01,
                    'units':'degrees C',
                    'long_name':'2m maximum air temperature',
                    'description':'daily devation from the monthly mean maximum air temperature at 2m'},
         'Tmin-2m':{'scale_factor':0.01,
                    'units':'degrees C',
                    'long_name':'2m minimum air temperature',
                    'description':'daily devation from the monthly mean minimum air temperature at 2m'},
         'Vapr':{'scale_factor':0.01,
                 'units':'hPa',
                 'long_name':'vapor pressure',
                 'description':'daily devation from the monthly mean surface vapor pressure'},
         'Wind-10m':{'scale_factor':0.001,
                     'units':'m/s',
                     'long_name':'10m wind speed',
                     'description':'daily devation from the monthly mean wind speed at 10m'}}

#  metadata & encoding to include in netcdf files
source_dirs=['on Orion '+temp_dir+'dat/'] # data location
file_link='https://github.com/kerriegeil/pyAEZ_data_prep/blob/main/global/hpc_shell_workflow/05_dat_to_nc_daily.py'
timeattrs={'standard_name':'time','long_name':'time','axis':'T'}
latattrs={'standard_name':'latitude','long_name':'latitude','units':'degrees_north','axis':'Y'}
lonattrs={'standard_name':'longitude','long_name':'longitude','units':'degrees_east','axis':'X'}

# encoding info for writing netcdf files
time_encoding={'calendar':'standard','units':'days since 1900-01-01 00:00:00','_FillValue':None}
lat_encoding={'_FillValue':None}
lon_encoding={'_FillValue':None}
var_encoding = {'zlib':True,'dtype':'float32'}    

# get dat file
infile=glob.glob(temp_dir+'dat/'+varname+'365_AgERA5_'+experiment+'_'+year+'_5m.dat')[0]
print('reading',infile)

# parse dat file into 2D numpy array
temp=open(infile).read().splitlines() # get each line from dat file as a string and remove carriage returns
ilatilon=temp[0::2] # grab the lines with lat/lons (every other line)
data=temp[1::2] # grab the lines with the data (every other line)
# get each string lat/lon as integer and put it in an numpy array
ilat=np.array([int(i.split()[0]) for i in ilatilon]).astype('int16') 
ilon=np.array([int(i.split()[1]) for i in ilatilon]).astype('int16') 
# put data in a numpy array too, takes 30-60s
data2D=np.loadtxt(data,dtype='int16')
nt=data2D.shape[1]
print('data dimensions:')
print(data2D.shape[0],'rows (each row represents a different grid cell)') 
print(nt,'cols (each col represents a day of the year)')
print('data min max (before scaling):',data2D.min(),data2D.max())

# per Gunther, each daily pxv file should have the following:
# 2287408 grids with data present
# 7950 grids equal to the fill value
# These two numbers combined equals the number of grids=1 in the ALOSmask (2295358)

# how many precip grids have data?  
nomissing=np.where(data2D==fillval,0,1)
ngrids=nomissing.sum()/nt

# how many grids are set to the missing value?
missing=np.where(data2D==fillval,1,0)
nmissing=missing.sum()/nt

# print some info
print(ngrids,'grids with data present, per Gunther this should be 2287408')
print(nmissing,'grids set to fill value')

if ngrids != 2287408.:
    flag=True
    print('incorrect number of data points found, mask will be applied')
else:
    flag=False

# get grid info from a mask
mask=xr.open_dataset(temp_dir+'static/'+maskfile)
spatial_ref=mask.spatial_ref
nlats,nlons=mask.mask.shape
lats=mask.mask.lat.data.astype('float32')
lons=mask.mask.lon.data.astype('float32')

# # create a time dimension
# time=pd.date_range(str(year)+'-01-01',str(year)+'-12-31',freq='D')

print('global data dimensions:',nlats,'latitudes by',nlons,'longitudes by',nt,'months')
print('building global data...')

# Put data from dat file on a global grid
# function for when data is present to put the data on a global grid
def build_full_lat(ixs,data,y,x,t):
    # create nan array of shape (all days, 1 lat, all lons)
    arr=np.empty((nt,len(y),len(x)),dtype='float32')
    arr[:]=np.nan    
    for i,ix in enumerate(ixs):
        arr[:,0,ix]=data[i,:]
    return arr

# function for when no data is present at an entire latitude to create a latitude of nan
def build_empty_lat(y,x,t):
    # create nan array of shape (all days, 1 lat, all lons)   
    arr=np.empty((nt,len(y),len(x)),dtype='float32')
    arr[:]=np.nan
    return arr

# we process one latitude at a time
# and store each latitude of data in a list
arr_list=[]
for iy in range(nlats):
    indices=np.where(ilat==iy+1)[0] # find which data rows apply to this latitude
    if np.any(indices):
        result=build_full_lat((ilon[indices]-1),data2D[indices,:],lats[iy:iy+1],lons,time)
        arr_list.append(result)
    else:
        result=build_empty_lat(lats[iy:iy+1],lons,time)     
        arr_list.append(result)

# concatenate the list of latitudes to get data on a global grid
bignp=np.concatenate(arr_list,axis=1,dtype='float32')
print('array shape',bignp.shape)

# assign the grid metadata
bigarr=xr.DataArray(bignp,name=varname,
                 dims=['time','lat','lon'],
                 coords={'time':('time',time),'lat':('lat',lats),'lon':('lon',lons)}).astype('float32')

if flag:
    # the range of some variable encompasses the -9999 fillval
    # in these cases we need to scale (because some -9999 values are true data) and then mask
    print('scaling by', varinfo[varname]['scale_factor'],'and applying mask...')
    bigarr=bigarr*varinfo[varname]['scale_factor'] # apply scale factor    
    bigarr=bigarr.where(mask.mask==1) # apply mask
    bigarr=bigarr.drop('spatial_ref') # remove bum metadata that where adds 
    datagrids=xr.where(np.isfinite(bigarr),1,0)
    ngrids=datagrids.sum().data/nt
    print(ngrids,'grids with data present in the output, should be 2287408')
else:
    print('scaling...')
    bigarr=bigarr.where(bigarr!=fillval) # replace the -9999 fill value with nan
    bigarr=bigarr*varinfo[varname]['scale_factor'] # apply scale factor

# variable/coordinate metadata
varattrs={'standard_name':varname,
          'long_name':varinfo[varname]['long_name'],
          'units':varinfo[varname]['units'],
          'description':varinfo[varname]['description']}

# assign metadata
bigarr.attrs=varattrs
bigarr['lat'].attrs=latattrs
bigarr['lon'].attrs=lonattrs
bigarr['time'].attrs=timeattrs

# array to dataset
ds=bigarr.to_dataset()
ds=ds.assign_coords({'spatial_ref':spatial_ref})
ds=ds.assign_attrs({'source_data':source_dirs,
                    'source_code':file_link})

print('size of global data for',varname,'=',round(bigarr.nbytes/1E9,3),'GB')

# visual check by plotting first day
figname=temp_dir+'plots/'+varname+'_'+experiment+'_01JanDev_'+year+'.png'
print('writing plot to',figname) 
fig,ax=plt.subplots()
ds[varname][0,:,:].plot(ax=ax)
fig.savefig(figname,bbox_inches='tight')
plt.close(fig)

# writing with compression
outfile=temp_dir+'netcdf/'+varname+'_'+experiment+'_DailyDev_'+year+'_5m.nc'
print('writing',outfile)
ds.to_netcdf(outfile,
            encoding={'lat':lat_encoding,
                      'lon':lon_encoding,
                      'time':time_encoding,
                      varname:var_encoding})


print('finished')