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

maskfile='mask_2268708_5m.nc' # using this mask because it is subset to 1800 lats

# monthly mean variable metadata
varinfo={'Precip':{'scale_factor':1,
                   'units':'mm/month',
                   'long_name':'precipitation',
                   'description':'monthly total accumulated precipitation'},
         'Srad':{'scale_factor':1,
                 'units':'J/m2/day',
                 'long_name':'surface short wave radiation down',
                 'description':'monthly mean downwelling shortwave radiation at surface'},
         'Tmax-2m':{'scale_factor':1,
                    'units':'degrees C',
                    'long_name':'2m maximum surface air temperature',
                    'description':'monthly mean maximum surface air temperature at 2m'},
         'Tmin-2m':{'scale_factor':1,
                    'units':'degrees C',
                    'long_name':'2m minimum surface air temperature',
                    'description':'monthly mean minimum surface air temperature at 2m'},
         'Vapr':{'scale_factor':1,
                 'units':'hPa',
                 'long_name':'vapor pressure',
                 'description':'monthly mean vapor pressure'},
         'Wind-10m':{'scale_factor':1,
                     'units':'m/s',
                     'long_name':'10m wind speed',
                     'description':'monthly mean wind speed at 10m above surface'}}

# additional metadata for output data files
source_dirs=['on Orion '+temp_dir+'dat/']
nb_link='https://github.com/kerriegeil/pyAEZ_data_prep/blob/main/global/02_dat_to_nc_monthly.ipynb'
timeattrs={'standard_name':'time','long_name':'time','axis':'T'}
latattrs={'standard_name':'latitude','long_name':'latitude','units':'degrees_north','axis':'Y'}
lonattrs={'standard_name':'longitude','long_name':'longitude','units':'degrees_east','axis':'X'}

# encoding info for writing netcdf files
time_encoding={'calendar':'standard','units':'days since 1900-01-01 00:00:00','_FillValue':None}
lat_encoding={'_FillValue':None}
lon_encoding={'_FillValue':None}
var_encoding = {'zlib':True,'dtype':'float32'}

# get dat file
infile=glob.glob(temp_dir+'dat/'+varname+'_AgERA5_'+experiment+'_'+year+'_5m.dat')[0]
print('reading',infile)

# get each line from dat file as a string and remove carriage returns
temp=open(infile).read().splitlines() 

# grab the lines with lat/lons (every other line)
ilatilon=temp[0::2] 

# grab the lines with the data (every other line)
data=temp[1::2]

# get each string lat/lon as integer and put it in an numpy array
ilat=np.array([int(i.split()[0]) for i in ilatilon]).astype('int16') 
ilon=np.array([int(i.split()[1]) for i in ilatilon]).astype('int16') 

# put data in a numpy array too, takes 30-60s
data2D=np.loadtxt(data,dtype='float32')
nt=data2D.shape[1]
print('data dimensions:')
print(data2D.shape[0],'rows (each row represents a different grid cell)')
print(nt,'cols (each col represents a month of the year)')
print('data min max before scaling:',data2D.min(),data2D.max())

# per Gunther, each monthly pxv file should have the following:
# 2268708 grids with data present
# 26650 grids equal to the fill value
# These two numbers combined equals the number of grids=1 in the ALOSmask (2295358)

# how many precip grids have data?  
nomissing=np.where(data2D==-9999,0,1)
ngrids=nomissing.sum()/nt

# how many grids are set to the missing value?
missing=np.where(data2D==-9999.,1,0)
nmissing=missing.sum()/nt

# print some info
print(ngrids,'grids with data present, per Gunther this should be 2268708')
print(nmissing,'grids set to fill value')

# get grid info from a mask
mask=xr.open_dataset(temp_dir+'static/'+maskfile)
spatial_ref=mask.spatial_ref
nlats,nlons=mask.mask.shape
lats=mask.mask.lat.data.astype('float32')
lons=mask.mask.lon.data.astype('float32')

# create a time dimension
if (int(year)>=1980) & (int(year)<=2024):
    time=pd.date_range(year+'-01-01',year+'-12-31',freq='MS')
else:
    time=pd.date_range('1900-01-01','1900-12-31',freq='MS')    

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
    return arr#,arrda

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
#     if iy%500==0: print('processing iy = ',iy,'of',nlats,'latitudes')
    indices=np.where(ilat==iy+1)[0] # find which data rows apply to this latitude
    if np.any(indices):
        result=build_full_lat((ilon[indices]-1),data2D[indices,:],lats[iy:iy+1],lons,time)
        arr_list.append(result)
    else:
        result=build_empty_lat(lats[iy:iy+1],lons,time)     
        arr_list.append(result)

# concatenate the list of latitudes to get data on a global grid
bignp=np.concatenate(arr_list,axis=1,dtype='float32')
bignp.shape

# assign the grid metadata
bigarr=xr.DataArray(bignp,name=varname,
                 dims=['time','lat','lon'],
                 coords={'time':('time',time),'lat':('lat',lats),'lon':('lon',lons)}).astype('float32')

# replace the -9999 fill value with nan
bigarr=bigarr.where(bigarr!=fillval)  

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
                    'source_code':nb_link})

print('size of global data for',varname,'=',round(bigarr.nbytes/1E9,3),'GB')

# visual check by plotting first month
figname=temp_dir+'plots/'+varname+'_'+experiment+'_JanMean_'+year+'.png'
print('writing plot to',figname) 
fig,ax=plt.subplots()
ds[varname][0,:,:].plot(ax=ax)
fig.savefig(figname,bbox_inches='tight')
plt.close(fig)

# writing with compression
outfile=temp_dir+'netcdf/'+varname+'_'+experiment+'_Monthly_'+year+'_5m.nc'
print('writing',outfile)
ds.to_netcdf(outfile,
            encoding={'lat':lat_encoding,
                      'lon':lon_encoding,
                      'time':time_encoding,
                      varname:var_encoding})
