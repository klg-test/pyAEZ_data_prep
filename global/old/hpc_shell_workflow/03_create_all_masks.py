# # Create binary admin masks

# There are 3 different masks in this data. 

# 1) the mask that is supposed to apply to everything is ALOSmask5m_fill.rst with 2,295,358 grids of data
# 2) the mask I create from the monthly mean pxv files with 2,268,708 grids of data + 26650 additional grids set to the missing value. These two numbers combined equals the number of grids with data in mask (1)
# 3) the mask I create from the daily dev pxv files. These files have 2,287,408 grids with data + 7950 additional grids set to the missing value. These two numbers combined equals the number of grids with data in mask (1)

# The grids set to the fillvalue are esseentially additional grids we always want to mask. Moving forward I will use the mask with the fewest number of grids with data present (2) to mask all other files.

# This code creates a netcdf for each of these masks. This allows for easy comparison of where there is and isn't data present across the different file types.

#### after running we will have 3 masks to work with at /work/hpc/datasets/gaez_v5_intermediate/static/
# 1) mask_2295358_5m.nc (from ALOSmask5m_fill.rst)
# 2) mask_2268708_5m.nc (from Precip_AgERA5_Hist_2020_01_5m.rst)
# 3) mask_2287408_5m.nc (from Precip365_AgERA5_Hist_2020_5m.dat)


import numpy as np
import xarray as xr
import pandas as pd 
import sys

# get command line arguments
ALOSmaskfile= str(sys.argv[1])
temp_dir=str(sys.argv[2])

varname='mask'
monthlyfileformask='/work/hpc/datasets/un_fao/gaez_v5_intermediate/dat/Precip_AgERA5_Hist_2020_5m.dat'
dailyfileformask='/work/hpc/datasets/un_fao/gaez_v5_intermediate/dat/Precip365_AgERA5_Hist_2020_5m.dat'
fillval=-9999.

# web link to notebook, same for everyone
nb_link='https://github.com/kerriegeil/pyAEZ_data_prep/blob/main/global/03_create_all_masks.ipynb'

# metadata and encoding for writing netcdf files
latattrs={'standard_name':'latitude','long_name':'latitude','units':'degrees_north','axis':'Y'}
lonattrs={'standard_name':'longitude','long_name':'longitude','units':'degrees_east','axis':'X'}
varattrs={'standard_name':'mask',
          'long_name':'mask',
          'units':'unitless',
          'description':'binary admin mask of 0 and 1, where 1 indicates cell with data'}
lat_encoding={'_FillValue':None}
lon_encoding={'_FillValue':None}
var_encoding = {'zlib':True,'dtype':'int32'}


##############################################################
### We start with (1) the mask provided in ALOSmask5m_fill.rst
##############################################################
infile=ALOSmaskfile
print('creating mask from',infile)

ds=xr.open_dataset(infile,engine='rasterio').squeeze() 
del ds.coords['band']

if 'y' in list(ds.coords): 
    ds['y']=ds['y'].astype('float32')
if 'x' in list(ds.coords):
    ds['x']=ds['x'].astype('float32')      

# rename variable and dimensions
ds=ds.rename({'x':'lon','y':'lat','band_data':varname})

# create binary mask
ds[varname]=xr.where(ds[varname]>0,1,0)
ds[varname]=xr.where(ds.lat<-60,0,ds[varname]) # eliminate antarctica
ds[varname]=ds[varname].astype('int32')

# variable/coordinate metadata
ds[varname].attrs=varattrs
ds['lat'].attrs=latattrs
ds['lon'].attrs=lonattrs

# global attributes
ds=ds.assign_attrs({'source_data':infile,
                    'source_code':nb_link})

# slice off Antarctica
ds=ds.sel(lat=slice(90,-60))

# checks
ngrids=ds[varname].data.sum() # test for correct mask application
print('mask contains values',np.unique(ds[varname]),'and has',ngrids,'grids with data')

# write to file
out_mask=temp_dir+'static/'+varname+'_'+str(ngrids)+'_5m.nc'
print('writing',out_mask)        
ds.to_netcdf(out_mask,
            encoding={'lat':lat_encoding,
                      'lon':lon_encoding,
                      varname:var_encoding})

# save grid info for other masks
nlats=len(ds.lat)
nlons=len(ds.lon)
lats=ds.lat.data
lons=ds.lon.data
spatial_ref=ds.spatial_ref

print('######################')

#######################################################################################
### Next, we create a mask from where data is present in the monthly mean pxv files (2) 
#######################################################################################
infile=monthlyfileformask
print('creating mask from',infile)

# parse dat file into 2D numpy array
temp=open(infile).read().splitlines() # get each line from dat file as a string and remove carriage returns
ilatilon=temp[0::2] # grab the lines with lat/lons (every other line)
data=temp[1::2] # grab the lines with the data (every other line)

# get each string lat/lon as integer and put it in an numpy array
ilat=np.array([int(i.split()[0]) for i in ilatilon]).astype('int16') 
ilon=np.array([int(i.split()[1]) for i in ilatilon]).astype('int16') 

# put data in a numpy array too
data2D=np.loadtxt(data,dtype='float32')
nt=data2D.shape[1]

# how many grids have data (excluding the missing value)?  
nomissing=np.where(data2D==-9999.,0,1)
ngrids=nomissing.sum()/nt

# how many grids are set to the missing value?
missing=np.where(data2D==-9999.,1,0)
nmissing=missing.sum()/nt

# print some info
print('in dat file:',ngrids,'grids with data present')
print('in dat file:',nmissing,'grids set to fill value')

# create a time dimension
time=pd.date_range('2020-01-01','2020-12-31',freq='MS')

# functions to build global data grid
def build_full_lat(ixs,data,y,x,t):
    # create nan array of shape (all days, 1 lat, all lons)
    arr=np.empty((nt,len(y),len(x)),dtype='float32')
    arr[:]=np.nan    
    for i,ix in enumerate(ixs):
        arr[:,0,ix]=data[i,:] # fill in data  
    return arr

def build_empty_lat(y,x,t):
    # create nan array of shape (all days, 1 lat, all lons)   
    arr=np.empty((nt,len(y),len(x)),dtype='float32')
    arr[:]=np.nan
    return arr

# build global data
arr_list=[]
for iy in range(nlats):
#     if iy%500==0: print('processing iy = ',iy,'of',nlats)
    indices=np.where(ilat==iy+1)[0] # find which data rows apply to this latitude
    if np.any(indices):
        result=build_full_lat((ilon[indices]-1),data2D[indices,:],lats[iy:iy+1],lons,time)
        arr_list.append(result)
    else:
        result=build_empty_lat(lats[iy:iy+1],lons,time)     
        arr_list.append(result)

# concat numpy arrays together
bignp=np.concatenate(arr_list,axis=1)

# convert to xarray for easier manipulation
bigarr=xr.DataArray(bignp,
                 dims=['time','lat','lon'],
                 coords={'time':('time',time),'lat':('lat',lats),'lon':('lon',lons)}).astype('float32')

# slice out antarctica
bigarr=bigarr.sel(lat=slice(90,-60))

# see how many grids have data present or missing value
nomissing=np.where((bigarr==-9999.)|(~np.isfinite(bigarr)),0,1) # where data is present
ngrids=nomissing.sum()/nt

missing=np.where(bigarr==-9999.,1,0) # where data is the missing value
nmissing=missing.sum()/nt

print('for mask, number of grids with data:',ngrids)
print('for mask, number of grid set to missing:',nmissing)
print('these numbers combined equals the number of grids=1 in the ALOS mask:',ngrids+nmissing)

# replace the -9999 fill value with nan
bigarr=bigarr.where(bigarr!=fillval)          
bigarr=xr.where(np.isfinite(bigarr),1,0)  # convert to a 0,1 mask

# convert to dataset
ds=bigarr[0,:,:].to_dataset(name=varname)  # subset in time (3D-->2D) and convert to xarray dataset
ds=ds.drop('time')        
# variable/coordinate metadata
ds=ds.assign_coords({'spatial_ref':spatial_ref})    
ds[varname].attrs=varattrs
ds['lat'].attrs=latattrs
ds['lon'].attrs=lonattrs

# checks
ngrids=ds[varname].data.sum() # test for correct mask application
print('mask contains values',np.unique(ds[varname]),'and has',ngrids,'grids with data')     
        
# write to file
out_mask=temp_dir+'static/'+varname+'_'+str(ngrids)+'_5m.nc'
print('writing',out_mask)        
ds.to_netcdf(temp_dir+'static/'+varname+'_'+str(ngrids)+'_5m.nc',
            encoding={'lat':lat_encoding,
                      'lon':lon_encoding,
                      varname:var_encoding})        
        
print('######################')

#######################################################################################
### Next, we create a mask from where data is present in the precip daily dev pxv file (3)  
#######################################################################################
infile=dailyfileformask
print('creating mask from',infile)

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

# how many grids have data (excluding the missing value)?  
nomissing=np.where(data2D==-9999.,0,1)
ngrids=nomissing.sum()/nt

# how many grids are set to the missing value?
missing=np.where(data2D==-9999.,1,0)
nmissing=missing.sum()/nt

# print some info
print('in dat file:',ngrids,'grids with data present')
print('in dat file:',nmissing,'grids set to fill value')

# create a time dimension
time=pd.date_range('2020-01-01','2020-12-31',freq='D')

# functions to build global data grid
def build_full_lat(ixs,data,y,x,t):
    # create nan array of shape (all days, 1 lat, all lons)
    arr=np.empty((nt,len(y),len(x)),dtype='float32')
    arr[:]=np.nan    
    for i,ix in enumerate(ixs):
        arr[:,0,ix]=data[i,:] # fill in data  
    return arr

def build_empty_lat(y,x,t):
    # create nan array of shape (all days, 1 lat, all lons)   
    arr=np.empty((nt,len(y),len(x)),dtype='float32')
    arr[:]=np.nan
    return arr

# build global data
arr_list=[]
for iy in range(nlats):
#     if iy%500==0: print('processing iy = ',iy,'of',nlats)
    indices=np.where(ilat==iy+1)[0] # find which data rows apply to this latitude
    if np.any(indices):
        result=build_full_lat((ilon[indices]-1),data2D[indices,:],lats[iy:iy+1],lons,time)
        arr_list.append(result)
    else:
        result=build_empty_lat(lats[iy:iy+1],lons,time)     
        arr_list.append(result)

# concat numpy arrays together
bignp=np.concatenate(arr_list,axis=1)

# convert to xarray for easier manipulation
bigarr=xr.DataArray(bignp,
                 dims=['time','lat','lon'],
                 coords={'time':('time',time),'lat':('lat',lats),'lon':('lon',lons)}).astype('float32')

# slice out antarctica
bigarr=bigarr.sel(lat=slice(90,-60))

# see how many grids have data present or missing value
nomissing=np.where((bigarr==-9999.)|(~np.isfinite(bigarr)),0,1) # where data is present
ngrids=nomissing.sum()/nt

missing=np.where(bigarr==-9999.,1,0) # where data is the missing value
nmissing=missing.sum()/nt

print('for mask, number of grids with data:',ngrids)
print('for mask, number of grid set to missing:',nmissing)
print('these numbers combined equals the number of grids=1 in the ALOS mask:',ngrids+nmissing)

# replace the -9999 fill value with nan
bigarr=bigarr.where(bigarr!=fillval)          
bigarr=xr.where(np.isfinite(bigarr),1,0)  # convert to a 0,1 mask

# convert to dataset
ds=bigarr[0,:,:].to_dataset(name=varname)  # subset in time (3D-->2D) and convert to xarray dataset
ds=ds.drop('time')        
# variable/coordinate metadata
ds=ds.assign_coords({'spatial_ref':spatial_ref})    
ds[varname].attrs=varattrs
ds['lat'].attrs=latattrs
ds['lon'].attrs=lonattrs
        
# checks
ngrids=ds[varname].data.sum() # test for correct mask application
print('mask contains values',np.unique(ds[varname]),'and has',ngrids,'grids with data')     
        
# write to file
out_mask=temp_dir+'static/'+varname+'_'+str(ngrids)+'_5m.nc'
print('writing',out_mask)        
ds.to_netcdf(temp_dir+'static/'+varname+'_'+str(ngrids)+'_5m.nc',
            encoding={'lat':lat_encoding,
                      'lon':lon_encoding,
                      varname:var_encoding})    