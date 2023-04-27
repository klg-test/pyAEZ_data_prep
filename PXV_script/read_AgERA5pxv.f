c
c	list contents of climate file
c
	parameter (MXERA5=2160 , MYERA5=4320, MXP=12)
	parameter (MDX=366)
c
	integer*1 isok(MXERA5,MYERA5)
	integer*1 leapyr
	integer*2 msk(MYERA5)
	integer*2 buf(MDX)
	integer year, kpx
c
	character*16 varnam, clinam, scenam
	character*80 fln365, flnout
	character*80 flnmsk
c
	data IRmin, IRmax /1,1800/
c
c	read land mask
c
	flnmsk = 'ALOSmask5m_fill.rst'
	len_msk = LEN_TRIM(flnmsk)
	write(6,*) 'Reading land mask:', flnmsk(1:len_msk)
	open(1, file=flnmsk, access='direct', recl=MYERA5*2)
	do ir = 1,MXERA5
	read(1, rec=ir) (msk(j), j=1,MYERA5)
	do ic=1,MYERA5
	if (msk(ic) .eq. 0) then
	  isok(ir,ic) = 0
	else
	  isok(ir,ic) = 1
	endif
	enddo
	enddo
	close(1)
c
	write(6,*) ' Variable name : e.g. Tmax-2m'
	read(5,'(a16)') varnam
	write(6,*) ' Enter year:'
	read(5,'(i4)') year
c
c	check for leap year
c
	isleap = leapyr(year)
	if (isleap .eq. 1) then
	  MD365 = 366
	else
	  MD365 = 365
	endif
	print*,"MD365=",MD365
c
	scenam = 'Hist'
	clinam = 'AgERA5'
c
c	construct output filename, e.g. Wind-10m365_AgERA5_Hist_1996_5m.pxv
c
	len_var = LEN_TRIM(varnam)
	len_cli = LEN_TRIM(clinam)
	len_sce = LEN_TRIM(scenam)
c
	fln365(1:len_var) = varnam(1:len_var)
	flnout(1:len_var) = varnam(1:len_var)
	i1 = len_var + 1
	i2 = i1 + 4 - 1
	fln365(i1:i2) = '365_'
	flnout(i1:i2) = '365_'
	i3 = i2 + 1
	i4 = i3 + len_cli - 1
	fln365(i3:i4) = clinam(1:len_cli)
	flnout(i3:i4) = clinam(1:len_cli)
	i4 = i4 + 1
	fln365(i4:i4) = '_'
	flnout(i4:i4) = '_'
	i5 = i4 + 1
	i6 = i5 + len_sce - 1
	fln365(i5:i6) = scenam(1:len_sce)
	flnout(i5:i6) = scenam(1:len_sce)
	i7 = i6 + 1
	i8 = i7 + 12 - 1
	write(fln365(i7:i8),'(a1,i4.4,a7)') '_',year,'_5m.pxv'
	write(flnout(i7:i8),'(a1,i4.4,a7)') '_',year,'_5m.dat'
c
	open(11,file=fln365,access='direct',recl=MD365*2)
	write(6,*) 'Reading file:', fln365(1:i8)
ccccc	open(12,file=flnout)
	open(12,file=flnout(1:i8))
c
c	read and convert data
c
	kpx = 0
	do ir=IRmin,IRmax
	if (mod(ir,100) .eq. 0) write(6,*) ' Processing row:',ir
	do ic=1,MYERA5
	if (isok(ir,ic) .eq. 1) then
	  kpx = kpx + 1
	  read(11,rec=kpx) (buf(j),j=1,MD365)
	  write(12,900) ir,ic,(buf(j),j=1,MD365)     
	endif
	enddo
	enddo
ccccc900	format(2i6/12i6)
900	format(2i6,/,*(i6))
	stop
	end
c
c	------------------------------------------------------------
c
c	is year a leap year?
c
	integer*1 function leapyr(iyear)
c
c	average climate files use 365 days
c
	if (iyear.lt.1981 .or. iyear.gt.2020) then
	  leapyr = 0
	  return
	endif
c	
	if (mod(iyear,400) .eq. 0) then
	  leapyr = 1
	else if (mod(iyear,100) .eq. 0) then
	  leapyr = 0
	else if (mod(iyear,4) .eq. 0) then
	  leapyr = 1
	else
	  leapyr = 0
	endif
	return
	end
