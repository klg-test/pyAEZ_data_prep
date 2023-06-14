PROGRAM read_AgERA5pxv

    IMPLICIT NONE
    
    !!!!!
    ! Author: code written by Gunther Fischer (IIASA), translated and modified by K Geil
    ! Date: 04/2023
    ! Description: read pxv data files containing daily deviations and output to dat files
    !
    ! input file source: Gianluca's Google Drive (link not included here for privacy)
    ! input file permanent storage location: /gri/projects/rgmg/UN_FAO/
    ! 
    ! compile this script at the command line where the gfortran compiler is available, eg on Orion HPC
    ! and then run the executable, like this:
    ! gfortran 00_pxv_to_dat.f95 -o 00_pxv_to_dat
    ! ./00_pxv_to_dat
    !!!!!

    INTEGER, PARAMETER :: MXERA5=2160, MYERA5=4320
    INTEGER(KIND=2), ALLOCATABLE :: buf(:)
    INTEGER(KIND=1) :: isok(MXERA5,MYERA5)
    INTEGER(KIND=2) :: msk(MYERA5)
    INTEGER :: ir, ic, j, year, kpx, leapyr, isleap, MD365, IRmin, IRmax, nchars_var, nchars_info, nchars_suf
    DATA IRmin,IRmax /1,1800/
    CHARACTER(LEN=*), PARAMETER :: datainfo='365_AgERA5_Hist_', suffpxv='_5m.pxv', suffdat='_5m.dat' 
    CHARACTER(LEN=*), PARAMETER :: flnmsk = '/work/hpc/datasets/un_fao/pyaez/static/rst/ALOSmask5m_fill.rst'
    CHARACTER(LEN=*), PARAMETER :: input_dir='/work/hpc/datasets/un_fao/gaez/global_1980/dailydev/pxv/' 
    CHARACTER(LEN=*), PARAMETER :: output_dir='/work/hpc/datasets/un_fao/gaez/global_1980/dailydev/dat/'
    CHARACTER(16) :: varnam
    CHARACTER(4) :: chyear
    CHARACTER(LEN=:), ALLOCATABLE :: fln365, flnout
    

    ! get info from user
    WRITE(6,*) ' Variable name : e.g. Tmax-2m, Tmin-2m, Wind-10m, Precip, Srad, Vapr'
    READ(5,'(a16)') varnam
    WRITE(6,*) ' Enter year:'
    READ(5,'(i4)') year
    
    WRITE(chyear,'(I4)') year


    ! determine if it's a leap year
    ! and allocate appropriate size to buf
    isleap = leapyr(year)
    IF (isleap .eq. 1) THEN
        MD365 = 366
    ELSE
        MD365 = 365
    ENDIF

    ALLOCATE(buf(MD365))


    !construct the input and output filenames    
    nchars_var = LEN_TRIM(varnam)
    nchars_info = LEN_TRIM(datainfo)
    nchars_suf = LEN_TRIM(suffpxv)    
    
    ALLOCATE(character(len=(nchars_var + nchars_info + nchars_suf)) :: fln365)
    ALLOCATE(character(len=(nchars_var + nchars_info + nchars_suf)) :: flnout)
        
    fln365=TRIM(varnam)//datainfo//chyear//suffpxv
    flnout=TRIM(varnam)//datainfo//chyear//suffdat


    ! read mask
    WRITE(6,*) 'Reading land mask:', flnmsk
    OPEN(1, file=flnmsk, access='direct', recl=MYERA5*2)
    DO ir = 1,MXERA5
        READ(1, rec=ir) (msk(j), j=1,MYERA5)
        DO ic=1,MYERA5
            IF (msk(ic) .eq. 0) THEN
                isok(ir,ic) = 0
            ELSE
                isok(ir,ic) = 1
            END IF
        END DO
    END DO
    CLOSE(1)


    ! convert data pxv to dat
    OPEN(11,file=input_dir//fln365,access='direct',recl=MD365*2) ! input pxv file
    WRITE(6,*) 'Reading file:', input_dir//fln365
    OPEN(12,file=output_dir//flnout) ! output dat file

    kpx = 0
    DO ir=IRmin,IRmax
        IF (mod(ir,100) .eq. 0) write(6,*) ' Processing row:',ir!,buf
        DO ic=1,MYERA5
            IF (isok(ir,ic) .eq. 1) THEN
                kpx = kpx + 1
                READ(11,rec=kpx) (buf(j),j=1,MD365)
                WRITE(12,'(2i7,/,*(i7))') ir,ic,(buf(j),j=1,MD365)
            END IF
        END DO
    END DO

    PRINT*,"Written file: ",output_dir//flnout

END PROGRAM read_AgERA5pxv


!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


! is the year a leap year?
! note average climate files use 365 days
INTEGER FUNCTION leapyr(iyear) 

    IMPLICIT NONE
    INTEGER, INTENT(IN) :: iyear
    
    IF (iyear.lt.1980 .or. iyear.gt.2020) THEN
        leapyr=0
        RETURN
    ENDIF
    
    IF (mod(iyear,400) .eq. 0) THEN
        leapyr=1
    ELSE IF (mod(iyear,400) .eq. 0) THEN
        leapyr=0
    ELSE IF (mod(iyear,4) .eq. 0) then
        leapyr=1
    ELSE
        leapyr=0
    ENDIF
END FUNCTION leapyr    

    
