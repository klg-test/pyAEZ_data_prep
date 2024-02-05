PROGRAM read_AgERA5pxv

    IMPLICIT NONE
    
    !!!!!
    ! Author: code written by Gunther Fischer (IIASA), translated and modified by K Geil
    ! Date: 04/2023
    ! Description: read pxv data files containing monthly means and output to dat files
    !
    ! input file source: /work/hpc/datasets/unfao/gaez_v5/clim/AgERA5/Hist/
    ! input file permanent storage location: /gri/projects/UN_FAO/
    ! 
    ! compile this script at the command line where the gfortran compiler is available, eg on Orion HPC
    ! and then run the executable, like this:
    ! gfortran 00_pxv_to_dat.f95 -o 00_pxv_to_dat
    ! ./00_pxv_to_dat
    !!!!!

    INTEGER, PARAMETER :: MXERA5=2160, MYERA5=4320,mskbytes=2,databytes=4,nmonths=12
    REAL(KIND=databytes) :: buf(nmonths)
    INTEGER(KIND=1) :: isok(MXERA5,MYERA5)
    INTEGER(KIND=mskbytes) :: msk(MYERA5)
    INTEGER :: ir, ic, j, year, kpx, leapyr, isleap, IRmin, IRmax, nchars_var, nchars_info, nchars_suf, nchars_exp
    DATA IRmin,IRmax /1,1800/
    CHARACTER(LEN=*), PARAMETER :: datainfo='_AgERA5_', suffpxv='_5m.pxv', suffdat='_5m.dat' 
    CHARACTER(16) :: varnam, experiment
    CHARACTER(4) :: chyear
    CHARACTER(150) :: input_dir,flnmsk,output_dir
    CHARACTER(LEN=:), ALLOCATABLE :: fln365, flnout

    ! get command line arguments
    CALL GET_COMMAND_ARGUMENT(1,chyear)
    CALL GET_COMMAND_ARGUMENT(2,varnam)
    CALL GET_COMMAND_ARGUMENT(3,experiment)
    CALL GET_COMMAND_ARGUMENT(4,input_dir)
    CALL GET_COMMAND_ARGUMENT(5,flnmsk)
    CALL GET_COMMAND_ARGUMENT(6,output_dir) 
    
    !construct the input and output filenames    
    nchars_var = LEN_TRIM(varnam)
    nchars_info = LEN_TRIM(datainfo)
    nchars_exp = LEN_TRIM(experiment)
    nchars_suf = LEN_TRIM(suffpxv)    
    
    ALLOCATE(character(len=(nchars_var + nchars_info + nchars_exp + nchars_suf)) :: fln365)
    ALLOCATE(character(len=(nchars_var + nchars_info + nchars_exp + nchars_suf)) :: flnout)
        
    fln365=TRIM(varnam)//datainfo//TRIM(experiment)//chyear//suffpxv
    flnout=TRIM(varnam)//datainfo//TRIM(experiment)//chyear//suffdat    
    
    ! read mask
    WRITE(6,*) 'Reading land mask:', TRIM(flnmsk)
    OPEN(1, file=TRIM(flnmsk), access='direct', recl=MYERA5*mskbytes)
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
    OPEN(11,file=TRIM(input_dir)//fln365,access='direct',recl=nmonths*databytes) ! input pxv file
    WRITE(6,*) 'Reading file:', TRIM(input_dir)//fln365
    OPEN(12,file=TRIM(output_dir)//flnout) ! output dat file    
    
    kpx = 0
    DO ir=IRmin,IRmax
        IF (mod(ir,100) .eq. 0) WRITE(6,*) ' Processing row:',ir
        DO ic=1,MYERA5
            IF (isok(ir,ic) .eq. 1) THEN
                kpx = kpx + 1
                READ(11,rec=kpx) (buf(j),j=1,nmonths)
                WRITE(12,'(2i8,/,*(f20.4))') ir,ic,(buf(j),j=1,nmonths)
            END IF
        END DO
    END DO

    PRINT*,"Written file: ",TRIM(output_dir)//flnout    
    
END PROGRAM read_AgERA5pxv