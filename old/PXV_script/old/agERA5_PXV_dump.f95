PROGRAM read_pxv_data

    IMPLICIT NONE
    
    !!!!!
    ! read AgERA5 pxv data files and output to prn
    ! this code does not run yet
    ! We're waiting for Gunter to provide more info
    ! We need global AgERA5 pxv files that match the ALOSmask5m_fill.rst (the pxv files in DataDownload03152023 are not global)
    ! input file source: Gianluca's Google Drive (link not included here for privacy)
    ! input file storage location: /gri/projects/rgmg/climate/UN_FAO/DataDownload03152023/
    ! We also need to know how each record is written in the pxv's (RECL, 900 format, units, scaling, and kind (2-byte int?))
    !!!!!
    
    INTEGER, PARAMETER :: NX=2160, NY=4320, MXP=365, mask_fu=1,pxv_fu=2, out_fu=3
    INTEGER(KIND=1) :: base(NY)
    INTEGER :: io, icol, irow, rec_num, ier, t, ivar
    REAL :: dVar(MXP) ! data array for 1 year of daily data
    REAL :: dVars ! scalar daily mean? we don't know if this is in AgERA5 pxv files
    CHARACTER(64) :: flngcm, flnout, flnbas ! file names
    
    ! open the base file which has the 0/1 mask on the appropriate grid
    flnbas='ALOSmask5m_fill.rst'
    OPEN(mask_fu, FILE=flnbas, IOSTAT=io, STATUS='old', ACCESS='direct', RECL=NY)
    
    ! open the pxv file
    ! we don't know if this RECL is correct
    flngcm='Precip365_AgERA5_Hist_1980_5m.pxv'
    OPEN(pxv_fu, FILE=flngcm, STATUS='old', ACCESS='direct', RECL=1*(MXP+1)*2) 
    
    ! open a prn file for writing data to 
    flnout = 'test_write_data.prn'
    OPEN(out_fu, FILE=flnout, STATUS='new')
    
    901 FORMAT(A2,A31,A15)
    902 FORMAT(A2,A33,A25)
    ! We don't know the format of the AgERA5 pxv files, this is a guess
    !900 FORMAT(2i5,12f8.2,f8.3)
    900 FORMAT(2i5,365f8.2,f8.3)
    
    ! set variable here
    ! 1=Tmax,2=Tmin,3=Pr,4=Sunfr,5=Wind,6=RH
    ivar=3
    
    ! write header in the prn out file
    ! we don't know the units of the AgERA5 pxv files
    IF (ivar .eq. 1) THEN
      WRITE(out_fu,901) '# ',flngcm(1:31),' delta in deg C'
    ELSE IF (ivar .eq. 2) THEN
      WRITE(out_fu,901) '# ',flngcm(1:31),' delta in deg C'
    ELSE IF (ivar .eq. 3) THEN
      WRITE(out_fu,902) '# ',flngcm(1:33),' d% (threshold min 20mm!)'
    ELSE IF (ivar .eq. 4) THEN
      WRITE(out_fu,901) '# ',flngcm(1:31),' percent change'
    ELSE IF (ivar .eq. 5) THEN
      WRITE(out_fu,901) '# ',flngcm(1:31),' percent change'
    ELSE IF (ivar .eq. 6) THEN
      WRITE(out_fu,901) '# ',flngcm(1:31),' percent change'
    END IF
    
    ! Each record in the rst mask file contains mask values for a single longitude at all latitudes
    ! We create an index value (rec_num) to access records from the pxv file
    ! Each record in the pxv file contains all variables at all times for a single grid cell
    DO irow=1,NX ! loop lons
        READ(mask_fu,REC=irow) base ! mask values for all lats
        DO icol=1,NY ! loop lats
            rec_num=(irow-1)*NY+icol ! create an index for pxv file
            print*, irow,icol,rec_num
            ! read and scale data from pxv file for a single grid cell
            ! we don't know how many variables are in each record of the AgERA5 pxv files, this is a guess
            CALL gcmdat(pxv_fu,rec_num,ier,dVar,dVars)

            ! write row/col indexes and all data for the grid cell to a single line in a prn file
            ! we don't know if this 100 scaling is necessary for AgERA5 pxv data
            IF (ier .eq. 0) THEN
                WRITE(out_fu,900) irow, icol, (100.*dVar(t),t=1,MXP), 100.*dVars
            END IF
        END DO
    END DO  
END PROGRAM read_pxv_data



SUBROUTINE gcmdat(fu,irec,ier,dVar,dVar0x) 

    IMPLICIT NONE
    
    INTEGER, INTENT(IN) :: fu, irec
    INTEGER, INTENT(OUT) :: ier     
    INTEGER :: t 
    INTEGER, PARAMETER :: NX=2160, NY=4320, MXP=365
    INTEGER(KIND=2) :: dVar4(MXP) ! read from gcm file, is pxv data 2-byte int we don't know
    INTEGER(KIND=2) :: dVar0 ! read from gcm file, is this in AgERA5 pxv's we don't know
    REAL(KIND=4), INTENT(OUT) :: dVar(MXP) ! rescaled arrays, returned
    REAL(KIND=4),INTENT(OUT) :: dVar0x ! rescaled scalars, returned

    ! get a single record from the pxv file
    ! we don't know what's in AgERA5 pxv files, this is a guess
    READ(fu,REC=irec) dVar4, dVar0

    ! this may need to be deleted altogether for AgERA5 pxv data
    IF (dVar0 .eq. -9999) THEN
      ier = 1
      RETURN
    END IF

    ! apply scale factors
    ! we don't know the appropriate scaling factors for AgERA5 data
    ! if there are multiple different scalings, then add if block here for different variables
    ier = 0
    DO t=1,MXP
        dVar(t) = 0.01 * dVar4(t)
    END DO
    
    ! this variable may not exist in AgERA5 pxv files, we don't know
    dVar0x = 0.01 * dVar0

    RETURN

END SUBROUTINE gcmdat