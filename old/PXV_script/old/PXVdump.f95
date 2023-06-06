PROGRAM read_pxv_data

    IMPLICIT NONE
    
    !!!!!
    ! this is a more modern, cleaned up, commented version of the PXVdump code that was provided by Gianluca on 3/20/23
    ! it essentially converts pxv data to prn
    ! reads mask info from base_cru30.rst, reads climate data from C2A22050.pxv, writes out the pxv data to a prn file
    ! input file source: PXV folder on Gianluca's Google Drive (link not included here for privacy)
    ! input file storage location: /gri/projects/rgmg/climate/UN_FAO/DataDownload03152023/PXV_script/
    ! the task is to modify this for converting the provided AgERA5 data pxv to prn. (see agERA5_PXV_dump.f95)
    !!!!!
    
    INTEGER, PARAMETER :: NX=360, NY=720, MXP=12, mask_fu=1,pxv_fu=2, out_fu=3
    INTEGER(KIND=1) :: base(NY)
    INTEGER :: io, icol, irow, rec_num, ier, t, ivar
    REAL :: dTx(MXP),dTn(MXP),dP(MXP),dS(MXP),dW(MXP),dRH(MXP) ! data arrays for 1 year of monthly data
    REAL :: dTxs,dTns,dPs,dSs,dWs,dRHs ! what are these
    CHARACTER(64) :: flngcm, flnout, flnbas ! file names
    
    ! open the base file which has the 0/1 mask on the appropriate grid
    flnbas = 'base_cru30.rst'
    OPEN(mask_fu, FILE=flnbas, IOSTAT=io, STATUS='old', ACCESS='direct', RECL=NY)
    
    ! open the pxv file
    flngcm='C2A22050.pxv'
    OPEN(pxv_fu, FILE=flngcm, STATUS='old', ACCESS='direct', RECL=6*(12+1)*2) 
    
    ! open a prn file for writing data to 
    flnout = 'test_write_data.prn'
    OPEN(out_fu, FILE=flnout, STATUS='new')
    
    901 FORMAT(A2,A12,A15)
    902 FORMAT(A2,A12,A25)
    900 FORMAT(2i5,12f8.2,f8.3)
    
    ! set variable here
    ! 1=Tmax,2=Tmin,3=Pr,4=Sunfr,5=Wind,6=RH
    ivar=3
    
    ! write header in the prn out file
    IF (ivar .eq. 1) THEN
      WRITE(out_fu,901) '# ',flngcm(1:12),' delta in deg C'
    ELSE IF (ivar .eq. 2) THEN
      WRITE(out_fu,901) '# ',flngcm(1:12),' delta in deg C'
    ELSE IF (ivar .eq. 3) THEN
      WRITE(out_fu,902) '# ',flngcm(1:12),' d% (threshold min 20mm!)'
    ELSE IF (ivar .eq. 4) THEN
      WRITE(out_fu,901) '# ',flngcm(1:12),' percent change'
    ELSE IF (ivar .eq. 5) THEN
      WRITE(out_fu,901) '# ',flngcm(1:12),' percent change'
    ELSE IF (ivar .eq. 6) THEN
      WRITE(out_fu,901) '# ',flngcm(1:12),' percent change'
    END IF
    
    ! Each record in the rst mask file contains mask values for a single longitude at all latitudes
    ! We create an index value (rec_num) to access records from the pxv file
    ! Each record in the pxv file contains all variables at all times for a single grid cell
    DO irow=1,NX ! loop lons
        READ(mask_fu,REC=irow) base ! mask values for all lats
        DO icol=1,NY ! loop lats
            rec_num=(irow-1)*NY+icol ! create an index for pxv file

            ! read and scale data from pxv file for a single grid cell
            CALL gcmdat(pxv_fu,rec_num,ier,dTx,dTn,dP,dS,dW,dRH,dTxs,dTns,dPs,dSs,dWs,dRHs)

            ! write row/col indexes and all data for the grid cell to a single line in a prn file
            IF (ier .eq. 0) THEN
                WRITE(out_fu,900) irow, icol, (100.*dP(t),t=1,MXP), 100.*dPs
            END IF
        END DO
    END DO  
END PROGRAM read_pxv_data



SUBROUTINE gcmdat(fu,irec,ier,dTx,dTn,dP,dS,dW,dRH,dTx0x,dTn0x,dP0x,dS0x,dW0x,dRH0x) 

    IMPLICIT NONE
    
    INTEGER, INTENT(IN) :: fu, irec
    INTEGER, INTENT(OUT) :: ier     
    INTEGER :: t 
    INTEGER, PARAMETER :: NX=360, NY=720, MXP=12
    INTEGER(KIND=2) :: dTx4(MXP),dTn4(MXP),dP4(MXP),dS4(MXP),dW4(MXP),dRH4(MXP) ! read from gcm file
    INTEGER(KIND=2) :: dTx0,dTn0,dP0,dS0,dW0,dRH0 ! read from gcm file
    REAL(KIND=4), INTENT(OUT) :: dTx(MXP),dTn(MXP),dP(MXP),dS(MXP),dW(MXP),dRH(MXP) ! rescaled arrays, returned
    REAL(KIND=4),INTENT(OUT) :: dTx0x, dTn0x, dP0x, dS0x, dW0x, dRH0x ! rescaled scalars, returned

    ! get a single record from the pxv file
    READ(fu,REC=irec) dTx4, dTn4, dP4, dS4, dW4, dRH4, dTx0, dTn0, dP0, dS0, dW0, dRH0

    ! what is dP0 and why do we jump out if it is missing?
    ! is it the annual mean?
    ! this likely doesn't apply to the AgERA5 pxv's
    IF (dP0 .eq. -9999) THEN
      ier = 1
      RETURN
    END IF

    ! apply scale factors
    ier = 0
    DO t=1,MXP
        dTx(t) = 0.01 * dTx4(t)
        dTn(t) = 0.01 * dTn4(t)
        dP(t)  = 0.0001 * dP4(t)
        dW(t)  = 0.0001 * dW4(t)
        dS(t)  = 0.0001 * dS4(t)
        dRH(t) = 0.0001 * dRH4(t)
    END DO
    
    dTx0x = 0.01 * dTx0
    dTn0x = 0.01 * dTn0
    dP0x  = 0.0001 * dP0
    dS0x  = 0.0001 * dS0
    dW0x  = 0.0001 * dW0
    dRH0x = 0.0001 * dRH0

    RETURN

END SUBROUTINE gcmdat