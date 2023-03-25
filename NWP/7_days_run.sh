#!/bin/sh -f
ulimit -s unlimited

echo " 1. Defining Directories"

WPS=/home/tensor/WRF_BUILD/WPS
WA_HOME=/home/tensor/WRF_BUILD
WA=/home/tensor/WRF_BUILD/WRF/run
GFS=/home/tensor/DST_HACKTHON/Data_Sourcing/Input_Data/GFS_DATA
WA_OUT=/home/tensor/DST_HACKTHON/OUTPUT/Week_ahead
CDO=/usr/bin/cdo
MPI=/home/tensor/WRF_BUILD/LIBRARIES/mpich/bin/mpirun
PYTHON=/home/tensor/miniconda3/bin/python
WA_CODE=/home/tensor/DST_HACKTHON/Data_Sourcing
CODEBASE=/home/tensor/DST_HACKTHON/DB_upload/TD-WRF
CLEARSKY=/home/tensor/DST_HACKTHON/DB_upload/TD_CS

sleep 1s

echo " 2. Selecting Simulation Date"

TIME=$(date '+%H')
echo " Script running time is $TIME/00 Hours "
sleep 1s

if [ $TIME -eq 22 ]
then

echo "    Running for Week-Ahead Forecast "
sleep 1.5s
# For Day Ahead
Day=$(date -d "+1 days"  +%d)
Month=$(date -d "+1 days" +%m)
Year=$(date -d "+1 days"  +%Y)
Eday=$(date -d "+7 days" +%d)
Emon=$(date -d "+7 days" +%m)
EYear=$(date -d "+7 days" +%Y)
GDay=$(date -d "-1 days" +%d)
GMonth=$(date -d "-1 days" +%m)
GYear=$(date -d "-1 days" +%Y)
SDay=$(date -d "+0 days"  +%d)
SMonth=$(date -d "+0 days" +%m)
SYear=$(date -d "+0 days"  +%Y)

else

echo "       GFS Data not available for Day-Ahead Forecast "
echo "       Running for Today's Forecast "
sleep 1.5s
# For Current Date
Day=$(date -d "-0 days" +%d)
Month=$(date -d "-0 days" +%m)
Year=$(date -d "-0 days" +%Y)
Eday=$(date -d "+6 days" +%d)
Emon=$(date -d "+6 days" +%m)
EYear=$(date -d "+6 days" +%Y)
GDay=$(date -d "-2 days" +%d)
GMonth=$(date -d "-2 days" +%m)
GYear=$(date -d "-2 days" +%Y)
SDay=$(date -d "-1 days"  +%d)
SMonth=$(date -d "-1 days" +%m)
SYear=$(date -d "-1 days"  +%Y)

fi


echo "       Forecast Simulation Date  $Day/$Month/$Year        "
sleep 2s


echo " 3. Checking GFS Data for $Day/$Month/$Year "

cd $GFS
mkdir gfs_$Day-$Month-$Year
cp $WA_CODE/gfs_backup.py $GFS/gfs_$Day-$Month-$Year/
cp $WA_CODE/gfs_download.py $GFS/gfs_$Day-$Month-$Year/

cd $GFS/gfs_$Day-$Month-$Year/
$PYTHON gfs_backup.py $Day $Month $Year
echo "       GFS Data Downloaded for 7 days simulation"
sleep 2s

echo " 4. WA Preprocessing System "

echo '       Building WPS'
sleep 1s
cd $WPS

cat namelist.wps | sed "s/^.*start_date.*$/ start_date = '$SYear-$SMonth-$SDay\_18:00:00','$SYear-$SMonth-$SDay\_18:00:00','$SYear-$SMonth-$SDay\_00:00:00',/" > namelist.wps.new
mv namelist.wps.new namelist.wps
cat namelist.wps | sed "s/^.*end_date.*$/ end_date = '$EYear-$Emon-$Eday\_18:00:00','$EYear-$Emon-$Eday\_18:00:00','$EYear-$Emon-$Eday\_00:00:00',/" > namelist.wps.new
mv namelist.wps.new namelist.wps
#ln -sf ungrib/Variable_Tables/Vtable.GFS Vtable
./link_grib.csh $GFS/gfs_$Day-$Month-$Year/gfs*

echo '      Grib2 Data found'
cd $WA
FILE8=met_em.d02.$EYear-$Emon-$Eday\_18:00:00.nc
if [ -f "$FILE8" ]; then
    echo "   $FILE8 exist "
    echo "   Met_em files found : Proceeding with WA Simulation"
else
    echo "   Met_em Files not Found : Running WPS "

cd $WPS
sleep 10s
#./geogrid.exe
./ungrib.exe
./metgrid.exe
echo '      Initial Condition files generated'
#echo '      Removing Temp Files'

sleep 2s
fi

echo " 5. WA Simulation "

cd $WA

cat namelist.input | sed "s/^.*run_days.*$/ run_days                            = 07,/" > namelist.input.new
mv namelist.input.new namelist.input
cat namelist.input | sed "s/^.*run_hours.*$/ run_hours                           = 00,/" > namelist.input.new
mv namelist.input.new namelist.input
cat namelist.input | sed "s/^.*start_year.*$/ start_year                          = $SYear, $SYear, $Year,/" > namelist.input.new
mv namelist.input.new namelist.input
cat namelist.input | sed "s/^.*start_month.*$/ start_month                         = $SMonth, $SMonth, $Month,/" > namelist.input.new
mv namelist.input.new namelist.input
cat namelist.input | sed "s/^.*start_day.*$/ start_day                           = $SDay, $SDay, $Day,/" > namelist.input.new
mv namelist.input.new namelist.input
cat namelist.input | sed "s/^.*start_hour.*$/ start_hour                          = 18, 18, 00,/" > namelist.input.new
mv namelist.input.new namelist.input

cat namelist.input | sed "s/^.*end_year.*$/ end_year                            = $EYear, $EYear, $EYear,/" > namelist.input.new
mv namelist.input.new namelist.input
cat namelist.input | sed "s/^.*end_month.*$/ end_month                           = $Emon, $Emon, $Month,/" > namelist.input.new
mv namelist.input.new namelist.input
cat namelist.input | sed "s/^.*end_day.*$/ end_day                             = $Eday, $Eday, $Eday,/" > namelist.input.new
mv namelist.input.new namelist.input
cat namelist.input | sed "s/^.*end_hour.*$/ end_hour                            = 18, 18, 00,/" > namelist.input.new
mv namelist.input.new namelist.input
cat namelist.input | sed "s/^.*history_interval.*$/ history_interval                            = 15,    15,/" > namelist.input.new
mv namelist.input.new namelist.input
cat namelist.input | sed "s/^.*num_metgrid_levels.*$/ num_metgrid_levels                            = 42, /" > namelist.input.new
mv namelist.input.new namelist.input
cat namelist.input | sed "s/^.*e_vert.*$/ e_vert                            = 36, 36,/" > namelist.input.new
mv namelist.input.new namelist.input
cat namelist.input | sed "s/^.*p_top_requested.*$/ p_top_requested                            = 5000,/" > namelist.input.new
mv namelist.input.new namelist.input

cd $WA_OUT
mkdir $Year-$Month-$Day

cd $WA
FILE9=wrfout_d02_$SYear-$SMonth-$SDay\_18:00:00

if [ -f "$FILE9" ]; then
    echo "   $FILE9 exist "
    echo "   WA file exist"
else
    echo "   Starting WA-Simulation Run "

#cd $WRF
./real.exe
tail rsl.out.0000 | grep "SUCCESS"
echo '    Initialization successfull'
ulimit -s unlimited
echo "    Starting WA Simulation for $Day $Month $Year "
$MPI -np 2 ./wrf.exe

sleep 10s

fi
sleep 2s

#echo '##############RUNNING_WRF_SUCCESSFUL###############'
cd $WPS
cd $WA

if grep -q "SUCCESS COMPLETE WRF" rsl.out.0000; then
    echo "   WA run Successfull "
    echo "   file copying to WA_OUT "
mv wrfout_d02_$SYear-$SMonth-$SDay\_18:00:00 $WA_OUT/$Year-$Month-$Day/waout_d02.nc
mv wrfout_d01_$SYear-$SMonth-$SDay\_18:00:00 $WA_OUT/$Year-$Month-$Day/waout_d01.nc
find . -name "met_em*" -print0 | xargs -0 rm
cd $WPS
find . -name "FILE*" -print0 | xargs -0 rm
find . -name "GRIBFILE.A*" -print0 | xargs -0 rm
cd $GFS
rm -r gfs_$Day-$Month-$Year
#find . -name "gfs*" -print0 | xargs -0 rm
else
    echo "  WA run Unsuccessfull : check for error"
fi

echo " 6. Post Processing "

cd $WA_OUT/$Year-$Month-$Day/

echo "     Running climate data operators to extract variables"

$CDO select,name=T2,Q2,U10,V10,PSFC,SWDOWN,SWDDNI,SWDDIF,SWNORM,SWDDIR,SWDNBC,SWDOWN2 waout_d02.nc  $Year-$Month-$Day\_d02.nc

$CDO select,name=T2,Q2,U10,V10,PSFC,SWDOWN,SWDDNI,SWDDIF,SWNORM,SWDDIR,SWDNBC,SWDOWN2 waout_d01.nc  $Year-$Month-$Day\_d01.nc
$CDO delete,timestep=1,2 $Year-$Month-$Day\_d02.nc $Year-$Month-$Day.nc

sleep 2s
echo "      done extracting variables"
sleep 2s


rm waout_d01.nc
rm waout_d02.nc
rm $Year-$Month-$Day\_d02.nc
echo " uploading to Database  "
cd $CODEBASE
$PYTHON main.py
echo " uploading to Database done "
 
cd $CLEARSKY
$PYTHON main.py
