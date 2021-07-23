export DWHOME=${DWHOME:-/app/HDSBAT}
export SCRIPTDIR=${DWHOME}/script

scriptname=`basename $0 |  cut -f1 -d'.'`
. ${SCRIPTDIR}/dw_master_hds.sh $scriptname N

jobmsg="Loading CNSS S11 FILES"

startup "${jobmsg}"

#dispatch_file_head_cp ${PROC_NUM} ${CNSSINPUTDIR} ${PROCESSDIR} "s11*csv.gz" 6000
function dispatch_file_head_cp {
        MVprocnum=$1
        MVsrcdir=$2
        MVdestdir=$3
        MVpattern=$4
        MVhead=$5

        MVproc_cnt=0;
        for fd in `seq $MVprocnum`
        do
                if [ ! -d ${MVdestdir}/proc_${fd} ];then
                        mkdir ${MVdestdir}/proc_${fd}
                        if [ $? -ne 0 ];then
                                echo "failed to create directory $destdir/proc_$fd"|tee -a $LOGFILE
                                errhandle "failed to create directory $destdir/proc_$fd"
                                exit 1
                        fi
                fi
        done
        ##for f in `ls -S $MVsrcdir/$MVpattern`
        chgdir $MVsrcdir
        for f in `find $MVsrcdir  -maxdepth 1 -name "$MVpattern"|sort|head -n${MVhead}`
        do
                MVdcnt=`expr $MVproc_cnt % $MVprocnum + 1`
                MVproc_cnt=`expr $MVproc_cnt + 1`
                cp $f ${MVdestdir}/proc_${MVdcnt} 1>>$LOGFILE 2>&1

                if [ $? -ne 0 ];then
                        echo "Dispatch File error"|tee -a $LOGFILE
                        errhandle "Dispatch File error"
                        exit 1
                fi
                echo "$f to cnss_s11" | tee -a $LSTFILE
                mv $f /opt/cnss/cnss_bkg
                if [ $? -ne 0 ];then
                        echo "mv File error"|tee -a $LOGFILE
                        errhandle "mv File error"
                        exit 1
                fi
        done
        echo "Dispatch File Finish"|tee -a $LOGFILE
}

CNSSDATADIR=${DATADIR}/CNSS_S11

CNSSINPUTDIR=/opt/cnss
CNSSINPUTDIR_BKG=/opt/cnss/cnss_bkg

COMPLETEDIR=${CNSSDATADIR}/complete
ERRORDIR=${CNSSDATADIR}/Error
PROCESSDIR=${CNSSDATADIR}/process

opttime=`date +"%Y%m%d%H%M%S"`

PROC_NUM=4
for p in `seq $PROC_NUM`;
do
if [ $p -le $PROC_NUM ];then
  if [ ! -d "${CNSSDATADIR}/process/proc_${p}" ];then
    mkdir -p ${CNSSDATADIR}/process/proc_${p}
  fi
fi
done

test -d ${ERRORDIR} || mkdir -p ${ERRORDIR}
test -d ${COMPLETEDIR} || mkdir -p ${COMPLETEDIR}

rm -rf ${CNSSDATADIR}/complete/*
rm -rf ${CNSSDATADIR}/*.gz


echo "[Step 1 Start Copy file .... ]  `date`" | tee -a $LOGFILE

dispatch_file_head_cp ${PROC_NUM} ${CNSSINPUTDIR} ${PROCESSDIR} "s11*csv.gz" 6000

echo "[Step 2 Start Converting .... ]  `date`" | tee -a $LOGFILE
for d in `seq $PROC_NUM`
do
{
file_cnt=`ls ${PROCESSDIR}/proc_$d |wc -l`
        if [ $file_cnt -ne 0 ];then
                perl $SCRIPTDIR/dw_load_cnss_s11.pl ${PROCESSDIR}/proc_$d ${CNSSDATADIR} proc_${d}

#               chgdir ${PROCESSDIR}/proc_$d
#               proc_tag="p"$d
#               zcat s11* | awk -F',' -v outpath=${CNSSDATADIR} -v proc_tag=$proc_tag -v sysdate="$opttime" -f ${SCRIPTDIR}/dw_load_cnss_s11.awk 
                if [ $? -ne 0 ];then
                        mv ${PROCESSDIR}/proc_$d/* ${ERRORDIR} 
                        errhandle "CNSS S11 convert ERROR"
                        exit 1
                else
                        mv ${PROCESSDIR}/proc_${d}/* ${COMPLETEDIR}/
                fi
        fi
}&
done
wait_subproc
echo "`date`" | tee -a $LOGFILE
echo "done for convert"| tee -a $LOGFILE


##echo "truncate table hive_GTP_GNGP partition (part_key='20170505');" >>${SQLDIR}/dw_dpi_asdr_load.sql
##
chgdir ${CNSSDATADIR}
cat /dev/null >${TMPDIR}/dw_cnss_load_s11.sql

#for cvt_file_name in `ls CNSSS11*.gz|sort|uniq`
for cvt_file_name in `ls CNSSS11*| grep -v 1970 |sort|uniq`
do
        gzip $cvt_file_name
        if [ $? -ne 0 ];then
                echo "Failed to gzip file ${CNSSDATADIR}/$cvt_file_name in S11 "|tee -a $LOGFILE
                errhandle "Failed to gzip file ${CNSSDATADIR}/$cvt_file_name in S11 "
                exit 1
        fi
        part_time=`echo $cvt_file_name|awk -F '_' '{print $2}'`

echo "alter table cnss_s11 add if not exists partition (part_key='$part_time') LOCATION '/HDS_VOL_HIVE/CNSS/cnss_s11/part_key=$part_time'; " >>${TMPDIR}/dw_cnss_load_s11.sql

done

echo "runhivesql;"| tee -a $LOGFILE
runhivesql ${TMPDIR}/dw_cnss_load_s11.sql

#for cv_file_name in `ls CNSSs11*.gz|sort`
for cv_file_name in `ls CNSSS11*.gz | grep -v 1970 |sort`
do
        part_time=`echo $cv_file_name|awk -F '_' '{print $2}'`
        echo "hadoop fs -copyFromLocal ${CNSSDATADIR}/$cv_file_name /HDS_VOL_HIVE/CNSS/cnss_s11/part_key=$part_time" | tee -a $LOGFILE
              hadoop fs -copyFromLocal ${CNSSDATADIR}/$cv_file_name /HDS_VOL_HIVE/CNSS/cnss_s11/part_key=$part_time 1>>$LOGFILE 2>&1
        if [ $? -ne 0 ];then
                echo "Failed to cp file ${CNSSDATADIR}/$cv_file_name in HDFS"|tee -a $LOGFILE
                errhandle "Failed to cp file ${CNSSDATADIR}/$cv_file_name in HDFS"
                exit 1
        fi

done
echo "copy done"| tee -a $LOGFILE

cleanup ${jobmsg}
exit 0









