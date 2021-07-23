[eloisewu@bigdataetl01 script]$ cat dw_load_cnss_s11.pl
#!/usr/bin/perl
                #perl $SCRIPTDIR/dw_load_cnss_s11.pl ${PROCESSDIR}/proc_$d ${CNSSDATADIR} proc_${d}
use Date::Manip::DM5;
use Time::Local;
use POSIX qw(strftime);
use POSIX qw(tzset);
$ENV{TZ} = 0;

my ($p_infile, $out_file, $cur_datetime);

my $srcdir = $ARGV[0];
my $targetdir = $ARGV[1];
my $fold_prefix = $ARGV[2];

my %ary_fh;
my $file_prefix="CNSSS11";

opendir(DIR,$srcdir) or die "Can't open $srcdir !";
@dirc = readdir(DIR);
$dirc=@dirc;

$cur_datetime=`date +'%Y%m%d%H%M%S'`;
chomp($cur_datetime);
#s11_16125_prb_stcnssmsp1_ipx_reports_data_2020_04_24_07_40_00_node1.csv.gz

foreach $file(@dirc)
{
        next if $file =~ /^\./;
        next if $file !~ /csv.gz$/;
        $p_infile = "$srcdir/$file";
        
        open IN, "gzip -dc $p_infile |" or die "Can't open $p_infile\n";
        while ( $line=<IN> )
        {
                $line =~ m/(\W[^,])(\w+)/;
                $record_time=substr($&,1);
                $v_fh=strftime("%Y%m%d",localtime($record_time));

                if (!defined($ary_fh{$v_fh})){
                        $ary_fh{$v_fh} = $v_fh;
 #CNSSS11_19700101_20200423221109_proc_1_00.gz
                        $out_file="$targetdir/$file_prefix\_$v_fh\_$cur_datetime\_$fold_prefix\_";
                        open ("$ary_fh{$v_fh}", "|split -d -l 100000000 --filter=\'gzip >> \$FILE.gz \' - $out_file");
                }

                $fh=$ary_fh{$v_fh};
                print $fh "$line";
        }

        close(IN);
}
        for $k (keys %ary_fh){
                close($k);
                close($ary_fh{$v_fh});
        }
closedir(DIR);


