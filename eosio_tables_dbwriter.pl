# install dependencies:
#  sudo apt install cpanminus libjson-xs-perl libjson-perl libmysqlclient-dev libdbi-perl
#  sudo cpanm Net::WebSocket::Server
#  sudo cpanm DBD::MariaDB

use strict;
use warnings;
use JSON;
use Getopt::Long;
use DBI;
use Time::HiRes qw (time);
use Time::Local 'timegm_nocheck';

use Net::WebSocket::Server;
use Protocol::WebSocket::Frame;

$Protocol::WebSocket::Frame::MAX_PAYLOAD_SIZE = 100*1024*1024;
$Protocol::WebSocket::Frame::MAX_FRAGMENTS_AMOUNT = 102400;
    
$| = 1;

my $network;

my $port = 8800;

my $dsn = 'DBI:MariaDB:database=eosio_tables;host=localhost';
my $db_user = 'eosio_tables';
my $db_password = 'Ohch3ook';
my $commit_every = 10;
my $endblock = 2**32 - 1;


my $ok = GetOptions
    ('network=s' => \$network,
     'port=i'    => \$port,
     'ack=i'     => \$commit_every,
     'endblock=i'  => \$endblock,
     'dsn=s'     => \$dsn,
     'dbuser=s'  => \$db_user,
     'dbpw=s'    => \$db_password,
    );


if( not $network or not $ok or scalar(@ARGV) > 0 )
{
    print STDERR "Usage: $0 --network=X [options...]\n",
        "Options:\n",
        "  --network=X        network name\n",
        "  --port=N           \[$port\] TCP port to listen to websocket connection\n",
        "  --ack=N            \[$commit_every\] Send acknowledgements every N blocks\n",
        "  --endblock=N       \[$endblock\] Stop before given block\n",
        "  --dsn=DSN          \[$dsn\]\n",
        "  --dbuser=USER      \[$db_user\]\n",
        "  --dbpw=PASSWORD    \[$db_password\]\n";
    exit 1;
}


my $dbh = DBI->connect($dsn, $db_user, $db_password,
                       {'RaiseError' => 1, AutoCommit => 0,
                        mariadb_server_prepare => 1});
die($DBI::errstr) unless $dbh;

my $committed_block = 0;
my $stored_block = 0;
my $uncommitted_block = 0;
{
    my $sth = $dbh->prepare
        ('SELECT block_num FROM SYNC WHERE network=?');
    $sth->execute($network);
    my $r = $sth->fetchall_arrayref();
    if( scalar(@{$r}) > 0 )
    {
        $stored_block = $r->[0][0];
        printf STDERR ("Starting from stored_block=%d\n", $stored_block);
    }
}

my $tbl_rows = $network . '_ROWS';
my $tbl_upd_prev = $network . '_UPD_PREV';
my $tbl_upd_new = $network . '_UPD_NEW';

{
    my $sth = $dbh->prepare
        ('SELECT count(*) FROM information_schema.tables where table_schema=DATABASE() and table_name=?');
           
    $sth->execute($tbl_rows);
    my $r = $sth->fetchall_arrayref();
    if( $r->[0][0] == 0 )
    {
        printf STDERR ("Creating tables for %s\n", $network);
        $dbh->do('CREATE TABLE ' . $tbl_rows . '( ' .
                 'contract  VARCHAR(13) NOT NULL, ' .
                 'scope     VARCHAR(13) NOT NULL, ' .
                 'tbl       VARCHAR(13) NOT NULL, ' .
                 'pk        BIGINT NOT NULL, ' .
                 'field     VARCHAR(64) NOT NULL, ' .
                 'fval      TEXT NOT NULL) ENGINE=InnoDB;');

        $dbh->do('CREATE UNIQUE INDEX ' . $tbl_rows . '_I01 ON ' . $tbl_rows .
                 '(contract, scope, tbl, pk, field)');

        $dbh->do('CREATE INDEX ' . $tbl_rows . '_I02 ON ' . $tbl_rows .
                 '(contract, scope, tbl, field, fval(64))');
        
        $dbh->do('CREATE INDEX ' . $tbl_rows . '_I03 ON ' . $tbl_rows .
                 '(contract, tbl, field, fval(64))');

        
        $dbh->do('CREATE TABLE ' . $tbl_upd_prev . '( ' .
                 'block_num  BIGINT NOT NULL, ' .
                 'contract  VARCHAR(13) NOT NULL, ' .
                 'scope     VARCHAR(13) NOT NULL, ' .
                 'tbl       VARCHAR(13) NOT NULL, ' .
                 'pk        BIGINT NOT NULL, ' .
                 'field     VARCHAR(64) NOT NULL, ' .
                 'fval      TEXT NOT NULL) ENGINE=InnoDB;');

        $dbh->do('CREATE UNIQUE INDEX ' . $tbl_upd_prev . '_I01 ON ' . $tbl_upd_prev .
                 '(block_num)');

        $dbh->do('CREATE TABLE ' . $tbl_upd_new . '( ' .
                 'block_num  BIGINT NOT NULL, ' .
                 'contract  VARCHAR(13) NOT NULL, ' .
                 'scope     VARCHAR(13) NOT NULL, ' .
                 'tbl       VARCHAR(13) NOT NULL, ' .
                 'pk        BIGINT NOT NULL) ENGINE=InnoDB;');

        $dbh->do('CREATE UNIQUE INDEX ' . $tbl_upd_new . '_I01 ON ' . $tbl_upd_new .
                 '(block_num)');

        $dbh->do
            ('INSERT INTO SYNC (network, block_num, block_time, irreversible) ' .
             'VALUES(\'' . $network . '\',0,\'2020-01-01\', 0) ' .
             'ON DUPLICATE KEY UPDATE block_num=0, block_time=\'2020-01-01\', irreversible=0');
    }
}


my $sth_upd_sync = $dbh->prepare
    ('UPDATE SYNC SET block_num=?, block_time=?, irreversible=? WHERE network=?');

my $sth_fork_sync = $dbh->prepare
    ('UPDATE SYNC SET block_num=? WHERE network = ?');

my $json = JSON->new;

my $blocks_counter = 0;
my $rows_counter = 0;
my $counter_start = time();


Net::WebSocket::Server->new(
    listen => $port,
    on_connect => sub {
        my ($serv, $conn) = @_;
        $conn->on(
            'binary' => sub {
                my ($conn, $msg) = @_;
                my ($msgtype, $opts, $js) = unpack('VVa*', $msg);
                my $data = eval {$json->decode($js)};
                if( $@ )
                {
                    print STDERR $@, "\n\n";
                    print STDERR $js, "\n";
                    exit;
                } 
                
                my $ack = process_data($msgtype, $data);
                if( $ack > 0 )
                {
                    $conn->send_binary(sprintf("%d", $ack));
                    print STDERR "ack $ack\n";
                }

                if( $ack >= $endblock )
                {
                    print STDERR "Reached end block\n";
                    exit(0);
                }
            },
            'disconnect' => sub {
                my ($conn, $code) = @_;
                print STDERR "Disconnected: $code\n";
                $dbh->rollback();
                $committed_block = 0;
                $uncommitted_block = 0;
            },
            
            );
    },
    )->start;


sub process_data
{
    my $msgtype = shift;
    my $data = shift;

    if( $msgtype == 1001 ) # CHRONICLE_MSGTYPE_FORK
    {
        my $block_num = $data->{'block_num'};
        print STDERR "fork at $block_num\n";
        $uncommitted_block = 0;
        return $block_num-1;
    }
    elsif( $msgtype == 1009 ) # CHRONICLE_MSGTYPE_RCVR_PAUSE
    {
        if( $uncommitted_block > $committed_block )
        {
            if( $uncommitted_block > $stored_block )
            {
                $sth_upd_sync->execute($network, $uncommitted_block, $uncommitted_block);
                $dbh->commit();
                $stored_block = $uncommitted_block;
            }
            $committed_block = $uncommitted_block;
            return $committed_block;
        }
    }
    elsif( $msgtype == 1010 ) # CHRONICLE_MSGTYPE_BLOCK_COMPLETED
    {
        $blocks_counter++;
        $uncommitted_block = $data->{'block_num'};
        if( $uncommitted_block - $committed_block >= $commit_every or
            $uncommitted_block >= $endblock )
        {
            $committed_block = $uncommitted_block;

            my $gap = 0;
            {
                my ($year, $mon, $mday, $hour, $min, $sec, $msec) =
                    split(/[-:.T]/, $data->{'block_timestamp'});
                my $epoch = timegm_nocheck($sec, $min, $hour, $mday, $mon-1, $year);
                $gap = (time() - $epoch)/3600.0;
            }
            
            my $period = time() - $counter_start;
            printf STDERR ("blocks/s: %8.2f, rows/block: %8.2f, gap: %8.2fh, ",
                           $blocks_counter/$period, $rows_counter/$blocks_counter, $gap);
            $counter_start = time();
            $blocks_counter = 0;
            $rows_counter = 0;
            
            if( $uncommitted_block > $stored_block )
            {
                $sth_upd_sync->execute($network, $uncommitted_block, $uncommitted_block);
                $dbh->commit();
                $stored_block = $uncommitted_block;
            }
            return $committed_block;
        }
    }

    return 0;
}





    
        


   
