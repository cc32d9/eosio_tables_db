# install dependencies:
#  sudo apt install cpanminus libjson-xs-perl libjson-perl libmysqlclient-dev libdbi-perl libredis-fast-perl
#  sudo cpanm Net::WebSocket::Server
#  sudo cpanm DBD::MariaDB

use strict;
use warnings;
use JSON;
use Getopt::Long;
use DBI;
use Time::HiRes qw (time);
use Time::Local 'timegm_nocheck';
use Redis::Fast;
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

my $use_redis;
my $redis_server;
my @redis_sentinels;
my $redis_service;
my $redis_queue = 'tblupd';


my $ok = GetOptions
    ('network=s' => \$network,
     'port=i'    => \$port,
     'ack=i'     => \$commit_every,
     'endblock=i'  => \$endblock,
     'dsn=s'     => \$dsn,
     'dbuser=s'  => \$db_user,
     'dbpw=s'    => \$db_password,
     'useredis'  => \$use_redis,
     'redis_server=s'    => \$redis_server,
     'redis_sentinels=s' => \@redis_sentinels,
     'redis_service=s'   => \$redis_service,
     'redis_queue=s'     => \$redis_queue,
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
        "  --dbpw=PASSWORD    \[$db_password\]\n",
        "  --useredis         push table updates to Redis queue\n",
        "  --redis_server=SRV\n",
        "  --redis_sentinels=S1 --redis_sentinels=S2...\n",
        "  --redis_service=SVC\n",
        "  --redis_queue=Q    \[$redis_queue\]\n";

    exit 1;
}


my $redis;
if( $use_redis )
{
    my %args;
    if( scalar(@redis_sentinels) > 0 )
    {
        die("--redis_service undefined") unless defined($redis_service);
        %args = (
            sentinels => \@redis_sentinels,
            service => $redis_service,
            sentinels_cnx_timeout => 0.1,
            sentinels_read_timeout => 1,
            sentinels_write_timeout => 1);
    }
    elsif( defined($redis_server) )
    {
        %args = (
            server => $redis_server,
            reconnect => 60, every => 1000000);
    }

    $redis = Redis::Fast->new(%args);
}


my $dbh = DBI->connect($dsn, $db_user, $db_password,
                       {'RaiseError' => 1, AutoCommit => 0,
                        mariadb_server_prepare => 1});
die($DBI::errstr) unless $dbh;

my $last_irreversible = 2**32 - 1;
my $committed_block = 0;
my $stored_block = 0;
my $uncommitted_block = 0;
my $opseq = 0;
{
    my $sth = $dbh->prepare
        ('SELECT block_num FROM SYNC WHERE network=?');
    $sth->execute($network);
    my $r = $sth->fetchall_arrayref();
    if( scalar(@{$r}) > 0 )
    {
        $uncommitted_block = $stored_block = $r->[0][0];
        printf STDERR ("Starting from stored_block=%d\n", $stored_block);
    }
}

my %watchcontracts;
{
    my $sth = $dbh->prepare
        ('SELECT contract FROM WATCH_CONTRACTS WHERE network=?');
    $sth->execute($network);
    my $r = $sth->fetchall_arrayref();
    die('nothing in WATCH_CONTRACTS') if scalar(@{$r}) == 0;
    foreach my $row (@{$r}) {
        $watchcontracts{$row->[0]} = 1;
        print STDERR "Watching contract: " . $row->[0] . "\n";
    }
}


my %redisexport;
if( $use_redis )
{
    my $sth = $dbh->prepare
        ('SELECT contract FROM EXPORT_TBL_UPDATES WHERE network=?');
    $sth->execute($network);
    my $r = $sth->fetchall_arrayref();
    foreach my $row (@{$r}) {
        $redisexport{$row->[0]} = 1;
        print STDERR "Exporting contract deltas: " . $row->[0] . "\n";
    }
}

my %lastupd;
my %prev_lastupd;

my $tbl_rows = $network . '_ROWS';
my $tbl_upd_op = $network . '_UPD_OP';     # row operation: new/modified/deleted
my $tbl_upd_prev = $network . '_UPD_PREV'; # row values before updates

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
                 'pk        BIGINT UNSIGNED NOT NULL, ' . # primary key as hex string
                 'field     VARCHAR(64) NOT NULL, ' .
                 'fval      LONGBLOB NULL) ENGINE=InnoDB;');

        $dbh->do('CREATE UNIQUE INDEX ' . $tbl_rows . '_I01 ON ' . $tbl_rows .
                 '(contract, scope, tbl, pk, field)');

        $dbh->do('CREATE INDEX ' . $tbl_rows . '_I02 ON ' . $tbl_rows .
                 '(contract, scope, tbl, field, fval(64))');

        $dbh->do('CREATE INDEX ' . $tbl_rows . '_I03 ON ' . $tbl_rows .
                 '(contract, tbl, field, fval(64))');

        $dbh->do('CREATE INDEX ' . $tbl_rows . '_I04 ON ' . $tbl_rows .
                 '(fval(64))');

        $dbh->do('CREATE TABLE ' . $tbl_upd_op . '( ' .
                 'block_num BIGINT UNSIGNED NOT NULL, ' .
                 'opseq     INT UNSIGNED NOT NULL, ' .
                 'op        TINYINT NOT NULL, ' .     # row operation: 1=new, 2=modified, 3=deleted
                 'contract  VARCHAR(13) NOT NULL, ' .
                 'scope     VARCHAR(13) NOT NULL, ' .
                 'tbl       VARCHAR(13) NOT NULL, ' .
                 'pk        BIGINT UNSIGNED NOT NULL) ENGINE=InnoDB;');

        $dbh->do('CREATE UNIQUE INDEX ' . $tbl_upd_op . '_I01 ON ' . $tbl_upd_op .
                 '(block_num, opseq)');


        $dbh->do('CREATE TABLE ' . $tbl_upd_prev . '( ' .
                 'block_num  BIGINT UNSIGNED NOT NULL, ' .
                 'opseq     INT UNSIGNED NOT NULL, ' .
                 'contract  VARCHAR(13) NOT NULL, ' .
                 'scope     VARCHAR(13) NOT NULL, ' .
                 'tbl       VARCHAR(13) NOT NULL, ' .
                 'pk        BIGINT UNSIGNED NOT NULL, ' .
                 'field     VARCHAR(64) NOT NULL, ' .
                 'fval      LONGBLOB NULL) ENGINE=InnoDB;');

        $dbh->do('CREATE INDEX ' . $tbl_upd_prev . '_I01 ON ' . $tbl_upd_prev .
                 '(block_num, opseq)');

        $dbh->do
            ('INSERT INTO SYNC (network, block_num, block_time, irreversible) ' .
             'VALUES(\'' . $network . '\',0,\'2000-01-01\', 0) ' .
             'ON DUPLICATE KEY UPDATE block_num=0, block_time=\'2020-01-01\', irreversible=0');
        $dbh->commit();
    }
}


my $sth_upd_sync = $dbh->prepare
    ('UPDATE SYNC SET block_num=?, block_time=?, irreversible=? WHERE network=?');

my $sth_fork_sync = $dbh->prepare
    ('UPDATE SYNC SET block_num=? WHERE network = ?');

my $sth_check_row = $dbh->prepare
    ('SELECT field, fval FROM ' . $tbl_rows . ' ' .
     'WHERE contract=? AND scope=? AND tbl=? AND pk=?');

my $sth_upd_field = $dbh->prepare
    ('UPDATE ' . $tbl_rows . ' SET fval=? ' .
     'WHERE contract=? AND scope=? AND tbl=? AND pk=? AND field=?');

my $sth_ins_field = $dbh->prepare
    ('INSERT INTO ' . $tbl_rows . ' (contract, scope, tbl, pk, field, fval) ' .
     'VALUES(?,?,?,?,?,?)');

my $sth_ins_fast_field = $dbh->prepare
    ('INSERT INTO ' . $tbl_rows . ' (contract, scope, tbl, pk, field, fval) ' .
     'VALUES(?,?,?,?,?,?) ' .
     'ON DUPLICATE KEY UPDATE fval=?');

my $sth_del_row = $dbh->prepare
    ('DELETE FROM ' . $tbl_rows . ' ' .
     'WHERE contract=? AND scope=? AND tbl=? AND pk=?');


my $sth_ins_op = $dbh->prepare
    ('INSERT INTO ' . $tbl_upd_op . ' (block_num, opseq, op, contract, scope, tbl, pk) ' .
     'VALUES(?,?,?,?,?,?,?)');

my $sth_del_op = $dbh->prepare
        ('DELETE FROM ' . $tbl_upd_op . ' WHERE block_num=?');

my $sth_clean_op = $dbh->prepare
    ('DELETE FROM ' . $tbl_upd_op . ' WHERE block_num <= ?');

my $sth_select_op_fork_blocks = $dbh->prepare
    ('SELECT DISTINCT block_num FROM ' . $tbl_upd_op . ' ' .
     'WHERE block_num >= ? ORDER BY block_num DESC');

my $sth_select_op = $dbh->prepare
    ('SELECT opseq, op, contract, scope, tbl, pk FROM ' . $tbl_upd_op . ' ' .
     'WHERE block_num=? ORDER BY opseq DESC');


my $sth_ins_prev = $dbh->prepare
    ('INSERT INTO ' . $tbl_upd_prev . ' (block_num, opseq, contract, scope, tbl, pk, field, fval) ' .
     'VALUES(?,?,?,?,?,?,?,?)');

my $sth_del_prev = $dbh->prepare
        ('DELETE FROM ' . $tbl_upd_prev . ' ' .
         'WHERE block_num=?');

my $sth_clean_prev = $dbh->prepare
    ('DELETE FROM ' . $tbl_upd_prev . ' WHERE block_num <= ?');

my $sth_select_prev = $dbh->prepare
    ('SELECT opseq, contract, scope, tbl, pk, field, fval FROM ' . $tbl_upd_prev . ' ' .
     'WHERE block_num=?');

my $sth_lastupd = $dbh->prepare
    ('INSERT INTO CONTRACT_LAST_UPD (network, contract, block_num) ' .
     'VALUES(?,?,?) ' .
     'ON DUPLICATE KEY UPDATE block_num=?');



my $json = JSON->new;
$json->canonical;

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

                my $ack = process_data($msgtype, $data, \$js);
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
    my $jsref = shift;

    if( $msgtype == 1001 ) # CHRONICLE_MSGTYPE_FORK
    {
        my $block_num = $data->{'block_num'};
        print STDERR "fork at $block_num\n";

        # roll back to the fork point
        $sth_select_op_fork_blocks->execute($block_num);
        my $forkblocks = $sth_select_op_fork_blocks->fetchall_arrayref();
        foreach my $fb (@{$forkblocks})
        {
            my $bn = $fb->[0];
            my %prevval;
            $sth_select_prev->execute($bn);
            while(my $vrow = $sth_select_prev->fetchrow_arrayref())
            {
                my ($seq, $contract, $scope, $tbl, $pk, $field, $fval) = @{$vrow};
                $prevval{$seq}{$contract}{$scope}{$tbl}{$pk}{$field} = $fval;
            }


            $sth_select_op->execute($bn);
            my $r = $sth_select_op->fetchall_arrayref();
            foreach my $oprec (@{$r})
            {
                my ($seq, $op, $contract, $scope, $tbl, $pk) = @{$oprec};
                $lastupd{$contract} = $block_num;
                if( $op == 1 )
                {
                    # row was inserted, now delete it
                    $sth_del_row->execute($contract, $scope, $tbl, $pk);
                }
                else
                {
                    if( not defined($prevval{$seq}{$contract}{$scope}{$tbl}{$pk}) )
                    {
                        print STDERR "Cannot find previous value for " .
                            join(',', $contract, $scope, $tbl, $pk) . "\n";
                    }
                    else
                    {
                        while(my ($field, $fval) = each %{$prevval{$seq}{$contract}{$scope}{$tbl}{$pk}})
                        {
                            if( $op == 2 )
                            {
                                # updated row, updating it back
                                $sth_upd_field->execute($fval, $contract, $scope, $tbl, $pk, $field);
                            }
                            else
                            {
                                # deleted row, restore it back
                                $sth_ins_field->execute($contract, $scope, $tbl, $pk, $field, $fval);
                            }
                        }
                    }
                }
            }
            $sth_del_op->execute($bn);
            $sth_del_prev->execute($bn);
            printf STDERR ("Rolled back block %d\n", $bn);
            $uncommitted_block--;
        }

        save_lastupd();
        $dbh->commit();
        $uncommitted_block = $block_num-1;
        $stored_block = $uncommitted_block;
        $opseq = 0;
        return $uncommitted_block;
    }
    elsif( $msgtype == 1007 ) # CHRONICLE_MSGTYPE_TBL_ROW
    {
        my $kvo = $data->{'kvo'};
        my $contract = $kvo->{'code'};

        return(0) unless exists($watchcontracts{$contract});
        return(0) unless ref($kvo->{'value'}) eq 'HASH';

        if( exists($redisexport{$contract}) ) {
            $redis->lpush($redis_queue, ${$jsref});
        }

        my $block_num = $data->{'block_num'};
        my $scope = $kvo->{'scope'};
        my $tbl = $kvo->{'table'};
        my $pk = $kvo->{'primary_key'};

        $lastupd{$contract} = $block_num;

        if( $block_num > $last_irreversible )
        {
            my $op;

            $sth_check_row->execute($contract, $scope, $tbl, $pk);
            my $prevrow = $sth_check_row->fetchall_arrayref();
            if( scalar(@{$prevrow}) > 0 )
            {
                if( $data->{'added'} eq 'true' )
                {
                    # this is an update of existing row
                    $op = 2;
                }
                else
                {
                    # the row is deleted
                    $op = 3;
                }
            }
            else
            {
                if( $data->{'added'} eq 'true' )
                {
                    # this is a new row
                    $op = 1;
                }
                else
                {
                    #print STDERR "Database corrupted, row deleted but does not exist: " .
                    #    join(',', $contract, $scope, $tbl, $pk) . "\n";
                    return 0;
                }
            }

            # row modified or deleted; save the previous values
            $sth_ins_op->execute($block_num, $opseq, $op, $contract, $scope, $tbl, $pk);
            if( $op != 1 )
            {
                foreach my $prevval (@{$prevrow})
                {
                    $sth_ins_prev->execute($block_num, $opseq, $contract, $scope, $tbl, $pk, @{$prevval});
                }
            }

            if( $op != 3 ) # row inserted or modified
            {
                while(my ($field, $fval) = each %{$kvo->{'value'}})
                {
                    if( ref($fval) ne '' )
                    {
                        if( JSON::is_bool($fval) )
                        {
                            $fval = scalar($fval);
                        }
                        else
                        {
                            $fval = $json->encode($fval);
                        }
                    }

                    if( $op == 1 )
                    {
                        # new row
                        $sth_ins_field->execute($contract, $scope, $tbl, $pk, $field, $fval);
                    }
                    elsif( $op == 2 )
                    {
                        # updated row
                        $sth_upd_field->execute($fval, $contract, $scope, $tbl, $pk, $field);
                    }
                }
            }
            else
            {
                # row deleted
                $sth_del_row->execute($contract, $scope, $tbl, $pk);
            }

            $opseq++;
        }
        else
        {
            # this is irreversible update, do the fast track
            if( $data->{'added'} eq 'true' )
            {
                while(my ($field, $fval) = each %{$kvo->{'value'}})
                {
                    if( ref($fval) ne '' )
                    {
                        if( JSON::is_bool($fval) )
                        {
                            $fval = scalar($fval);
                        }
                        else
                        {
                            $fval = $json->encode($fval);
                        }
                    }

                    $sth_ins_fast_field->execute($contract, $scope, $tbl, $pk, $field, $fval, $fval);
                }
            }
            else
            {
                # row deleted
                $sth_del_row->execute($contract, $scope, $tbl, $pk);
            }
        }

        $rows_counter++;
    }
    elsif( $msgtype == 1009 ) # CHRONICLE_MSGTYPE_RCVR_PAUSE
    {
        if( $uncommitted_block > $committed_block )
        {
            if( $uncommitted_block > $stored_block )
            {
                $sth_fork_sync->execute($network, $uncommitted_block);
                save_lastupd();
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
        $opseq = 0;

        my $old_last_irrev = $last_irreversible;
        $last_irreversible = $data->{'last_irreversible'};
        if( $old_last_irrev < $last_irreversible )
        {
            $sth_clean_prev->execute($last_irreversible);
            $sth_clean_op->execute($last_irreversible);
        }

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
                my $block_time = $data->{'block_timestamp'};
                $block_time =~ s/T/ /;
                $sth_upd_sync->execute($uncommitted_block, $block_time, $last_irreversible, $network);
                save_lastupd();
                $dbh->commit();
                $stored_block = $uncommitted_block;
            }
            return $committed_block;
        }
    }

    return 0;
}



sub save_lastupd
{
    foreach my $contract (keys %lastupd)
    {
        my $block_num = $lastupd{$contract};
        if( not exists($prev_lastupd{$contract}) or $prev_lastupd{$contract} != $block_num )
        {
            $sth_lastupd->execute($network, $contract, $block_num, $block_num);
            $prev_lastupd{$contract} = $block_num;
        }
    }
}
