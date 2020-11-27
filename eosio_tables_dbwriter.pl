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

my $last_irreversible = 0;
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
                 'pk        BIGINT UNSIGNED NOT NULL, ' .
                 'field     VARCHAR(64) NOT NULL, ' .
                 'fval      TEXT NULL) ENGINE=InnoDB;');

        $dbh->do('CREATE UNIQUE INDEX ' . $tbl_rows . '_I01 ON ' . $tbl_rows .
                 '(contract, scope, tbl, pk, field)');

        $dbh->do('CREATE INDEX ' . $tbl_rows . '_I02 ON ' . $tbl_rows .
                 '(contract, scope, tbl, field, fval(64))');
        
        $dbh->do('CREATE INDEX ' . $tbl_rows . '_I03 ON ' . $tbl_rows .
                 '(contract, tbl, field, fval(64))');

        
        $dbh->do('CREATE TABLE ' . $tbl_upd_op . '( ' .
                 'block_num BIGINT UNSIGNED NOT NULL, ' .
                 'op        TINYINT NOT NULL, ' .     # row operation: 1=new, 2=modified, 3=deleted
                 'opseq     INT UNSIGNED NOT NULL, ' .
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
                 'fval      TEXT NULL) ENGINE=InnoDB;');

        $dbh->do('CREATE INDEX ' . $tbl_upd_prev . '_I01 ON ' . $tbl_upd_prev .
                 '(block_num, opseq)');
        
        $dbh->do
            ('INSERT INTO SYNC (network, block_num, block_time, irreversible) ' .
             'VALUES(\'' . $network . '\',0,\'2000-01-01\', 0) ' .
             'ON DUPLICATE KEY UPDATE block_num=0, block_time=\'2020-01-01\', irreversible=0');
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

my $sth_select_op = $dbh->prepare
    ('SELECT op, opseq, contract, scope, tbl, pk FROM ' . $tbl_upd_op . ' ' .
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

        # roll back to the fork point
        while( $uncommitted_block >= $block_num )
        {
            my %prevval;
            $sth_select_prev->execute($uncommitted_block);
            while(my $vrow = $sth_select_prev->fetchrow_arrayref())
            {
                my ($seq, $contract, $scope, $tbl, $pk, $field, $fval) = @{$vrow};
                $prevval{$seq}{$contract}{$scope}{$tbl}{$pk}{$field} = $fval;
            }

            
            $sth_select_op->execute($uncommitted_block);
            my $r = $sth_select_op->fetchall_arrayref();
            foreach my $oprec (@{$r})
            {
                my ($seq, $op, $contract, $scope, $tbl, $pk) = @{$oprec};
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
            $sth_del_op->execute($uncommitted_block);
            $sth_del_prev->execute($uncommitted_block);
            printf STDERR ("Rolled back block %d\n", $uncommitted_block);
            $uncommitted_block--;
        }
        
        $dbh->commit();
        $stored_block = $uncommitted_block;
        $opseq = 0;
        return $block_num-1;
    }
    elsif( $msgtype == 1007 ) # CHRONICLE_MSGTYPE_TBL_ROW
    {
        my $kvo = $data->{'kvo'};
        return(0) unless ref($kvo->{'value'}) eq 'HASH';

        my $block_num = $data->{'block_num'};
        my $contract = $kvo->{'code'};
        my $scope = $kvo->{'scope'};
        my $tbl = $kvo->{'table'};
        my $pk = $kvo->{'primary_key'};

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
        
        if( $block_num > $last_irreversible and $op != 1 )
        {
            # row modified or deleted; save the previous values
            $sth_ins_op->execute($block_num, $opseq, $op, $contract, $scope, $tbl, $pk);
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
        $rows_counter++;
    }
    elsif( $msgtype == 1009 ) # CHRONICLE_MSGTYPE_RCVR_PAUSE
    {
        if( $uncommitted_block > $committed_block )
        {
            if( $uncommitted_block > $stored_block )
            {
                $sth_fork_sync->execute($network, $uncommitted_block);
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
                $dbh->commit();
                $stored_block = $uncommitted_block;
            }
            return $committed_block;
        }
    }

    return 0;
}





    
        


   
