use strict;
use warnings;

use Test::More;
use lib qw|lib/ t/lib/ t/brokers/|;

require 'broker-tests.inc.pl';

use IO::Socket::INET;

# Test workers
use Gearman::ReverseStringWorker;
use Gearman::FailsAlwaysWorker;
use Gearman::FailsOnceWorker;
use Gearman::FailsOnceWillRetryWorker;

sub _gearmand_is_started()
{
    my $socket = IO::Socket::INET->new(
        PeerAddr => 'localhost',
        PeerPort => 4730,
        Proto    => 'tcp',
        Type     => SOCK_STREAM
    );
    if ( $socket )
    {
        close( $socket );
        return 1;
    }
    else
    {
        return 0;
    }
}

sub main()
{
    unless ( _gearmand_is_started() )
    {
        plan skip_all => "'gearmand' is not started";
    }
    else
    {
        plan tests => 18;

        run_tests( 'Gearman' );

        Test::NoWarnings::had_no_warnings();
    }
}

main();
