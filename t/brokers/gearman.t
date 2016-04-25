use strict;
use warnings;

use Test::More;
use lib qw|lib/ t/lib/ t/brokers/|;

require 'broker-tests.inc.pl';

use IO::Socket::INET;

sub _gearmand_is_installed()
{
    return ( system( 'gearmand --version' ) == 0 );
}

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
    unless ( _gearmand_is_installed() and _gearmand_is_started() )
    {
        plan skip_all => "'gearmand' is not installed or not started";
    }
    else
    {
        plan tests => 18;

        run_tests( 'Gearman' );

        Test::NoWarnings::had_no_warnings();
    }
}

main();
