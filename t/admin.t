use strict;
use warnings;

use Test::More;
use Test::NoWarnings;
use Proc::Background;
use File::Temp;
use File::Slurp;

require 'helpers.inc.pl';

unless ( _gearmand_is_installed() and _gearmand_is_started() ) {
    plan skip_all => "'gearmand' is not installed or not started";
} else {
    plan tests => 1 + 5;
}

use lib qw|lib/ t/lib/|;

use_ok( 'Gearman::JobScheduler::Admin' );
use_ok( 'ReverseStringWorker' );


sub _configuration()
{
    return ReverseStringWorker->configuration();
}

sub test_server_version()
{
    my $server_version = Gearman::JobScheduler::Admin::server_version(_configuration());

    ok( $server_version, 'server_version() - basic' );
    isa_ok( $server_version, ref({}), 'server_version() - returns hashref' );
    ok( scalar keys (%{ $server_version }) > 0, 'server_version() - returns at least one server' );
}

sub main()
{
    test_server_version()
}

main();
