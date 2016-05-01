use strict;
use warnings;

use Test::More;
use lib qw|lib/ t/lib/ t/brokers/|;

require 'broker-tests.inc.pl';

use IO::Socket::INET;

sub _rabbitmq_is_installed_and_started()
{
    return ( system( 'rabbitmqctl status' ) == 0 );
}

sub main()
{
    unless ( _rabbitmq_is_installed_and_started() )
    {
        plan skip_all => "'rabbitmq-server' is not installed or not started";
    }
    else
    {
        plan tests => 18;

        run_tests( 'RabbitMQ' );

        Test::NoWarnings::had_no_warnings();
    }
}

main();
