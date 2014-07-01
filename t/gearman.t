use strict;
use warnings;

use Test::More;
use Proc::Background;
use IO::Socket::INET;


sub _gearmand_is_installed()
{
    return (system( 'gearmand --version' ) == 0);
}

sub _gearmand_is_started()
{
    my $socket = IO::Socket::INET->new(
        PeerAddr => 'localhost',
        PeerPort => 4730,
        Proto => 'tcp',
        Type => SOCK_STREAM
    );
    if ( $socket ) {
        close($socket);
        return 1;
    } else {
        return 0;
    }
}

unless ( _gearmand_is_installed() and _gearmand_is_started() ) {
    plan skip_all => "'gearmand' is not installed or not started";
} else {
    plan tests => 17;
}

use lib qw|lib/ t/lib/|;

use_ok( 'Gearman::JobScheduler' );
use_ok( 'Gearman::JobScheduler::AbstractFunction' );
use_ok( 'Gearman::JobScheduler::Admin' );
use_ok( 'Gearman::JobScheduler::Configuration' );
use_ok( 'Gearman::JobScheduler::ErrorLogTrapper' );
use_ok( 'Gearman::JobScheduler::Worker' );

# Test workers
use_ok( 'ReverseStringWorker' );
use_ok( 'FailsAlwaysWorker' );
use_ok( 'FailsOnceWorker' );


sub _worker_process($)
{
    my $function_name = shift;

    my $command = "perl ./script/gjs_worker.pl t/lib/${function_name}.pm";
    my $opts  = { 'die_upon_destroy' => 1 };

    my $proc = Proc::Background->new( $opts, $command );
    sleep( 1 );

    unless( $proc->alive ) {
        $proc = undef;
        die "Process '$command' failed to start.";
    }

    return $proc;
}


sub test_run_locally()
{
    {
        my $string = 'Hello World!';
        my $result = ReverseStringWorker->run_locally({ 'string' => $string });
        is( $result, reverse($string), 'run_locally() ReverseStringWorker' );
    }

    {
        eval {
            FailsAlwaysWorker->run_locally({ 'foo' => 'bar' });
        };
        ok( $@, 'run_locally() FailsAlwaysWorker');
    }

    {
        # 1
        eval {
            FailsOnceWorker->run_locally({ 'foo' => 'bar' });
        };
        ok( $@, 'run_locally() FailsOnceWorker #1');

        # 2
        my $result = FailsOnceWorker->run_locally({ 'foo' => 'bar' });
        is( $result, 42, 'run_locally() FailsOnceWorker #2');
    }
}

sub test_run_on_gearman()
{
    my $proc_1 = _worker_process( 'ReverseStringWorker');
    my $proc_2 = _worker_process( 'FailsAlwaysWorker');
    my $proc_3 = _worker_process( 'FailsOnceWorker');

    {
        my $string = 'Hello World!';
        my $result = ReverseStringWorker->run_on_gearman({ 'string' => $string });
        is( $result, reverse($string), 'run_on_gearman() ReverseStringWorker' );
    }

    {
        eval {
            FailsAlwaysWorker->run_on_gearman({ 'foo' => 'bar' });
        };
        ok( $@, 'run_on_gearman() FailsAlwaysWorker');
    }

    {
        # 1
        eval {
            FailsOnceWorker->run_on_gearman({ 'foo' => 'bar' });
        };
        ok( $@, 'run_on_gearman() FailsOnceWorker #1');

        # 2
        my $result = FailsOnceWorker->run_on_gearman({ 'foo' => 'bar' });
        is( $result, 42, 'run_on_gearman() FailsOnceWorker #2');
    }
}


sub main()
{
    test_run_locally();
    test_run_on_gearman();
}

main();
