package Addition;

use strict;
use warnings;
use Modern::Perl "2012";

use Moose;
with 'MediaCloud::JobManager::Job';

# Run job
sub run($;$)
{
    my ( $self, $args ) = @_;

    my $a = $args->{ a };
    my $b = $args->{ b };

    say STDERR "Going to add $a and $b";

    unless ( defined $a and defined $b )
    {
        die "Operands 'a' and 'b' must be defined.";
    }

    return $a + $b;
}

sub priority()
{
    return $MediaCloud::JobManager::Job::MJM_JOB_PRIORITY_LOW;
}

sub lazy_queue()
{
    return 1;
}

sub configuration()
{
    my $configuration = MediaCloud::JobManager::Configuration->new();
    $configuration->broker( MediaCloud::JobManager::Broker::RabbitMQ->new() );
    return $configuration;
}

no Moose;    # gets rid of scaffolding

# Return package name instead of 1 or otherwise worker.pl won't know the name of the package it's loading
__PACKAGE__;
