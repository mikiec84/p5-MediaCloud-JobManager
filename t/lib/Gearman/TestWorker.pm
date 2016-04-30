package Gearman::TestWorker;

use strict;
use warnings;

use Moose::Role;
with 'MediaCloud::JobManager::Job' => { -excludes => [ 'configuration' ], };

use Data::Dumper;

use MediaCloud::JobManager::Configuration;
use MediaCloud::JobManager::Broker::Gearman;

sub configuration
{
    say STDERR "Using Gearman as a test job broker";

    # Configure TestWorker with Gearman broker
    my $configuration = MediaCloud::JobManager::Configuration->new();
    $configuration->broker( MediaCloud::JobManager::Broker::Gearman->new() );
    return $configuration;
}

1;
