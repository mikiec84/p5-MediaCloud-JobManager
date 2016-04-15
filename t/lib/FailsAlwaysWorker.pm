package FailsAlwaysWorker;

use strict;
use warnings;

use Moose::Role;
use lib qw|lib/ t/lib/ t/brokers/|;
with 'MediaCloud::JobManager::Job';

# Run job
sub run($;$)
{
    my ( $self, $args ) = @_;

    die "The FailsAlwaysWorker failed (naturally).";
}

no Moose;    # gets rid of scaffolding

# Return package name instead of 1 or otherwise worker.pl won't know the name of the package it's loading
__PACKAGE__;
