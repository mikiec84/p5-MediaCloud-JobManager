package FailsOnceWorker;

use strict;
use warnings;

use Moose;
with 'Gearman::JobScheduler::AbstractFunction';

my $second_run;

# Run job
sub run($;$)
{
    my ($self, $args) = @_;

    if ( $second_run ) {
        say STDERR "It's not the first time I'm being run, so not failing today.";
        return 42;
    } else {
        $second_run = 1;
        die "It's the first time I'm being run, so I'm failing.";
    }
}

no Moose;    # gets rid of scaffolding

# Return package name instead of 1 or otherwise worker.pl won't know the name of the package it's loading
__PACKAGE__;
