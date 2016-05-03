#!/usr/bin/env perl

use strict;
use warnings;
use Modern::Perl "2012";

use FindBin;
use lib "$FindBin::Bin/../lib";
use lib "$FindBin::Bin/../samples";

use MediaCloud::JobManager;
use MediaCloud::JobManager::Admin;
use NinetyNineBottlesOfBeer;
use Addition;
use AdditionAlwaysFails;
use Data::Dumper;

sub main()
{
    for ( my $x = 1 ; $x < 100 ; ++$x )
    {
        my $job_id = Addition->add_to_queue( { a => 3, b => 5 } );
        say STDERR "Job ID: $job_id";
    }
}

main();
