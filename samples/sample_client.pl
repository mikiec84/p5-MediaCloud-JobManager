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
        say STDERR "Will run NinetyNineBottlesOfBeer remotely";
        my $job_id = NinetyNineBottlesOfBeer->add_to_queue( { how_many_bottles => $x } );
        say STDERR "Job ID: $job_id";
    }
}

main();
