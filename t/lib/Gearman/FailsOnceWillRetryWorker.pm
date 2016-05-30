package Gearman::FailsOnceWillRetryWorker;

use strict;
use warnings;

use lib qw|lib/ t/lib/ t/lib/Gearman/ t/brokers/|;

use Moose;
with 'Gearman::TestWorker', 'FailsOnceWillRetryWorker' => { -excludes => [ 'configuration', 'lazy_queue', 'retries' ], };

sub retries()
{
    return 3;
}

no Moose;

__PACKAGE__;
