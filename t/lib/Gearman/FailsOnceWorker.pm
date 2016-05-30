package Gearman::FailsOnceWorker;

use strict;
use warnings;

use lib qw|lib/ t/lib/ t/lib/Gearman/ t/brokers/|;

use Moose;
with 'Gearman::TestWorker', 'FailsOnceWorker' => { -excludes => [ 'configuration', 'lazy_queue' ], };

no Moose;

__PACKAGE__;
