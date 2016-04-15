package Gearman::FailsOnceWorker;

use strict;
use warnings;

use lib qw|lib/ t/lib/ t/lib/Gearman/ t/brokers/|;

use Moose;
with 'FailsOnceWorker', 'Gearman::TestWorker' => { -excludes => [ 'configuration' ], };

no Moose;

__PACKAGE__;
