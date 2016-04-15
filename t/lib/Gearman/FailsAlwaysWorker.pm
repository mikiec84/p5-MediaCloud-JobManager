package Gearman::FailsAlwaysWorker;

use strict;
use warnings;

use lib qw|lib/ t/lib/ t/lib/Gearman/ t/brokers/|;

use Moose;
with 'FailsAlwaysWorker', 'Gearman::TestWorker' => { -excludes => [ 'configuration' ], };

no Moose;

__PACKAGE__;
