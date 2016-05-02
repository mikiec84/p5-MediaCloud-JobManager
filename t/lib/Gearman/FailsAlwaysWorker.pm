package Gearman::FailsAlwaysWorker;

use strict;
use warnings;

use lib qw|lib/ t/lib/ t/lib/Gearman/ t/brokers/|;

use Moose;
with 'Gearman::TestWorker', 'FailsAlwaysWorker' => { -excludes => [ 'configuration' ], };

no Moose;

__PACKAGE__;