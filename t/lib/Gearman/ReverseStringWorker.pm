package Gearman::ReverseStringWorker;

use strict;
use warnings;

use lib qw|lib/ t/lib/ t/lib/Gearman/ t/brokers/|;

use Moose;
with 'ReverseStringWorker', 'Gearman::TestWorker' => { -excludes => [ 'configuration' ], };

no Moose;

__PACKAGE__;
