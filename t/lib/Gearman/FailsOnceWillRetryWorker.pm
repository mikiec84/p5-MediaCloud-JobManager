package Gearman::FailsOnceWillRetryWorker;

use strict;
use warnings;

use lib qw|lib/ t/lib/ t/lib/Gearman/ t/brokers/|;

use Moose;
with 'FailsOnceWillRetryWorker', 'Gearman::TestWorker' => { -excludes => [ 'configuration', 'retries' ], };

no Moose;

__PACKAGE__;
