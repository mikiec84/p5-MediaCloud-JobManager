package RabbitMQ::FailsOnceWillRetryWorker;

use strict;
use warnings;

use lib qw|lib/ t/lib/ t/lib/RabbitMQ/ t/brokers/|;

use Moose;
with 'RabbitMQ::TestWorker', 'FailsOnceWillRetryWorker' => { -excludes => [ 'configuration', 'retries' ], };

no Moose;

__PACKAGE__;
