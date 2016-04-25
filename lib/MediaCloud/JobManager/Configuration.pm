package MediaCloud::JobManager::Configuration;

#
# Default configuration
#

use strict;
use warnings;
use Modern::Perl "2012";

use Moose 2.1005;
use MooseX::Singleton;    # ->instance becomes available
use MediaCloud::JobManager::Broker;
use MediaCloud::JobManager::Broker::Gearman;

# Instance of specific job broker
has 'broker' => (
    is      => 'rw',
    isa     => 'MediaCloud::JobManager::Broker',
    default => sub { return MediaCloud::JobManager::Broker::Gearman->new(); },
);

no Moose;                 # gets rid of scaffolding

1;
