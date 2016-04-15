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

# Where should the worker put the logs
has 'worker_log_dir' => (
    is      => 'rw',
    isa     => 'Str',
    default => $ENV{ MJM_WORKER_LOG_DIR } || '/var/tmp/mediacloud-jobmanager-logs/'
);

# Default email address to send the email from
has 'notifications_from_address' => (
    is      => 'rw',
    isa     => 'Str',
    default => 'mjm_donotreply@example.com'
);

# Notification email subject prefix
has 'notifications_subject_prefix' => (
    is      => 'rw',
    isa     => 'Str',
    default => '[MediaCloud::JobManager]'
);

# Emails that should receive notifications about failed jobs
has 'notifications_emails' => (
    is  => 'rw',
    isa => 'ArrayRef[Str]',

    # No one gets no mail by default:
    default => sub { [] }
);

no Moose;    # gets rid of scaffolding

1;
