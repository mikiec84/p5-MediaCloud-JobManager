package MediaCloud::JobManager::Broker::Null;

#
# Null broker used for initialization
#
# Usage:
#
# MediaCloud::JobManager::Broker::Null->new();
#

use strict;
use warnings;
use Modern::Perl "2012";

use Moose;
with 'MediaCloud::JobManager::Broker';

use Readonly;

use Log::Log4perl qw(:easy);
Log::Log4perl->easy_init(
    {
        level  => $DEBUG,
        utf8   => 1,
        layout => "%d{ISO8601} [%P]: %m%n"
    }
);

# flush sockets after every write
$| = 1;

use MediaCloud::JobManager;
use MediaCloud::JobManager::Job;

# Constructor
sub BUILD
{
    my $self = shift;
    my $args = shift;
}

sub start_worker($$)
{
    my ( $self, $function_name ) = @_;

    LOGDIE( "FIXME not implemented." );
}

sub run_job_sync($$$$)
{
    my ( $self, $function_name, $args, $priority ) = @_;

    LOGDIE( "FIXME not implemented." );
}

sub run_job_async($$$$)
{
    my ( $self, $function_name, $args, $priority ) = @_;

    LOGDIE( "FIXME not implemented." );
}

sub job_id_from_handle($$)
{
    my ( $self, $job_handle ) = @_;

    LOGDIE( "FIXME not implemented." );
}

sub set_job_progress($$$$)
{
    my ( $self, $job, $numerator, $denominator ) = @_;

    LOGDIE( "FIXME not implemented." );
}

sub job_status($$$)
{
    my ( $self, $function_name, $job_id ) = @_;

    LOGDIE( "FIXME not implemented." );
}

sub show_jobs($)
{
    my $self = shift;

    LOGDIE( "FIXME not implemented." );
}

sub cancel_job($)
{
    my ( $self, $job_id ) = @_;

    LOGDIE( "FIXME not implemented." );
}

sub server_status($$)
{
    my $self = shift;

    LOGDIE( "FIXME not implemented." );
}

sub workers($)
{
    my $self = shift;

    LOGDIE( "FIXME not implemented." );
}

no Moose;    # gets rid of scaffolding

1;
