package MediaCloud::JobManager::Broker::Gearman;

#
# Gearman job broker
#
# Usage:
#
# MediaCloud::JobManager::Broker::Gearman->new( servers => [ '127.0.0.1:4731' ] );
#

use strict;
use warnings;
use Modern::Perl "2012";

use Moose;
with 'MediaCloud::JobManager::Broker';

use Gearman::XS qw(:constants);
use Gearman::XS::Client;
use Gearman::XS::Task;
use Gearman::XS::Worker;
use JSON;
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

# Neither "Gearman" nor "Gearman::XS" modules provide the administration
# functionality, so we'll connect directly to Gearman and send / receive
# commands ourselves.
use Net::Telnet;

use MediaCloud::JobManager;
use MediaCloud::JobManager::Job;

# Gearman connection timeout
Readonly my $GEARMAND_ADMIN_TIMEOUT => 10;

# JSON (de)serializer
my $json = JSON->new->allow_nonref->canonical->utf8;

# Arrayref of default Gearman servers to connect to
has '_servers' => (
    is      => 'rw',
    isa     => 'ArrayRef[Str]',
    default => sub { [ '127.0.0.1:4730' ] }
);

# Constructor
sub BUILD
{
    my $self = shift;
    my $args = shift;

    if ( $args->{ servers } )
    {
        unless ( ref $args->{ servers } eq ref [] )
        {
            LOGDIE( "'servers' is not an arrayref" );
        }
        $self->_servers( $args->{ servers } );
    }
}

sub start_worker($$)
{
    my ( $self, $function_name ) = @_;

    my $worker = new Gearman::XS::Worker;

    INFO( "Will use Gearman servers: " . join( ' ', @{ $self->_servers } ) );

    my $ret = $worker->add_servers( join( ',', @{ $self->_servers } ) );
    unless ( $ret == GEARMAN_SUCCESS )
    {
        LOGDIE( "Unable to add Gearman servers: " . $worker->error() );
    }

    $ret = $worker->add_function(
        $function_name,
        0,
        sub {
            my ( $job ) = shift;

            my $job_handle = $job->handle();
            my $result_serialized;
            eval {

                # Args were serialized by run_job_[a]sync()
                my $args = _unserialize_hashref( $job->workload() );

                # Do the thing
                my $result = $function_name->run_locally( $args, $job );

                # Create a hashref and serialize result because it's going to be passed over to job broker
                $result_serialized = _serialize_hashref( { 'result' => $result } );
            };
            if ( $@ )
            {
                ERROR( "Job '$job_handle' died: $@" );
                $job->send_fail();
                return undef;
            }
            else
            {
                $job->send_complete( $result_serialized );

                # run_job_sync() will unserialize if needed
                return $result_serialized;
            }
        },
        0
    );
    unless ( $ret == GEARMAN_SUCCESS )
    {
        LOGDIE( "Unable to add function '$function_name': " . $worker->error() );
    }

    INFO( "Worker is ready and accepting jobs" );
    while ( 1 )
    {
        $ret = $worker->work();
        unless ( $ret == GEARMAN_SUCCESS )
        {
            LOGDIE( "Unable to execute job: " . $worker->error() );
        }
    }
}

sub run_job_sync($$$$$)
{
    my ( $self, $function_name, $args, $priority, $unique ) = @_;

    my $args_serialized = _serialize_hashref( $args );

    # Gearman::XS::Client seems to not like undefined or empty workload()
    # so we pass 0 instead
    $args_serialized ||= 0;

    my $client = $self->_gearman_xs_client();

    # Client arguments
    my @client_args;
    if ( $unique )
    {
        # If the job is set to be "unique", we need to pass a "unique identifier"
        # to broker so that it knows which jobs to merge into one
        @client_args = ( $function_name, $args_serialized, MediaCloud::JobManager::unique_job_id( $function_name, $args ) );
    }
    else
    {
        @client_args = ( $function_name, $args_serialized );
    }

    # Choose the client subroutine to use (based on the priority)
    my $client_do_ref = undef;
    if ( $priority eq $MediaCloud::JobManager::Job::MJM_JOB_PRIORITY_LOW )
    {
        $client_do_ref = sub { $client->do_low( @_ ) };
    }
    elsif ( $priority eq $MediaCloud::JobManager::Job::MJM_JOB_PRIORITY_NORMAL )
    {
        $client_do_ref = sub { $client->do( @_ ) };
    }
    elsif ( $priority eq $MediaCloud::JobManager::Job::MJM_JOB_PRIORITY_HIGH )
    {
        $client_do_ref = sub { $client->do_high( @_ ) };
    }
    else
    {
        LOGDIE( "Unknown job priority: $priority" );
    }

    # Do the job
    my ( $ret, $result ) = &{ $client_do_ref }( @client_args );
    unless ( $ret == GEARMAN_SUCCESS )
    {
        LOGDIE( "Job broker failed: " . $client->error() );
    }

    # Deserialize the results (because they were serialized and put into
    # hashref by worker())
    my $result_deserialized = _unserialize_hashref( $result );
    if ( ref $result_deserialized eq ref {} )
    {
        return $result_deserialized->{ result };
    }
    else
    {
        # No result
        return undef;
    }
}

sub run_job_async($$$$$)
{
    my ( $self, $function_name, $args, $priority, $unique ) = @_;

    my $args_serialized = _serialize_hashref( $args );

    # Gearman::XS::Client seems to not like undefined or empty workload()
    # so we pass 0 instead
    $args_serialized ||= 0;

    my $client = $self->_gearman_xs_client();

    # Client arguments
    my @client_args;
    if ( $unique )
    {
        # If the job is set to be "unique", we need to pass a "unique identifier"
        # to broker so that it knows which jobs to merge into one
        @client_args = ( $function_name, $args_serialized, MediaCloud::JobManager::unique_job_id( $function_name, $args ) );
    }
    else
    {
        @client_args = ( $function_name, $args_serialized );
    }

    # Choose the client subroutine to use (based on the priority)
    my $client_do_ref = undef;
    if ( $priority eq $MediaCloud::JobManager::Job::MJM_JOB_PRIORITY_LOW )
    {
        $client_do_ref = sub { $client->do_low_background( @_ ) };
    }
    elsif ( $priority eq $MediaCloud::JobManager::Job::MJM_JOB_PRIORITY_NORMAL )
    {
        $client_do_ref = sub { $client->do_background( @_ ) };
    }
    elsif ( $priority eq $MediaCloud::JobManager::Job::MJM_JOB_PRIORITY_HIGH )
    {
        $client_do_ref = sub { $client->do_high_background( @_ ) };
    }
    else
    {
        LOGDIE( "Unknown job priority: $priority" );
    }

    # Do the job
    my ( $ret, $job_id ) = &{ $client_do_ref }( @client_args );
    unless ( $ret == GEARMAN_SUCCESS )
    {
        LOGDIE( "Job broker failed: " . $client->error() );
    }

    INFO( "Added job '$job_id' to queue" );

    return $job_id;
}

sub job_id_from_handle($$)
{
    my ( $self, $job ) = @_;

    my $job_handle;
    if ( ref $job eq ref '' )
    {
        $job_handle = $job;
    }
    else
    {
        unless ( defined $job->handle() )
        {
            LOGDIE( "Unable to find a job ID to be used for logging" );
        }
        $job_handle = $job->handle();
    }

    my $job_id;

    # Strip the host part (if present)
    if ( index( $job_handle, '//' ) != -1 )
    {
        # "127.0.0.1:4730//H:localhost.localdomain:8"
        my ( $server, $job_id ) = split( '//', $job_handle );
    }
    else
    {
        # "H:localhost.localdomain:8"
        $job_id = $job_handle;
    }

    # Validate
    unless ( $job_id =~ /^H:.+?:\d+?$/ )
    {
        LOGDIE( "Invalid job ID: $job_id" );
    }

    return $job_id;
}

sub set_job_progress($$$$)
{
    my ( $self, $job, $numerator, $denominator ) = @_;

    my $ret = $self->job->send_status( $numerator, $denominator );
    unless ( $ret == GEARMAN_SUCCESS )
    {
        LOGDIE( "Unable to send job status: " . $self->_job->error() );
    }
}

sub job_status($$$)
{
    my ( $self, $function_name, $job_id ) = @_;

    my $client = _gearman_xs_client();
    my ( $ret, $known, $running, $numerator, $denominator ) = $client->job_status( $job_id );

    unless ( $ret == GEARMAN_SUCCESS )
    {
        LOGDIE( "Unable to determine status for job '$job_id': " . $client->error() );
    }

    unless ( $known )
    {
        # No such job
        return undef;
    }

    my $response = {
        'job_id'      => $job_id,
        'running'     => $running,
        'numerator'   => $numerator,
        'denominator' => $denominator
    };
    return $response;
}

sub show_jobs($)
{
    my $self = shift;

    my $jobs = {};

    foreach my $server ( @{ $self->_servers } )
    {
        my $server_jobs = _show_jobs_on_server( $server );
        unless ( defined $server_jobs )
        {
            ERROR( "Unable to fetch jobs from server $server." );
            return undef;
        }

        $jobs->{ $server } = $server_jobs;
    }

    return $jobs;
}

sub _show_jobs_on_server($)
{
    my $server = shift;

    my $telnet = _net_telnet_instance_for_server( $server );
    my $jobs   = {};

    $telnet->print( 'show jobs' );

    while ( my $line = $telnet->getline() )
    {
        chomp $line;
        last if $line eq '.';

        my @job = split( "\t", $line );
        unless ( scalar @job == 4 )
        {
            ERROR( "Unable to parse line from server $server: $line" );
            return undef;
        }

        my $job_id          = $job[ 0 ];
        my $job_running     = $job[ 1 ] + 0;
        my $job_numerator   = $job[ 2 ] + 0;
        my $job_denominator = $job[ 3 ] + 0;

        if ( defined $jobs->{ $job_id } )
        {
            ERROR( "Job with job ID '$job_id' already exists in the jobs hashref, strange." );
            return undef;
        }

        $jobs->{ $job_id } = {
            'running'     => $job_running,
            'numerator'   => $job_numerator,
            'denominator' => $job_denominator
        };
    }

    return $jobs;
}

sub cancel_job($)
{
    my ( $self, $job_id ) = @_;

    foreach my $server ( @{ $self->_servers } )
    {
        my $result = _cancel_job_on_server( $job_id, $server );
        unless ( $result )
        {
            ERROR( "Unable to cancel job '$job_id' on server $server." );
            return undef;
        }
    }

    return 1;
}

sub _cancel_job_on_server($$)
{
    my ( $job_id, $server ) = @_;

    unless ( $job_id )
    {
        ERROR( "Job ID is empty." );
        return undef;
    }
    if ( $job_id =~ /\n/ or $job_id =~ /\r/ )
    {
        ERROR( "Job ID can't contain line breaks." );
        return undef;
    }

    my $telnet = _net_telnet_instance_for_server( $server );

    $telnet->print( 'cancel job ' . $job_id );
    my $job_cancelled = $telnet->getline();
    chomp $job_cancelled;

    unless ( $job_cancelled eq 'OK' )
    {
        ERROR( "Server $server didn't respond with 'OK': $job_cancelled" );
        return undef;
    }

    return 1;
}

sub server_status($$)
{
    my $self = shift;

    my $statuses = {};

    foreach my $server ( @{ $self->_servers } )
    {
        my $status = _server_status_on_server( $server );
        unless ( defined $status )
        {
            ERROR( "Unable to fetch status from server $server." );
            return undef;
        }

        $statuses->{ $server } = $status;
    }

    return $statuses;
}

sub _server_status_on_server($)
{
    my $server = shift;

    my $telnet    = _net_telnet_instance_for_server( $server );
    my $functions = {};

    $telnet->print( 'status' );

    while ( my $line = $telnet->getline() )
    {
        chomp $line;
        last if $line eq '.';

        my @function = split( "\t", $line );
        unless ( scalar @function == 4 )
        {
            ERROR( "Unable to parse line from server $server: $line" );
            return undef;
        }

        my $function_name              = $function[ 0 ];
        my $function_total             = $function[ 1 ] + 0;
        my $function_running           = $function[ 2 ] + 0;
        my $function_available_workers = $function[ 3 ] + 0;

        if ( defined $functions->{ $function_name } )
        {
            ERROR( "Function with name '$function_name' already exists in the functions hashref, strange." );
            return undef;
        }

        $functions->{ $function_name } = {
            'total'             => $function_total,
            'running'           => $function_running,
            'available_workers' => $function_available_workers
        };
    }

    return $functions;
}

sub workers($)
{
    my $self = shift;

    my $workers = {};

    foreach my $server ( @{ $self->_servers } )
    {
        my $server_workers = _workers_on_server( $server );
        unless ( defined $server_workers )
        {
            ERROR( "Unable to fetch workers from server $server." );
            return undef;
        }

        $workers->{ $server } = $server_workers;
    }

    return $workers;
}

sub _workers_on_server($)
{
    my $server = shift;

    my $telnet  = _net_telnet_instance_for_server( $server );
    my $workers = [];

    $telnet->print( 'workers' );

    while ( my $line = $telnet->getline() )
    {
        chomp $line;
        last if $line eq '.';

        my $colon_pos = index( $line, ':' );
        if ( $colon_pos == -1 )
        {
            ERROR( "Unable to parse line from server $server: $line" );
            return undef;
        }

        my @worker_description = split( /\s+/, substr( $line, 0, $colon_pos ) );
        unless ( scalar @worker_description == 3 )
        {
            ERROR( "Unable to parse line from server $server: $line" );
            return undef;
        }
        my @worker_functions = split( /\s+/, substr( $line, $colon_pos + 1 ) );

        my $worker_file_descriptor = $worker_description[ 0 ] + 0;
        my $worker_ip_address      = $worker_description[ 1 ];
        my $worker_client_id       = ( $worker_description[ 2 ] ne '-' ? $worker_description[ 2 ] : undef );

        push(
            @{ $workers },
            {
                'file_descriptor' => $worker_file_descriptor,
                'ip_address'      => $worker_ip_address,
                'client_id'       => $worker_client_id,
                'functions'       => \@worker_functions
            }
        );
    }

    return $workers;
}

# Create and return a configured instance of Gearman::Client
sub _gearman_xs_client($)
{
    my ( $self ) = @_;

    my $client = new Gearman::XS::Client;

    unless ( scalar( @{ $self->_servers } ) )
    {
        LOGDIE( "No Gearman servers are configured." );
    }

    my $ret = $client->add_servers( join( ',', @{ $self->_servers } ) );
    unless ( $ret == GEARMAN_SUCCESS )
    {
        LOGDIE( "Unable to add Gearman servers: " . $client->error() );
    }

    $client->set_created_fn(
        sub {
            my $task = shift;
            DEBUG( "Gearman task created: '" . $task->job_handle() . '"' );
            return GEARMAN_SUCCESS;
        }
    );

    $client->set_data_fn(
        sub {
            my $task = shift;
            DEBUG( "Data sent to Gearman task '" . $task->job_handle() . "': " . $task->data() );
            return GEARMAN_SUCCESS;
        }
    );

    $client->set_status_fn(
        sub {
            my $task = shift;
            DEBUG( "Status updated for Gearman task '" .
                  $task->job_handle() . "': " . $task->numerator() . " / " . $task->denominator() );
            return GEARMAN_SUCCESS;
        }
    );

    $client->set_complete_fn(
        sub {
            my $task = shift;
            DEBUG( "Gearman task '" . $task->job_handle() . "' completed with data: " . ( $task->data() || '' ) );
            return GEARMAN_SUCCESS;
        }
    );

    $client->set_fail_fn(
        sub {
            my $task = shift;
            DEBUG( "Gearman task failed: '" . $task->job_handle() . '"' );
            return GEARMAN_SUCCESS;
        }
    );

    return $client;
}

# (static) Connects to Gearman, returns Net::Telnet instance
sub _net_telnet_instance_for_server($)
{
    my $server = shift;

    my ( $host, $port ) = split( ':', $server );
    $port //= 4730;

    my $telnet = new Net::Telnet(
        Host    => $host,
        Port    => $port,
        Timeout => $GEARMAND_ADMIN_TIMEOUT
    );
    $telnet->open();

    return $telnet;
}

# (static) Serialize a hashref into string (to be passed to Gearman)
#
# Parameters:
# * hashref that is serializable by JSON module (may be undef)
#
# Returns:
# * a string (string is empty if the hashref is undef)
#
# Dies on error.
sub _serialize_hashref($)
{
    my $hashref = shift;

    unless ( defined $hashref )
    {
        return '';
    }

    unless ( ref $hashref eq 'HASH' )
    {
        LOGDIE( "Parameter is not a hashref." );
    }

    # Gearman accepts only scalar arguments
    my $hashref_serialized = undef;
    eval { $hashref_serialized = $json->encode( $hashref ); };
    if ( $@ )
    {
        LOGDIE( "Unable to serialize hashref with the JSON module: $@" );
    }

    return $hashref_serialized;
}

# (static) Unserialize string (coming from Gearman) back into hashref
#
# Parameters:
# * string to be unserialized; may be empty or undef
#
# Returns:
# * hashref (of the unserialized string), or
# * undef if the string is undef or empty
#
# Dies on error.
sub _unserialize_hashref($)
{
    my $string = shift;

    unless ( $string )
    {
        return undef;
    }

    my $hashref = undef;
    eval {

        # Unserialize
        $hashref = $json->decode( $string );

        unless ( defined $hashref )
        {
            LOGDIE( "Unserialized hashref is undefined." );
        }

        unless ( ref $hashref eq 'HASH' )
        {
            LOGDIE( "Result is not a hashref." );
        }

    };
    if ( $@ )
    {
        LOGDIE( "Unable to unserialize string '$string' with the JSON module: $@" );
    }

    return $hashref;
}

no Moose;    # gets rid of scaffolding

1;
