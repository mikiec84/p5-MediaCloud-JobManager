package MediaCloud::JobManager::Broker::RabbitMQ;

#
# RabbitMQ job broker (using Celery protocol)
#
# Usage:
#
# MediaCloud::JobManager::Broker::RabbitMQ->new();
#
# FIXME unique tasks (http://engineroom.trackmaven.com/blog/announcing-celery-once/)
# FIXME try deleting queue with responses when client exits
# FIXME remove unique(), consider adding do_not_expect_result()

use strict;
use warnings;
use Modern::Perl "2012";

use Moose;
with 'MediaCloud::JobManager::Broker';

use Net::AMQP::RabbitMQ;
use UUID::Tiny ':std';
use Tie::Cache;
use JSON;
use Data::Dumper;
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

# RabbitMQ default timeout
Readonly my $RABBITMQ_DEFAULT_TIMEOUT => 60;

# RabbitMQ delivery modes
Readonly my $RABBITMQ_DELIVERY_MODE_NONPERSISTENT => 1;
Readonly my $RABBITMQ_DELIVERY_MODE_PERSISTENT    => 2;

# RabbitMQ queue durability
Readonly my $RABBITMQ_QUEUE_TRANSIENT => 0;
Readonly my $RABBITMQ_QUEUE_DURABLE   => 1;

# RabbitMQ priorities
Readonly my %RABBITMQ_PRIORITIES => (
    $MediaCloud::JobManager::Job::MJM_JOB_PRIORITY_LOW    => 0,
    $MediaCloud::JobManager::Job::MJM_JOB_PRIORITY_NORMAL => 1,
    $MediaCloud::JobManager::Job::MJM_JOB_PRIORITY_HIGH   => 2,
);

# JSON (de)serializer
my $json = JSON->new->allow_nonref->canonical->utf8;

# RabbitMQ connection credentials
has '_hostname' => ( is => 'rw', isa => 'Str' );
has '_port'     => ( is => 'rw', isa => 'Int' );
has '_username' => ( is => 'rw', isa => 'Str' );
has '_password' => ( is => 'rw', isa => 'Str' );
has '_vhost'    => ( is => 'rw', isa => 'Str' );
has '_timeout'  => ( is => 'rw', isa => 'Int' );

# RabbitMQ connection pool for every connection ID (PID + credentials)
my %_rabbitmq_connection_for_connection_id;

# "reply_to" queues for connection ID + function name
#
# We emulate Celery's RPC via RabbitMQ behavior in which results are being
# stuffed in per-client result queues and can be retrieved only by the same
# client that requested the job using run_remotely() or add_to_queue():
#
# http://docs.celeryproject.org/en/latest/userguide/tasks.html#rpc-result-backend-rabbitmq-qpid
my %_reply_to_queues_for_connection_id_function_name;

# Memory-limited results cache for connection ID + function name
#
# When fetching messages from "reply_to" queue for a specific name,
# run_remotely() can't requeue messages that don't belong to a specific job ID
# so it has to put it somewhere. This hash of hashes serves as a backlog for
# unused job results.
#
# It's not ideal that some job results might get invalidated but Celery does
# that too (purges results deemed too old).
my %_results_caches_for_connection_id_function_name;

# Limits of results cache above
Readonly my $RABBITMQ_RESULTS_CACHE_MAXCOUNT => 1024 * 100;
Readonly my $RABBITMQ_RESULTS_CACHE_MAXBYTES => 1024 * 1024 * 10;

# Constructor
sub BUILD
{
    my $self = shift;
    my $args = shift;

    $self->_hostname( $args->{ hostname } // 'localhost' );
    $self->_port( $args->{ port }         // 5672 );
    $self->_username( $args->{ username } // 'guest' );
    $self->_password( $args->{ password } // 'guest' );
    my $default_vhost = '/';
    $self->_vhost( $args->{ vhost }     // $default_vhost );
    $self->_timeout( $args->{ timeout } // $RABBITMQ_DEFAULT_TIMEOUT );

    # Connect to the current connection ID (PID + credentials)
    my $mq = $self->_mq();
}

# Used to uniquely identify RabbitMQ connections (by connection credentials and
# PID) so that we know when to reconnect
sub _connection_identifier($)
{
    my $self = shift;

    # Reconnect when running on a fork too
    my $pid = $$;

    return sprintf( 'PID=%d; hostname=%s; port=%d; username: %s; password=%s; vhost=%s, timeout=%d',
        $pid, $self->_hostname, $self->_port, $self->_username, $self->_password, $self->_vhost, $self->_timeout );
}

# Returns RabbitMQ connection handler for the current connection ID
sub _mq($)
{
    my $self = shift;

    my $conn_id = $self->_connection_identifier();

    unless ( $_rabbitmq_connection_for_connection_id{ $conn_id } )
    {

        # Connect to RabbitMQ, open channel
        DEBUG( "Connecting to RabbitMQ (PID: $$, hostname: " .
              $self->_hostname . ", port: " . $self->_port . ", username: " . $self->_username . ")..." );
        my $mq = Net::AMQP::RabbitMQ->new();
        eval {
            $mq->connect(
                $self->_hostname,
                {
                    user     => $self->_username,
                    password => $self->_password,
                    port     => $self->_port,
                    vhost    => $self->_vhost,
                    timeout  => $self->_timeout,
                }
            );
        };
        if ( $@ )
        {
            LOGDIE( "Unable to connect to RabbitMQ: $@" );
        }

        my $channel_number = _channel_number();
        unless ( $channel_number )
        {
            LOGDIE( "Channel number is unset." );
        }

        eval {
            $mq->channel_open( $channel_number );

            # Fetch one message at a time
            $mq->basic_qos( $channel_number, { prefetch_count => 1 } );
        };
        if ( $@ )
        {
            LOGDIE( "Unable to open channel $channel_number: $@" );
        }

        $_rabbitmq_connection_for_connection_id{ $conn_id }           = $mq;
        $_reply_to_queues_for_connection_id_function_name{ $conn_id } = ();
        $_results_caches_for_connection_id_function_name{ $conn_id }  = ();
    }

    return $_rabbitmq_connection_for_connection_id{ $conn_id };
}

# Returns "reply_to" queue name for current connection and provided function name
sub _reply_to_queue($$)
{
    my ( $self, $function_name ) = @_;

    my $conn_id = $self->_connection_identifier();

    unless ( defined $_reply_to_queues_for_connection_id_function_name{ $conn_id } )
    {
        # Should have been defined in _mq()
        WARN( "'reply_to' queue for connection ID '$conn_id' is not a hash." );
        $_reply_to_queues_for_connection_id_function_name{ $conn_id } = ();
    }

    unless ( $_reply_to_queues_for_connection_id_function_name{ $conn_id }{ $function_name } )
    {
        my $reply_to_queue = _random_uuid();
        $_reply_to_queues_for_connection_id_function_name{ $conn_id }{ $function_name } = $reply_to_queue;
    }

    return $_reply_to_queues_for_connection_id_function_name{ $conn_id }{ $function_name };
}

# Returns reference to results cache for current connection and provided function name
sub _results_cache_hashref($$)
{
    my ( $self, $function_name ) = @_;

    my $conn_id = $self->_connection_identifier();

    unless ( defined $_results_caches_for_connection_id_function_name{ $conn_id } )
    {
        # Should have been defined in _mq()
        WARN( "Results cache for connection ID '$conn_id' is not a hash." );
        $_results_caches_for_connection_id_function_name{ $conn_id } = ();
    }

    unless ( defined $_results_caches_for_connection_id_function_name{ $conn_id }{ $function_name } )
    {
        $_results_caches_for_connection_id_function_name{ $conn_id }{ $function_name } = {};

        tie %{ $_results_caches_for_connection_id_function_name{ $conn_id }{ $function_name } }, 'Tie::Cache',
          {
            MaxCount => $RABBITMQ_RESULTS_CACHE_MAXCOUNT,
            MaxBytes => $RABBITMQ_RESULTS_CACHE_MAXBYTES
          };
    }

    return $_results_caches_for_connection_id_function_name{ $conn_id }{ $function_name };
}

# Channel number we should be talking to
sub _channel_number()
{
    # Each PID + credentials pair has its own connection so we can just use constant channel
    return 1;
}

sub _declare_queue($$$$)
{
    my ( $self, $queue_name, $durable, $declare_and_bind_exchange ) = @_;

    my $mq = $self->_mq();

    my $channel_number = _channel_number();
    my $options        = {
        durable     => $durable,
        auto_delete => 0,
    };
    my $arguments = { 'x-max-priority' => _priority_count(), };

    eval { $mq->queue_declare( $channel_number, $queue_name, $options, $arguments ); };
    if ( $@ )
    {
        LOGDIE( "Unable to declare queue '$queue_name': $@" );
    }

    if ( $declare_and_bind_exchange )
    {
        my $exchange_name = $queue_name;
        my $routing_key   = $queue_name;

        eval {
            $mq->exchange_declare(
                $channel_number,
                $exchange_name,
                {
                    durable     => $durable,
                    auto_delete => 0,
                }
            );
            $mq->queue_bind( $channel_number, $queue_name, $exchange_name, $routing_key );
        };
        if ( $@ )
        {
            LOGDIE( "Unable to bind queue '$queue_name' to exchange '$exchange_name': $@" );
        }
    }
}

sub _declare_task_queue($$)
{
    my ( $self, $queue_name ) = @_;

    my $durable                   = $RABBITMQ_QUEUE_DURABLE;
    my $declare_and_bind_exchange = 1;

    return $self->_declare_queue( $queue_name, $durable, $declare_and_bind_exchange );
}

sub _declare_results_queue($$)
{
    my ( $self, $queue_name ) = @_;

    my $durable                   = $RABBITMQ_QUEUE_TRANSIENT;
    my $declare_and_bind_exchange = 0;

    return $self->_declare_queue( $queue_name, $durable, $declare_and_bind_exchange );
}

sub _publish_json_message($$$;$$)
{
    my ( $self, $routing_key, $payload, $extra_options, $extra_props ) = @_;

    my $mq = $self->_mq();

    my $payload_json;
    eval { $payload_json = $json->encode( $payload ); };
    if ( $@ )
    {
        LOGDIE( "Unable to encode JSON message: $@" );
    }

    my $channel_number = _channel_number();

    my $options = {};
    if ( $extra_options )
    {
        $options = { %{ $options }, %{ $extra_options } };
    }
    my $props = {
        content_type     => 'application/json',
        content_encoding => 'utf-8',
    };
    if ( $extra_props )
    {
        $props = { %{ $props }, %{ $extra_props } };
    }

    eval { $mq->publish( $channel_number, $routing_key, $payload_json, $options, $props ); };
    if ( $@ )
    {
        LOGDIE( "Unable to publish message to routing key '$routing_key': $@" );
    }
}

sub _random_uuid()
{
    # Celery uses v4 (random) UUIDs
    return create_uuid_as_string( UUID_RANDOM );
}

sub _priority_to_int($)
{
    my $priority = shift;

    unless ( exists $RABBITMQ_PRIORITIES{ $priority } )
    {
        LOGDIE( "Unknown job priority: $priority" );
    }

    return $RABBITMQ_PRIORITIES{ $priority };
}

sub _priority_count()
{
    return scalar( keys( %RABBITMQ_PRIORITIES ) );
}

sub _process_worker_message($$$)
{
    my ( $self, $function_name, $message ) = @_;

    my $mq = $self->_mq();

    my $correlation_id = $message->{ props }->{ correlation_id };
    unless ( $correlation_id )
    {
        LOGDIE( '"correlation_id" is empty.' );
    }

    my $reply_to = $message->{ props }->{ reply_to };
    unless ( $reply_to )
    {
        LOGDIE( '"reply_to" is empty.' );
    }

    my $priority = $message->{ props }->{ priority } // 0;

    my $delivery_tag = $message->{ delivery_tag };
    unless ( $delivery_tag )
    {
        LOGDIE( "'delivery_tag' is empty." );
    }

    my $payload_json = $message->{ body };
    unless ( $payload_json )
    {
        LOGDIE( 'Message payload is empty.' );
    }

    my $payload;
    eval { $payload = $json->decode( $payload_json ); };
    if ( $@ or ( !$payload ) or ( ref( $payload ) ne ref( {} ) ) )
    {
        LOGDIE( 'Unable to decode payload JSON: ' . $@ );
    }

    if ( $payload->{ task } ne $function_name )
    {
        LOGDIE( "Task name is not '$function_name'; maybe you're using same queue for multiple types of jobs?" );
    }

    my $celery_job_id = $payload->{ id };
    my $args          = $payload->{ kwargs };

    # Do the job
    my $job_result;
    eval { $job_result = $function_name->run_locally( $args, $celery_job_id ); };
    my $error_message = $@;

    # Construct response based on whether the job succeeded or failed
    my $response;
    if ( $error_message )
    {
        ERROR( "Job '$celery_job_id' died: $@" );
        $response = {
            'status'    => 'FAILURE',
            'traceback' => "Job died: $error_message",
            'result'    => {
                'exc_message' => 'Task has failed',
                'exc_type'    => 'Exception',
            },
            'task_id'  => $celery_job_id,
            'children' => []
        };
    }
    else
    {
        $response = {
            'status'    => 'SUCCESS',
            'traceback' => undef,
            'result'    => $job_result,
            'task_id'   => $celery_job_id,
            'children'  => []
        };
    }

    # Send message back with the job result
    eval {
        $self->_declare_results_queue( $reply_to );
        $self->_publish_json_message(
            $reply_to,
            $response,
            {
                # Options
            },
            {
                # Properties
                delivery_mode  => $RABBITMQ_DELIVERY_MODE_NONPERSISTENT,
                priority       => $priority,
                correlation_id => $celery_job_id,
            }
        );
    };
    if ( $@ )
    {
        LOGDIE( "Unable to publish job $celery_job_id result: $@" );
    }

    # ACK the message (mark the job as completed)
    eval { $mq->ack( _channel_number(), $delivery_tag ); };
    if ( $@ )
    {
        LOGDIE( "Unable to mark job $celery_job_id as completed: $@" );
    }
}

sub start_worker($$)
{
    my ( $self, $function_name ) = @_;

    my $mq = $self->_mq();

    $self->_declare_task_queue( $function_name );

    my $consume_options = {

        # Don't assume that the job is finished when it reaches the worker
        no_ack => 0,
    };
    my $consumer_tag = $mq->consume( _channel_number(), $function_name, $consume_options );

    INFO( "Consumer tag: $consumer_tag" );
    INFO( "Worker is ready and accepting jobs" );
    my $recv_timeout = 0;    # block until message is received
    while ( my $message = $mq->recv( 0 ) )
    {
        $self->_process_worker_message( $function_name, $message );
    }
}

sub run_job_sync($$$$$)
{
    my ( $self, $function_name, $args, $priority, $unique ) = @_;

    my $mq = $self->_mq();

    # Post the job
    my $celery_job_id = $self->run_job_async( $function_name, $args, $priority, $unique );

    # Declare result queue
    my $reply_to_queue = $self->_reply_to_queue( $function_name );
    eval { $self->_declare_results_queue( $reply_to_queue ); };
    if ( $@ )
    {
        LOGDIE( "Unable to declare results queue '$reply_to_queue': $@" );
    }

    my $results_cache = $self->_results_cache_hashref( $function_name );

    my $message;
    if ( exists $results_cache->{ $celery_job_id } )
    {
        # Result for this job ID was fetched previously -- return from cache
        DEBUG( "Results message for job ID '$celery_job_id' found in cache" );
        $message = $results_cache->{ $celery_job_id };
        delete $results_cache->{ $celery_job_id };

    }
    else
    {
        # Result not yet fetched -- process the result queue

        my $channel_number  = _channel_number();
        my $consume_options = {};
        my $consumer_tag    = $mq->consume( $channel_number, $reply_to_queue, $consume_options );

        my $recv_timeout = 0;    # block until message is received

        while ( my $queue_message = $mq->recv( 0 ) )
        {
            my $correlation_id = $queue_message->{ props }->{ correlation_id };
            unless ( $correlation_id )
            {
                LOGDIE( '"correlation_id" is empty.' );
            }

            if ( $correlation_id eq $celery_job_id )
            {
                DEBUG( "Found results message with job ID '$celery_job_id'." );
                $message = $queue_message;
                last;

            }
            else
            {
                # Message belongs to some other job -- add to cache and continue
                DEBUG( "Results message '$correlation_id' does not belong to job ID '$celery_job_id'." );
                $results_cache->{ $correlation_id } = $queue_message;
            }
        }
    }

    unless ( $message )
    {
        LOGDIE( "At this point, message should have been fetched either from broker or from cache" );
    }

    my $correlation_id = $message->{ props }->{ correlation_id };
    unless ( $correlation_id )
    {
        LOGDIE( '"correlation_id" is empty.' );
    }
    if ( $correlation_id ne $celery_job_id )
    {
        # Message belongs to some other job -- requeue and skip
        DEBUG( "'correlation_id' ('$correlation_id') is not equal to job ID ('$celery_job_id')." );
        next;
    }

    my $payload_json = $message->{ body };
    unless ( $payload_json )
    {
        LOGDIE( 'Message payload is empty.' );
    }

    my $payload;
    eval { $payload = $json->decode( $payload_json ); };
    if ( $@ or ( !$payload ) or ( ref( $payload ) ne ref( {} ) ) )
    {
        LOGDIE( 'Unable to decode payload JSON: ' . $@ );
    }

    if ( $payload->{ task_id } ne $celery_job_id )
    {
        LOGDIE( "'task_id' ('$payload->{ task_id }') is not equal to job ID ('$celery_job_id')." );
    }

    # Return job result
    if ( $payload->{ status } eq 'SUCCESS' )
    {
        # Job completed successfully
        return $payload->{ result };

    }
    elsif ( $payload->{ status } eq 'FAILURE' )
    {
        # Job failed -- pass the failure to the caller
        LOGDIE( "Job '$celery_job_id' failed: " . $payload->{ traceback } );

    }
    else
    {
        # Unknown value
        LOGDIE( "Unknown 'status' value: " . $payload->{ status } );
    }
}

sub run_job_async($$$$$)
{
    my ( $self, $function_name, $args, $priority, $unique ) = @_;

    unless ( defined( $args ) )
    {
        $args = {};
    }
    unless ( ref( $args ) eq ref( {} ) )
    {
        LOGDIE( "'args' is not a hashref." );
    }

    my $celery_job_id = create_uuid_as_string( UUID_RANDOM );

    # Encode payload
    my $payload = {
        'expires'   => undef,
        'utc'       => JSON::true,
        'args'      => [],
        'chord'     => undef,
        'callbacks' => undef,
        'errbacks'  => undef,
        'taskset'   => undef,
        'id'        => $celery_job_id,
        'retries'   => $function_name->retries(),
        'task'      => $function_name,
        'timelimit' => [ undef, undef, ],
        'eta'       => undef,
        'kwargs'    => $args,
    };

    # Declare task queue
    $self->_declare_task_queue( $function_name );

    # Declare result queue before posting a job (just like Celery does)
    my $reply_to_queue = $self->_reply_to_queue( $function_name );
    $self->_declare_results_queue( $reply_to_queue );

    # Post a job
    eval {
        my $priority = _priority_to_int( $function_name->priority() );
        $self->_publish_json_message(
            $function_name,
            $payload,
            {
                # Options
                exchange => $function_name
            },
            {
                # Properties
                delivery_mode  => $RABBITMQ_DELIVERY_MODE_PERSISTENT,
                priority       => $priority,
                correlation_id => $celery_job_id,
                reply_to       => $reply_to_queue,
            }
        );
    };
    if ( $@ )
    {
        LOGDIE( "Unable to add job '$celery_job_id' to queue: $@" );
    }

    return $celery_job_id;
}

sub job_id_from_handle($$)
{
    my ( $self, $job_handle ) = @_;

    return $job_handle;
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
