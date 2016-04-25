
=head1 NAME

C<MediaCloud::JobManager> - Perl worker / client library for running jobs
asynchronously.

=head1 SYNOPSIS

  use MediaCloud::JobManager;

=head1 DESCRIPTION

Run jobs locally, remotely or remotely + asynchronously.

=head2 EXPORT

None by default.

=head1 AUTHOR

Linas Valiukas, E<lt>lvaliukas@cyber.law.harvard.eduE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2013- Linas Valiukas, 2013- Berkman Center for Internet &
Society.

This library is free software; you can redistribute it and/or modify it under
the same terms as Perl itself, either Perl version 5.18.2 or, at your option,
any later version of Perl 5 you may have available.

=cut

package MediaCloud::JobManager;

our $VERSION = '0.16.1';

use strict;
use warnings;
use Modern::Perl "2012";

use MediaCloud::JobManager::Configuration;

use Data::UUID;

use Digest::SHA qw(sha256_hex);

use Carp;

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

# Max. job ID length for MediaCloud::JobManager jobs (when
# MediaCloud::JobManager::Job comes up with a job ID of its own)
use constant MJM_JOB_ID_MAX_LENGTH => 256;

=head2 (static) C<job_status($function_name, $job_id[, $config])>

Get job status.

Parameters:

=over 4

=item * Function name (e.g. "NinetyNineBottlesOfBeer")

=item * Job ID (e.g. "H:localhost.localdomain:8")

=back

Returns hashref with the job status, e.g.:

=begin text

{     # Job ID that was passed as a parameter     'job_id' =>
'H:tundra.home:8',

	# Whether or not the job is currently running
	'running' => 1,

	# Numerator and denominator of the job's progress
	# (in this example, job is 1333/2000 complete)
	'numerator' => 1333,
	'denominator' => 2000
};

=end text

Returns undef if the job ID was not found; dies on error.

=cut

sub job_status($$)
{
    my ( $function_name, $job_id ) = @_;

    my $config = $function_name->configuration();

    return $config->{ broker }->job_status( $function_name, $job_id );
}

# (static) Return an unique job ID that will identify a particular job with its
# arguments
#
# * function name, e.g. 'NinetyNineBottlesOfBeer'
# * hashref of job arguments, e.g. "{ 'how_many_bottles' => 13 }"
#
# Returns: SHA256 of the unique job ID, e.g. "18114c0e14fe5f3a568f73da16130640b1a318ba"
# (SHASUM of "NinetyNineBottlesOfBeer(how_many_bottles_=_2000)"
#
# FIXME maybe use Data::Dumper?
sub unique_job_id($$)
{
    my ( $function_name, $job_args ) = @_;

    unless ( $function_name )
    {
        return undef;
    }

    # Convert to string
    $job_args =
      ( $job_args and scalar keys %{ $job_args } )
      ? join( ', ', map { $_ . ' = ' . ( $job_args->{ $_ } // 'undef' ) } sort( keys %{ $job_args } ) )
      : '';
    my $unique_id = "$function_name($job_args)";

    # Gearman limits the "unique" parameter of a task to 64 bytes (see
    # GEARMAN_MAX_UNIQUE_SIZE in
    # https://github.com/sni/gearmand/blob/master/libgearman-1.0/limits.h)
    # which is usually not enough for most functions, so we hash the
    # parameter instead
    $unique_id = sha256_hex( $unique_id );

    return $unique_id;
}

# (static) Return an unique, path-safe job name which is suitable for writing
# to the filesystem (e.g. for logging)
#
# Parameters:
# * function name, e.g. 'NinetyNineBottlesOfBeer'
# * hashref of job arguments, e.g. "{ 'how_many_bottles' => 13 }"
# * (optional) Job ID, e.g.:
#     * "H:tundra.home:18" (as reported by an instance of Gearman::Client), or
#     * "127.0.0.1:4730//H:tundra.home:18" (as reported by gearmand)
#
# Returns: unique job ID, e.g.:
# * "084567C4146F11E38F00CB951DB7256D.NinetyNineBottlesOfBeer(how_many_bottles_=_2000)", or
# * "H_tundra.home_18.NinetyNineBottlesOfBeer(how_many_bottles_=_2000)"
sub _unique_path_job_id($$;$)
{
    my ( $function_name, $job_args, $job_id ) = @_;

    unless ( $function_name )
    {
        return undef;
    }

    my $unique_id;
    if ( $job_id )
    {

        # If job ID was passed as a parameter, this means that the job
        # was run remotely (by running run_remotely() or add_to_queue()).
        # Thus, the job has to be logged to a location that can later be found
        # by knowing the job ID.

        # Strip the host part (if present)
        $unique_id = _job_id_from_handle( $job_id );

    }
    else
    {

        # If no job ID was provided, this means that the job is being
        # run locally.
        # The job's output still has to be logged somewhere, so we generate an
        # UUID to serve in place of job ID.

        my $ug   = new Data::UUID;
        my $uuid = $ug->create_str();    # e.g. "059303A4-F3F1-11E2-9246-FB1713B42706"
        $uuid =~ s/\-//gs;               # e.g. "059303A4F3F111E29246FB1713B42706"

        $unique_id = $uuid;
    }

    # ID goes first in case the job name shortener decides to cut out a part of the job ID
    my $mjm_job_id = $unique_id . '.' . unique_job_id( $function_name, $job_args );
    if ( length( $mjm_job_id ) > MJM_JOB_ID_MAX_LENGTH )
    {
        $mjm_job_id = substr( $mjm_job_id, 0, MJM_JOB_ID_MAX_LENGTH );
    }

    # Sanitize for paths
    $mjm_job_id = _sanitize_for_path( $mjm_job_id );

    return $mjm_job_id;
}

sub _sanitize_for_path($)
{
    my $string = shift;

    $string =~ s/[^a-zA-Z0-9\.\-_\(\)=,]/_/gi;

    return $string;
}

# Return job ID from job handle
#
# Parameters:
# * job handle, e.g.:
#     * "H:tundra.home:18" (as reported by an instance of Gearman::Job), or
#     * "127.0.0.1:4730//H:tundra.home:18" (as reported by gearmand)
#
# Returns: job ID (e.g. "H:localhost.localdomain:8")
#
# Dies on error.
sub _job_id_from_handle($)
{
    my $job_handle = shift;

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

1;
