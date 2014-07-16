use strict;
use warnings;

use IO::Socket::INET;

sub _gearmand_is_installed()
{
    return (system( 'gearmand --version' ) == 0);
}

sub _gearmand_is_started()
{
    my $socket = IO::Socket::INET->new(
        PeerAddr => 'localhost',
        PeerPort => 4730,
        Proto => 'tcp',
        Type => SOCK_STREAM
    );
    if ( $socket ) {
        close($socket);
        return 1;
    } else {
        return 0;
    }
}

1;
