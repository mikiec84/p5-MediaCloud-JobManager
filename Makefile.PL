use 5.016003;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME         => 'MediaCloud::JobManager',
    AUTHOR       => 'Linas Valiukas <lvaliukas@cyber.law.harvard.edu>',
    VERSION_FROM => 'lib/MediaCloud/JobManager.pm',
    ABSTRACT     => 'Perl worker / client library for running jobs asynchronously',
    ( $ExtUtils::MakeMaker::VERSION >= 6.3002
        ? ( 'LICENSE' => 'perl' )
        : () ),
    PREREQ_PM => {
        'App::githook_perltidy' => '0.10.2',
        'Capture::Tiny'         => 0,
        'Carp'                  => 0,
        'Data::Compare'         => 0,
        'Data::Dumper'          => 0,
        'Data::UUID'            => 0,
        'DateTime'              => 0,
        'Email::MIME'           => 0,
        'Email::Sender::Simple' => 0,
        'File::Path'            => 0,
        'File::Slurp'           => 0,
        'File::Temp'            => 0,
        'Gearman::XS'           => 0,
        'IO::File'              => 0,
        'IO::Socket::INET'      => 0,
        'JSON'                  => 0,
        'Log::Log4perl'         => 0,
        'Modern::Perl'          => 0,
        'Moose'                 => '2.1005',
        'Moose::Role'           => '2.1005',
        'MooseX::Singleton'     => 0,
        'Net::Telnet'           => 0,
        'Proc::Background'      => 0,
        'Readonly'              => 0,
        'Sys::Hostname'         => 0,
        'Test::More'            => 0,
        'Test::NoWarnings'      => 0,
        'Test::Strict'          => 0,
        'Time::HiRes'           => 0,
    },
    test => { TESTS => 't/*.t t/brokers/*.t', RECURSIVE_TEST_FILES => 1 },
    clean =>
      { FILES => 'MediaCloud-JobManager-* MANIFEST.bak MANIFEST.SKIP.bak Makefile Makefile.old MYMETA.* _build pm_to_blib' },
    META_MERGE => {
        resources => {
            type => 'git',
            url  => 'git://github.com/berkmancenter/p5-Gearman-JobScheduler.git',
            web  => 'https://github.com/berkmancenter/p5-Gearman-JobScheduler',
        }
    },

);
