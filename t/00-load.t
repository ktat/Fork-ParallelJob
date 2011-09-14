#!perl -T

use Test::More tests => 1;

BEGIN {
    use_ok( 'Fork::ParallelJob' ) || print "Bail out!
";
}

diag( "Testing Fork::ParallelJob $Fork::ParallelJob::VERSION, Perl $], $^X" );
