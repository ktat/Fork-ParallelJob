#!perl -T

use Test::More tests => 1;

BEGIN {
    use_ok( 'Fork::ParalellJob' ) || print "Bail out!
";
}

diag( "Testing Fork::ParalellJob $Fork::ParalellJob::VERSION, Perl $], $^X" );
