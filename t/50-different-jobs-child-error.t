use Test::More;

use strict;
use warnings;
use POSIX ':sys_wait_h', 'setsid';
use Fork::ParallelJob;
use Time::HiRes qw/sleep/;

my $fork = Fork::ParallelJob->new(max_process => 3, name =>1, data_format => 'YAML', wait_sleep => 0.1, use_data => 0, tmp_name => 't/tmp/data');
my $pid = $$;
my $code = sub {
  my $fork = shift;
  sleep 0.5;
  my $child = $fork->child(max_process => 2, data_format => 'YAML');
  $child->do_fork([
                   (sub {
                      my $fork = shift;
                      return 1;
                    }) x 4,
                   sub {
                     return 0
                   },
                  ]);
  return $child->result;
};


$fork->do_fork([($code) x 6]);
ok ! $fork->result, 'one/some process failed';

$fork->cleanup;

done_testing;
