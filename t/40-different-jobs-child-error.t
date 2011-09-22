use Test::More;

use strict;
use warnings;
use POSIX ':sys_wait_h', 'setsid';
use Fork::ParallelJob;
use Time::HiRes qw/sleep/;

my $fork = Fork::ParallelJob->new(max_process => 3, name =>1, data_format => 'YAML', wait_sleep => 0.1, tmp_name => 't/tmp/data');
my $pid = $$;
my $code = sub {
  my $fork = shift;
  sleep 0.5;
  chomp(my $pid_num = qx/ps -ef |grep -E '^$ENV{USER} +[0-9]+ +$pid ' | grep -v 'grep' | wc -l/);
  $fork->child_data->lock_store(sub {my $data = shift; $data->{pid_num} = $pid_num; $data});
  my $child_pid = $$;
  my $child = $fork->child(max_process => 2, data_format => 'YAML');
  $child->do_fork([
                   (sub {
                      my $fork = shift;
                      sleep 0.5;
                      chomp(my $pid_num = qx/ps -ef |grep -E '^$ENV{USER} +[0-9]+ +$child_pid ' | grep -v grep| wc -l/);
                      $fork->child_data->lock_store(sub{my $data = shift; $data->{pid_num} = $pid_num; $data});
                      sleep 0.5;
                    }) x 4,
                   sub {
                     sleep 0.5;
                     return 0
                   }
                  ]);
  return $child->result;
};

$fork->do_fork([($code) x 6]);
ok ! $fork->result, 'one/some process failed';

my $r = 0;

my %max;
my $n = 0;
foreach my $data (@{$fork->child_data->get_all}) {
  $n++;
  $max{$data->{pid_num}}++   if exists $data->{pid_num};
}

is $max{3}, 6;
is $max{2}, 24;
is $n, 36, "parent's child data(6) and child's child data(30)";

$fork->cleanup;
done_testing;
