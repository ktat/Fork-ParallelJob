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
  my $code = sub {
    my $fork = shift;
    sleep 0.5;
    chomp(my $pid_num = qx/ps -ef |grep -E '^$ENV{USER} +[0-9]+ +$child_pid ' | grep -v grep| wc -l/);
    $fork->child_data->lock_store(sub { my $data = shift; $data->{pid_num} = $pid_num; $data});
  };
  $child->do_fork
    ([
      (sub {
         my $fork = shift;
         sleep 0.5;
         chomp(my $pid_num = qx/ps -ef |grep -E '^$ENV{USER} +[0-9]+ +$child_pid ' | grep -v grep| wc -l/);
         $fork->child_data->lock_store(sub{my $data = shift; $data->{pid_num} = $pid_num; $data});
         my $child = $fork->child(max_process => 1);
         my $child_child_pid = $$;
         $child->do_fork
           (
            [sub {
               my $fork = shift;
               sleep 0.5;
               chomp(my $pid_num = qx/ps -ef |grep -E '^$ENV{USER} +[0-9]+ +$child_child_pid ' | grep -v grep| wc -l/);
               $fork->child_data->lock_store(sub{my $data = shift; $data->{pid_num} = $pid_num; $data});
             }
            ]);
       }) x 4,
     ]);
};

$fork->do_fork([($code) x 6]);
ok $fork->result, 'all process success';

my $r = 0;

my %max;
my $n = 0;
foreach my $data (@{$fork->child_data->get_all}) {
  $n++;
  $max{$data->{pid_num}}++   if exists $data->{pid_num};
}

is $max{1}, 24;
is $max{3}, 6;
is $max{2}, 24;
is $n, 54, "parent's child data and child's child data(24) + child-child's child data(24)";

$fork->cleanup;

done_testing;
