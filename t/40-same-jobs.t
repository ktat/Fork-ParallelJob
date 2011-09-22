use Test::More;

use strict;
use warnings;
use POSIX ':sys_wait_h', 'setsid';
use Fork::ParallelJob;
use Time::HiRes qw/sleep/;

ok my $fork = Fork::ParallelJob->new(name => "fork1", nowait => 1, wait_sleep => 0.1, tmp_name => 't/tmp/data');

my $job = sub {
  my $f = shift;
  my $data = shift;
  sleep 0.5;
  $f->child_data->set({n => $data});
};

my @data = (1 .. 3);
my $sum = 0;
$sum += $_ for @data;

$fork->do_fork($job, \@data);
chomp(my $pid_num = qx/ps -ef |grep -E '^$ENV{USER} +[0-9]+ +$$ ' | wc -l/);
is $pid_num, scalar @data + 1;

$fork->wait_all_children;

ok $fork->result, 'all process success';

my $r = 0;

foreach my $data (@{$fork->child_data->get_all}) {
  $r += $data->{n};
}

is $r, $sum, "sum is $sum";

$fork->cleanup;

done_testing;
