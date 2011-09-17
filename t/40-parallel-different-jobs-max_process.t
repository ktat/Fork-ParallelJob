use Test::More;

use strict;
use warnings;
use POSIX ':sys_wait_h', 'setsid';
use Fork::ParallelJob;

my %max;

for my $max (1, 3) {
  my $pid = $$;
  ok my $fork = Fork::ParallelJob->new(name => "fork1", max_process => $max, wait_sleep => 0.1, tmp_name => 't/tmp/data');

  my $job = sub {
    my $f = shift;
    my $data = shift;
    sleep 1;
    $|=1;
    chomp (my $pid_num = qx/ps -ef |grep -E '^$ENV{USER} +[0-9]+ +$pid ' | wc -l/);
    sleep 1;
    $f->child_data->set({n => $data, pid_num => $pid_num});
  };

  my ($sum, @data) = (0 .. 3 * $max);
  $sum += $_ for @data;
  $fork->do_fork([($job) x @data], \@data);
  ok $fork->result, 'all process success';
  my $r = 0;
  foreach my $data (@{$fork->child_data->get_all}) {
    $r += $data->{n};
    $max{$data->{pid_num}}++;
  }
  is $r, $sum, "sum is $sum";
  sleep 2;
  $fork->cleanup;
}
is_deeply \%max, {
                  1 => 3,
                  3 => 9,
                 };


done_testing;
