use Test::More;

use strict;
use warnings;
use POSIX ':sys_wait_h', 'setsid';
use Fork::ParallelJob;

ok my $fork = Fork::ParallelJob->new(name => "fork1", nowait => 1, wait_sleep => 0.1, data_format => 'YAML', tmp_name => 't/tmp/data');

my @jobs = (
	    sub { sleep 1; my $f = shift; my $data = shift; $f->child_data->set({n => $data})    ; sleep 2},
	    sub { sleep 1; my $f = shift; my $data = shift; $f->child_data->set({n => $data * 2}); sleep 2},
	    sub { sleep 1; my $f = shift; my $data = shift; $f->child_data->set({n => $data * 3}); sleep 2},
	    sub { sleep 1; my $f = shift; my $data = shift; $f->child_data->set({n => $data * 4}); sleep 2},
	   );

my @data = (1 .. 4);
$fork->do_fork(\@jobs, \@data);
chomp(my $pid_num = qx/ps -ef |grep -E '^$ENV{USER} +[0-9]+ +$$ ' | wc -l/);
is $pid_num, scalar @data + 1;
$fork->wait_all_children;

ok $fork->result, 'all process success';

my $r = 0;
my $sum = 0;
$sum += $_ * $_ for 1 .. scalar @data;
foreach my $data (@{$fork->child_data->get_all}) {
  $r += $data->{n};
}

is $r, $sum, "sum is $sum";

$fork->cleanup;

done_testing;
