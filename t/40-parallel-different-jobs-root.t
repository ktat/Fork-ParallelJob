use Test::More;

use strict;
use warnings;
use POSIX ':sys_wait_h', 'setsid';
use Fork::ParallelJob;

my $fork = Fork::ParallelJob->new(max_process => 3, name =>1, data_format => 'YAML', root_data_format => 'Storable', wait_sleep => 0.1, jobs_in_root => 1, tmp_name => 't/tmp/data');
my $pid = $$;

my $code = sub {
  my $f = shift;
  sleep 1;
  my $child = $f->child(max_process => 2, data_format => 'YAML');
  $f->add_job(sub {
		my $f2 = shift;
		$f2->child_data->lock_store(sub {my $d = shift; $d->{add_from_child1} = 1; $d});
		sleep 2;
	      });
  my $child_code = sub {
    my $f2 = shift;
    $f2->add_job(sub {
		     my $f3 = shift;
		     $f3->child_data->lock_store(sub {my $d = shift; $d->{add_from_child2} = 1; $d});
		     sleep 2;
		   });
    sleep 2;
  };
  $child->do_fork([($child_code) x 2]);
  sleep 2;
};
$fork->do_fork([($code) x 3]);
ok $fork->result, 'all process success';

my $r = 0;

my %cnt;
my $n = 0;
foreach my $d (@{$fork->child_data->get_all}) {
  $n++;
  $cnt{1} += $d->{add_from_child1} if $d->{add_from_child1};
  $cnt{2} += $d->{add_from_child2} if $d->{add_from_child2};
}

is $n, (3 + 6) * 2, "(3 + 6) * 2 = 18";
is $cnt{1}, 3;
is $cnt{2}, 6;

$fork->cleanup;


done_testing;
