use Test::More;

use strict;
use warnings;
use POSIX ':sys_wait_h', 'setsid';
use Fork::ParallelJob;
use Time::HiRes qw/sleep/;

my $fork = Fork::ParallelJob->new(max_process => 3, name =>1, data_format => 'JSON', root_data_format => 'Storable', wait_sleep => 0.1, root_jobs_in_data => 1, tmp_name => 't/tmp/data');
my $pid = $$;

my $code = sub {
  my $f = shift;
  my $child = $f->child;
  $child->add_root_job({y => sub {
                         my $f2 = shift;
                         $f2->current_data->lock_store(sub {my $d = shift; $d->{add_from_child1_to_root} = 1; $d});
                         return 1;
                       }});
  $child->add_job([{a => sub {
                      # warn "from child $$";
                      my $f2 = shift;
                      $f2->add_root_job({x => sub {
                                          my $f2 = shift;
                                          $f2->current_data->lock_store(sub {my $d = shift; $d->{add_from_child2_to_root} = 1; $d});
                                          return 1;
                                        }});
                      $f2->current_data->lock_store(sub {my $d = shift; $d->{add_from_child1} = 1; $d});
                      return 1;
                    }},
                   {b => sub {
                      my $f3 = shift;
                      # warn "from child of child $$";
                      $f3->current_data->lock_store(sub {my $d = shift; $d->{add_from_child2} = 1; $d});
                      return 1;
                    }}]);
  $child->do_fork;
  return 1;
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
  $cnt{3} += $d->{add_from_child1_to_root} if $d->{add_from_child1_to_root};
  $cnt{4} += $d->{add_from_child2_to_root} if $d->{add_from_child2_to_root};
}

is $n, 15, 'num of files = 3(from parent) + 3(yield from parent) * 6(parent(3) * child(2)) + 3(yield from child)';
is $cnt{1}, 3;
is $cnt{2}, 3;
is $cnt{3}, 3;
is $cnt{4}, 3;

$fork->cleanup;
done_testing;
