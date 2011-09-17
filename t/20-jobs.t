use Test::More;

use strict;
use warnings;

use Fork::ParallelJob::Jobs;

ok my $jobs = Fork::ParallelJob::Jobs->new;

is $jobs->num_of_jobs, 0;
$jobs->add(sub { return 1 });
is $jobs->num_of_jobs, 1;
$jobs->add(sub { return 2 });
is $jobs->num_of_jobs, 2;
$jobs->add_multi([sub { return 3 }, sub { return 4}]);
is $jobs->num_of_jobs, 4;

for my $n (1, 2, 3, 4) {
  my ($job, $data) = $jobs->take;
  is $job->{$n}->($data), $n;
  is $jobs->num_of_jobs, 4 - $n;
}

$jobs->add({'name1' => sub {return 1}}, 1);
is $jobs->num_of_jobs, 1;
$jobs->add({'name2' => sub {return 2}}, 2);
is $jobs->num_of_jobs, 2;
$jobs->add_multi([{'name3' => sub {return 3}}, {'name4' => sub {return 4}}], [3, 4]);
is $jobs->num_of_jobs, 4;

for my $n (1, 2, 3, 4) {
  my ($job, $data) = $jobs->take;
  is $data, $n;
  is $job->{'name' . $n}->(), $n;
  is $jobs->num_of_jobs, 4 - $n;
}

done_testing;
