use Test::More;

use strict;
use warnings;

use Fork::ParallelJob::Jobs::RootData;
use Fork::ParallelJob::Data::Storable;

ok my $data = Fork::ParallelJob::Data::Storable->new(format => 'Storable', base_dir => 't/tmp');
ok my $jobs = Fork::ParallelJob::Jobs::RootData->new($data);

is $jobs->num_of_jobs, 0;
$jobs->add(sub { return 1 });
is $jobs->num_of_jobs, 1;
$jobs->add(sub { return 2 });
is $jobs->num_of_jobs, 2;
$jobs->add_multi([sub { return 3 }, sub { return 4}]);
is $jobs->num_of_jobs, 4;

for my $n (1, 2, 3, 4) {
  my ($job, $job_data) = $jobs->take;
  is $job->{$n}->(), $n;
  is $jobs->num_of_jobs, 4 - $n;
}

$jobs->add({'name1' => sub {return 1}}, 1);
is $jobs->num_of_jobs, 1;
$jobs->add({'name2' => sub {return 2}}, 2);
is $jobs->num_of_jobs, 2;
$jobs->add_multi([{'name3' => sub {return 3}}, {'name4' => sub {return 4}}], [3 , 4]);
is $jobs->num_of_jobs, 4;

for my $n (1, 2, 3, 4) {
  my ($job, $job_data) = $jobs->take;
  is $n, $job_data;
  is $job->{'name' . $n}->(), $n;
  is $jobs->num_of_jobs, 4 - $n;
}

$data->cleanup;
done_testing;
