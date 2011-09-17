use Test::More;

use Fork::ParallelJob::Data::Storable;
use Fork::ParallelJob::Data::JSON;
use Fork::ParallelJob::Data::YAML;

foreach my $format (qw/Storable YAML JSON/) {
  my $class = 'Fork::ParallelJob::Data::' . $format;
  ok my $d = $class->new(base_dir => "t/tmp", format => $format);
  ok -d 't/tmp';
  is $d->{format}, $format;
  ok $d->set_worker_id(123);
  is $d->worker_id, 123;
  is $d->file_name, "t/tmp/status_123";
  $d->set({a => 123});
  my $data = $d->get;
  is_deeply $data, {a => 123};

  ok $d->set_worker_id(456);
  is $d->worker_id, 456;
  is $d->file_name, "t/tmp/status_456";
  $d->set({b => 456});
  my $data2 = $d->get;
  is_deeply $data2, {b => 456};

  my @data = @{$d->get_all};
  foreach my $data (@data) {
    $data->{a} and is $data->{a}, 123;
    $data->{b} and is $data->{b}, 456;
  }
  $d->lock_store(sub { my $d = shift; $d->{c} = 789; $d});
  is $d->get->{c}, 789;
  $d->cleanup;
  ok ! -d 't/tmp';
}

done_testing;
