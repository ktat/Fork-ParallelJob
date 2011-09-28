use Test::More;

use strict;
use warnings;

use Fork::ParallelJob::Data::Storable;
use Fork::ParallelJob::Data::JSON;
use Fork::ParallelJob::Data::YAML;

my $pid = $$;

foreach my $format (qw/Storable JSON YAML/) {
  my $class = 'Fork::ParallelJob::Data::' . $format;
  ok my $d = $class->new(storage => {class => 'File', base_dir => "t/tmp"}, format => $format);
  ok -d 't/tmp';
  is $d->worker_id, $pid;
  is $d->storage->name, $pid;
  is $d->{format}, $format;
  ok $d->set_worker_id(123);
  is $d->worker_id, 123;
  is $d->storage->file_name, "t/tmp/status_123", 'file_name check';
  $d->set({a => 123});
  my $data = $d->get;
  is_deeply $data, {a => 123}, 'get';

  ok $d->set_worker_id(456);
  is $d->worker_id, 456;
  is $d->storage->file_name, "t/tmp/status_456";
  $d->set({b => 456});
  my $data2 = $d->get;
  is_deeply $data2, {b => 456};

  foreach my $data (@{$d->get_all}) {
    $data->{a} and is $data->{a}, 123,  ' - ' .123;
    $data->{b} and is $data->{b}, 456,  ' - ' .456;
  }
  use Clone qw/clone/;
  $d->lock_store(sub { my $d = shift; $d->{c} = 789; $d});
  is $d->get->{c}, 789;
  foreach my $data (@{$d->get_all}) {
    $data->{a} and is $data->{a}, 123, ' - ' .123;
    $data->{b} and is $data->{b}, 456, ' - ' .456;
    $data->{c} and is $data->{c}, 789, ' - ' .789;
  }
  $d->cleanup;
  ok ! -d 't/tmp';
}

done_testing;
