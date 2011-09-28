package Fork::ParallelJob::Data::YAML;

use parent qw/Fork::ParallelJob::Data/;
use YAML::XS qw/Load Dump/;
use strict;
use warnings;

sub _deserialize {
  my ($self, $data) = @_;
  return $data ? Load($data) : {};
}

sub _serialize {
  my ($self, $status) = @_;
  Dump($status);
}

1; # Endo of Fork::ParallelJob::Data::YAML
