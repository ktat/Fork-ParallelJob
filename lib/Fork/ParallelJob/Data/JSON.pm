package Fork::ParallelJob::Data::JSON;

use parent qw/Fork::ParallelJob::Data/;
use JSON::XS;
use strict;
use warnings;

my $json = JSON::XS->new;

sub _deserialize {
  my ($self, $data) = @_;
  $data ? $json->decode($data) : {};
}

sub _serialize {
  my ($self, $status) = @_;
  $json->encode($status);
}

1; # Endo of Fork::ParallelJob::Data::JSON
