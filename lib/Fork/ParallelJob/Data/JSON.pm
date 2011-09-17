package Fork::ParallelJob::Data::JSON;

use parent qw/Fork::ParallelJob::Data/;
use JSON::XS;
use strict;
use warnings;

my $json = JSON::XS->new;

sub _get {
  my ($self, $filename) = @_;
  my $fh = $self->{fh} || $self->_fh($filename);
  seek $fh, 0, 0;
  local $/;
  my $data = <$fh>;
  return $data ? $json->decode($data) : {};
}

sub _set {
  my ($self, $filename, $status) = @_;
  my $fh = $self->{fh} || $self->_fh($filename);
  seek $fh, 0, 0;
  print $fh $json->encode($status);
}

1; # Endo of Fork::ParallelJob::Data::JSON
