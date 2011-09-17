package Fork::ParallelJob::Data::YAML;

use parent qw/Fork::ParallelJob::Data/;
use YAML::XS qw/DumpFile LoadFile Load Dump/;
use strict;
use warnings;

sub _get {
  my ($self, $filename) = @_;
  if (my $fh = $self->{fh}) {
    local $/;
    seek $fh, 0, 0;
    my $data = <$fh>;
    return $data ? Load($data) : {};
  } else {
    return -e $filename ? LoadFile($filename) : {};
  }
}
use Data::Dumper;
sub _set {
  my ($self, $filename, $status) = @_;
  if (my $fh = $self->{fh}) {
    seek $fh, 0, 0;
    print $fh Dump($status);
  } else {
    DumpFile($filename, $status);
  }
}

1; # Endo of Fork::ParallelJob::Data::YAML
