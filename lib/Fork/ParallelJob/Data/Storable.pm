package Fork::ParallelJob::Data::Storable;

use parent qw/Fork::ParallelJob::Data/;
use Storable qw/nstore retrieve nstore_fd fd_retrieve thaw freeze/;
use strict;
use warnings;

sub _get {
  my ($self, $filename) = @_;
  local $Storable::Deparse = 1;
  local $Storable::Eval = 1;

  if (my $fh = $self->{fh}) {
    seek $fh, 0, 0;
    return -s $filename ? fd_retrieve($fh) : {};
  } else {
    return -s $filename ? retrieve $filename : {};
  }
}

sub _set {
  my ($self, $filename, $status) = @_;
  local $Storable::Deparse = 1;
  local $Storable::Eval = 1;

  if (my $fh = $self->{fh}) {
    seek $fh, 0, 0;
    eval {
      nstore_fd($status, $fh);
    };
    Carp::cluck($@) if $@;
  } else {
    nstore($status, $filename);
  }
}

sub can_job_store { 1 }

1; # Endo of Fork::ParallelJob::Data::Storable
