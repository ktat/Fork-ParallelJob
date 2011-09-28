package Fork::ParallelJob::Data::Storable;

use parent qw/Fork::ParallelJob::Data/;
use Storable qw/nstore retrieve nstore_fd fd_retrieve thaw nfreeze/;
use strict;
use warnings;

sub _deserialize {
  my ($self, $data) = @_;
  local $Storable::Deparse = 1;
  local $Storable::Eval = 1;
  $data ? thaw($data) : {};
}

sub _serialize {
  my ($self, $status) = @_;
  local $Storable::Deparse = 1;
  local $Storable::Eval = 1;
  nfreeze($status);
}

sub can_job_store { 1 }

1; # Endo of Fork::ParallelJob::Data::Storable
