package Fork::ParallelJob::Jobs::ParentData;

use parent qw/Fork::ParallelJob::Jobs/;
use strict;
use warnings;

sub new {
  my $class = shift;
  my $pd = shift;
  bless {pd => $pd}, $class;
}

sub pd {
  my ($self) = @_;
  return $self->{pd};
}

sub add {
  my ($self, $job) = @_;
  my $pd = $self->pd;
  $pd->lock_store
    (
     sub {my $data = shift;
          push @{$data->{jobs} ||= []}, {(ref $job eq 'CODE' ? ($self->num_of_jobs + 1, $job) : %$job)};
          $data;
        });
}

sub add_multi {
  my ($self, @jobs) = @_;
  my $pd = $self->pd;
  $pd->lock_store
    (
     sub {my $data = shift;
          foreach my $job (@jobs) {
            push @{$data->{jobs} ||= []}, {(ref $job eq 'CODE' ? ($self->num_of_jobs + 1, $job) : %$job)};
          }
          $data;
        });
}

sub take {
  my ($self) = @_;
  my $job;
  $self->pd->lock_store(sub {my $data = shift; $job = shift @{$data->{jobs} ||= []}; $data});
  return $job ? $job : ();
}

sub num_of_jobs {
  my $self = shift;
  scalar @{$self->pd->get->{jobs} || []};
}

1;

=head1 NAME

Fork::ParallelJob::Jobs::ParentData -- jobs object for Fork::ParallelJob to stroe in ParentData

=head1 ACKNOWLEDGEMENTS


=head1 LICENSE AND COPYRIGHT

Copyright 2011 Ktat.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

=cut

1; # Endo of Fork::ParallelJob::Job::ParentData
