package Fork::ParallelJob::Jobs::RootData;

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
  my ($self, $job, $job_data) = @_;
  my $pd = $self->pd;
  $pd->lock_store
    (
     sub {my $d = shift;
          push @{$d->{_}{jobs} ||= []}, {(ref $job eq 'CODE' ? ($self->num_of_jobs + 1, $job) : %$job)};
          push @{$d->{_}{job_data} ||= []}, $job_data;
          $d;
        });
}

sub add_multi {
  my ($self, $jobs, $job_data) = @_;
  $job_data ||= [];
  my $pd = $self->pd;
  my $num_of_jobs = $self->num_of_jobs;
  $pd->lock_store
    (
     sub {
       my $d = shift;
       foreach my $i (0 .. $#{$jobs}) {
         my $job  = $jobs->[$i];
         my $data = $job_data->[$i];
         push @{$d->{_}{jobs} ||= []}, {(ref $job eq 'CODE' ? (++$num_of_jobs, $job) : %$job)};
         push @{$d->{_}{job_data} ||= []}, $data;
       }
       $d;
     });
}

sub take {
  my ($self) = @_;
  my ($job, $job_data);
  $self->pd->lock_store
    (
     sub {
       my $d = shift;
       $job      = shift @{$d->{_}{jobs} ||= []};
       $job_data = shift @{$d->{_}{job_data} ||= []};
       $d
     }
    );
  return $job ? ($job, $job_data) : ();
}

sub num_of_jobs {
  my $self = shift;
  scalar @{$self->pd->get->{_}{jobs} || []};
}

1;

=head1 NAME

Fork::ParallelJob::Jobs::RootData -- jobs object for Fork::ParallelJob to stroe in RootData

=head1 METHODS

all methods are as same as Fork::ParallelJob::Jobs.

=head1 ACKNOWLEDGEMENTS


=head1 LICENSE AND COPYRIGHT

Copyright 2011 Ktat.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

=cut

1; # Endo of Fork::ParallelJob::Job::RootData
