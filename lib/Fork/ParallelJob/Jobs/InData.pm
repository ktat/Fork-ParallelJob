package Fork::ParallelJob::Jobs::InData;

use parent qw/Fork::ParallelJob::Jobs/;
use strict;
use warnings;
use Clone;

sub new {
  my $class = shift;
  my $pd = shift;
  my $self = bless {pd => $pd->clone}, $class;
  $pd->lock_store(
                  sub {
                    my $d = shift;
                    $d->{_}{jobs}      = [];
                    $d->{_}{jobs_data} = [];
                    $d->{_}{jobs_hash} = {};
                    $d;
                  }
                 );
  return $self;
}

sub pd {
  my ($self) = @_;
  return $self->{pd};
}

sub add {
  my ($self, $job, $job_data) = @_;
  my $pd = $self->pd;
  my $num_of_jobs = $self->num_of_jobs;
  $pd->lock_store
    (
     sub {my $d = shift;
          $self->_add(\$num_of_jobs,
                      $d->{_}->{jobs} ||= [],
                      $d->{_}->{job_data} ||= [],
                      $d->{_}->{jobs_hash} ||= {},
                      $job,
                      $job_data
                     );
          $d;
        }
    );
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
         $self->_add(\$num_of_jobs,
                     $d->{_}->{jobs} ||= [],
                     $d->{_}->{job_data} ||= [],
                     $d->{_}->{jobs_hash} ||= {},
                     $jobs->[$i], $job_data->[$i]);
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
       $job      = shift @{$d->{_}{jobs}     ||= []};
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

sub jobs_hash {
  my $self = shift;
  my $jobs_hash;
  if (@_) {
    my $jobs = ref $_[0] eq 'HASH' ? $_[0] : { @_ };
    $self->pd->lock_store
      (
       sub {
         my $d = shift;
         $jobs_hash = $d->{_}{'jobs_hash'} ||= {};
         @{$jobs_hash}{keys %$jobs} = values %$jobs;
         $d
       }
      );
  } else {
    $jobs_hash = $self->pd->get->{_}{'jobs_hash'};
  }
  return $jobs_hash;
}

1;

=head1 NAME

Fork::ParallelJob::Jobs::InData -- jobs object for Fork::ParallelJob to stroe in InData

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

1; # Endo of Fork::ParallelJob::Job::InData
