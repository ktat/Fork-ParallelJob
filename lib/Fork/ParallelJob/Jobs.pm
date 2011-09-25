package Fork::ParallelJob::Jobs;

use strict;
use warnings;
use Carp;

sub new {
  my $class = shift;
  bless { jobs => [], data => [], jobs_hash => {}}, $class;
}

sub add {
  my ($self, $job, $data) = @_;
  my $num_of_jobs = $self->num_of_jobs;
  $self->_add(\$num_of_jobs, $self->{jobs}, $self->{data}, $self->{jobs_hash}, $job, $data);
}

sub _add {
  my ($self, $num_of_jobs, $jobs, $job_data, $jobs_hash, $job, $data) = @_;
  if (ref $job eq 'CODE') {
    push @$jobs, {$$num_of_jobs += 1, $job};
  } elsif (ref $job eq 'HASH') {
    my ($name, $code) = %$job;
    $jobs_hash->{$name} = $code;
    push @$jobs, {$name, $code};
  } else {
    # $job is job_name
    Carp::croak("job name($job) doesn't exist")  unless $jobs_hash->{$job};
    push @$jobs, {$job, $jobs_hash->{$job}};
  }
  push @$job_data, $data;
}

sub add_multi {
  my ($self, $jobs, $data) = @_;
  $self->add($jobs->[$_], $data->[$_]) for 0 .. $#{$jobs};
}

sub take {
  my ($self) = @_;
  return(shift @{$self->{jobs}}, shift @{$self->{data}});
}

sub _take {
  my ($self, $jobs, $job_data) = @_;
  return(shift @$jobs, shift @$job_data);
}

sub num_of_jobs {
  my ($self) = @_;
  scalar @{$self->{jobs}};
}

=head1 NAME

Fork::ParallelJob::Jobs -- jobs object for Fork::ParallelJob

=head1 SYNOPSIS

 my $jobs = Fork::ParallelJobs->new;
 
 $jobs->add($job);
 $jobs->add_multi(@jobs);
 my $job = $jobs->take;

=head1 DESCRIPTION

=head2 new

 my $jobs = Fork::ParallelJob::Job->new;

=head2 add

 $jobs->add($job);

add job. $job is code ref or hash ref like {key => sub {}}.

=head2 add_multi

 $jobs->add_multi(@jobs);

add jobs.

=head2 take

 my $job = $jobs->take;

take one job.

=head1 AUTHOR

Ktat, C<< <ktat at cpan.org> >>

=head1 ACKNOWLEDGEMENTS


=head1 LICENSE AND COPYRIGHT

Copyright 2011 Ktat.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

=cut

1; # Endo of Fork::ParallelJob::Job
