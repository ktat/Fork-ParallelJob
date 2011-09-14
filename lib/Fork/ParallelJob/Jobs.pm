package Fork::ParallelJob::Jobs;

use strict;
use warnings;

sub new {
  my $class = shift;
  bless [], $class;
}

sub add {
  my ($self, $job) = @_;
  push @$self, {(ref $job eq 'CODE' ? ($self->num_of_jobs + 1, $job) : %$job)};
}

sub add_multi {
  my ($self, @jobs) = @_;
  $self->add($_) for @jobs;
}

sub take {
  my ($self) = @_;
  shift @$self;
}

sub num_of_jobs {
  my ($self) = @_;
  scalar @{$self};
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

add job.

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
