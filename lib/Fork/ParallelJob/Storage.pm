package Fork::ParallelJob::Storage;

use strict;
use warnings;
use Clone;

sub new {
  my $self = shift;
  return bless {}, $self;
}

sub name {
  my $self = shift;
  $self->{name} = shift if @_;
  $self->{name};
}

sub clone {
  my $self = shift;
  Clone::clone($self);
}

sub prepare {}

sub cleanup { Carp::croak "implement in subclass" }

sub lock { Carp::croak "implement in subclass" }

sub get { Carp::croak "implement in subclass" }

sub _get { Carp::croak "implement in subclass" }

sub set { Carp::croak "implement in subclass" }

sub get_all { Carp::croak "implement in subclass" }


=head1 NAME

Fork::ParallelJob::Storage -- storage object for Fork::ParallelJob::Data

=head1 SYNOPSIS

 my $data = Fork::ParallelJob::Storage::File->new();;
 
 $data->set($data);
 $data->get();

=head1 DESCRIPTION

=head2 new

=head2 name

=head2 prepare

=head2 cleanup

=head2 lock

=head2 get

=head2 _get

=head2 set

=head2 get_all

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

1; # Endo of Fork::ParallelJob::Storage


