package Fork::ParallelJob::Data;

use strict;
use warnings;
use Class::Load qw/load_class/;

my $first_cleanup = 0;

sub new {
  my $klass = shift;
  my %args = @_;
  my $self = bless {
                    worker_id => undef,
                    format    => 'Storable',
                    storage   => {
                                  class  => 'File',
                                 },
                    %args,
                   }, $klass;

  unless ($self->{storage_object}) {
    my $storage_class = 'Fork::ParallelJob::Storage::' . delete $self->{storage}->{class};
    load_class($storage_class);
    $self->{storage_object} = $storage_class->new($self->{storage});
    $self->set_worker_id($$) unless $self->worker_id;
  }

  unless ($first_cleanup) {
    $first_cleanup = 1;
    $self->storage->cleanup;
  }
  $self->storage->prepare;

  return $self;
}

sub storage {
  my ($self) = @_;
  $self->{storage_object};
}

sub set_worker_id {
  my ($self) = shift;
  if (@_) {
    $self->{worker_id} = shift;
    $self->storage->name($self->{worker_id});
  }
  $self->{worker_id};
}

sub worker_id {
  my ($self) = @_;
  return $self->{worker_id};
}

sub cleanup {
  my ($self) = @_;
  $self->storage->cleanup;
}

sub lock_store {
  my ($self, $code) = @_;
  $self->storage->lock(sub { $self->set($code->($self->get)) });
}

sub get {
  my $self = shift;
  $self->_deserialize($self->storage->get);
}

sub set {
  my ($self, $status) = @_;
  my $s = $self->_serialize($status);
  $self->storage->set($s);
}

sub get_all {
  my ($self) = @_;
  my $size = 0;
  return [map $self->_deserialize($_), @{$self->storage->get_all}];
}

sub can_job_store { 0 }

=head1 NAME

Fork::ParallelJob::Data -- data object for Fork::ParallelJob

=head1 SYNOPSIS

 my $data = Fork::ParallelJob::Data->new('foramt' => 'Storable', base_dir => '/path/to/dir');
 
 $data->set("file_name", $data);
 $data->get("file_name");

=head1 DESCRIPTION

=head2 new

 my $data = Fork::ParallelJob::Data->new('foramt' => 'Storable', base_dir => '/path/to/dir');

foramt   ... Storable(default) or YAML
base_dir ... directry to store data

=head2 set

 $data->set('file_name', $data);

set data.

=head2 get

 $data->get('file_name');

get data.

=head2 lock

 $data->lock(
    sub {
       my $d = $data->get;
       $d->{key} = $value;
       $data->set($d);
    }
 );

This method lock file while coderef is done.

=head2 lock_store

 $data->lock_store(
    sub {
      my $data = shift;
      $data->{test} = 'test'
      $data;
    }
 });

The given coderef is execute whle locking.
arguments of coderef is data and coderef must return modified data.
returned value is atuomatically set.

=head2 get_all

 my $data = $data->get_all;
 foreach my $d (@$data) {
   # ...
 }

=head2 can_job_store

 $data->can_job_store

It returns 1, if the storage can store job.

get all data.

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

1; # Endo of Fork::ParallelJob::Data
