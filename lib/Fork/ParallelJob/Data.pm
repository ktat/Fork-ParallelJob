package Fork::ParallelJob::Data;

use strict;
use warnings;
use Fcntl qw/:DEFAULT :flock/;

sub new {
  my $klass = shift;
  my %args = @_;
  die "mandatory parameter:base_dir is missing"
    unless $args{base_dir};
  # build object
  my $self = bless {
                    worker_id => $$,
                    format    => 'Storable',
                    %args,
                   }, $klass;
  # create base_dir if necessary
  $self->cleanup if -e $args{base_dir};

  mkdir $args{base_dir}
    or die "failed to create directory:$args{base_dir}:$!";

  return $self;
}

sub set_worker_id {
  my $self = shift;
  my $id = shift;
  $self->{worker_id} = $id if $id;
}

sub worker_id {
  my ($self) = @_;
  return $self->{worker_id};
}

sub cleanup {
  my ($self) = @_;
  my @files = glob "$self->{base_dir}/status_*";
  for my $fn (@files) {
    unlink $fn;
  }
  rmdir $self->{base_dir};
}

sub file_name {
  my $self = shift;
  return "$self->{base_dir}/status_" . $self->worker_id;
}

sub _fh {
  shift;
  my $fn = shift;
  sysopen my $fh, $fn, O_RDWR | O_CREAT or die $!;
  return $fh;
}

sub lock_store {
  my ($self, $code) = @_;
  $self->lock(sub { $self->set($code->($self->get)) });
}

sub lock ($&) {
  my ($self, $code) = @_;
  my $fn = $self->file_name;
  local $@;
  eval {
    my $fh = $self->_fh($fn);
    flock $fh, LOCK_EX;
    $self->{fh} = $fh;
    $code->();
    flock $fh, LOCK_UN;
    close $fh;
    delete $self->{fh};
  };
  if ($@) {
    warn $@;
    return 0;
  } else {
    return 1;
  }
}

sub get {
  my $self = shift;
  $self->_get(shift || $self->file_name);
}

sub set {
  my ($self, $status) = @_;
  $self->_set($self->file_name, $status);
}

sub get_all {
  my ($self) = @_;
  my @files = glob "$self->{base_dir}/status_*";
  my @data;
  for my $fn (@files) {
    push @data, $self->get($fn);
  }
  return \@data;
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
